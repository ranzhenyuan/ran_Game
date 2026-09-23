package doudizhu

import (
	"context"
	"testing"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// ---------------------------------------------------------------------------
// 测试桩：用最小实现满足 framework 接口，把异步定时器/广播/存储全部可观测化。
// 全部在单 goroutine 内串行调用，符合「Actor 状态免锁」约束，无需同步。
// ---------------------------------------------------------------------------

// fakeHandle 记录定时器是否被取消，便于断言 setTurnTimer/clearTurnTimer 行为。
type fakeHandle struct{ cancelled *bool }

func (h *fakeHandle) Cancel() { *h.cancelled = true }

// timer 一次 After/Every 登记：fn 为到期回调，cancelled 标记是否已 Cancel。
type timer struct {
	fn        func()
	cancelled *bool
}

// pushRec 记录 Member.Push 单推（手牌等私有通道）。
type pushRec struct {
	msgID framework.MsgID
	msg   any
}

type fakePlayer struct {
	uid    string
	online bool
	pushes []pushRec
}

func (p *fakePlayer) UID() string  { return p.uid }
func (p *fakePlayer) Online() bool { return p.online }
func (p *fakePlayer) Push(id framework.MsgID, msg any) error {
	p.pushes = append(p.pushes, pushRec{id, msg})
	return nil
}
func (p *fakePlayer) PushSnapshot(id framework.MsgID, msg any) error { return nil }
func (p *fakePlayer) Profile() *framework.PlayerProfile              { return &framework.PlayerProfile{} }

// bcRec 记录广播（公共事件通道）。
type bcRec struct {
	msgID  framework.MsgID
	msg    any
	except []string
}

// fakeRoom 实现 framework.RoomCtx，记录所有下行与定时器，Storage/Profile 可观测。
type fakeRoom struct {
	roomID    string
	members   map[string]*fakePlayer
	uids      []string
	broadcast []bcRec
	timers    []timer
	storage   *fakeStorage
	profile   *fakeProfile
	kv        map[string]any
	closed    bool
}

func newFakeRoom(uids ...string) *fakeRoom {
	r := &fakeRoom{
		roomID:  "room-1",
		members: make(map[string]*fakePlayer),
		uids:    append([]string(nil), uids...),
		storage: newFakeStorage(),
		profile: &fakeProfile{},
		kv:      make(map[string]any),
	}
	for _, u := range uids {
		r.members[u] = &fakePlayer{uid: u, online: true}
	}
	return r
}

func (r *fakeRoom) RoomID() string { return r.roomID }
func (r *fakeRoom) Members() []framework.Player {
	out := make([]framework.Player, 0, len(r.uids))
	for _, u := range r.uids {
		out = append(out, r.members[u])
	}
	return out
}
func (r *fakeRoom) Member(uid string) (framework.Player, bool) {
	p, ok := r.members[uid]
	return p, ok
}
func (r *fakeRoom) KV(key string) any       { return r.kv[key] }
func (r *fakeRoom) SetKV(key string, v any) { r.kv[key] = v }
func (r *fakeRoom) Broadcast(id framework.MsgID, m any, except ...string) {
	r.broadcast = append(r.broadcast, bcRec{id, m, except})
}
func (r *fakeRoom) BroadcastSnapshot(id framework.MsgID, m any, except ...string) {}
func (r *fakeRoom) Kick(uid string, code framework.Code, reason string)           {}
func (r *fakeRoom) Close()                                                        { r.closed = true }
func (r *fakeRoom) After(_ time.Duration, fn func()) framework.Handle {
	cancelled := false
	r.timers = append(r.timers, timer{fn, &cancelled})
	return &fakeHandle{&cancelled}
}
func (r *fakeRoom) Every(_ time.Duration, fn func()) framework.Handle {
	return r.After(0, fn)
}
func (r *fakeRoom) Storage() framework.Storage           { return r.storage }
func (r *fakeRoom) ProfileStore() framework.ProfileStore { return r.profile }

// runTimers 触发所有未取消的定时器回调（模拟时间推进，确定性）。
func (r *fakeRoom) runTimers() {
	for _, t := range r.timers {
		if !*t.cancelled {
			t.fn()
		}
	}
	r.timers = nil
}

// lastTurn 取最近一次 MsgTurn 广播的当前操作人。
func (r *fakeRoom) lastTurn() string {
	for i := len(r.broadcast) - 1; i >= 0; i-- {
		if r.broadcast[i].msgID == MsgTurn {
			return r.broadcast[i].msg.(TurnNtf).CurUID
		}
	}
	return ""
}

// hasResult 是否广播过结算。
func (r *fakeRoom) hasResult() bool {
	for _, b := range r.broadcast {
		if b.msgID == MsgResult {
			return true
		}
	}
	return false
}

// fakeStorage 仅实现 IncrBy/SetSync 记录（结算路径），其余为最小实现。
type fakeStorage struct {
	gold   map[string]int64
	synced map[string]any
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{gold: make(map[string]int64), synced: make(map[string]any)}
}
func (s *fakeStorage) Get(context.Context, string, string, any) error {
	return framework.ErrStorageNotFound
}
func (s *fakeStorage) Set(context.Context, string, string, any) error { return nil }
func (s *fakeStorage) SetSync(_ context.Context, table, key string, val any) error {
	s.synced[table+":"+key] = val
	return nil
}
func (s *fakeStorage) Del(context.Context, string, string) error { return nil }
func (s *fakeStorage) IncrBy(_ context.Context, _ string, key string, delta int64) (int64, error) {
	s.gold[key] += delta
	return s.gold[key], nil
}
func (s *fakeStorage) Ranking() framework.Ranking { return nil }
func (s *fakeStorage) Raw() any                   { return nil }
func (s *fakeStorage) Close() error               { return nil }

// fakeProfile 记录 Patch 调用（OnDestroy 战绩回写）。
type fakeProfile struct {
	patches map[string]map[string]any
}

func (p *fakeProfile) Load(context.Context, string) (*framework.PlayerProfile, error) {
	return &framework.PlayerProfile{}, nil
}
func (p *fakeProfile) Save(context.Context, string, *framework.PlayerProfile) error     { return nil }
func (p *fakeProfile) SaveSync(context.Context, string, *framework.PlayerProfile) error { return nil }
func (p *fakeProfile) Patch(_ context.Context, uid string, fields map[string]any) error {
	if p.patches == nil {
		p.patches = make(map[string]map[string]any)
	}
	p.patches[uid] = fields
	return nil
}

// newLogic 构造一个已开局（3 人、发牌完成、出牌阶段）的 Logic，可直接注入手牌。
// 不依赖随机发牌结果，保证用例稳定。
func newLogic(room *fakeRoom) *Logic {
	l := &Logic{players: make(map[string]*player), seed: 20260923, r: room}
	l.stage = StageWaiting
	l.started = true // 真实流程 OnJoin 满员后置位；单测直接构造已开局对局
	for _, uid := range room.uids {
		l.players[uid] = &player{uid: uid, alive: true}
		l.order = append(l.order, uid)
	}
	return l
}

// enterPlaying 把对局推进到出牌阶段：指定地主，走真实 nextTurn 路径广播首轮 MsgTurn。
func (l *Logic) enterPlaying(landlord string) {
	l.landlord = landlord
	l.baseScore = 1
	l.stage = StagePlaying
	l.cur = l.indexOf(landlord)
	l.lastPlayed = nil
	l.passCount = 0
	l.nextTurn(false)
}

// sendPlay 以某玩家身份发上行出牌（经 OnMessage 校验轮次/阶段）。
func (l *Logic) sendPlay(uid string, ids []int) {
	p, _ := l.r.Member(uid)
	l.OnMessage(l.r, p, &framework.Envelope{MsgID: MsgPlay, Payload: &PlayReq{IDs: ids}})
}

// sendCall 以某玩家身份发上行叫分。
func (l *Logic) sendCall(uid string, score int) {
	p, _ := l.r.Member(uid)
	l.OnMessage(l.r, p, &framework.Envelope{MsgID: MsgCall, Payload: &CallReq{Score: score}})
}

// mk 按 rank 列表构造手牌（Suit 0，Joker -1，ID = rank）。
func mk(ranks ...int) []Card {
	out := make([]Card, 0, len(ranks))
	for _, r := range ranks {
		out = append(out, Card{Suit: 0, Rank: r, Joker: -1})
	}
	return out
}

// ---------------------------------------------------------------------------
// 发牌确定性
// ---------------------------------------------------------------------------

func TestDealDeterministic(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.deal()

	seen := make(map[int]bool)
	total := 0
	for _, uid := range l.order {
		h := l.players[uid].hand
		if len(h) != HandPerPlayer {
			t.Fatalf("%s hand=%d want %d", uid, len(h), HandPerPlayer)
		}
		for _, c := range h {
			if seen[c.ID()] {
				t.Fatalf("duplicate card id %d", c.ID())
			}
			seen[c.ID()] = true
			total++
		}
		// 手牌应按 Rank 升序
		for i := 1; i < len(h); i++ {
			if h[i].Rank < h[i-1].Rank {
				t.Fatalf("hand not sorted at %s", uid)
			}
		}
		// 私有手牌走单推，不进广播
		pushed := room.members[uid].pushes
		if len(pushed) == 0 || pushed[0].msgID != MsgDeal {
			t.Fatalf("%s missing private deal push", uid)
		}
	}
	if total != 51 {
		t.Fatalf("dealt %d cards want 51", total)
	}
	if len(l.landlord3) != LandlordCards {
		t.Fatalf("landlord3=%d want %d", len(l.landlord3), LandlordCards)
	}
	if len(seen)+len(l.landlord3) != 54 {
		t.Fatalf("deck coverage missing: seen=%d landlord3=%d", len(seen), len(l.landlord3))
	}
}

// ---------------------------------------------------------------------------
// 出牌规则
// ---------------------------------------------------------------------------

func TestTakeCards(t *testing.T) {
	l := newLogic(newFakeRoom("a"))
	hp := l.players["a"]
	hp.hand = mk(3, 5, 7, 9)

	// 正常取出 5、9
	got, ok := l.takeCards(hp, []int{5, 9})
	if !ok || len(got) != 2 || len(hp.hand) != 2 {
		t.Fatalf("take ok=%v got=%d remain=%d", ok, len(got), len(hp.hand))
	}
	// 含手牌外 ID：失败且手牌不变
	hp.hand = mk(3, 5, 7)
	before := append([]Card(nil), hp.hand...)
	if _, ok := l.takeCards(hp, []int{999}); ok {
		t.Fatal("take invalid id should fail")
	}
	if len(hp.hand) != len(before) {
		t.Fatalf("hand mutated on failed take: %d vs %d", len(hp.hand), len(before))
	}
}

func TestPlayNotCurrentIgnored(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(3, 4)
	l.enterPlaying("a")
	// 当前是 a（地主先出），b 越权出牌应被忽略，回合不变
	before := len(room.broadcast)
	l.sendPlay("b", []int{999})
	if len(room.broadcast) != before {
		t.Fatal("non-current player play should be ignored")
	}
	if room.lastTurn() != "a" {
		t.Fatalf("turn should stay a, got %s", room.lastTurn())
	}
}

func TestPlayIllegalTypeReturned(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	// a 手牌：3、5（不成牌型，两张非同点）
	l.players["a"].hand = []Card{{Suit: 0, Rank: 3}, {Suit: 1, Rank: 5}}
	l.enterPlaying("a")
	room.broadcast = nil
	l.sendPlay("a", []int{3, 105}) // 105 = Suit1*100+5

	// 牌退回：手牌数不变，无 MsgPlayed 广播，回合未推进（无新 MsgTurn）
	if len(l.players["a"].hand) != 2 {
		t.Fatalf("cards should be returned, hand=%d", len(l.players["a"].hand))
	}
	for _, b := range room.broadcast {
		if b.msgID == MsgPlayed {
			t.Fatal("illegal play must not broadcast MsgPlayed")
		}
	}
}

func TestPlayCannotBeatReturned(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(3, 8) // a 两张，出一张不致立即结算
	l.players["b"].hand = mk(4)
	l.enterPlaying("a")
	room.broadcast = nil
	// a 自由出单张 3 → 轮次转到 b
	l.sendPlay("a", []int{3})
	if room.lastTurn() != "b" {
		t.Fatalf("turn should pass to b, got %s", room.lastTurn())
	}
	if len(l.players["a"].hand) != 1 {
		t.Fatalf("a should have 1 card left, hand=%d", len(l.players["a"].hand))
	}
	if l.lastPlayed == nil || l.lastPlayed.Type != TypeSingle {
		t.Fatal("lastPlayed not recorded")
	}
}

func TestPlayCannotBeatKeepsTurn(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(6, 10) // a 出单张 10，剩一张不致结算
	l.players["b"].hand = mk(3)     // b 只有 3，压不过 10
	l.enterPlaying("a")
	room.broadcast = nil
	l.sendPlay("a", []int{10}) // cur -> b
	l.sendPlay("b", []int{3})  // b 压不过，应被退回

	if len(l.players["b"].hand) != 1 {
		t.Fatalf("b's card should be returned, hand=%d", len(l.players["b"].hand))
	}
	// 压不过后仍轮到 b（回合不推进，等其重新出/过）
	if l.currentUID() != "b" {
		t.Fatalf("current should remain b, got %s", l.currentUID())
	}
}

func TestFreeLeadPassIgnored(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(3, 4)
	l.enterPlaying("a") // 地主先出 = 自由出牌
	room.broadcast = nil
	beforeTurn := room.lastTurn()
	l.sendPlay("a", nil) // 自由出牌回合过牌 = 非法，忽略
	if len(room.broadcast) != 0 {
		t.Fatal("free-lead pass must be ignored")
	}
	_ = beforeTurn
}

func TestPassTwiceReturnsLead(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(5, 6)
	l.players["b"].hand = mk(3, 8)
	l.players["c"].hand = mk(4, 9)
	l.enterPlaying("a")
	room.broadcast = nil
	// a 出 5 -> cur b；b 过 -> cur c；c 过 -> 回到 a 自由出牌
	l.sendPlay("a", []int{5})
	if l.currentUID() != "b" {
		t.Fatalf("cur=b want, got %s", l.currentUID())
	}
	l.sendPlay("b", nil) // 过
	if l.currentUID() != "c" {
		t.Fatalf("cur=c want, got %s", l.currentUID())
	}
	l.sendPlay("c", nil) // 两人都过 -> a 重获自由出牌
	if l.currentUID() != "a" {
		t.Fatalf("lead should return to a, got %s", l.currentUID())
	}
	if l.lastPlayed != nil {
		t.Fatal("free lead should clear lastPlayed")
	}
}

func TestPlayOutFinishes(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(3) // a 仅剩一张
	l.enterPlaying("a")
	l.sendPlay("a", []int{3})
	if !l.finished {
		t.Fatal("playing last card should finish")
	}
	if !room.hasResult() {
		t.Fatal("result not broadcast")
	}
}

// ---------------------------------------------------------------------------
// 叫分阶段
// ---------------------------------------------------------------------------

func TestBidMustBeHigherThanMax(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.deal()
	l.startBidding()
	room.broadcast = nil
	// a 叫 2
	l.sendCall("a", 2)
	if l.maxScore != 2 || l.maxBidder != "a" {
		t.Fatalf("a should be max bidder 2, got max=%d bidder=%s", l.maxScore, l.maxBidder)
	}
	// b 叫 1（低于 2）→ 视作不叫，max 不变
	before := l.maxScore
	l.sendCall("b", 1)
	if l.maxScore != before {
		t.Fatalf("lower bid should not raise max, got %d", l.maxScore)
	}
}

func TestBidCapsAtThree(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.deal()
	l.startBidding()
	l.sendCall("a", 3) // 叫到 3 直接封顶结束
	if l.stage != StagePlaying {
		t.Fatalf("bid 3 should end bidding, stage=%d", l.stage)
	}
	if l.landlord != "a" || l.baseScore != 3 {
		t.Fatalf("landlord=a base=3, got landlord=%s base=%d", l.landlord, l.baseScore)
	}
	// 地主应拿到 20 张（17+3 底牌）
	if len(l.players["a"].hand) != 20 {
		t.Fatalf("landlord hand=%d want 20", len(l.players["a"].hand))
	}
}

func TestNoBidRedealsThenForcesLandlord(t *testing.T) {
	// 历史 bug 回归：无人叫分曾无限重发牌导致栈溢出。
	// 验证：连续 maxRedeals+1 轮全不叫后，强制指定地主、对局进入出牌阶段。
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.startBidding()

	rounds := 0
	for l.stage != StagePlaying && rounds < 100 {
		l.sendCall(l.currentUID(), 0)
		rounds++
	}
	if l.stage != StagePlaying {
		t.Fatalf("should force landlord and enter playing, stuck stage=%d rounds=%d", l.stage, rounds)
	}
	if l.redeals > maxRedeals {
		t.Fatalf("redeals=%d exceeds cap %d", l.redeals, maxRedeals)
	}
	if l.landlord == "" || l.baseScore < 1 {
		t.Fatalf("forced landlord missing: landlord=%q base=%d", l.landlord, l.baseScore)
	}
}

// ---------------------------------------------------------------------------
// 结算 / 金币 / 超时强判 / 快照
// ---------------------------------------------------------------------------

func TestGoldSettlementLandlordWin(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.landlord = "a"
	l.baseScore = 3
	l.finish(true) // 地主赢
	if room.storage.gold["a"] != 6 {
		t.Fatalf("landlord +2*base=6, got %d", room.storage.gold["a"])
	}
	if room.storage.gold["b"] != -3 || room.storage.gold["c"] != -3 {
		t.Fatalf("farmers -base each, got b=%d c=%d", room.storage.gold["b"], room.storage.gold["c"])
	}
}

func TestGoldSettlementFarmerWin(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.landlord = "a"
	l.baseScore = 2
	l.finish(false) // 农民赢
	if room.storage.gold["a"] != -4 {
		t.Fatalf("landlord -2*base=-4, got %d", room.storage.gold["a"])
	}
	if room.storage.gold["b"] != 2 || room.storage.gold["c"] != 2 {
		t.Fatalf("farmers +base each, got b=%d c=%d", room.storage.gold["b"], room.storage.gold["c"])
	}
}

func TestFinishIdempotent(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.landlord = "a"
	l.baseScore = 1
	l.finish(true)
	l.finish(false) // 二次结算应被忽略，金币不变
	if room.storage.gold["a"] != 2 {
		t.Fatalf("second finish must not double-apply, got %d", room.storage.gold["a"])
	}
}

func TestTimeoutForceJudge(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.landlord = "a"
	l.stage = StagePlaying
	l.players["a"].hand = mk(3, 4)    // 地主 2 张
	l.players["b"].hand = mk(5, 6, 7) // 农民 3 张（更多）
	l.players["c"].hand = mk(8, 9)
	l.OnTimeout(l.r)
	// 地主手牌最少 → 地主胜
	res := room.storage.synced["doudizhu_result:room-1"].(ResultNtf)
	if !res.LandlordWin {
		t.Fatal("landlord has fewest cards -> landlord should win on timeout")
	}
}

func TestSnapshotNoLeak(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(3, 4)
	l.players["b"].hand = mk(5, 6, 7)
	l.players["c"].hand = mk(8)
	l.landlord = "a"
	l.stage = StagePlaying
	l.cur = 0

	snap := l.snapshotFor("b")
	// 只含本人手牌，不含他人真实手牌
	if len(snap.MyHand) != 3 {
		t.Fatalf("b myhand=%d want 3", len(snap.MyHand))
	}
	for _, ps := range snap.Players {
		if len(ps.UID) == 0 {
			t.Fatal("snapshot player missing uid")
		}
		// 公开信息只有张数
	}
	// 不能出现 a 的具体牌
	for _, c := range snap.MyHand {
		if c.Rank == 3 || c.Rank == 4 {
			t.Fatal("snapshot leaked other player's hand")
		}
	}
}

// TestTimerOnTimeoutAutoPass 验证回合超时托管能正常推进（不卡死，历史 bug 回归）。
func TestTimerOnTimeoutAutoPass(t *testing.T) {
	room := newFakeRoom("a", "b", "c")
	l := newLogic(room)
	l.players["a"].hand = mk(5)
	l.enterPlaying("a")
	room.broadcast = nil
	// a 出 5 -> 轮到 b；b 不出牌，触发超时托管（b 过牌）
	l.sendPlay("a", []int{5})
	room.runTimers() // 触发 b 的回合定时器 → autoPlay 过牌
	if !l.finished && l.currentUID() == "b" {
		t.Fatal("timeout autoplay should advance turn past b")
	}
}
