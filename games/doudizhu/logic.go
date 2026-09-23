package doudizhu

import (
	"context"
	"sort"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// 对局参数。
const (
	HandPerPlayer = 17               // 每人初始手牌
	LandlordCards = 3                // 地主底牌
	TurnTimeout   = 15 * time.Second // 单回合操作超时（超时托管）
	maxRedeals    = 2                // 无人叫分时最多重发牌次数，超限强制指定地主
)

// player 单个玩家的对局状态（仅 RoomActor goroutine 访问）。
type player struct {
	uid   string
	hand  []Card // 手牌，按 Rank 升序维护
	alive bool
}

// Logic 斗地主房间逻辑（3 人回合制，事件驱动，Tick 仅做离线兜底）。
//
// 全部状态只在 RoomActor 串行访问；下行分两类：
//   - 公共事件（叫分/出牌/轮次）走可靠通道广播，保证三人收到顺序一致；
//   - 手牌走 Member.Push 单推，绝不出现在广播里，杜绝透视。
type Logic struct {
	r   framework.RoomCtx
	cfg framework.RoomConfig

	players map[string]*player
	order   []string // 座位顺序（进房先后），决定叫分/出牌轮转

	stage     int
	seed      int64 // 确定性洗牌种子
	landlord3 []Card
	redeals   int // 流局重发牌计数（防无限递归）
	started   bool
	finished  bool

	// 叫分阶段
	bidIndex  int // 当前轮到谁叫分（order 下标）
	maxScore  int
	maxBidder string
	bidCount  int

	// 出牌阶段
	landlord   string
	cur        int // order 下标：当前出牌人
	lastPlayed *PlayedNtf
	baseScore  int
	passCount  int // 连续过牌数；达到 2（其余两人都过）→ 上一手出牌人获得自由出牌权

	turnTimer framework.Handle
}

// NewLogic 由模块工厂在创建房间时调用。
func NewLogic() framework.RoomLogic {
	return &Logic{players: make(map[string]*player), seed: 20260923}
}

func (l *Logic) OnCreate(r framework.RoomCtx, cfg framework.RoomConfig) {
	l.r = r
	l.cfg = cfg
	l.stage = StageWaiting
}

func (l *Logic) OnJoin(r framework.RoomCtx, p framework.Player) {
	l.players[p.UID()] = &player{uid: p.UID(), alive: true}
	l.order = append(l.order, p.UID())
	r.Broadcast(framework.MsgMemberChange, memberEvent(p.UID(), "join"))

	// 满 3 人开局：洗牌发牌 → 进入叫分阶段。
	if len(l.players) >= 3 && !l.started {
		l.started = true
		l.deal()
		l.startBidding()
	}
}

// deal 确定性洗牌（xorshift）并发牌：每人 17 张，留 3 张为地主底牌。
func (l *Logic) deal() {
	deck := fullDeck()
	// Fisher-Yates 确定性洗牌。
	for i := len(deck) - 1; i > 0; i-- {
		l.seed = l.seed*1103515245 + 12345
		j := int((uint64(l.seed>>16) % uint64(i+1)))
		deck[i], deck[j] = deck[j], deck[i]
	}
	// 发牌：每人交错拿 17 张。
	hands := make([][]Card, len(l.order))
	for i := 0; i < len(l.order); i++ {
		hands[i] = make([]Card, 0, HandPerPlayer)
	}
	for i := 0; i < HandPerPlayer*len(l.order); i++ {
		hands[i%len(l.order)] = append(hands[i%len(l.order)], deck[i])
	}
	l.landlord3 = deck[HandPerPlayer*len(l.order) : HandPerPlayer*len(l.order)+LandlordCards]
	for i, uid := range l.order {
		sort.Slice(hands[i], func(a, b int) bool { return hands[i][a].Less(hands[i][b]) })
		l.players[uid].hand = hands[i]
		if m, ok := l.r.Member(uid); ok {
			_ = m.Push(MsgDeal, HandNtf{Hand: hands[i]})
		}
	}
}

// landlord3 地主底牌（叫分结束后随阶段下发给地主）。
func (l *Logic) startBidding() {
	l.stage = StageBidding
	l.bidIndex = 0
	l.maxScore = 0
	l.maxBidder = ""
	l.bidCount = 0
	l.r.Broadcast(MsgStage, StageNtf{Stage: StageBidding})
	l.askBid()
}

func (l *Logic) askBid() {
	uid := l.order[l.bidIndex]
	// 通知当前叫分人（复用 MsgTurn，Landlord/BaseScore 在叫分阶段为空/0）。
	// 不广播 turn 客户端无从知道轮到自己，只能等超时——必须先广播再启超时托管。
	l.r.Broadcast(MsgTurn, TurnNtf{CurUID: uid, DeadlineMs: time.Now().Add(TurnTimeout).UnixMilli()})
	if m, ok := l.r.Member(uid); ok && !m.Online() {
		// 离线自动不叫。
		l.applyBid(uid, 0)
		return
	}
	l.setTurnTimer(uid)
}

func (l *Logic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {
	if !l.started || l.finished {
		return
	}
	// 服务端权威：只接受「意图」，必须校验当前阶段与轮到谁。
	curUID := l.currentUID()
	if p.UID() != curUID {
		return // 非当前操作人，忽略（防越权）
	}
	switch env.MsgID {
	case MsgCall:
		if l.stage != StageBidding {
			return
		}
		req, ok := env.Payload.(*CallReq)
		if !ok {
			return
		}
		l.applyBid(p.UID(), req.Score)
	case MsgPlay:
		if l.stage != StagePlaying {
			return
		}
		req, ok := env.Payload.(*PlayReq)
		if !ok {
			return
		}
		l.applyPlay(p.UID(), req.IDs)
	}
}

// applyBid 处理叫分。score: 0=不叫，1-3 叫分（必须高于当前最高分）。
func (l *Logic) applyBid(uid string, score int) {
	if score < 0 || score > 3 {
		score = 0
	}
	if score > 0 && score <= l.maxScore {
		score = 0 // 叫分必须高于已叫最高分，否则视作不叫
	}
	ntf := CallNtf{UID: uid, Score: score, Passed: score == 0}
	l.r.Broadcast(MsgCallNtf, ntf)
	l.bidCount++
	if score > l.maxScore {
		l.maxScore = score
		l.maxBidder = uid
	}
	l.clearTurnTimer()

	// 叫到 3 直接封顶结束；或三人都叫过一轮结束。
	if l.maxScore == 3 || l.bidCount >= len(l.order) {
		l.finishBidding()
		return
	}
	l.bidIndex = (l.bidIndex + 1) % len(l.order)
	l.askBid()
}

// finishBidding 叫分结束：确定地主、发底牌、进入出牌阶段。
// 若无人叫分（maxScore==0），重新发牌再来一轮；超过 maxRedeals 仍无人叫分则强制指定地主，
// 防止无限重发牌递归（历史 bug：无次数保护导致栈溢出）。
func (l *Logic) finishBidding() {
	if l.maxScore == 0 || l.maxBidder == "" {
		if l.redeals < maxRedeals {
			l.redeals++
			l.deal()
			l.startBidding()
			return
		}
		// 仍无人叫分：强制首位为地主、底分 1，保证对局能结束。
		l.maxBidder = l.order[0]
		l.maxScore = 1
	}
	l.landlord = l.maxBidder
	l.baseScore = l.maxScore
	// 地主拿底牌。
	hp := l.players[l.landlord]
	hp.hand = append(hp.hand, l.landlord3...)
	sort.Slice(hp.hand, func(a, b int) bool { return hp.hand[a].Less(hp.hand[b]) })
	if m, ok := l.r.Member(l.landlord); ok {
		_ = m.Push(MsgDeal, HandNtf{Hand: hp.hand, Landlord3: l.landlord3})
	}

	l.stage = StagePlaying
	l.r.Broadcast(MsgStage, StageNtf{Stage: StagePlaying})
	// 地主先出（自由出牌）。
	for i, uid := range l.order {
		if uid == l.landlord {
			l.cur = i
			break
		}
	}
	l.lastPlayed = nil
	l.passCount = 0
	l.nextTurn(false)
}

// nextTurn 进入下一出牌回合。freeLead=true 表示当前出牌人获得自由出牌权（上家被两人过掉）。
func (l *Logic) nextTurn(freeLead bool) {
	uid := l.order[l.cur]
	l.r.Broadcast(MsgTurn, TurnNtf{
		CurUID: uid, Landlord: l.landlord, BaseScore: l.baseScore,
		DeadlineMs: time.Now().Add(TurnTimeout).UnixMilli(),
	})
	if freeLead {
		l.lastPlayed = nil
		l.passCount = 0
	}
	if m, ok := l.r.Member(uid); ok && !m.Online() {
		// 离线托管：过牌（若非自由出牌）；自由出牌则出最小单张。
		l.autoPlay(uid, freeLead)
		return
	}
	l.setTurnTimer(uid)
}

// applyPlay 处理出牌请求。IDs 为空=过牌。服务端校验牌型合法性与能否压过。
// 注意：不在此处提前 clearTurnTimer——非法出牌（过期/越权牌、压不过）必须保留定时器，
// 让 15s 超时托管兜底，否则客户端发一次垃圾帧就会把房间永久卡死。
// 合法分支走 nextTurn→setTurnTimer，其内部会先清旧定时器。
func (l *Logic) applyPlay(uid string, ids []int) {
	hp := l.players[uid]
	if hp == nil {
		return
	}

	if len(ids) == 0 {
		// 过牌：自由出牌回合不允许过。
		if l.lastPlayed == nil {
			return
		}
		l.passCount++
		l.r.Broadcast(MsgPlayed, PlayedNtf{UID: uid, Pass: true})
		if l.passCount >= 2 {
			// 其余两人都过 → 上一手出牌人自由出牌。
			l.cur = l.indexOf(l.lastPlayed.UID)
			l.nextTurn(true)
		} else {
			l.cur = (l.cur + 1) % len(l.order)
			l.nextTurn(false)
		}
		return
	}

	// 校验：这些牌是否都在本人手牌里（防作弊：不能出不存在的牌）。
	selected, ok := l.takeCards(hp, ids)
	if !ok {
		return // 非法：含手牌外的牌，直接忽略
	}
	// 识别牌型。
	typ, mainRank := classify(selected)
	if typ == TypeInvalid {
		// 牌型非法，把牌退回手牌。
		hp.hand = append(hp.hand, selected...)
		sort.Slice(hp.hand, func(a, b int) bool { return hp.hand[a].Less(hp.hand[b]) })
		return
	}
	// 校验能否压过上家（非自由出牌）。
	if l.lastPlayed != nil && l.lastPlayed.UID != uid {
		if !canBeat(typ, mainRank, len(selected), l.lastPlayed.Type, mainRankOf(l.lastPlayed.Cards), len(l.lastPlayed.Cards)) {
			hp.hand = append(hp.hand, selected...)
			sort.Slice(hp.hand, func(a, b int) bool { return hp.hand[a].Less(hp.hand[b]) })
			return // 压不过，退回
		}
	}

	// 合法出牌：从手牌移除（已 take）、广播、判胜负。
	ntf := PlayedNtf{UID: uid, Cards: selected, Type: typ}
	l.lastPlayed = &ntf
	l.passCount = 0
	l.r.Broadcast(MsgPlayed, ntf)

	if len(hp.hand) == 0 {
		// 该玩家出完 → 判定胜负。
		l.finish(uid == l.landlord)
		return
	}
	l.cur = (l.cur + 1) % len(l.order)
	l.nextTurn(false)
}

// takeCards 从手牌按 ID 取出选中的牌；含非法 ID 返回 ok=false（不修改手牌）。
func (l *Logic) takeCards(hp *player, ids []int) ([]Card, bool) {
	want := make(map[int]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	kept := make([]Card, 0, len(hp.hand))
	taken := make([]Card, 0, len(ids))
	for _, c := range hp.hand {
		if want[c.ID()] {
			taken = append(taken, c)
			delete(want, c.ID())
		} else {
			kept = append(kept, c)
		}
	}
	if len(want) != 0 || len(taken) != len(ids) {
		return nil, false // 有 ID 不在手牌中
	}
	hp.hand = kept
	return taken, true
}

// autoPlay 离线托管：过牌；自由出牌时出最小单张。
func (l *Logic) autoPlay(uid string, freeLead bool) {
	if freeLead {
		hp := l.players[uid]
		if len(hp.hand) > 0 {
			l.applyPlay(uid, []int{hp.hand[0].ID()})
			return
		}
	}
	l.applyPlay(uid, nil) // 过牌
}

func (l *Logic) currentUID() string {
	if l.stage == StageBidding {
		return l.order[l.bidIndex]
	}
	if l.stage == StagePlaying {
		return l.order[l.cur]
	}
	return ""
}

func (l *Logic) indexOf(uid string) int {
	for i, u := range l.order {
		if u == uid {
			return i
		}
	}
	return -1
}

func (l *Logic) setTurnTimer(uid string) {
	l.clearTurnTimer()
	l.turnTimer = l.r.After(TurnTimeout, func() {
		// 超时托管：叫分阶段不叫；出牌阶段过牌/出最小单张。
		if l.stage == StageBidding && l.currentUID() == uid {
			l.applyBid(uid, 0)
		} else if l.stage == StagePlaying && l.currentUID() == uid {
			l.autoPlay(uid, l.lastPlayed == nil)
		}
	})
}

func (l *Logic) clearTurnTimer() {
	if l.turnTimer != nil {
		l.turnTimer.Cancel()
		l.turnTimer = nil
	}
}

// Tick 仅做兜底：当前操作人离线且无定时器时触发托管（定时器已覆盖，此为二次保险）。
func (l *Logic) Tick(r framework.RoomCtx, _ time.Duration) {
	if !l.started || l.finished {
		return
	}
	uid := l.currentUID()
	if uid == "" {
		return
	}
	if m, ok := l.r.Member(uid); ok && !m.Online() && l.turnTimer == nil {
		if l.stage == StageBidding {
			l.applyBid(uid, 0)
		} else if l.stage == StagePlaying {
			l.autoPlay(uid, l.lastPlayed == nil)
		}
	}
}

func (l *Logic) OnLeave(r framework.RoomCtx, p framework.Player) {
	delete(l.players, p.UID())
	for i, u := range l.order {
		if u == p.UID() {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
	r.Broadcast(framework.MsgMemberChange, memberEvent(p.UID(), "leave"), p.UID())
	// 对局中有人退出：简化为直接结算（地主在场判地主阵营，否则按剩余）。
	if l.started && !l.finished {
		l.finish(false)
	}
}

func (l *Logic) OnEmpty(r framework.RoomCtx) { r.Close() }

// OnTimeout MaxDuration 到期强结算：按当前状态判定（§8.2）。
func (l *Logic) OnTimeout(r framework.RoomCtx) {
	if l.finished {
		return
	}
	if l.stage == StagePlaying {
		// 地主手牌最少则地主胜，否则农民胜（超时强判）。
		lh := len(l.players[l.landlord].hand)
		minF := 1 << 30
		for uid, hp := range l.players {
			if uid != l.landlord && len(hp.hand) < minF {
				minF = len(hp.hand)
			}
		}
		l.finish(lh <= minF)
	}
}

// finish 终局：可靠广播结算 → 金币原子扣发（幂等）→ 落库 → 关房。
func (l *Logic) finish(landlordWin bool) {
	if l.finished {
		return
	}
	l.finished = true
	l.clearTurnTimer()
	farmers := make([]string, 0, 2)
	for uid := range l.players {
		if uid != l.landlord {
			farmers = append(farmers, uid)
		}
	}
	res := ResultNtf{
		RoomID: l.r.RoomID(), Landlord: l.landlord, Farmers: farmers,
		LandlordWin: landlordWin, BaseScore: l.baseScore,
	}
	l.r.Broadcast(MsgResult, res)

	// 金币结算：地主赢 → 每个农民 -base，地主 +2*base；反之相反。
	// 走 Storage.IncrBy 原子计数（§10.4.1 货币类约束）。
	st := l.r.Storage()
	ctx := context.Background()
	delta := int64(l.baseScore)
	if landlordWin {
		_, _ = st.IncrBy(ctx, "gold", l.landlord, 2*delta)
		for _, f := range farmers {
			_, _ = st.IncrBy(ctx, "gold", f, -delta)
		}
	} else {
		_, _ = st.IncrBy(ctx, "gold", l.landlord, -2*delta)
		for _, f := range farmers {
			_, _ = st.IncrBy(ctx, "gold", f, delta)
		}
	}
	if err := st.SetSync(ctx, "doudizhu_result", l.r.RoomID(), res); err != nil {
		_ = err
	}
	l.r.Close()
}

// OnDestroy 回写战绩到玩家档案 extra（单写者路由，§10.4.4）。
func (l *Logic) OnDestroy(r framework.RoomCtx) {
	ps := r.ProfileStore()
	if ps == nil {
		return
	}
	ctx := context.Background()
	for uid := range l.players {
		_ = ps.Patch(ctx, uid, map[string]any{
			"extra.doudizhu_plays": 1,
		})
	}
}

// ---- 内部：快照构建（重连用）----

func (l *Logic) snapshotFor(uid string) SnapshotNtf {
	snap := SnapshotNtf{
		Stage: l.stage, Landlord: l.landlord, CurUID: l.currentUID(),
		BaseScore: l.baseScore,
	}
	for _, u := range l.order {
		hp := l.players[u]
		snap.Players = append(snap.Players, PlayerSnap{UID: u, HandCnt: len(hp.hand)})
	}
	if l.lastPlayed != nil {
		snap.LastPlayed = *l.lastPlayed
	}
	if hp := l.players[uid]; hp != nil {
		snap.MyHand = append([]Card(nil), hp.hand...)
	}
	return snap
}

// memberNtf 成员变更通知体（复用通用 MsgMemberChange）。
type memberNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Event string `json:"event" protobuf:"bytes,2,opt,name=event,proto3"`
}

func memberEvent(uid, event string) memberNtf {
	return memberNtf{UID: uid, Event: event}
}
