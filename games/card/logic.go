package card

import (
	"context"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// cardPlayer 单个玩家的对局状态（仅 RoomActor goroutine 访问）。
type cardPlayer struct {
	uid   string
	hand  []Card       // 开局发的手牌
	used  map[int]bool // 已出过的手牌下标
	play  int          // 本回合已出的下标；-1 = 未出
	score int
}

// Logic 回合制卡牌房间逻辑（事件驱动，Tick 仅做离线托管）。
type Logic struct {
	r       framework.RoomCtx
	cfg     framework.RoomConfig
	players map[string]*cardPlayer
	order   []string // 稳定遍历顺序（进房先后）

	round    int // 已结算回合数
	started  bool
	finished bool
	seed     int64 // 确定性发牌种子（xorshift），同房间序列固定
}

// NewLogic 由模块工厂在创建房间时调用。
func NewLogic() framework.RoomLogic {
	return &Logic{players: make(map[string]*cardPlayer), seed: 20260922}
}

func (l *Logic) OnCreate(r framework.RoomCtx, cfg framework.RoomConfig) {
	l.r = r
	l.cfg = cfg
}

func (l *Logic) OnJoin(r framework.RoomCtx, p framework.Player) {
	cp := &cardPlayer{uid: p.UID(), play: -1, used: make(map[int]bool)}
	l.players[p.UID()] = cp
	l.order = append(l.order, p.UID())
	r.Broadcast(framework.MsgMemberChange, memberEvent(p.UID(), "join"))

	// 满员开局：确定性发牌 + 单推各自手牌（可靠通道）。
	if len(l.players) >= l.cfg.MaxPlayers {
		l.started = true
		l.deal()
	}
}

// deal 交错发牌：玩家 order[i] 拿 deck[i], deck[i+MaxPlayers] ... 各 HandCards 张。
func (l *Logic) deal() {
	deck := make([]Card, 0, len(l.order)*HandCards)
	for i := 0; i < len(l.order)*HandCards; i++ {
		l.seed = l.seed*1103515245 + 12345
		deck = append(deck, Card{Point: int((l.seed>>16)%13 + 1)})
	}
	for i, uid := range l.order {
		cp := l.players[uid]
		for j := 0; j < HandCards; j++ {
			cp.hand = append(cp.hand, deck[i+j*len(l.order)])
		}
		if m, ok := l.r.Member(uid); ok {
			_ = m.Push(MsgDeal, DealNtf{Hand: cp.hand, TotalRounds: HandCards})
		}
	}
}

// OnMessage 上行出牌；双方都出齐后立即结算该回合（事件驱动，不依赖 Tick）。
func (l *Logic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {
	req, ok := env.Payload.(*PlayReq)
	if !ok || !l.started || l.finished {
		return
	}
	cp := l.players[p.UID()]
	if cp == nil || cp.play >= 0 || len(cp.hand) == 0 {
		return
	}
	// 下标越界或已出过：忽略（客户端状态错误，不回错以免刷屏）。
	if req.Idx < 0 || req.Idx >= len(cp.hand) || cp.used[req.Idx] {
		return
	}
	cp.play = req.Idx
	l.tryResolveRound()
}

// Tick 离线托管：离线且未出牌者自动打出第一张未用牌（§5.1 最小实现）。
func (l *Logic) Tick(r framework.RoomCtx, _ time.Duration) {
	if !l.started || l.finished {
		return
	}
	for _, uid := range l.order {
		cp := l.players[uid]
		if cp == nil || cp.play >= 0 {
			continue
		}
		if m, ok := l.r.Member(uid); ok && !m.Online() {
			for idx := range cp.hand {
				if !cp.used[idx] {
					cp.play = idx
					break
				}
			}
			l.tryResolveRound()
		}
	}
}

// tryResolveRound 双方出齐 → 比点数 → 广播回合结果；最后一回合后终局。
func (l *Logic) tryResolveRound() {
	// 未满员/有人未出：不结算。
	if len(l.order) < l.cfg.MaxPlayers {
		return
	}
	for _, uid := range l.order {
		if cp := l.players[uid]; cp == nil || cp.play < 0 {
			return
		}
	}

	l.round++
	best := -1
	var winners []string
	plays := make([]PlayNtf, 0, len(l.order))
	for _, uid := range l.order {
		cp := l.players[uid]
		pt := cp.hand[cp.play].Point
		switch {
		case pt > best:
			best, winners = pt, []string{uid}
		case pt == best:
			winners = append(winners, uid)
		}
		cp.used[cp.play] = true
		plays = append(plays, PlayNtf{UID: uid, Card: cp.hand[cp.play]})
		cp.play = -1 // 进入下一回合
	}
	// 唯一最高点者胜并 +1 分；平局双方标记 Win 仅作展示，不加分（既定规则）。
	if len(winners) == 1 {
		l.players[winners[0]].score++
	}
	for i := range plays {
		for _, w := range winners {
			if plays[i].UID == w {
				plays[i].Win = true
			}
		}
	}

	l.r.Broadcast(MsgRound, RoundNtf{Round: l.round, Plays: plays, IsLast: l.round >= HandCards})
	if l.round >= HandCards {
		l.finish("", false)
	}
}

// OnLeave 对局中退出：剩者直接获胜（与 snake 语义一致）。
func (l *Logic) OnLeave(r framework.RoomCtx, p framework.Player) {
	delete(l.players, p.UID())
	for i, u := range l.order {
		if u == p.UID() {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
	r.Broadcast(framework.MsgMemberChange, memberEvent(p.UID(), "leave"), p.UID())

	if l.started && !l.finished && len(l.order) == 1 {
		l.finish(l.order[0], false)
	}
}

func (l *Logic) OnEmpty(r framework.RoomCtx) { r.Close() }

// OnTimeout MaxDuration 到期强结算：按当前累计分判胜（§8.2）。
func (l *Logic) OnTimeout(r framework.RoomCtx) {
	if l.finished {
		return
	}
	winner, best := "", -1
	for _, uid := range l.order {
		if cp := l.players[uid]; cp != nil && cp.score > best {
			best, winner = cp.score, uid
		}
	}
	l.finish(winner, true)
}

// finish 终局：可靠广播结算 → 落库（强一致同步路径）→ 排行榜 → 关房。
func (l *Logic) finish(winner string, timeout bool) {
	l.finished = true
	scores := make([]ScoreNtf, 0, len(l.order))
	for _, uid := range l.order {
		scores = append(scores, ScoreNtf{UID: uid, Score: l.players[uid].score})
	}
	res := ResultNtf{
		RoomID: l.r.RoomID(), Winner: winner, Rounds: l.round,
		Scores: scores, Timeout: timeout,
	}
	l.r.Broadcast(MsgResult, res)

	st := l.r.Storage()
	ctx := context.Background()
	if err := st.SetSync(ctx, "card_result", l.r.RoomID(), res); err != nil {
		_ = err // 内存存储不失败；远程存储由熔断/WAL 兜底（§10.3）
	}
	if winner != "" {
		_, _ = st.Ranking().ZIncrBy(ctx, "card_score", winner, 1)
	}
	l.r.Close()
}

// OnDestroy 房间销毁：回写本局得分到玩家档案 extra（§10.4.4 / §14 第 5 步）。
// Patch 已由框架路由到 PlayerActor 单写者，与退出强存不冲突。
func (l *Logic) OnDestroy(r framework.RoomCtx) {
	ps := r.ProfileStore()
	if ps == nil {
		return
	}
	ctx := context.Background()
	for uid, cp := range l.players {
		if err := ps.Patch(ctx, uid, map[string]any{
			"extra.card_last_score": cp.score,
		}); err != nil {
			_ = err // 档案写入失败不影响房间销毁
		}
	}
}

// memberNtf 成员变更通知体（复用通用 MsgMemberChange 消息段）。
type memberNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Event string `json:"event" protobuf:"bytes,2,opt,name=event,proto3"`
}

func memberEvent(uid, event string) memberNtf {
	return memberNtf{UID: uid, Event: event}
}
