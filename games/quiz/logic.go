package quiz

import (
	"context"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// quizPlayer 单个玩家的对局状态（仅 RoomActor goroutine 访问）。
type quizPlayer struct {
	uid      string
	answered map[int]bool // 已作答的题号（每题一次机会）
	score    int
}

// Logic 答题房间逻辑：r.After 链式出题，答对加分，题尽结算。
type Logic struct {
	r       framework.RoomCtx
	cfg     framework.RoomConfig
	players map[string]*quizPlayer
	order   []string // 稳定遍历顺序（进房先后）

	cur      int // 当前题号（0 起）
	started  bool
	finished bool
	qTimer   framework.Handle // 当前题的超时跳题定时器
}

// NewLogic 由模块工厂在创建房间时调用。
func NewLogic() framework.RoomLogic {
	return &Logic{players: make(map[string]*quizPlayer)}
}

func (l *Logic) OnCreate(r framework.RoomCtx, cfg framework.RoomConfig) {
	l.r = r
	l.cfg = cfg
}

func (l *Logic) OnJoin(r framework.RoomCtx, p framework.Player) {
	l.players[p.UID()] = &quizPlayer{uid: p.UID(), answered: make(map[int]bool)}
	l.order = append(l.order, p.UID())
	r.Broadcast(framework.MsgMemberChange, memberEvent(p.UID(), "join"))

	// 满员即开局：广播第一题并挂超时定时器。
	if len(l.players) >= l.cfg.MaxPlayers {
		l.started = true
		l.askQuestion()
	}
}

// OnMessage 上行答题：每题每人一次机会，答对 +1；全员答完立即跳题。
func (l *Logic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {
	req, ok := env.Payload.(*AnswerReq)
	if !ok || !l.started || l.finished {
		return
	}
	qp := l.players[p.UID()]
	if qp == nil || l.cur >= Questions {
		return
	}
	// 题号过期/重复作答/选项越界：忽略。
	if req.Idx != l.cur || qp.answered[l.cur] || req.Option < 0 || req.Option >= len(Bank[l.cur].Options) {
		return
	}
	qp.answered[l.cur] = true
	if req.Option == Bank[l.cur].Answer {
		qp.score++
	}
	// 全员答完 → 取消超时，立即出下一题。
	for _, uid := range l.order {
		if !l.players[uid].answered[l.cur] {
			return
		}
	}
	l.next()
}

// askQuestion 广播当前题并挂超时跳题定时器（§14 定时器面验证点）。
func (l *Logic) askQuestion() {
	q := Bank[l.cur]
	l.r.Broadcast(MsgQuestion, QuestionNtf{
		Idx:         l.cur,
		Text:        q.Text,
		Options:     q.Options,
		TimeLimitMS: TimeLimitMS,
		Total:       Questions,
	})
	l.qTimer = l.r.After(time.Duration(TimeLimitMS)*time.Millisecond, l.next)
}

// next 进入下一题或终局；由"全员答完"或"超时"触发（二者先到者生效）。
func (l *Logic) next() {
	if l.finished {
		return
	}
	if l.qTimer != nil {
		l.qTimer.Cancel()
		l.qTimer = nil
	}
	l.cur++
	if l.cur >= Questions {
		l.finish()
		return
	}
	l.askQuestion()
}

// Tick 答题玩法无帧推进，空实现（接口必须提供）。
func (l *Logic) Tick(framework.RoomCtx, time.Duration) {}

// OnLeave 对局中退出：剩者直接获胜（与 card/snake 语义一致）。
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
		if l.qTimer != nil {
			l.qTimer.Cancel()
		}
		l.finish()
	}
}

func (l *Logic) OnEmpty(r framework.RoomCtx) { r.Close() }

// finish 终局：可靠广播结算 → 落库（强一致同步路径）→ 排行榜 → 关房。
func (l *Logic) finish() {
	l.finished = true
	winner, best := "", -1
	tied := false
	scores := make([]ScoreNtf, 0, len(l.order))
	for _, uid := range l.order {
		s := l.players[uid].score
		scores = append(scores, ScoreNtf{UID: uid, Score: s})
		switch {
		case s > best:
			best, winner, tied = s, uid, false
		case s == best:
			tied = true
		}
	}
	if tied {
		winner = "" // 平局无胜者
	}
	res := ResultNtf{RoomID: l.r.RoomID(), Winner: winner, Scores: scores}
	l.r.Broadcast(MsgResult, res)

	st := l.r.Storage()
	ctx := context.Background()
	if err := st.SetSync(ctx, "quiz_result", l.r.RoomID(), res); err != nil {
		_ = err // 内存存储不失败；远程存储由熔断/WAL 兜底（§10.3）
	}
	if winner != "" {
		_, _ = st.Ranking().ZIncrBy(ctx, "quiz_score", winner, 1)
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
	for uid, qp := range l.players {
		if err := ps.Patch(ctx, uid, map[string]any{
			"extra.quiz_last_score": qp.score,
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
