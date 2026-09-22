package snake

import (
	"context"
	"time"

	"github.com/rangame/server/pkg/framework"
)

const (
	gridW = 15
	gridH = 15
)

// snake 单条蛇的服务端状态（仅 RoomActor goroutine 访问）。
type snake struct {
	uid   string
	body  []Point // body[0] 为蛇头
	dir   int
	alive bool
	score int
}

// Logic 贪吃蛇房间逻辑。
type Logic struct {
	r   framework.RoomCtx
	cfg framework.RoomConfig

	snakes   map[string]*snake
	order    []string
	food     Point
	started  bool
	finished bool
	tick     int64
	seed     int64 // 食物位置的确定性伪随机种子
}

// NewLogic 由模块工厂在创建房间时调用。
func NewLogic() framework.RoomLogic {
	return &Logic{snakes: make(map[string]*snake)}
}

func (g *Logic) OnCreate(r framework.RoomCtx, cfg framework.RoomConfig) {
	g.r = r
	g.cfg = cfg
}

func (g *Logic) OnJoin(r framework.RoomCtx, p framework.Player) {
	s := &snake{uid: p.UID(), alive: true, dir: DirRight}
	switch len(g.order) {
	case 0:
		s.body = []Point{{3, 7}, {2, 7}, {1, 7}}
		s.dir = DirRight
	case 1:
		s.body = []Point{{11, 7}, {12, 7}, {13, 7}}
		s.dir = DirLeft
	default: // 理论上被 MaxPlayers 拦截，兜底给个不重叠位置
		s.body = []Point{{7, 3}, {7, 2}, {7, 1}}
	}
	g.snakes[p.UID()] = s
	g.order = append(g.order, p.UID())

	r.Broadcast(framework.MsgMemberChange, MemberNtf{UID: p.UID(), Event: "join"})

	// 满人即刻开局。
	if len(g.snakes) >= g.cfg.MaxPlayers {
		g.started = true
		g.relocateFood()
	}
}

func (g *Logic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {
	req, ok := env.Payload.(*MoveReq)
	if !ok {
		return
	}
	s := g.snakes[p.UID()]
	if s == nil || !s.alive || !g.started {
		return
	}
	// 禁止 180° 掉头（长度>=2 会立即自撞）。
	if (s.dir == DirUp && req.Dir == DirDown) ||
		(s.dir == DirDown && req.Dir == DirUp) ||
		(s.dir == DirLeft && req.Dir == DirRight) ||
		(s.dir == DirRight && req.Dir == DirLeft) {
		return
	}
	if req.Dir >= DirUp && req.Dir <= DirRight {
		s.dir = req.Dir
	}
}

// Tick 每 100ms：推进 → 碰撞判定 → 快照广播 → 终局结算。
func (g *Logic) Tick(r framework.RoomCtx, _ time.Duration) {
	if !g.started || g.finished {
		return
	}
	g.tick++

	// 离线玩家不托管方向，保持直行（§5.1 掉线托管的最小实现）。
	for _, uid := range g.order {
		s := g.snakes[uid]
		if s == nil || !s.alive {
			continue
		}
		head := s.body[0]
		switch s.dir {
		case DirUp:
			head.Y--
		case DirDown:
			head.Y++
		case DirLeft:
			head.X--
		case DirRight:
			head.X++
		}
		s.body = append([]Point{head}, s.body...)
		if head == g.food {
			s.score++
			g.relocateFood() // 吃到食物：不弹尾（长度+1）
		} else {
			s.body = s.body[:len(s.body)-1]
		}
	}

	g.resolveCollisions()
	g.broadcastState()

	alive := g.aliveSnakes()
	if alive <= 1 {
		winner := ""
		if alive == 1 {
			for _, uid := range g.order {
				if s := g.snakes[uid]; s != nil && s.alive {
					winner = uid
				}
			}
		}
		g.finish(winner)
	}
}

func (g *Logic) resolveCollisions() {
	occupy := func(x, y int) (string, bool) {
		for _, uid := range g.order {
			s := g.snakes[uid]
			if s == nil {
				continue
			}
			for i, c := range s.body {
				if c.X == x && c.Y == y {
					// 头部格子属于"占用者"，但撞头判定单独处理
					if i == 0 {
						continue
					}
					return uid, true
				}
			}
		}
		return "", false
	}

	heads := make(map[string]Point)
	for _, uid := range g.order {
		s := g.snakes[uid]
		if s != nil && s.alive {
			heads[uid] = s.body[0]
		}
	}
	// 同格撞头：全部死亡。
	headCell := make(map[Point][]string)
	for uid, h := range heads {
		headCell[h] = append(headCell[h], uid)
	}
	for _, uids := range headCell {
		if len(uids) > 1 {
			for _, uid := range uids {
				if s := g.snakes[uid]; s != nil {
					s.alive = false
				}
			}
		}
	}

	for _, uid := range g.order {
		s := g.snakes[uid]
		if s == nil || !s.alive {
			continue
		}
		h := s.body[0]
		if h.X < 0 || h.X >= gridW || h.Y < 0 || h.Y >= gridH {
			s.alive = false // 撞墙
			continue
		}
		if _, hit := occupy(h.X, h.Y); hit {
			s.alive = false // 撞身体
		}
	}
}

func (g *Logic) broadcastState() {
	ntf := StateNtf{Tick: g.tick, Food: g.food, Snakes: make([]SnakeState, 0, len(g.order))}
	for _, uid := range g.order {
		s := g.snakes[uid]
		if s == nil {
			continue
		}
		body := make([]Point, len(s.body))
		copy(body, s.body)
		ntf.Snakes = append(ntf.Snakes, SnakeState{
			UID: uid, Body: body, Alive: s.alive, Score: s.score,
		})
	}
	// 全量局面走快照通道：客户端 latest-wins 覆盖，断线重连拉一次即同步（§5.3）。
	g.r.BroadcastSnapshot(MsgState, ntf)
}

// finish 终局：可靠广播结算 → 落库（强一致同步路径）→ 排行榜 → 关房。
func (g *Logic) finish(winner string) {
	g.finished = true
	res := ResultNtf{RoomID: g.r.RoomID(), Winner: winner, Rounds: g.tick}
	g.r.Broadcast(MsgResult, res)

	st := g.r.Storage()
	ctx := context.Background()
	if err := st.SetSync(ctx, "snake_result", g.r.RoomID(), res); err != nil {
		// v1 内存存储不会失败；阶段 6 接入 MySQL 后此处触发熔断/补写（§10）。
		_ = err
	}
	if winner != "" {
		_, _ = st.Ranking().ZIncrBy(ctx, "snake_score", winner, 1)
	}
	g.r.Close()
}

func (g *Logic) OnLeave(r framework.RoomCtx, p framework.Player) {
	delete(g.snakes, p.UID())
	for i, u := range g.order {
		if u == p.UID() {
			g.order = append(g.order[:i], g.order[i+1:]...)
			break
		}
	}
	r.Broadcast(framework.MsgMemberChange, MemberNtf{UID: p.UID(), Event: "leave"}, p.UID())

	// 对局中退出：2 人局直接判剩下的人获胜。
	if g.started && !g.finished && g.aliveSnakes() <= 1 {
		winner := ""
		for _, uid := range g.order {
			if s := g.snakes[uid]; s != nil && s.alive {
				winner = uid
			}
		}
		g.finish(winner)
	}
}

func (g *Logic) OnEmpty(r framework.RoomCtx) { r.Close() }

func (g *Logic) OnDestroy(framework.RoomCtx) {}

// OnTimeout MaxDuration 到期强结算（§8.2 TimeoutLogic 可选实现）。
func (g *Logic) OnTimeout(r framework.RoomCtx) {
	if g.finished {
		return
	}
	// 超时按当前存活最高分判胜。
	winner := ""
	best := -1
	for _, uid := range g.order {
		if s := g.snakes[uid]; s != nil && s.alive && s.score > best {
			best, winner = s.score, uid
		}
	}
	g.finish(winner)
}

func (g *Logic) aliveSnakes() int {
	n := 0
	for _, s := range g.snakes {
		if s.alive {
			n++
		}
	}
	return n
}

// relocateFood 确定性地把食物放到首个空格（xorshift 步进），棋盘满则原地不动。
func (g *Logic) relocateFood() {
	g.seed = g.seed*1103515245 + 12345 + g.tick
	occupied := func(p Point) bool {
		for _, s := range g.snakes {
			for _, c := range s.body {
				if c == p {
					return true
				}
			}
		}
		return false
	}
	for i := int64(0); i < gridW*gridH; i++ {
		g.seed = g.seed*1103515245 + 12345
		p := Point{X: int((g.seed>>16)%gridW+gridW) % gridW, Y: int((g.seed>>8)%gridH+gridH) % gridH}
		if !occupied(p) {
			g.food = p
			return
		}
	}
}
