// matcher.go 抽象 Matcher 接口（架构文档 §13.5 业务零改动）。
//
// 进程内 Maker 与分布式 RedisMatcher（internal/cluster）都实现本接口；
// 装配层根据 server.role 选择实现：standalone/logic-default 用 Maker，
// 多副本 logic 用 RedisMatcher。Router 代码与业务模块完全不变。

package match

import "github.com/rangame/server/pkg/framework"

// Matcher 抽象匹配器接口。
//
// 实现方：
//   - *Maker：进程内同步匹配器（默认）
//   - *cluster.RedisMatcher：分布式匹配器（演进态）
type Matcher interface {
	Enqueue(uid string, rule framework.MatchRule, seq uint32) error
	Cancel(uid string) bool
	QueueDepth(module, code string) int
}

// 编译期断言：*Maker 实现 Matcher。
var _ Matcher = (*Maker)(nil)
