package app

import (
	"log/slog"
	"testing"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/gateway"
)

// newWarmupPair 起 gw/logic 一对真实 TCP 内部 Transport；gw 侧以注册表为地址解析源。
func newWarmupPair(t *testing.T, reg cluster.NodeRegistry) (gwTr, logicTr *gateway.TCPNodeTransport, logicAddr string) {
	t.Helper()
	logicTr = gateway.NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil,
		gateway.WithKeepaliveInterval(10*time.Millisecond),
		gateway.WithReconnCheck(20*time.Millisecond))
	addr, err := logicTr.Listen()
	if err != nil {
		t.Fatalf("logic listen: %v", err)
	}
	// gw 侧地址解析走注册表（logic 注册时写的内部通告地址）。
	gwTr = gateway.NewTCPNodeTransport("gw-0", "127.0.0.1:0", registryPeerProvider{r: reg},
		gateway.WithKeepaliveInterval(10*time.Millisecond),
		gateway.WithReconnCheck(20*time.Millisecond))
	return gwTr, logicTr, addr
}

// 注册表 Active 事件应触发对端连接预热；Down 事件应清理连接并停止后台重连。
func TestWireTransportWarmup_RegistryDriven(t *testing.T) {
	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })

	gwTr, logicTr, logicAddr := newWarmupPair(t, reg)
	defer gwTr.Close()
	defer logicTr.Close()

	// gw 侧联动：对端角色 RoleLogic。
	wireTransportWarmup(gwTr, reg, "gw-0", cluster.RoleLogic, slog.Default())

	// logic 节点上线 → gw 应自动预热出一条到 logic 的活连接。
	_ = reg.Register(cluster.NodeInfo{ID: "logic-0", Role: cluster.RoleLogic, Addr: logicAddr, State: cluster.StateActive})

	deadline := time.Now().Add(2 * time.Second)
	for !gwTr.OutboundAlive("logic-0") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !gwTr.OutboundAlive("logic-0") {
		t.Fatal("expected outbound connection warmed up after logic active event")
	}

	// logic 节点 Down → gw 应清理连接（outConns 移除，后台不再重连）。
	_ = reg.SetState("logic-0", cluster.StateDown)
	time.Sleep(150 * time.Millisecond) // 等待 watch 协程消费事件
	if gwTr.OutboundAlive("logic-0") {
		t.Fatal("expected outbound connection removed after logic down event")
	}
}
