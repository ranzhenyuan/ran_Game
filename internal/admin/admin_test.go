// admin_test.go admin API 端到端走查测试（架构文档 §11）。

package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/pkg/framework"
)

// newTestServer 创建测试 admin server（含 metrics + exporter + registry + drainFn）。
func newTestServer(t *testing.T, registry cluster.NodeRegistry, drainFn DrainFn) *Server {
	t.Helper()
	metrics := obs.NewRegistry()
	exporter := obs.NewPrometheusExporter(metrics)
	obs.RegisterStandard(metrics, exporter)

	srv := NewServer(Config{Addr: "127.0.0.1:0"}, slog.Default(), exporter, registry, drainFn, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

// TestHealthz_Readyz 存活与就绪探针。
func TestHealthz_Readyz(t *testing.T) {
	srv := newTestServer(t, nil, nil)

	// 初始未就绪
	_, body := doReq(t, srv, "GET", "/readyz", "")
	if !strings.Contains(body, "not ready") {
		t.Fatalf("readyz initial: got %q", body)
	}

	// 标记就绪
	srv.SetReady(true)
	status, body := doReq(t, srv, "GET", "/readyz", "")
	if status != 200 || !strings.Contains(body, "ready") {
		t.Fatalf("readyz after ready: status=%d body=%q", status, body)
	}

	// healthz 始终 200
	status, _ = doReq(t, srv, "GET", "/healthz", "")
	if status != 200 {
		t.Fatalf("healthz: status=%d", status)
	}
}

// TestMetrics_PrometheusFormat /metrics 端点导出。
func TestMetrics_PrometheusFormat(t *testing.T) {
	srv := newTestServer(t, nil, nil)

	status, body := doReq(t, srv, "GET", "/metrics", "")
	if status != 200 {
		t.Fatalf("metrics: status=%d", status)
	}
	if !strings.Contains(body, "# HELP online_connections") {
		t.Fatalf("metrics missing online_connections:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE online_connections gauge") {
		t.Fatalf("metrics missing TYPE:\n%s", body)
	}
	if !strings.Contains(body, "online_connections 0") {
		t.Fatalf("metrics missing value:\n%s", body)
	}
}

// TestAdminNodes_WithRegistry /admin/nodes 列出节点。
func TestAdminNodes_WithRegistry(t *testing.T) {
	reg := cluster.NewMemRegistry()
	defer reg.Close()
	_ = reg.Register(cluster.NodeInfo{
		ID:      "logic-0",
		Role:    cluster.RoleLogic,
		Modules: []string{"snake"},
		State:   cluster.StateActive,
	})

	srv := newTestServer(t, reg, nil)
	status, body := doReq(t, srv, "GET", "/admin/nodes", "")
	if status != 200 {
		t.Fatalf("nodes: status=%d body=%q", status, body)
	}
	var resp struct {
		Nodes []cluster.NodeInfo `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, body)
	}
	if len(resp.Nodes) != 1 || resp.Nodes[0].ID != "logic-0" {
		t.Fatalf("expected 1 node logic-0, got %+v", resp.Nodes)
	}
}

// TestAdminDrain_Triggered /admin/drain 触发排水回调。
func TestAdminDrain_Triggered(t *testing.T) {
	called := false
	drainFn := func(ctx context.Context, deadline time.Duration) error {
		called = true
		if deadline != 20*time.Minute {
			t.Errorf("deadline: want 20m, got %v", deadline)
		}
		return nil
	}

	srv := newTestServer(t, nil, drainFn)
	status, body := doReq(t, srv, "POST", "/admin/drain?deadline=20m", "")
	if status != 200 {
		t.Fatalf("drain: status=%d body=%q", status, body)
	}
	if !called {
		t.Fatalf("drainFn not called")
	}
	if !strings.Contains(body, "drained") {
		t.Fatalf("drain body: %q", body)
	}
}

// TestAdminDrain_NoDrainFn standalone/gateway 角色无 drainFn 时应返回 400。
func TestAdminDrain_NoDrainFn(t *testing.T) {
	srv := newTestServer(t, nil, nil)
	status, body := doReq(t, srv, "POST", "/admin/drain", "")
	if status != 400 {
		t.Fatalf("expected 400, got %d body=%q", status, body)
	}
}

// TestIPGuard_RejectExternal 外网来源被拒绝。
func TestIPGuard_RejectExternal(t *testing.T) {
	srv := newTestServer(t, nil, nil)
	// 用 httptest 模拟非 loopback 来源（构造一个带 X-Forwarded-For 的请求）
	req := httptest.NewRequest("POST", "/admin/drain", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatalf("expected 403 for external source, got %d", w.Code)
	}
}

// doReq 用真实 HTTP 客户端访问 admin server（端到端）。
func doReq(t *testing.T, srv *Server, method, path, body string) (int, string) {
	t.Helper()
	url := "http://" + srv.listener.Addr().String() + path
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// stubProfileQuery 用于测试的 ProfileQueryService 桩实现。
type stubProfileQuery struct {
	profiles map[string]*framework.PlayerProfile
}

func (s *stubProfileQuery) Query(ctx context.Context, uid string) (*framework.PlayerProfile, error) {
	if p, ok := s.profiles[uid]; ok {
		return p, nil
	}
	return &framework.PlayerProfile{UID: uid}, nil // 零值
}

func (s *stubProfileQuery) BatchQuery(ctx context.Context, uids []string) (map[string]*framework.PlayerProfile, error) {
	out := make(map[string]*framework.PlayerProfile, len(uids))
	for _, uid := range uids {
		p, _ := s.Query(ctx, uid)
		out[uid] = p
	}
	return out, nil
}

// TestPlayerProfile_Query 离线查询端点（§10.4.3）。
func TestPlayerProfile_Query(t *testing.T) {
	stub := &stubProfileQuery{profiles: map[string]*framework.PlayerProfile{
		"u1": {UID: "u1", Level: 10, Coin: 999},
	}}
	srv := newTestServer(t, nil, nil)
	srv.SetProfileQueryService(stub)

	// 查存在的玩家
	status, body := doReq(t, srv, "GET", "/admin/players/u1", "")
	if status != 200 {
		t.Fatalf("status=%d, want 200; body=%q", status, body)
	}
	if !strings.Contains(body, "\"uid\":\"u1\"") || !strings.Contains(body, "999") {
		t.Fatalf("body mismatch: %q", body)
	}

	// 查不存在的玩家（应返回零值档案）
	status, body = doReq(t, srv, "GET", "/admin/players/ghost", "")
	if status != 200 {
		t.Fatalf("status=%d, want 200; body=%q", status, body)
	}
	if !strings.Contains(body, "\"uid\":\"ghost\"") {
		t.Fatalf("body should contain zero profile: %q", body)
	}
}

// TestPlayerProfile_BatchQuery 批量查询。
func TestPlayerProfile_BatchQuery(t *testing.T) {
	stub := &stubProfileQuery{profiles: map[string]*framework.PlayerProfile{
		"a": {UID: "a", Level: 1},
		"b": {UID: "b", Level: 2},
	}}
	srv := newTestServer(t, nil, nil)
	srv.SetProfileQueryService(stub)

	status, body := doReq(t, srv, "GET", "/admin/players/x?batch=a,b", "")
	if status != 200 {
		t.Fatalf("status=%d, want 200; body=%q", status, body)
	}
	if !strings.Contains(body, "profiles") {
		t.Fatalf("body should contain profiles: %q", body)
	}
}

// TestPlayerProfile_NotInjected 未注入查询服务时返回 501。
func TestPlayerProfile_NotInjected(t *testing.T) {
	srv := newTestServer(t, nil, nil)
	// 不调用 SetProfileQueryService
	status, _ := doReq(t, srv, "GET", "/admin/players/any", "")
	if status != 501 {
		t.Fatalf("status=%d, want 501 (Not Implemented)", status)
	}
}

// TestPlayerProfile_MethodNotAllowed 非 GET 拒绝。
func TestPlayerProfile_MethodNotAllowed(t *testing.T) {
	stub := &stubProfileQuery{}
	srv := newTestServer(t, nil, nil)
	srv.SetProfileQueryService(stub)
	status, _ := doReq(t, srv, "POST", "/admin/players/u1", "")
	if status != 405 {
		t.Fatalf("status=%d, want 405", status)
	}
}
