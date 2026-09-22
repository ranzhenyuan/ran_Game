// Package admin 运维面 HTTP API（架构文档 §11 / §13.4 / §13.7）。
//
// 端点：
//
//	GET  /healthz          存活探针：进程在跑即 200
//	GET  /readyz           就绪探针：Acceptor 已启动且会话表非满即 200
//	GET  /metrics          Prometheus 文本格式指标导出
//	GET  /admin/nodes      列出 NodeRegistry 全部节点
//	POST /admin/drain      触发 Logic 排水（K8s preStop 钩子调用，§13.3）
//
// 安全基线（§11.4）：
//   - admin server 必须监听内网地址（不对外暴露）
//   - 真实部署加 mTLS / Token 鉴权（演进态骨架简化，仅校验来源 IP）
//   - /admin/* 端点拒绝外网来源
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/pkg/framework"
)

// Server admin HTTP 服务器。
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	logger     *slog.Logger
	exporter   *obs.PrometheusExporter
	registry   cluster.NodeRegistry          // 可空：standalone 形态
	profileQ   framework.ProfileQueryService // 可空：未注入时 /admin/players 返回 501
	ready      atomic.Value                  // readyz 探针：true=就绪
	drainFn    DrainFn                       // 可空：logic 角色排水触发回调
	mu         sync.Mutex
	closed     bool
}

// DrainFn 排水触发回调（§13.3 preStop 等价）。
//
//	deadline：排水截止时间
//	返回：error 非 nil 表示排水未在 deadline 内完成
type DrainFn func(ctx context.Context, deadline time.Duration) error

// Config admin server 配置。
type Config struct {
	// Addr 监听地址（必须内网，§11.4）。
	Addr string
	// TrustedCIDRs 信任的来源 IP CIDR；空表示仅 127.0.0.1。
	TrustedCIDRs []string
	// DrainDeadline 默认排水截止时间（§13.3，由 BuildLogic 注入）。
	DrainDeadline time.Duration
}

// NewServer 创建 admin server（不启动，需调用 Start）。
func NewServer(
	cfg Config,
	logger *slog.Logger,
	exporter *obs.PrometheusExporter,
	registry cluster.NodeRegistry,
	drainFn DrainFn,
	profileQ framework.ProfileQueryService,
) *Server {
	s := &Server{
		logger:   logger,
		exporter: exporter,
		registry: registry,
		drainFn:  drainFn,
		profileQ: profileQ,
	}
	s.ready.Store(false)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/admin/nodes", s.ipGuard(s.handleNodes))
	mux.HandleFunc("/admin/drain", s.ipGuard(s.handleDrain))
	mux.HandleFunc("/admin/players/", s.ipGuard(s.handlePlayerProfile))

	s.httpServer = &http.Server{
		Handler:           s.withRecover(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if cfg.Addr != "" {
		s.httpServer.Addr = cfg.Addr
	}

	return s
}

// Addr 返回实际监听地址（":0" 随机端口测试用；未启动时返回配置地址）。
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

// Start 启动 HTTP 监听；非阻塞，返回 listener 错误。
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("admin: listen %s: %w", s.httpServer.Addr, err)
	}
	s.listener = ln
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("admin server stopped", "err", err)
		}
	}()
	s.logger.Info("admin server started", "addr", ln.Addr().String())
	return nil
}

// SetReady 设置就绪探针状态。
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// SetDrainFn 注入排水触发回调（BuildLogic 在 drain 构造后注入）。
func (s *Server) SetDrainFn(fn DrainFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainFn = fn
}

// SetProfileQueryService 注入玩家档案查询服务（Build 在 storage 装配后注入，§10.4.3）。
func (s *Server) SetProfileQueryService(q framework.ProfileQueryService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profileQ = q
}

// Shutdown 优雅停机。
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.httpServer.Shutdown(ctx)
}

// ---------------- 端点 ----------------

// handleHealthz 存活探针：进程在跑即 200。
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleReadyz 就绪探针：Acceptor 已启动且会话表非满。
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if v, ok := s.ready.Load().(bool); ok && v {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("not ready"))
}

// handleMetrics Prometheus 文本格式指标导出。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if s.exporter == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	_, _ = s.exporter.PrometheusText(w)
}

// handleNodes 列出 NodeRegistry 全部节点。
func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if s.registry == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"nodes":   []any{},
			"message": "standalone mode, no registry",
		})
		return
	}
	nodes, err := s.registry.List("", false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

// handleDrain 触发 Logic 排水（§13.3 preStop 等价）。
//
//	POST /admin/drain?deadline=20m
//	演进态骨架：deadline 来自查询参数或 BuildLogic 注入的默认值。
func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	if s.drainFn == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "drain not available",
			"message": "standalone/gateway role has no drain controller",
		})
		return
	}

	deadline := 20 * time.Minute // 默认值
	if d := r.URL.Query().Get("deadline"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			deadline = parsed
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), deadline+5*time.Second)
	defer cancel()
	if err := s.drainFn(ctx, deadline); err != nil {
		s.logger.Warn("drain did not complete", "deadline", deadline, "err", err)
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{
			"error":   "drain_timeout",
			"message": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "drained"})
}

// handlePlayerProfile 查询玩家档案（离线查询入口，§10.4.3）。
//
//	GET /admin/players/{uid}
//	GET /admin/players/{uid}?batch=uid1,uid2  批量查询
func (s *Server) handlePlayerProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
		return
	}
	if s.profileQ == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{
			"error":   "profile_query_unavailable",
			"message": "profile query service not injected",
		})
		return
	}

	// /admin/players/{uid}
	uid := strings.TrimPrefix(r.URL.Path, "/admin/players/")
	if uid == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "uid required"})
		return
	}

	// 批量查询
	if batch := r.URL.Query().Get("batch"); batch != "" {
		uids := strings.Split(batch, ",")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		profiles, err := s.profileQ.BatchQuery(ctx, uids)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"profiles": profiles})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := s.profileQ.Query(ctx, uid)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// ---------------- 中间件 ----------------

// ipGuard 拒绝外网来源（§11.4）。
// 演进态骨架：仅校验 X-Forwarded-For 不存在 + RemoteAddr 是 loopback；
// 真实部署加 TrustedCIDRs 校验 + mTLS。
func (s *Server) ipGuard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// X-Forwarded-For 存在表示经过反向代理，演进态骨架拒绝（避免被绕过）
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "external source rejected"})
			return
		}
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "bad remote addr"})
			return
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "non-loopback source rejected"})
			return
		}
		h(w, r)
	}
}

// withRecover panic 恢复中间件。
func (s *Server) withRecover(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rv := recover(); rv != nil {
				s.logger.Error("admin handler panic", "path", r.URL.Path, "panic", rv)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			}
		}()
		h.ServeHTTP(w, r)
	})
}

// ---------------- 工具 ----------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
