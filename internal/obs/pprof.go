package obs

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"time"
)

// StartPProf 在独立 mux/端口上启动 pprof（架构文档 §11.1：生产环境需内网限制）。
// 返回 shutdown 函数。
func StartPProf(addr string) (shutdown func(context.Context) error, err error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			panic(serveErr)
		}
	}()

	return srv.Shutdown, nil
}
