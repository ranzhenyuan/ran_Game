package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrTextFrameNotSupported 客户端发来 Text 帧：框架只接受 Binary（帧协议为二进制）。
var ErrTextFrameNotSupported = errors.New("transport: text websocket message not supported, use binary")

// WSOptions WebSocket 接入参数。
type WSOptions struct {
	Options                 // 复用通用参数（ReadTimeout/WriteChannelSize/MaxFrameSize...）
	AllowedOrigins []string // 空=仅同源；"*"=放开（仅限测试/内网）
	// ProtocolPingInterval 协议层 ping 间隔（§5.2 WS 叠加）；<=0 取 ReadTimeout 的 2/3。
	ProtocolPingInterval time.Duration
	HandshakeTimeout     time.Duration // <=0 默认 5s
}

// WSAcceptor WebSocket 接入器：HTTP Upgrade → 统一 Conn 抽象。
type WSAcceptor struct {
	httpSrv  *http.Server
	ln       net.Listener
	upgrader websocket.Upgrader

	connCh    chan Conn
	closeCh   chan struct{}
	closeOnce sync.Once
	nextID    atomic.Uint64

	opts WSOptions
}

// NewWSAcceptor 在 addr/path 上监听 WebSocket。
func NewWSAcceptor(addr, path string, opts WSOptions) (*WSAcceptor, error) {
	applyWSDefaults(&opts)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}

	a := &WSAcceptor{
		ln:      ln,
		opts:    opts,
		connCh:  make(chan Conn, 128),
		closeCh: make(chan struct{}),
	}
	a.upgrader = websocket.Upgrader{
		HandshakeTimeout: opts.HandshakeTimeout,
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
		CheckOrigin:      a.checkOrigin,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, a.serveWS)
	a.httpSrv = &http.Server{Handler: mux}

	go func() { _ = a.httpSrv.Serve(ln) }()
	return a, nil
}

func applyWSDefaults(o *WSOptions) {
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 30 * time.Second
	}
	if o.WriteChannelSize <= 0 {
		o.WriteChannelSize = 256
	}
	if o.WriteFlushInterval <= 0 {
		o.WriteFlushInterval = 2 * time.Millisecond
	}
	if o.WriteFlushBytes <= 0 {
		o.WriteFlushBytes = 4096
	}
	if o.MaxFrameSize <= 0 {
		o.MaxFrameSize = DefaultMaxFrame
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = 5 * time.Second
	}
	if o.ProtocolPingInterval <= 0 {
		o.ProtocolPingInterval = o.ReadTimeout * 2 / 3
	}
}

func (a *WSAcceptor) Addr() string { return a.ln.Addr().String() }

func (a *WSAcceptor) Close() error {
	var err error
	a.closeOnce.Do(func() {
		close(a.closeCh)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = a.httpSrv.Shutdown(ctx)
	})
	return err
}

// Accept 等待下一个升级成功的连接；关闭或 ctx 取消时返回错误。
func (a *WSAcceptor) Accept(ctx context.Context) (Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.closeCh:
		return nil, net.ErrClosed
	case c := <-a.connCh:
		return c, nil
	}
}

// checkOrigin §11.4 安全基线：无 Origin 头（非浏览器客户端）放行；
// 浏览器请求要求同源或显式白名单。
func (a *WSAcceptor) checkOrigin(r *http.Request) bool {
	for _, o := range a.opts.AllowedOrigins {
		if o == "*" {
			return true
		}
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, allowed := range a.opts.AllowedOrigins {
		// 白名单条目既支持裸 Host（trusted.example），也支持完整 Origin（https://trusted.example）。
		host := allowed
		if strings.Contains(host, "://") {
			if au, perr := url.Parse(allowed); perr == nil {
				host = au.Host
			}
		}
		if strings.EqualFold(host, u.Host) {
			return true
		}
	}
	return false
}

func (a *WSAcceptor) serveWS(w http.ResponseWriter, r *http.Request) {
	select {
	case <-a.closeCh:
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	default:
	}

	raw, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade 失败已由 upgrader 写回 HTTP 响应
	}

	conn := newWSConn(a.nextID.Add(1), raw, a.opts)
	select {
	case a.connCh <- conn:
	case <-a.closeCh:
		_ = conn.Close("acceptor closing")
	}
}

// wsConn WebSocket 连接的 Conn 实现：与 tcpConn 同样的每连接 1 个写聚合协程。
type wsConn struct {
	id   uint64
	raw  *websocket.Conn
	opts WSOptions

	writeCh   chan []byte
	done      chan struct{}
	closeOnce sync.Once
	meta      sync.Map
}

func newWSConn(id uint64, raw *websocket.Conn, opts WSOptions) *wsConn {
	c := &wsConn{
		id:      id,
		raw:     raw,
		opts:    opts,
		writeCh: make(chan []byte, opts.WriteChannelSize),
		done:    make(chan struct{}),
	}

	// 收到客户端 pong 即刷新读超时（§5.2 协议层保活）。
	_ = raw.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
	raw.SetPongHandler(func(string) error {
		return raw.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
	})

	go c.writeLoop()
	return c
}

func (c *wsConn) ID() uint64         { return c.id }
func (c *wsConn) RemoteAddr() string { return c.raw.RemoteAddr().String() }
func (c *wsConn) Meta() *sync.Map    { return &c.meta }

func (c *wsConn) Read(_ context.Context) (*Frame, error) {
	// 应用层消息同样刷新读超时（心跳 ping 在应用层，§5.2）。
	_ = c.raw.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	mt, data, err := c.raw.ReadMessage()
	if err != nil {
		return nil, err
	}
	if mt == websocket.TextMessage {
		return nil, ErrTextFrameNotSupported
	}
	return DecodeFrame(bytes.NewReader(data), c.opts.MaxFrameSize)
}

func (c *wsConn) Push(data []byte) error {
	select {
	case c.writeCh <- data:
		return nil
	case <-c.done:
		return net.ErrClosed
	default:
		return ErrSlowConsumer
	}
}

func (c *wsConn) Close(reason string) error {
	c.closeOnce.Do(func() {
		close(c.done)
		// 尽力发 close 握手（WriteControl 可与写协程并发安全使用）。
		_ = c.raw.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason),
			time.Now().Add(time.Second),
		)
		_ = c.raw.Close()
	})
	return nil
}

// writeLoop 写聚合协程：业务帧攒批为单个 WS Binary 消息；协议 ping 独立发送。
func (c *wsConn) writeLoop() {
	buf := make([]byte, 0, c.opts.WriteFlushBytes)
	ping := time.NewTicker(c.opts.ProtocolPingInterval)
	defer ping.Stop()

	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		_ = c.raw.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := c.raw.WriteMessage(websocket.BinaryMessage, buf); err != nil {
			_ = c.Close("write error")
			return false
		}
		buf = buf[:0]
		return true
	}

outer:
	for {
		select {
		case <-c.done:
			return
		case <-ping.C:
			// 协议层 ping；写失败不立即致命，读超时会兜底判死。
			_ = c.raw.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
		case first, ok := <-c.writeCh:
			if !ok {
				return
			}
			buf = append(buf, first...)
			t := time.NewTimer(c.opts.WriteFlushInterval)

			for {
				select {
				case more, ok := <-c.writeCh:
					if !ok {
						flush()
						return
					}
					buf = append(buf, more...)
					if len(buf) >= c.opts.WriteFlushBytes {
						if !flush() {
							return
						}
						if !t.Stop() {
							<-t.C
						}
						continue outer
					}
				case <-t.C:
					if !flush() {
						return
					}
					continue outer
				case <-c.done:
					return
				}
			}
		}
	}
}
