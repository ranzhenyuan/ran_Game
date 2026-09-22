package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ErrSlowConsumer 写 channel 已满：客户端过慢，按 §3.3 策略处理（默认 Kick）。
var ErrSlowConsumer = errors.New("transport: slow consumer, write channel full")

// Acceptor 连接接收入口。
type Acceptor interface {
	Addr() string
	Accept(ctx context.Context) (Conn, error)
	Close() error
}

// Options Acceptor / 连接参数。
type Options struct {
	ReadTimeout        time.Duration // 读空闲超时；<=0 不设超时
	WriteChannelSize   int           // 每连接写 channel 容量
	WriteFlushInterval time.Duration // 写聚合等待窗口
	WriteFlushBytes    int           // 写聚合字节上限
	MaxFrameSize       int
	NoDelay            bool
}

// DefaultOptions 默认参数（与 configs/server.yaml 对齐）。
func DefaultOptions() Options {
	return Options{
		ReadTimeout:        30 * time.Second,
		WriteChannelSize:   256,
		WriteFlushInterval: 2 * time.Millisecond,
		WriteFlushBytes:    4 * 1024,
		MaxFrameSize:       DefaultMaxFrame,
		NoDelay:            true,
	}
}

// TCPAcceptor TCP 接入器。
type TCPAcceptor struct {
	ln     net.Listener
	opts   Options
	nextID atomic.Uint64
}

// NewTCPAcceptor 在 addr 上监听。
func NewTCPAcceptor(addr string, opts Options) (*TCPAcceptor, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCPAcceptor{ln: ln, opts: opts}, nil
}

func (a *TCPAcceptor) Addr() string { return a.ln.Addr().String() }

func (a *TCPAcceptor) Close() error { return a.ln.Close() }

// Accept 接受下一条连接；Acceptor 关闭后返回错误。
func (a *TCPAcceptor) Accept(ctx context.Context) (Conn, error) {
	// ctx 取消时从阻塞 Accept 中退出。
	type acceptResult struct {
		c   net.Conn
		err error
	}
	resCh := make(chan acceptResult, 1)
	go func() {
		c, err := a.ln.Accept()
		resCh <- acceptResult{c, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resCh:
		if res.err != nil {
			return nil, res.err
		}
		return newTCPConn(a.nextID.Add(1), res.c, a.opts), nil
	}
}

// tcpConn 每连接：读由上层读泵同步调用；写聚合单独 1 个 goroutine（§3.2）。
type tcpConn struct {
	id        uint64
	raw       net.Conn
	opts      Options
	writeCh   chan []byte
	done      chan struct{}
	closeOnce sync.Once
	meta      sync.Map
}

func newTCPConn(id uint64, raw net.Conn, opts Options) *tcpConn {
	if opts.WriteChannelSize <= 0 {
		opts.WriteChannelSize = 256
	}
	if opts.WriteFlushInterval <= 0 {
		opts.WriteFlushInterval = 2 * time.Millisecond
	}
	if opts.WriteFlushBytes <= 0 {
		opts.WriteFlushBytes = 4 * 1024
	}
	if opts.MaxFrameSize <= 0 {
		opts.MaxFrameSize = DefaultMaxFrame
	}
	if tc, ok := raw.(*net.TCPConn); ok && opts.NoDelay {
		_ = tc.SetNoDelay(true)
	}

	c := &tcpConn{
		id:      id,
		raw:     raw,
		opts:    opts,
		writeCh: make(chan []byte, opts.WriteChannelSize),
		done:    make(chan struct{}),
	}
	go c.writeLoop()
	return c
}

func (c *tcpConn) ID() uint64         { return c.id }
func (c *tcpConn) RemoteAddr() string { return c.raw.RemoteAddr().String() }
func (c *tcpConn) Meta() *sync.Map    { return &c.meta }

func (c *tcpConn) Read(ctx context.Context) (*Frame, error) {
	if c.opts.ReadTimeout > 0 {
		_ = c.raw.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	}
	f, err := DecodeFrame(c.raw, c.opts.MaxFrameSize)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (c *tcpConn) Push(data []byte) error {
	select {
	case c.writeCh <- data:
		return nil
	case <-c.done:
		return net.ErrClosed
	default:
		return ErrSlowConsumer // 慢消费者：上层按 Kick/Drop 策略处理（§3.3）
	}
}

func (c *tcpConn) Close(reason string) error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.raw.Close()
	})
	return err
}

// writeLoop 写聚合协程：首帧触发攒批窗口，窗口到期或累计字节达上限时一次 Write（§3.2）。
func (c *tcpConn) writeLoop() {
	buf := make([]byte, 0, c.opts.WriteFlushBytes)
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if _, err := c.raw.Write(buf); err != nil {
			_ = c.Close("write error")
		}
		buf = buf[:0]
	}

outer:
	for {
		select {
		case <-c.done:
			return
		case first, ok := <-c.writeCh:
			if !ok {
				return
			}
			buf = append(buf, first...)
			timer.Reset(c.opts.WriteFlushInterval)

			// 攒批：窗口内尽量多取；窗口到期或字节达上限即 flush，回外层等下一帧。
			for {
				select {
				case more, ok := <-c.writeCh:
					if !ok {
						flush()
						return
					}
					buf = append(buf, more...)
					if len(buf) >= c.opts.WriteFlushBytes {
						flush()
						continue outer
					}
				case <-timer.C:
					flush()
					continue outer
				case <-c.done:
					return
				}
			}
		}
	}
}
