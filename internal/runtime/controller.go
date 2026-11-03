package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/cloudwego/kitex/pkg/klog"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// Controller 持有单实例配置、主动检查任务及 HTTP 服务，不使用全局健康标志。
type Controller struct {
	mu                          sync.RWMutex
	checks                      []*check
	cfg                         settings
	initialErr                  error
	configured, started, closed bool
	startup, ready, live        bool
	state                       func() string
	report                      func(bool, bool)
	server                      *http.Server
	listener                    net.Listener
	cancel                      context.CancelFunc
	done                        chan struct{}
	closeDone                   chan struct{}
	fatal                       chan error
	closeErr                    error
}

// New 只保存检查声明，不读取环境、安装钩子、绑定端口或启动协程。
func New(declarations []Check) *Controller {
	checks, err := validateChecks(declarations)
	return &Controller{checks: checks, initialErr: err, closeDone: make(chan struct{})}
}

// ConfigureProbe 单次读取 core 下发的 probe 节。
func (c *Controller) ConfigureProbe(body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialErr != nil {
		return c.initialErr
	}
	if c.configured || c.closed {
		return errors.New("probe: 配置只允许在启动前应用一次")
	}
	cfg, err := parseSettings(body)
	if err != nil {
		return err
	}
	c.cfg, c.configured = cfg, true
	return nil
}

// Start 同步绑定 HTTP 端口后主动检查；错误通道仅报告 HTTP 服务自身致命故障。
func (c *Controller) Start(parent context.Context, state func() string, report func(bool, bool)) (<-chan error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.configured || c.started || c.closed || parent == nil || state == nil || report == nil {
		return nil, errors.New("probe: 启动参数或生命周期无效")
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", c.cfg.Port))
	if err != nil {
		return nil, errors.New("probe: HTTP 端口绑定失败")
	}
	ctx, cancel := context.WithCancel(parent)
	c.state, c.report, c.listener, c.cancel = state, report, listener, cancel
	c.done, c.fatal = make(chan struct{}), make(chan error, 1)
	c.live = true
	for _, item := range c.checks {
		if item.Kind == "liveness" {
			c.live = false
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/live", c.handler("live"))
	mux.HandleFunc("/ready", c.handler("ready"))
	mux.HandleFunc("/startup", c.handler("startup"))
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second, ErrorLog: log.New(httpErrors{}, "", 0)}
	c.started = true
	go func() {
		if err := c.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case c.fatal <- errors.New("probe: HTTP 服务异常退出"):
			default:
			}
		}
	}()
	go c.loop(ctx)
	klog.Infof("probe: HTTP 探针已启动：addr=[%s]", listener.Addr())
	return c.fatal, nil
}

// loop 主动刷新检查结果，无需 Kubernetes 先访问 HTTP。
func (c *Controller) loop(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		c.evaluate(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// evaluate 锁存启动结果并更新就绪缓存，最终响应仍受 core 停机阶段约束。
func (c *Controller) evaluate(ctx context.Context) {
	live := c.group(ctx, "liveness")
	phase := c.state()
	c.mu.RLock()
	startup := c.startup
	c.mu.RUnlock()
	ready := false
	if phase == "Running" {
		if !startup {
			startup = c.group(ctx, "startup")
		}
		if startup {
			ready = c.group(ctx, "readiness")
		}
	}
	c.mu.Lock()
	c.live = live
	c.startup = c.startup || startup
	c.ready = ready
	started := c.startup
	c.mu.Unlock()
	c.report(started, ready)
}

// group 为每项检查设置独立超时，同类检查必须全部通过。
func (c *Controller) group(parent context.Context, kind string) bool {
	passing := true
	for _, item := range c.checks {
		if item.Kind != kind {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, c.cfg.Timeout)
		ok := item.run(ctx)
		cancel()
		if parent.Err() != nil {
			return false
		}
		item.observe(ok)
		passing = passing && ok
	}
	return passing && parent.Err() == nil
}

// handler 只读取缓存并返回固定正文，不暴露用户回调错误或连接信息。
func (c *Controller) handler(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		phase := c.state()
		c.mu.RLock()
		healthy := false
		switch kind {
		case "live":
			healthy = c.live && phase != "Failed" && phase != "Stopped"
		case "startup":
			healthy = c.startup
		case "ready":
			healthy = phase == "Running" && c.startup && c.ready && !c.closed
		}
		c.mu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// Close 幂等取消检查并关闭 HTTP；并发调用等待同一次清理，不等待忽略取消的业务回调。
func (c *Controller) Close() error {
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	}
	c.closed, c.ready = true, false
	cancel, srv, done := c.cancel, c.server, c.done
	if cancel != nil {
		cancel()
	}
	c.mu.Unlock()
	if done != nil {
		<-done
	}
	var err error
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = srv.Shutdown(ctx)
		cancel()
		if err != nil {
			_ = srv.Close()
		}
	}
	c.mu.Lock()
	c.closeErr = err
	close(c.closeDone)
	c.mu.Unlock()
	return err
}

// httpErrors 将 HTTP 内部错误转入 Kitex 日志。
type httpErrors struct{}

// Write 输出固定错误，避免回显可能含请求信息的原始诊断。
func (httpErrors) Write(data []byte) (int, error) {
	klog.Errorf("probe: HTTP 服务内部错误")
	return len(data), nil
}
