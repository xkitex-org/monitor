package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/prometheus/client_golang/prometheus"
)

// Controller 持有单实例配置、主动检查任务及 HTTP 服务，不使用全局健康标志。
type Controller struct {
	mu                          sync.RWMutex
	collectors                  []prometheus.Collector
	metrics                     *metrics
	sampler                     *sampler
	sampleDone                  chan struct{}
	checks                      []*check
	cfg                         settings
	initialErr                  error
	configured, started, closed bool
	startup, ready, live        bool
	evaluated                   bool
	serviceName                 string
	statuses                    [3]string
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
func New(declarations []Check, collectors ...prometheus.Collector) *Controller {
	checks, err := validateChecks(declarations)
	for _, collector := range collectors {
		if collector == nil || reflect.ValueOf(collector).Kind() == reflect.Pointer && reflect.ValueOf(collector).IsNil() {
			err = errors.New("monitor: 自定义指标不能为 nil")
		}
	}
	return &Controller{serviceName: "未确定", checks: checks, collectors: collectors, sampler: newSampler(readProcess), initialErr: err, closeDone: make(chan struct{})}
}

// AddReadiness 在启动前接收 core 自动挂载的依赖检查，不允许覆盖业务同名检查。
func (c *Controller) AddReadiness(name string, callback func(context.Context) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialErr != nil {
		return c.initialErr
	}
	if c.started || c.closed {
		return errors.New("monitor: 就绪检查只能在启动前注册")
	}
	checks, err := validateChecks([]Check{{Kind: "readiness", Name: name, Func: callback}})
	if err != nil {
		return err
	}
	for _, existing := range c.checks {
		if existing.Kind == "readiness" && existing.Name == name {
			return errors.New("monitor: 存在同名就绪检查")
		}
	}
	c.checks = append(c.checks, checks...)
	return nil
}

// ConfigureMonitor 单次读取 core 下发的 monitor 节。
func (c *Controller) ConfigureMonitor(body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialErr != nil {
		return c.initialErr
	}
	if c.configured || c.closed {
		return errors.New("monitor: 配置只允许在启动前应用一次")
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
		return nil, errors.New("monitor: 启动参数或生命周期无效")
	}
	if err := c.prepareMetrics(); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", c.cfg.Port))
	if err != nil {
		return nil, errors.New("monitor: HTTP 端口绑定失败")
	}
	ctx, cancel := context.WithCancel(parent)
	c.state, c.report, c.listener, c.cancel = state, report, listener, cancel
	c.done, c.fatal = make(chan struct{}), make(chan error, 1)
	c.sampleDone = make(chan struct{})
	c.sampler.serviceName = c.serviceName
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
	mux.HandleFunc("/info", c.infoHandler)
	mux.Handle("/metrics", c.metricsHandler())
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second, ErrorLog: log.New(httpErrors{}, "", 0)}
	c.started = true
	c.logStatusesLocked(c.state())
	go func() {
		if err := c.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case c.fatal <- errors.New("monitor: HTTP 服务异常退出"):
			default:
			}
		}
	}()
	go c.loop(ctx)
	go func() { defer close(c.sampleDone); c.sampler.loop(ctx, c.cfg.SampleInterval, c.cfg.SampleTimeout) }()
	klog.Infof("monitor: HTTP 监控已启动：addr=[%s] endpoints=[/live,/ready,/startup,/info,/metrics]", listener.Addr())
	return c.fatal, nil
}

// loop 主动刷新检查结果，无需 Kubernetes 先访问 HTTP。
func (c *Controller) loop(ctx context.Context) {
	defer close(c.done)
	defer c.observePhase()
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
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	c.live = live
	c.evaluated = true
	c.startup = c.startup || startup
	c.ready = ready
	started := c.startup
	c.logStatusesLocked(c.state())
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
		item.observe(c.serviceName, ok)
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
		name := kind
		if kind == "live" {
			name = "liveness"
		}
		if kind == "ready" {
			name = "readiness"
		}
		healthy := c.status(name, phase) == "Passing"
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
	if c.sampleDone != nil {
		<-c.sampleDone
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
	if c.started {
		c.logStatusesLocked(c.state())
	}
	close(c.closeDone)
	c.mu.Unlock()
	return err
}

// httpErrors 将 HTTP 内部错误转入 Kitex 日志。
type httpErrors struct{}

// Write 输出固定错误，避免回显可能含请求信息的原始诊断。
func (httpErrors) Write(data []byte) (int, error) {
	klog.Errorf("monitor: HTTP 服务内部错误")
	return len(data), nil
}
