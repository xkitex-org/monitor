package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 等待后台状态转换，避免测试依赖某次调度时序。
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("state transition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// testController 创建使用本机随机端口的实例，业务配置仍拒绝显式端口 0。
func testController(t *testing.T, checks []Check, phase *atomic.Value, report func(bool, bool)) *Controller {
	t.Helper()
	c := New(checks)
	if err := c.ConfigureMonitor([]byte("interval: 5ms\ntimeout: 10ms")); err != nil {
		t.Fatal(err)
	}
	c.cfg.Port = 0
	if _, err := c.Start(context.Background(), func() string { return phase.Load().(string) }, report); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

// status 请求真实 HTTP 端点并读取固定正文。
func status(t *testing.T, c *Controller, path string) (int, string) {
	t.Helper()
	port := c.listener.Addr().(*net.TCPAddr).Port
	response, err := http.Get("http://127.0.0.1:" + fmt.Sprint(port) + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

// TestAutonomousChecksRecoveryAndShutdown 验证没有 HTTP 访问也能完成检查，startup 锁存、readiness 恢复及停机优先。
func TestAutonomousChecksRecoveryAndShutdown(t *testing.T) {
	var phase atomic.Value
	phase.Store("Starting")
	var startup, ready, reportedStartup, reportedReady atomic.Bool
	var startupCalls atomic.Int32
	c := testController(t, []Check{
		{Kind: "startup", Name: "warmup", Func: func(context.Context) error {
			startupCalls.Add(1)
			if startup.Load() {
				return nil
			}
			return errors.New("private-startup-value")
		}},
		{Kind: "readiness", Name: "database", Func: func(context.Context) error {
			if ready.Load() {
				return nil
			}
			return errors.New("private-database-value")
		}},
	}, &phase, func(s, r bool) { reportedStartup.Store(s); reportedReady.Store(r) })
	if code, _ := status(t, c, "/live"); code != 200 {
		t.Fatal("default live failed during initialization")
	}
	if code, _ := status(t, c, "/ready"); code != 503 {
		t.Fatal("starting instance ready")
	}
	phase.Store("Running")
	startup.Store(true)
	waitFor(t, reportedStartup.Load)
	if reportedReady.Load() {
		t.Fatal("unhealthy dependency ignored")
	}
	ready.Store(true)
	waitFor(t, reportedReady.Load)
	if code, _ := status(t, c, "/ready"); code != 200 {
		t.Fatal("checks passed without readiness")
	}
	calls := startupCalls.Load()
	startup.Store(false)
	ready.Store(false)
	waitFor(t, func() bool { return !reportedReady.Load() })
	if code, _ := status(t, c, "/startup"); code != 200 || startupCalls.Load() != calls {
		t.Fatal("startup completion was reset or reevaluated")
	}
	ready.Store(true)
	waitFor(t, reportedReady.Load)
	phase.Store("Stopping")
	if code, _ := status(t, c, "/ready"); code != 503 {
		t.Fatal("stopping state revived")
	}
	if code, _ := status(t, c, "/live"); code != 200 {
		t.Fatal("normal draining failed liveness")
	}
}

// TestTimeoutPanicAndBoundedWorkers 验证超时和 panic 返回失败，慢回调不会阻塞 HTTP 或无限创建协程。
func TestTimeoutPanicAndBoundedWorkers(t *testing.T) {
	var phase atomic.Value
	phase.Store("Running")
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	c := testController(t, []Check{
		{Kind: "readiness", Name: "slow", Func: func(context.Context) error { calls.Add(1); <-release; return nil }},
		{Kind: "liveness", Name: "panic", Func: func(context.Context) error { panic("private-panic-value") }},
	}, &phase, func(bool, bool) {})
	waitFor(t, func() bool { return calls.Load() == 1 })
	time.Sleep(35 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("uncooperative callback spawned overlapping workers")
	}
	before := time.Now()
	code, body := status(t, c, "/ready")
	if code != 503 || body != "unavailable" || time.Since(before) > 200*time.Millisecond {
		t.Fatal("request executed checks or exposed error")
	}
	if code, _ := status(t, c, "/live"); code != 503 {
		t.Fatal("panic did not fail liveness")
	}
	before = time.Now()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(before) > 200*time.Millisecond {
		t.Fatal("close waited for uncooperative callback")
	}
}

// TestPortConflictFatalAndConcurrentClose 验证端口占用返回启动错误，HTTP 自身故障可观察且并发关闭安全。
func TestPortConflictFatalAndConcurrentClose(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	c := New(nil)
	if err := c.ConfigureMonitor([]byte(fmt.Sprintf("port: %d", listener.Addr().(*net.TCPAddr).Port))); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Start(context.Background(), func() string { return "Running" }, func(bool, bool) {}); err == nil {
		t.Fatal("occupied HTTP port accepted")
	}
	_ = c.Close()
	var phase atomic.Value
	phase.Store("Running")
	running := testController(t, nil, &phase, func(bool, bool) {})
	_ = running.listener.Close()
	select {
	case err := <-running.fatal:
		if err == nil {
			t.Fatal("empty fatal error")
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP fatal error not reported")
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); _ = running.Close() }()
	}
	group.Wait()
}

// TestStrictConfigurationAndOptions 验证配置缺省、未知字段、非法值及无效检查声明。
func TestStrictConfigurationAndOptions(t *testing.T) {
	cfg, err := parseSettings(nil)
	if err != nil || cfg.Port != 9091 {
		t.Fatal("bad defaults")
	}
	for _, body := range []string{"null", "[]", "port: 0", "port: 9091.0", "port: '9091'", "port: 65536", "interval: 0s", "timeout: invalid-private-value", "enabled: true", "sample_interval: 0s", "sample_timeout: bad", "sample_interval: null", "sample_timeout: 10", "port: null", "{}\n---\n{}", "port: 1\nport: 2"} {
		if _, err := parseSettings([]byte(body)); err == nil {
			t.Fatalf("invalid config accepted: %q", body)
		}
	}
	for _, checks := range [][]Check{{{}}, {{Kind: "readiness", Name: "x"}}, {{Kind: "readiness", Name: "x", Func: func(context.Context) error { return nil }}, {Kind: "readiness", Name: "x", Func: func(context.Context) error { return nil }}}} {
		if err := New(checks).ConfigureMonitor(nil); err == nil {
			t.Fatal("invalid checks accepted")
		}
	}
}
