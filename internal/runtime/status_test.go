package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/kitex/pkg/klog"
)

// statusOutput 支持后台日志写入和测试读取并发，避免日志断言引入数据竞争。
type statusOutput struct {
	mu   sync.Mutex
	body bytes.Buffer
}

// Write 记录 klog 输出。
func (o *statusOutput) Write(body []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.body.Write(body)
}

// String 返回当前日志快照。
func (o *statusOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.body.String()
}

// TestNamedProbeTransitionLogs 验证三个探针分别记录首次状态、失败恢复和停机，并与 HTTP 结果一致。
func TestNamedProbeTransitionLogs(t *testing.T) {
	var output statusOutput
	klog.SetOutput(&output)
	defer klog.SetOutput(os.Stderr)
	var phase atomic.Value
	phase.Store("Starting")
	var ready, live atomic.Bool
	live.Store(true)
	c := New([]Check{
		{Kind: "startup", Name: "warmup", Func: func(context.Context) error { return nil }},
		{Kind: "readiness", Name: "database", Func: func(context.Context) error {
			if ready.Load() {
				return nil
			}
			return errors.New("private-database")
		}},
		{Kind: "liveness", Name: "worker", Func: func(context.Context) error {
			if live.Load() {
				return nil
			}
			return errors.New("private-worker")
		}},
	})
	c.SetServiceName("zmall-product")
	if err := c.ConfigureProbe([]byte("interval: 5ms\ntimeout: 10ms")); err != nil {
		t.Fatal(err)
	}
	c.cfg.Port = 0
	if _, err := c.Start(context.Background(), func() string { return phase.Load().(string) }, func(bool, bool) {}); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	phase.Store("Running")
	waitFor(t, func() bool {
		return strings.Contains(output.String(), "type=[startup] from=[Waiting] to=[Passing]") && strings.Contains(output.String(), "type=[readiness] from=[Waiting] to=[Failing]")
	})
	if code, _ := status(t, c, "/ready"); code != 503 {
		t.Fatal("failed readiness returned success")
	}
	ready.Store(true)
	waitFor(t, func() bool { return strings.Contains(output.String(), "type=[readiness] from=[Failing] to=[Passing]") })
	if code, _ := status(t, c, "/ready"); code != 200 {
		t.Fatal("recovered readiness not reflected in HTTP")
	}
	live.Store(false)
	waitFor(t, func() bool { return strings.Contains(output.String(), "type=[liveness] from=[Passing] to=[Failing]") })
	if code, _ := status(t, c, "/live"); code != 503 {
		t.Fatal("failed liveness returned success")
	}
	live.Store(true)
	waitFor(t, func() bool { return strings.Contains(output.String(), "type=[liveness] from=[Failing] to=[Passing]") })
	phase.Store("Stopping")
	waitFor(t, func() bool { return strings.Contains(output.String(), "type=[readiness] from=[Passing] to=[Failing]") })
	if code, _ := status(t, c, "/ready"); code != 503 {
		t.Fatal("stopping readiness returned success")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	logs := output.String()
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "探针状态变化") || strings.Contains(line, "检查未通过") || strings.Contains(line, "检查已恢复") {
			if !strings.Contains(line, "service=[zmall-product]") {
				t.Fatalf("missing service: %s", line)
			}
		}
	}
	for _, transition := range []string{
		"type=[startup] from=[Unknown] to=[Waiting] checks=[1]",
		"type=[startup] from=[Waiting] to=[Passing] checks=[1]",
		"type=[readiness] from=[Failing] to=[Passing] checks=[1]",
		"type=[liveness] from=[Passing] to=[Failing] checks=[1]",
		"type=[liveness] from=[Failing] to=[Passing] checks=[1]",
		"type=[readiness] from=[Failing] to=[Stopped] checks=[1]",
	} {
		if strings.Count(logs, transition) != 1 {
			t.Fatalf("missing or repeated %s: %s", transition, logs)
		}
	}
	if strings.Contains(logs, "private-") {
		t.Fatal("callback error leaked")
	}
}

// TestDefaultProbeLogsWithoutPolling 验证没有业务检查和 HTTP 轮询时，默认行为仍分别输出且不刷屏。
func TestDefaultProbeLogsWithoutPolling(t *testing.T) {
	var output statusOutput
	klog.SetOutput(&output)
	defer klog.SetOutput(os.Stderr)
	c := New(nil)
	c.SetServiceName("zmall-product")
	if err := c.ConfigureProbe(nil); err != nil {
		t.Fatal(err)
	}
	c.state = func() string { return "Running" }
	c.report = func(bool, bool) {}
	for i := 0; i < 5; i++ {
		c.evaluate(context.Background())
	}
	logs := output.String()
	for _, kind := range []string{"startup", "readiness", "liveness"} {
		if strings.Count(logs, "type=["+kind+"] from=[Unknown] to=[Passing] checks=[0]") != 1 {
			t.Fatalf("missing default state: %s", logs)
		}
	}
	if strings.Count(logs, "探针状态变化") != 3 {
		t.Fatal("unchanged probe results generated repeated logs")
	}
}
