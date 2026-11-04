package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/prometheus/client_golang/prometheus"
)

// TestMonitoringHTTPAndLifecycle 验证真实 HTTP 的运行时与进程数据、独立注册表、健康状态和停机约束。
func TestMonitoringHTTPAndLifecycle(t *testing.T) {
	global := prometheus.NewGauge(prometheus.GaugeOpts{Name: "monitor_test_global_marker", Help: "不应进入实例注册表"})
	prometheus.MustRegister(global)
	defer prometheus.Unregister(global)
	var phase atomic.Value
	phase.Store("Running")
	var healthy atomic.Bool
	c := New([]Check{{Kind: "readiness", Name: "database.primary", Func: func(context.Context) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("secret-marker")
	}}}, prometheus.NewGauge(prometheus.GaugeOpts{Name: "business_inventory", Help: "业务指标"}))
	if err := c.ConfigureMonitor([]byte("interval: 5ms\ntimeout: 10ms\nsample_interval: 10ms\nsample_timeout: 1s")); err != nil {
		t.Fatal(err)
	}
	c.cfg.Port = 0
	if _, err := c.Start(context.Background(), func() string { return phase.Load().(string) }, func(bool, bool) {}); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, func() bool { sample, _ := c.sampler.snapshot(); return sample.CollectionSuccess })
	code, body := status(t, c, "/info")
	var view info
	if code != 200 || json.Unmarshal([]byte(body), &view) != nil {
		t.Fatalf("invalid info: %d %s", code, body)
	}
	if view.Process.PID != os.Getpid() || view.Process.ResidentBytes == 0 || view.GoRuntime.Goroutines == 0 || view.GoRuntime.HeapAlloc == 0 || view.Available || view.GoRuntime.Version == "" {
		t.Fatalf("invalid process/go/health snapshot: %s", body)
	}
	if strings.Contains(body, "secret-marker") || strings.Contains(body, "environment") {
		t.Fatal("info leaked implementation inputs")
	}
	code, body = status(t, c, "/metrics")
	for _, name := range []string{"go_goroutines", "go_memstats_heap_alloc_bytes", "process_cpu_seconds_total", "process_resident_memory_bytes", "xkitex_process_collection_success", "xkitex_probe_success", "business_inventory"} {
		if code != 200 || !strings.Contains(body, name) {
			t.Fatalf("missing metric %s: %d", name, code)
		}
	}
	if strings.Contains(body, "monitor_test_global_marker") {
		t.Fatal("default registry leaked into monitor")
	}
	healthy.Store(true)
	waitFor(t, func() bool { return c.snapshot().Available })
	phase.Store("Stopping")
	if c.snapshot().Available {
		t.Fatal("stopping revived cached readiness")
	}
	if code, body = status(t, c, "/metrics"); code != 200 || !strings.Contains(body, "xkitex_service_available{application=\"未确定\"} 0") {
		t.Fatal("metrics ignored stopping state")
	}
	for _, path := range []string{"/info", "/metrics"} {
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d%s", c.listener.Addr().(*net.TCPAddr).Port, path), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 || resp.Header.Get("Allow") != "GET" {
			t.Fatal("monitor endpoint accepted mutation")
		}
	}
}

// TestCPUUsageAndStaleSamples 验证 CPU 按逻辑核计量，首次无用量，失败保留有效值与时间戳。
func TestCPUUsageAndStaleSamples(t *testing.T) {
	s := newSampler(nil)
	now := time.Now()
	s.update(processSample{CPUSeconds: 2, SampleTime: now}, nil)
	sample, _ := s.snapshot()
	if sample.CPUUsageRatio != nil {
		t.Fatal("first sample invented CPU ratio")
	}
	s.update(processSample{CPUSeconds: 8, SampleTime: now.Add(2 * time.Second)}, nil)
	sample, _ = s.snapshot()
	if sample.CPUUsageRatio == nil || *sample.CPUUsageRatio != 3 {
		t.Fatal("CPU ratio is not per logical core")
	}
	s.update(processSample{}, errors.New("read failed"))
	stale, _ := s.snapshot()
	if stale.CollectionSuccess || stale.CPUSeconds != 8 || !stale.SampleTime.Equal(sample.SampleTime) {
		t.Fatal("failed sample erased last good metrics")
	}
}

// TestSamplerBoundsWorkersAndCancellation 验证系统采集失控时不累积工作协程、不阻塞关闭。
func TestSamplerBoundsWorkersAndCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	s := newSampler(func(context.Context) (processSample, error) { calls.Add(1); <-release; return processSample{}, nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.loop(ctx, time.Millisecond, time.Millisecond) }()
	waitFor(t, func() bool { return calls.Load() == 1 })
	time.Sleep(15 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("overlapping sampler workers")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sampler did not stop")
	}
}

// TestRPCMetricsRetainResults 验证业务返回值、错误和 panic 不被观测中间件改变，并且在途数归零。
func TestRPCMetricsRetainResults(t *testing.T) {
	c := New(nil)
	c.SetServiceName("zmall-product")
	c.mu.Lock()
	err := c.prepareMetrics()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	invocation := rpcinfo.NewInvocation("ProductService", "GetProduct")
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), rpcinfo.NewRPCInfo(nil, nil, invocation, nil, nil))
	failure := errors.New("business failed")
	for _, result := range []string{"ok", "error", "panic"} {
		handler := c.Middleware()(func(context.Context, interface{}, interface{}) error {
			if result == "error" {
				return failure
			}
			if result == "panic" {
				panic(failure)
			}
			return nil
		})
		func() {
			defer func() {
				if value := recover(); value != nil {
					if result != "panic" || value != failure {
						t.Fatal("panic changed")
					}
				} else if result == "panic" {
					t.Fatal("panic swallowed")
				}
			}()
			err := handler(ctx, nil, nil)
			if result == "error" && err != failure || result == "ok" && err != nil {
				t.Fatal("error changed")
			}
		}()
	}
	families, err := c.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() == "xkitex_rpc_requests_total" {
			for _, metric := range family.Metric {
				total += metric.GetCounter().GetValue()
			}
		}
		if family.GetName() == "xkitex_rpc_in_flight" {
			for _, metric := range family.Metric {
				if metric.GetGauge().GetValue() != 0 {
					t.Fatal("in-flight leaked")
				}
			}
		}
		if family.GetName() == "xkitex_rpc_duration_seconds" && family.Metric[0].GetHistogram().GetSampleCount() != 3 {
			t.Fatal("duration lost calls")
		}
	}
	if total != 3 {
		t.Fatalf("lost RPCs: %v", total)
	}
}

// TestCollectorValidationAndInstanceIsolation 验证指标名冲突阻止监听，多个实例不发生全局注册冲突。
func TestCollectorValidationAndInstanceIsolation(t *testing.T) {
	for _, custom := range [][]prometheus.Collector{
		{nil}, {(*prometheus.CounterVec)(nil)},
		{prometheus.NewGauge(prometheus.GaugeOpts{Name: "process_cpu_seconds_total", Help: "collision"})},
	} {
		c := New(nil, custom...)
		if err := c.ConfigureMonitor(nil); err != nil {
			continue
		}
		if _, err := c.Start(context.Background(), func() string { return "Running" }, func(bool, bool) {}); err == nil || c.listener != nil {
			t.Fatal("invalid collector bound port")
		}
		_ = c.Close()
	}
	var phase atomic.Value
	phase.Store("Running")
	a := testController(t, nil, &phase, func(bool, bool) {})
	b := testController(t, nil, &phase, func(bool, bool) {})
	for _, c := range []*Controller{a, b} {
		if code, _ := status(t, c, "/metrics"); code != 200 {
			t.Fatal("instance metric conflict")
		}
	}
}
