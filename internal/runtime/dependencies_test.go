package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestDependencyReadinessRegistrationAndRecovery 验证自动依赖参与就绪、可失败恢复且不影响存活。
func TestDependencyReadinessRegistrationAndRecovery(t *testing.T) {
	c := New(nil)
	var healthy, ready atomic.Bool
	if err := c.ConfigureMonitor([]byte("interval: 5ms\ntimeout: 20ms")); err != nil {
		t.Fatal(err)
	}
	c.cfg.Port = 0
	if err := c.AddReadiness("database.primary", func(context.Context) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("连接失败")
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddReadiness("database.primary", func(context.Context) error { return nil }); err == nil {
		t.Fatal("duplicate dependency replaced check")
	}
	if _, err := c.Start(context.Background(), func() string { return "Running" }, func(_, r bool) { ready.Store(r) }); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.AddReadiness("late", func(context.Context) error { return nil }); err == nil {
		t.Fatal("late check accepted")
	}
	waitFor(t, func() bool { code, _ := status(t, c, "/startup"); return code == 200 })
	if code, _ := status(t, c, "/ready"); code != 503 {
		t.Fatal("unhealthy dependency was ready")
	}
	if code, _ := status(t, c, "/live"); code != 200 {
		t.Fatal("dependency failure affected liveness")
	}
	healthy.Store(true)
	waitFor(t, ready.Load)
	healthy.Store(false)
	waitFor(t, func() bool { return !ready.Load() })
	healthy.Store(true)
	waitFor(t, ready.Load)
}
