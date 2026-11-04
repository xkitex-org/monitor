package monitor_test

import (
	"context"
	"testing"

	"go.chaoxizhongzhi.com/xkitex/monitor"
)

// TestConstructionHasNoStartupSideEffects 验证声明检查和创建实例不执行回调，关闭未启动实例安全。
func TestConstructionHasNoStartupSideEffects(t *testing.T) {
	called := false
	p := monitor.New(monitor.WithReadiness("database", func(context.Context) error { called = true; return nil }))
	if called {
		t.Fatal("constructor executed checks")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
