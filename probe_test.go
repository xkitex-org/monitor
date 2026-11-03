package probe_test

import (
	"context"
	"go.chaoxizhongzhi.com/xkitex/probe"
	"testing"
)

// TestConstructionHasNoStartupSideEffects 验证声明检查和创建实例不执行回调，关闭未启动实例安全。
func TestConstructionHasNoStartupSideEffects(t *testing.T) {
	called := false
	p := probe.New(probe.WithReadiness("database", func(context.Context) error { called = true; return nil }))
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
