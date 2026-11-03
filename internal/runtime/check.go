package runtime

import (
	"context"
	"errors"
	"github.com/cloudwego/kitex/pkg/klog"
	"regexp"
	"sync/atomic"
)

// Check 是内部检查声明，不暴露可变检查状态。
type Check struct {
	Kind, Name string
	Func       func(context.Context) error
}

// check 持有执行占用位，同一不遵守取消的回调最多占用一个工作协程。
type check struct {
	Check
	busy              atomic.Bool
	observed, passing bool
}

var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// validateChecks 拒绝空回调、无效声明和同类重名，不输出用户输入值。
func validateChecks(checks []Check) ([]*check, error) {
	seen := make(map[string]bool)
	result := make([]*check, 0, len(checks))
	for _, item := range checks {
		if item.Kind != "startup" && item.Kind != "readiness" && item.Kind != "liveness" || !validName.MatchString(item.Name) || item.Func == nil || seen[item.Kind+"/"+item.Name] {
			return nil, errors.New("probe: 检查类型、名称或回调无效，或者存在同类重名检查")
		}
		seen[item.Kind+"/"+item.Name] = true
		result = append(result, &check{Check: item})
	}
	return result, nil
}

// run 在取消或超时时返回失败，恢复回调 panic，不回显原始错误。
func (c *check) run(ctx context.Context) bool {
	if ctx.Err() != nil || !c.busy.CompareAndSwap(false, true) {
		return false
	}
	result := make(chan bool, 1)
	go func() {
		passing := false
		defer func() { _ = recover(); c.busy.Store(false); result <- passing }()
		passing = c.Func(ctx) == nil
	}()
	select {
	case passing := <-result:
		return passing && ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

// observe 仅在结果变化时输出安全日志，不输出回调错误。
func (c *check) observe(service string, passing bool) {
	if !c.observed || c.passing != passing {
		if !passing {
			klog.Warnf("probe: 检查未通过：service=[%s] type=[%s] name=[%s]", service, c.Kind, c.Name)
		}
		if c.observed && passing {
			klog.Infof("probe: 检查已恢复：service=[%s] type=[%s] name=[%s]", service, c.Kind, c.Name)
		}
	}
	c.observed, c.passing = true, passing
}
