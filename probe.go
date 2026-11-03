// Package probe 提供显式接入 core 的独立 HTTP 探针，包引入和实例构造均不启动资源。
package probe

import (
	"context"
	"go.chaoxizhongzhi.com/xkitex/probe/internal/runtime"
)

// Controller 是独立接入协议，不依赖 core 模块；业务通常只使用 New 和检查选项。
// state 返回 Server 阶段；report 报告启动完成和就绪结果，不操作注册中心。
type Controller interface {
	ConfigureProbe([]byte) error
	Start(context.Context, func() string, func(started, ready bool)) (<-chan error, error)
	Close() error
}

// Option 保存一项业务检查声明，零值无效，不含启动副作用。
type Option struct{ check runtime.Check }

// New 创建探针控制器，配置校验和端口绑定延迟到 core 调用接入协议时执行。
func New(options ...Option) Controller {
	checks := make([]runtime.Check, len(options))
	for i, option := range options {
		checks[i] = option.check
	}
	return runtime.New(checks)
}

// WithStartup 增加启动检查；全部成功后锁存完成，不因后续依赖抖动复位。
func WithStartup(name string, check func(context.Context) error) Option {
	return Option{runtime.Check{Kind: "startup", Name: name, Func: check}}
}

// WithReadiness 增加周期性就绪检查，影响可用性，恢复后可以重新就绪。
func WithReadiness(name string, check func(context.Context) error) Option {
	return Option{runtime.Check{Kind: "readiness", Name: name, Func: check}}
}

// WithLiveness 增加存活检查，只改变 HTTP 结果，不退出或重启进程。
func WithLiveness(name string, check func(context.Context) error) Option {
	return Option{runtime.Check{Kind: "liveness", Name: name, Func: check}}
}
