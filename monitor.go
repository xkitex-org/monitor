// Package monitor 提供应用进程、Go 运行时、RPC 和生命周期的统一 HTTP 监控出口。
package monitor

import (
	"context"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/prometheus/client_golang/prometheus"
	"go.chaoxizhongzhi.com/xkitex/monitor/internal/runtime"
)

// Controller 是独立接入协议；构造时不启动资源，业务通过 core.WithMonitor 使用。
type Controller interface {
	// ConfigureMonitor 接收单次合并后的 monitor 配置节。
	ConfigureMonitor([]byte) error
	// Start 启动监控资源，报告启动与就绪结果；错误通道只用于监控服务自身致命故障。
	Start(context.Context, func() string, func(started, ready bool)) (<-chan error, error)
	// Middleware 生成入站 RPC 指标中间件，不改变业务返回值。
	Middleware() endpoint.Middleware
	// Close 在 RPC 排空后取消后台任务并释放 HTTP 监听，允许重复调用。
	Close() error
}

// Option 保存检查或自定义指标声明，零值无效。
type Option struct {
	check     *runtime.Check
	collector prometheus.Collector
}

// New 创建监控控制器；配置、指标注册和端口绑定由 core 的启动流程触发。
func New(options ...Option) Controller {
	checks := make([]runtime.Check, 0, len(options))
	collectors := make([]prometheus.Collector, 0, len(options))
	for _, option := range options {
		if option.check != nil {
			checks = append(checks, *option.check)
		} else {
			// 零值和带类型 nil 的 collector 交给启动校验统一拒绝。
			collectors = append(collectors, option.collector)
		}
	}
	return runtime.New(checks, collectors...)
}

// WithStartup 增加启动检查；全部成功后锁存完成，不因后续依赖抖动复位。
func WithStartup(name string, check func(context.Context) error) Option {
	return Option{check: &runtime.Check{Kind: "startup", Name: name, Func: check}}
}

// WithReadiness 增加周期性就绪检查，失败时摘除流量，恢复后重新就绪。
func WithReadiness(name string, check func(context.Context) error) Option {
	return Option{check: &runtime.Check{Kind: "readiness", Name: name, Func: check}}
}

// WithLiveness 增加存活检查，只改变探针结果，不退出或重启进程。
func WithLiveness(name string, check func(context.Context) error) Option {
	return Option{check: &runtime.Check{Kind: "liveness", Name: name, Func: check}}
}

// WithCollector 将业务指标加入当前实例的独立注册表；不使用 Prometheus 全局注册表。
func WithCollector(collector prometheus.Collector) Option { return Option{collector: collector} }
