package runtime

import (
	"context"
	"errors"
	"log"
	"net/http"
	"reflect"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metrics 保存当前实例的独立注册表与 RPC 指标，避免重复启动时污染全局注册表。
type metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight *prometheus.GaugeVec
}

// prepareMetrics 在监听前校验自定义指标；调用方持有控制器锁。
func (c *Controller) prepareMetrics() error {
	labels := prometheus.Labels{"application": c.serviceName}
	m := &metrics{registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "xkitex_rpc_requests_total", Help: "已完成的入站 RPC 数，包含业务错误、限流拒绝和 panic。", ConstLabels: labels}, []string{"rpc_service", "method", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "xkitex_rpc_duration_seconds", Help: "入站 RPC 执行耗时，单位秒。", ConstLabels: labels, Buckets: prometheus.DefBuckets}, []string{"rpc_service", "method"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "xkitex_rpc_in_flight", Help: "正在执行的入站 RPC 数。", ConstLabels: labels}, []string{"rpc_service", "method"}),
	}
	all := []prometheus.Collector{collectors.NewGoCollector(), newSnapshotCollector(c, labels), m.requests, m.duration, m.inFlight}
	all = append(all, c.collectors...)
	for _, collector := range all {
		if collector == nil {
			return errors.New("monitor: 自定义指标不能为 nil")
		}
		value := reflect.ValueOf(collector)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return errors.New("monitor: 自定义指标不能为 nil")
			}
		}
		if err := m.registry.Register(collector); err != nil {
			return errors.New("monitor: 指标注册失败，请检查重复名称或无效描述")
		}
	}
	c.metrics = m
	return nil
}

// metricsHandler 限制并发抓取数和响应时间，自定义 collector 应遵守 Prometheus 的并发与及时返回约定。
func (c *Controller) metricsHandler() http.Handler {
	return getOnly(promhttp.HandlerFor(c.metrics.registry, promhttp.HandlerOpts{
		ErrorLog: log.New(httpErrors{}, "", 0), MaxRequestsInFlight: 5, Timeout: time.Second,
	}))
}

// Middleware 记录真实入站调用；应包在 Sentinel 外层，以统计被限流和熔断拒绝的请求。
func (c *Controller) Middleware() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request, response interface{}) (err error) {
			c.mu.RLock()
			m := c.metrics
			c.mu.RUnlock()
			if m == nil {
				return next(ctx, request, response)
			}
			service, method := "unknown", "unknown"
			if info := rpcinfo.GetRPCInfo(ctx); info != nil && info.Invocation() != nil {
				if name := info.Invocation().ServiceName(); name != "" {
					service = name
				}
				if name := info.Invocation().MethodName(); name != "" {
					method = name
				}
			}
			started := time.Now()
			returned := false
			inFlight := m.inFlight.WithLabelValues(service, method)
			inFlight.Inc()
			defer func() {
				result := "ok"
				if !returned {
					result = "panic"
				} else if err != nil {
					result = "error"
				}
				inFlight.Dec()
				m.requests.WithLabelValues(service, method, result).Inc()
				m.duration.WithLabelValues(service, method).Observe(time.Since(started).Seconds())
			}()
			err = next(ctx, request, response)
			returned = true
			return err
		}
	}
}

// snapshotCollector 将缓存视图导出为指标，抓取不会触发业务回调或阻塞的系统采样。
type snapshotCollector struct {
	controller   *Controller
	descriptions map[string]*prometheus.Desc
}

// newSnapshotCollector 预先声明有限标签；业务输入、配置 key 和凭据不进入指标标签。
func newSnapshotCollector(c *Controller, labels prometheus.Labels) *snapshotCollector {
	definitions := map[string]string{
		"process_cpu_seconds_total":          "当前进程累计用户态与内核态 CPU 时间，单位秒。",
		"process_resident_memory_bytes":      "当前进程驻留内存，单位字节。",
		"process_virtual_memory_bytes":       "当前进程虚拟内存，单位字节。",
		"process_start_time_seconds":         "当前进程启动的 Unix 时间，单位秒。",
		"xkitex_process_cpu_usage_ratio":     "CPU 时间与墙上时间的比值，1 表示占满一个逻辑核，可大于 1。",
		"xkitex_process_collection_success":  "最近一次进程采样是否成功，未采样为 0。",
		"xkitex_process_sample_time_seconds": "最近一次成功进程采样的 Unix 时间。",
		"xkitex_process_uptime_seconds":      "当前进程存活时长，单位秒。",
		"xkitex_service_available":           "实例是否已完成启动并通过 readiness。",
		"xkitex_service_phase":               "实例生命周期阶段的一位有效状态。",
		"xkitex_probe_success":               "startup、readiness、liveness 各自是否通过。",
		"xkitex_check_success":               "具名健康检查是否通过，仅在实际执行后输出。",
		"xkitex_check_observed":              "具名健康检查是否已实际执行。",
	}
	result := &snapshotCollector{controller: c, descriptions: make(map[string]*prometheus.Desc, len(definitions))}
	for name, help := range definitions {
		var variable []string
		switch name {
		case "xkitex_service_phase":
			variable = []string{"phase"}
		case "xkitex_probe_success":
			variable = []string{"probe"}
		case "xkitex_check_success", "xkitex_check_observed":
			variable = []string{"kind", "name"}
		}
		result.descriptions[name] = prometheus.NewDesc(name, help, variable, labels)
	}
	return result
}

// Describe 提交固定指标描述，不进行进程采样。
func (s *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, description := range s.descriptions {
		ch <- description
	}
}

// Collect 从最近有效采样与实时生命周期生成指标；采样失败时保留最后有效值和时间戳。
func (s *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	value := s.controller.snapshot()
	emit := func(name string, kind prometheus.ValueType, number float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(s.descriptions[name], kind, number, labels...)
	}
	emit("xkitex_process_collection_success", prometheus.GaugeValue, boolValue(value.Process.CollectionSuccess))
	emit("xkitex_service_available", prometheus.GaugeValue, boolValue(value.Available))
	phases := []string{"Unknown", "Starting", "Running", "Stopping", "Stopped", "Failed"}
	phase := value.Phase
	known := false
	for _, item := range phases {
		known = known || item == phase
	}
	if !known {
		phase = "Unknown"
	}
	for _, item := range phases {
		emit("xkitex_service_phase", prometheus.GaugeValue, boolValue(phase == item), item)
	}
	for _, kind := range []string{"startup", "readiness", "liveness"} {
		emit("xkitex_probe_success", prometheus.GaugeValue, boolValue(value.Health[kind] == "Passing"), kind)
	}
	for _, item := range value.Checks {
		emit("xkitex_check_observed", prometheus.GaugeValue, boolValue(item.Status != "Unknown"), item.Kind, item.Name)
		if item.Status != "Unknown" {
			emit("xkitex_check_success", prometheus.GaugeValue, boolValue(item.Status == "Passing"), item.Kind, item.Name)
		}
	}
	if value.Process.SampleTime.IsZero() {
		return
	}
	emit("process_cpu_seconds_total", prometheus.CounterValue, value.Process.CPUSeconds)
	emit("process_resident_memory_bytes", prometheus.GaugeValue, float64(value.Process.ResidentBytes))
	emit("process_virtual_memory_bytes", prometheus.GaugeValue, float64(value.Process.VirtualBytes))
	emit("process_start_time_seconds", prometheus.GaugeValue, float64(value.Process.StartTime.UnixMilli())/1000)
	emit("xkitex_process_sample_time_seconds", prometheus.GaugeValue, float64(value.Process.SampleTime.UnixMilli())/1000)
	emit("xkitex_process_uptime_seconds", prometheus.GaugeValue, value.UptimeSeconds)
	if value.Process.CPUUsageRatio != nil {
		emit("xkitex_process_cpu_usage_ratio", prometheus.GaugeValue, *value.Process.CPUUsageRatio)
	}
}

// boolValue 将布尔状态转换为 Prometheus 的 0/1 约定。
func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
