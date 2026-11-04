package runtime

import (
	"encoding/json"
	"net/http"
	"time"
)

// checkInfo 只输出检查名称和状态，不包含回调返回的错误。
type checkInfo struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// info 是单实例的监控视图，不输出环境变量、配置正文和连接凭据。
type info struct {
	Service       string            `json:"service"`
	Phase         string            `json:"phase"`
	Available     bool              `json:"available"`
	UptimeSeconds float64           `json:"uptime_seconds"`
	Health        map[string]string `json:"health"`
	Checks        []checkInfo       `json:"checks"`
	Process       processSample     `json:"process"`
	GoRuntime     goSample          `json:"go"`
}

// snapshot 读取生命周期和资源缓存，HTTP 与指标导出共用同一套健康状态语义。
func (c *Controller) snapshot() info {
	c.mu.RLock()
	defer c.mu.RUnlock()
	phase := "Unknown"
	if c.state != nil {
		phase = c.state()
	}
	value := info{Service: c.serviceName, Phase: phase, Health: make(map[string]string, 3), Checks: make([]checkInfo, 0, len(c.checks))}
	for _, kind := range []string{"startup", "readiness", "liveness"} {
		value.Health[kind] = c.status(kind, phase)
	}
	value.Available = value.Health["readiness"] == "Passing"
	for _, item := range c.checks {
		status := "Unknown"
		switch item.status.Load() {
		case 1:
			status = "Passing"
		case 2:
			status = "Failing"
		}
		value.Checks = append(value.Checks, checkInfo{Kind: item.Kind, Name: item.Name, Status: status})
	}
	value.Process, value.GoRuntime = c.sampler.snapshot()
	if !value.Process.StartTime.IsZero() {
		value.UptimeSeconds = max(0, time.Since(value.Process.StartTime).Seconds())
	}
	return value
}

// getOnly 给所有监控数据端点添加只读约束，避免 HTTP 访问改变生命周期。
func getOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// infoHandler 只编码缓存，监控访问不会触发系统采样或业务检查。
func (c *Controller) infoHandler(w http.ResponseWriter, r *http.Request) {
	getOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(c.snapshot())
	})).ServeHTTP(w, r)
}
