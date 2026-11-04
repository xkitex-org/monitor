package runtime

import "github.com/cloudwego/kitex/pkg/klog"

// SetServiceName 接收 core 已校验的实例服务名；可选协议不改变独立模块依赖和既有 Start 接口。
func (c *Controller) SetServiceName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started && !c.closed {
		if name == "" {
			name = "未确定"
		}
		c.serviceName = name
	}
}

// observePhase 在后台任务取消时同步停机约束，不执行新的业务检查。
func (c *Controller) observePhase() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logStatusesLocked(c.state())
}

// logStatusesLocked 分别记录与 HTTP 成功条件一致的状态，初次状态与变化各输出一次。
func (c *Controller) logStatusesLocked(phase string) {
	service := c.serviceName
	if service == "" {
		service = "未确定"
	}
	for i, kind := range []string{"startup", "readiness", "liveness"} {
		count := 0
		for _, item := range c.checks {
			if item.Kind == kind {
				count++
			}
		}
		next := c.status(kind, phase)
		if c.statuses[i] == next {
			continue
		}
		previous := c.statuses[i]
		if previous == "" {
			previous = "Unknown"
		}
		c.statuses[i] = next
		klog.Infof("monitor: 探针状态变化：service=[%s] type=[%s] from=[%s] to=[%s] checks=[%d] phase=[%s]", service, kind, previous, next, count, phase)
	}
}

// status 在持锁期间生成独立状态；startup 保留完成锁存，关闭后其他探针不可用。
func (c *Controller) status(kind, phase string) string {
	if kind == "startup" && c.startup {
		return "Passing"
	}
	if c.closed {
		return "Stopped"
	}
	switch kind {
	case "startup":
		if phase == "Starting" || !c.evaluated {
			return "Waiting"
		}
	case "readiness":
		if phase == "Running" && c.startup && c.ready {
			return "Passing"
		}
		if phase == "Starting" || phase == "Running" && !c.startup {
			return "Waiting"
		}
	case "liveness":
		if !c.evaluated && !c.live && phase != "Failed" && phase != "Stopped" {
			return "Waiting"
		}
		if c.live && phase != "Failed" && phase != "Stopped" {
			return "Passing"
		}
	}
	return "Failing"
}
