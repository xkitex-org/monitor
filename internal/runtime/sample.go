package runtime

import (
	"context"
	"errors"
	"math"
	"os"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/shirou/gopsutil/v4/process"
)

// processSample 只保留当前进程的资源信息，不采集宿主机、命令行或环境变量。
type processSample struct {
	PID               int       `json:"pid"`
	CPUSeconds        float64   `json:"cpu_seconds"`
	CPUUsageRatio     *float64  `json:"cpu_usage_ratio"`
	ResidentBytes     uint64    `json:"resident_memory_bytes"`
	VirtualBytes      uint64    `json:"virtual_memory_bytes"`
	StartTime         time.Time `json:"start_time"`
	SampleTime        time.Time `json:"sample_time"`
	CollectionSuccess bool      `json:"collection_success"`
}

// goSample 是与进程采样同时更新的 Go 运行时快照；Prometheus 另由官方 GoCollector 导出完整指标。
type goSample struct {
	Version     string    `json:"version"`
	OS          string    `json:"os"`
	Arch        string    `json:"arch"`
	Goroutines  int       `json:"goroutines"`
	LogicalCPUs int       `json:"logical_cpus"`
	MaxProcs    int       `json:"gomaxprocs"`
	HeapAlloc   uint64    `json:"heap_alloc_bytes"`
	HeapInuse   uint64    `json:"heap_inuse_bytes"`
	HeapSys     uint64    `json:"heap_sys_bytes"`
	NumGC       uint32    `json:"gc_cycles"`
	PauseTotal  uint64    `json:"gc_pause_total_ns"`
	SampleTime  time.Time `json:"sample_time"`
}

// sampler 缓存最近一次有效进程采样，并限制忽略取消的采集器最多占用一个工作协程。
type sampler struct {
	mu          sync.RWMutex
	process     processSample
	goRuntime   goSample
	read        func(context.Context) (processSample, error)
	busy        atomic.Bool
	observed    bool
	serviceName string
}

// newSampler 仅保存采集函数，不启动后台任务。
func newSampler(read func(context.Context) (processSample, error)) *sampler {
	return &sampler{read: read, process: processSample{PID: os.Getpid()}}
}

// readProcess 使用系统进程接口读取当前 PID，CPU 时间是用户态和内核态时间之和。
func readProcess(ctx context.Context) (processSample, error) {
	p, err := process.NewProcessWithContext(ctx, int32(os.Getpid()))
	if err != nil {
		return processSample{}, err
	}
	cpu, err := p.TimesWithContext(ctx)
	if err != nil {
		return processSample{}, err
	}
	memory, err := p.MemoryInfoWithContext(ctx)
	if err != nil {
		return processSample{}, err
	}
	started, err := p.CreateTimeWithContext(ctx)
	if err != nil {
		return processSample{}, err
	}
	seconds := cpu.User + cpu.System
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) || started <= 0 {
		return processSample{}, errors.New("monitor: 无效进程采样")
	}
	return processSample{PID: os.Getpid(), CPUSeconds: seconds, ResidentBytes: memory.RSS, VirtualBytes: memory.VMS, StartTime: time.UnixMilli(started), SampleTime: time.Now(), CollectionSuccess: true}, nil
}

// readGo 收集 Go 堆、GC、调度器信息，数据不包含业务对象内容。
func readGo() goSample {
	var mem goruntime.MemStats
	goruntime.ReadMemStats(&mem)
	return goSample{Version: goruntime.Version(), OS: goruntime.GOOS, Arch: goruntime.GOARCH,
		Goroutines: goruntime.NumGoroutine(), LogicalCPUs: goruntime.NumCPU(), MaxProcs: goruntime.GOMAXPROCS(0),
		HeapAlloc: mem.HeapAlloc, HeapInuse: mem.HeapInuse, HeapSys: mem.HeapSys, NumGC: mem.NumGC, PauseTotal: mem.PauseTotalNs, SampleTime: time.Now()}
}

// loop 独立刷新资源缓存，不阻塞健康检查或 HTTP 请求；取消后不等待失控的系统调用。
func (s *sampler) loop(ctx context.Context, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		sampleCtx, cancel := context.WithTimeout(ctx, timeout)
		sample, err := s.collect(sampleCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		s.update(sample, err)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// collect 通过缓冲结果通道隔离采集超时，后台任务不直接修改快照。
func (s *sampler) collect(ctx context.Context) (processSample, error) {
	if !s.busy.CompareAndSwap(false, true) {
		return processSample{}, errors.New("monitor: 上一次采样尚未返回")
	}
	type result struct {
		sample processSample
		err    error
	}
	done := make(chan result, 1)
	go func() {
		value := result{err: errors.New("monitor: 进程采样异常")}
		defer func() { _ = recover(); s.busy.Store(false); done <- value }()
		value.sample, value.err = s.read(ctx)
	}()
	select {
	case value := <-done:
		if ctx.Err() != nil {
			return processSample{}, ctx.Err()
		}
		return value.sample, value.err
	case <-ctx.Done():
		return processSample{}, ctx.Err()
	}
}

// update 保留最近有效值；CPU 用量 1 表示占满一个逻辑核，首次有效采样没有用量值。
func (s *sampler) update(next processSample, err error) {
	goInfo := readGo()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.goRuntime = goInfo
	service := s.serviceName
	if service == "" {
		service = "未确定"
	}
	if err != nil && (!s.observed || s.process.CollectionSuccess) {
		klog.Warnf("monitor: 进程指标采样失败：service=[%s]，保留最近有效样本", service)
	} else if err == nil && s.observed && !s.process.CollectionSuccess {
		klog.Infof("monitor: 进程指标采样已恢复：service=[%s]", service)
	}
	s.observed = true
	if err != nil {
		s.process.CollectionSuccess = false
		return
	}
	next.CollectionSuccess = true
	next.CPUUsageRatio = nil
	previous := s.process
	if !previous.SampleTime.IsZero() && next.SampleTime.After(previous.SampleTime) && next.CPUSeconds >= previous.CPUSeconds {
		ratio := (next.CPUSeconds - previous.CPUSeconds) / next.SampleTime.Sub(previous.SampleTime).Seconds()
		if !math.IsNaN(ratio) && !math.IsInf(ratio, 0) {
			next.CPUUsageRatio = &ratio
		}
	}
	s.process = next
}

// snapshot 返回不可变的采样副本，CPU 指针指向创建后不再修改的值。
func (s *sampler) snapshot() (processSample, goSample) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.process, s.goRuntime
}
