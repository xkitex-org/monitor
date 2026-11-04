# monitor：应用监控出口

独立模块 `go.chaoxizhongzhi.com/xkitex/monitor`。原 probe 的全部健康检查迁入 monitor，同时提供当前应用进程、Go 运行时和入站 RPC 指标。构造实例不读取环境变量、不绑定端口、不启动后台任务；业务通过 `x.WithMonitor` 显式接入。

## 业务接入

```go
import (
    "go.chaoxizhongzhi.com/xkitex/core"
    "go.chaoxizhongzhi.com/xkitex/monitor"
)

err := x.Server(
    x.WithMonitor(monitor.New(
        monitor.WithStartup("warmup", checkWarmup),
        monitor.WithReadiness("dependency", checkDependency),
    )),
    x.WithService(productservice.RegisterService, productHandler),
    x.WithService(categoryservice.RegisterService, categoryHandler),
)
```

不需要业务检查时直接使用 `x.WithMonitor(monitor.New())`。core 根包的包名就是 x，无须导入别名。未接入 monitor 时不启动监控端口、不添加 RPC 指标中间件、不等待检查，保持原有直接注册为可用的行为。

检查回调为 `func(context.Context) error`。还提供 `WithLiveness`；名称以字母或数字开头，只允许字母、数字、点、下划线和连字符。同类重名、空回调和零值选项均报错。

## Consul 配置

```yaml
monitor:
  port: 9091
  interval: 1s
  timeout: 1s
  sample_interval: 5s
  sample_timeout: 1s
```

配置根为 monitor，五项均可省略，默认值如上。只在启动时从 core 已合并的配置读取，沿用 imports 和 application.yaml 覆盖规则。interval、timeout 分别控制健康检查间隔和每项检查超时；sample_interval、sample_timeout 控制进程采样。端口为 1..65535，时长必须为正值；未知字段、重复字段、null、错误类型和多文档均启动失败。

HTTP 使用独立端口并绑定所有网卡；监听失败阻止 RPC 启动。没有环境变量开关或热更新。Kubernetes startupProbe 的预算应覆盖前置的 Consul 配置读取时间。

## HTTP 端点

| 端点 | 内容 |
| --- | --- |
| `/live` | liveness 检查；正常排空 RPC 时保持存活 |
| `/ready` | core 处于 Running、startup 完成且全部 readiness 通过 |
| `/startup` | core 进入运行阶段且全部 startup 通过，完成后锁存 |
| `/info` | JSON：服务名、实例阶段、可用性、进程运行时长、三类探针、具名检查、进程与 Go 运行时快照 |
| `/metrics` | Prometheus 抓取端点，包含进程、Go、生命周期、检查和 RPC 指标 |

只允许 GET，其他方法返回 405，不存在的路径返回 404，响应禁止缓存。三个健康端点成功返回 200/ok，未通过返回 503/unavailable。`/info` 和 `/metrics` 用于观察状态，即使 readiness 失败也可正常读取。

`/info` 包含进程 PID、RSS、虚拟内存、累计 CPU 时间、CPU 用量、启动和采样时间；Go 部分包含版本、平台、协程数、逻辑 CPU 数、GOMAXPROCS、堆分配/占用/申请量和 GC 次数/累计暂停时间。不输出环境变量、命令行、工作目录、配置正文、连接地址、回调错误或凭据。

## 资源采样口径

只采集当前应用进程和 Go 运行时，不采集宿主机整体 CPU、内存或其他进程。CPU 累计时间是用户态加内核态的秒数；CPU 用量是相邻有效样本的 CPU 增量除以时间增量，**1 表示占满一个逻辑核，可以大于 1**，不是宿主机 CPU 百分比，也未按容器配额归一化。

首次有效采样尚不能计算用量，`/info` 的 cpu_usage_ratio 为 null，对应指标暂不输出。采样失败保留上次有效值和时间戳，collection_success 置为 false/0；通过采样时间可判断数据新鲜度。首次失败、后续恢复通过 Kitex klog 记录，不输出底层错误。采样故障本身不改变 readiness 或 liveness。

进程采集在独立后台任务运行，HTTP 不触发系统进程采样或业务检查。采集超时不会无限累积工作协程，关闭不等待忽略取消的系统调用。仅缓存当前快照，历史保存交给 Prometheus。`/info` 的 Go 快照随采样更新，`/metrics` 的 go_* 由官方 GoCollector 在抓取时读取。

## Prometheus 指标

| 指标 | 含义 |
| --- | --- |
| `process_cpu_seconds_total` | 进程累计 CPU 秒数 |
| `process_resident_memory_bytes`、`process_virtual_memory_bytes` | RSS 和虚拟内存 |
| `process_start_time_seconds` | 进程启动时间 |
| `xkitex_process_cpu_usage_ratio` | CPU 用量，一个逻辑核为 1 |
| `xkitex_process_collection_success` | 最近一次进程采集是否成功 |
| `xkitex_process_sample_time_seconds`、`xkitex_process_uptime_seconds` | 有效采样时间、进程运行时长 |
| `go_*` | 官方 GoCollector 的协程、堆、GC 等运行时指标 |
| `xkitex_service_available`、`xkitex_service_phase{phase}` | 实例可用性和生命周期阶段 |
| `xkitex_probe_success{probe}` | startup、readiness、liveness 的独立通过状态 |
| `xkitex_check_observed{kind,name}`、`xkitex_check_success{kind,name}` | 具名检查是否执行及结果；执行前不输出 success |
| `xkitex_rpc_requests_total{rpc_service,method,result}` | 完成的入站 RPC，result 为 ok、error 或 panic |
| `xkitex_rpc_duration_seconds{rpc_service,method}` | RPC 耗时直方图，单位秒 |
| `xkitex_rpc_in_flight{rpc_service,method}` | 正在执行的 RPC 数 |

框架指标带 application 标签；GoCollector 和业务指标保持各自标签。IDL Service 与方法分开统计，不带用户、请求参数或追踪 ID。RPC 中间件放在 Sentinel 外层，拒绝请求计入 error；业务返回值、原始错误和 panic 保持原样。RPC 指标在该 Service/方法首次调用后出现。传输层在进入中间件之前拒绝的请求不属于该统计范围。

每个 Controller 拥有独立注册表，不污染或读取全局 DefaultRegisterer/DefaultGatherer，不与 Sentinel 或其他实例的全局指标混合。抓取最多同时处理 5 个请求，响应超时为 1 秒。

业务可显式加入自定义 collector：

```go
requests := prometheus.NewCounter(prometheus.CounterOpts{
    Name: "zmall_orders_total",
    Help: "创建的订单数",
})
x.WithMonitor(monitor.New(monitor.WithCollector(requests)))
// 业务成功后 requests.Inc()，不要再向全局注册表注册。
```

自定义 collector 遵守 Prometheus 的并发安全和及时返回约定；名称冲突和 nil 在监听前失败。框架不限制业务 collector 自己声明的标签。

Prometheus 抓取示例：

```yaml
scrape_configs:
  - job_name: zmall-product
    static_configs:
      - targets: ["应用地址:9091"]
```

## 健康检查与资源释放

检查在后台主动执行，HTTP 只读取结果。startup 通过后不再执行；readiness 失败撤销 Consul 可用性，恢复后重新可用；liveness 只报告结果，不直接退出进程或驱动注册。未声明业务检查时，startup/readiness 随 Running 阶段通过，liveness 默认通过。

接入 database/cache 后，core 在启动前自动加入 `database.NAME`、`cache.NAME` 的 Ping 就绪检查，不加入 liveness。检查失败及恢复继续使用既有 Consul 状态同步。没有 monitor 时不启动这些周期检查。可选 AddReadiness 协议只允许启动前添加，不能覆盖同类同名检查。

core 与 monitor 通过结构化协议衔接，互不导入。基础协议是 ConfigureMonitor、Start、Close；core 可选识别 Middleware、SetServiceName 和 AddReadiness。原生 Controller 实现全部协议。检查失败不退出服务，HTTP 服务自身异常退出才通过致命错误通道通知 core。

停止时先撤销可用性、排空 RPC，再取消检查与采样、关闭 HTTP，之后释放连接池和日志。core 的停机阶段优先于缓存就绪结果，迟到的成功不能恢复实例。Close 幂等且并发安全，不等待忽略取消的业务回调。所有框架日志通过 Kitex klog，分别输出三类探针状态变化，含服务名、前后状态和检查数量。

## 本地迁移和验证

v0.0.1 迁移方式：本地目录与模块路径由 probe 改为 monitor，业务调用由 WithProbe 改为 WithMonitor，Consul 配置根相应改为 monitor，端口和三个健康路径保持不变。旧 probe API 不再保留。保留原有 Git 历史，正式仓库与 GoHome 模块路径均使用 monitor。

```sh
GOWORK=off GOTOOLCHAIN=local go test -race -timeout 90s ./...
# 在相邻的独立模块之间使用临时模块代理验证，不写 go.work 或 replace：
python3 ../database/scripts/test-local.py
```
