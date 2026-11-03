# probe：生命周期与 HTTP 健康探针

独立模块 `go.chaoxizhongzhi.com/xkitex/probe`，与 core 通过小型结构化接口对接，两个模块互不依赖。构造实例不读取配置、不绑定端口、不启动协程；只有业务显式传入 `x.WithProbe(...)` 才接入。

## 业务调用

```go
import (
    "go.chaoxizhongzhi.com/xkitex/core"
    "go.chaoxizhongzhi.com/xkitex/probe"
)

err := x.Server(
    x.WithProbe(probe.New()),
    x.WithService(productservice.RegisterService, productHandler),
    x.WithService(categoryservice.RegisterService, categoryHandler),
)
```

增加业务检查：

```go
x.WithProbe(probe.New(
    probe.WithStartup("warmup", checkWarmup),
    probe.WithReadiness("database", checkDatabase),
))
```

检查回调类型为 `func(context.Context) error`，nil 表示成功。检查名称以字母或数字开头，只允许字母、数字、点、下划线和连字符；同类检查不可重名，nil 回调或零值选项均报错。还提供 `WithLiveness`，由业务明确选择需要导致 Kubernetes 重启的存活检查，默认不检查数据库或注册中心。

该接入需要包含 WithProbe 的 core v0.0.1，与 probe v0.0.1 一起通过正式模块依赖使用，不引入 go.work 或 replace。

## Consul 配置

core 从已合并的应用配置中单次下发 probe 节：

```yaml
probe:
  port: 22222
  interval: 1s
  timeout: 1s
```

三项可缺省，默认如上。端口为 1..65535，间隔和超时必须为正时长；timeout 对每项回调分别生效。未知字段、重复字段、null、错误类型、多文档或无效值均失败，不回显原始值。启用由 WithProbe 控制，不增加 enabled 或环境变量开关。

配置只在启动时读取，支持 core 原有 imports 合并。HTTP 绑定所有接口，使用独立端口；绑定失败阻止 RPC 启动。Consul 配置读取完成前 HTTP 尚未启动，Kubernetes 的 startupProbe 预算需要覆盖配置加载时间。

## 接口和检查行为

| HTTP 接口 | 成功条件 |
| --- | --- |
| GET /live | 存活检查通过，core 未失败或终止；正常排空期间保持存活 |
| GET /startup | core 进入运行阶段，且全部业务启动检查通过；成功后保持完成 |
| GET /ready | core 正在运行、startup 完成、全部就绪检查通过、没有关闭 |

HTTP 成功返回 200/ok，失败返回 503/unavailable，不输出依赖地址、回调错误或堆栈。仅允许 GET，其他方法返回 405。不存在的路径返回 404。

检查在后台主动执行，HTTP 仅读取缓存，不依赖 Kubernetes 先访问。startup 完成后不再执行启动回调；readiness 失败会撤销可用性，恢复后重新可用。liveness 只报告结果，不直接退出或重启进程，也不作为服务注册驱动。

每项回调带独立超时，panic 按失败处理。回调应遵守上下文取消；对忽略取消的回调，最多保留该项的一个执行协程，不重复并发创建。HTTP 和关闭流程不等待该回调完成。

## core 对接及资源释放

Controller 只有 ConfigureProbe、Start、Close 三个协议方法。core 下发独立配置节，提供当前实例的只读阶段函数，并接收启动完成和就绪报告。阶段为 Starting、Running、Stopping、Stopped、Failed。

core 的终止状态优先于检查结果，迟到的成功不能恢复停机中的实例。HTTP 就绪响应也检查当前阶段。检查失败不会退出服务；HTTP 服务自身异常退出会通过错误通道通知 core，撤销可用性后进入正常 RPC 清理流程。

服务正常停止或运行失败时，先撤销可用性、排空 RPC，再取消检查和关闭 HTTP，最后关闭日志。Close 幂等，并发 Close 等待同一次清理；配置失败和端口绑定失败同样安全清理。

本次不执行 Consul 服务登记、健康上报或注销。core 只保存后续注册模块要消费的可用性状态。

## 本地验证

```sh
GOWORK=off GOTOOLCHAIN=local go test -race -timeout 60s ./...
GOWORK=off GOTOOLCHAIN=local go build ./...
```


## 状态变化日志

使用 Kitex klog 分别记录 startup、readiness、liveness 的首次状态与后续变化。每条包含服务名、探针类型、前后状态、业务检查数量和采样时的 core 阶段；状态未变不重复输出。检查项失败和恢复日志也包含服务名，不输出回调错误。

```text
probe: 探针状态变化：service=[zmall-product] type=[startup] from=[Unknown] to=[Waiting] checks=[0] phase=[Starting]
probe: 探针状态变化：service=[zmall-product] type=[startup] from=[Waiting] to=[Passing] checks=[0] phase=[Running]
probe: 探针状态变化：service=[zmall-product] type=[readiness] from=[Waiting] to=[Passing] checks=[0] phase=[Running]
probe: 探针状态变化：service=[zmall-product] type=[liveness] from=[Unknown] to=[Passing] checks=[0] phase=[Starting]
```

`Unknown` 表示首次记录前没有状态，`Waiting` 表示等待框架、启动前置条件或首次检查，`Passing` 表示当前成功条件满足，`Failing` 表示当前不满足成功条件，`Stopped` 表示探针已关闭。startup 完成后保持 `Passing`，体现启动完成的历史锁存。正常停机撤销 readiness，HTTP 关闭后 readiness、liveness 记录 `Stopped`。

`checks=[0]` 明确表示没有配置该类业务检查：startup、readiness 随实例运行阶段通过；liveness 默认通过。它不代表自动检查过数据库、Redis 或 RPC。服务名由新版 core 的可选身份协议传入；独立使用或旧版 core 未传入时记作 `未确定`，不从环境变量自行推断。
