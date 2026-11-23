# gap

[![Language](https://img.shields.io/badge/language-英文-blue.svg)](https://github.com/loopopen/gap/blob/main/README.md) [![CI](https://github.com/loopopen/gap/actions/workflows/ci.yml/badge.svg)](https://github.com/loopopen/gap/actions/workflows/ci.yml)

GAP 是一个轻量、事件驱动的 Go 消息库。它采用 Outbox 模式，将消息与业务数据写入同一个数据库事务，并在事务提交后投递消息。目前支持 RabbitMQ 和 Kafka，也可以将消息存储在 MySQL 或 GORM 支持的任意数据库中。未来还会支持更多消息代理和数据库；GAP 的扩展机制让你无需大幅改动现有代码即可接入它们。

v1.0.0 是稳定版本。变更记录见 [CHANGELOG.md](CHANGELOG.md)。从 v1.0.0-beta.7 升级见 [UPGRADING.zh-cn.md](UPGRADING.zh-cn.md)。

## 运行看板

通过内置的运行看板快速掌握消息投递情况，在一个页面中查看消息发布与消费健康状态、成功率、处理中任务、失败消息、积压时长以及当前运行组件。

[![GAP 运行看板](./docs/images/dashboard-overview.jpg)](./docs/images/dashboard-overview.jpg)

## 功能

- **Outbox Pattern**：通过数据库事务保证可靠的消息发布
- **Multiple Brokers**：支持 RabbitMQ、Kafka，且可扩展到其他 broker
- **Storage Backends**：与 GORM 集成，支持 MySQL、PostgreSQL 等
- **Code Generation**：使用 `gapc` 自动生成处理器代码
- **Type Safety**：基于泛型的 API 提供类型安全的消息处理
- **Dependency Injection**：内置处理器依赖注入容器

## 安装

GAP 的消息代理和存储适配器是独立的 Go 模块。请在核心模块之外，安装应用实际使用的适配器。下方快速开始使用 RabbitMQ 和 GORM，因此需要执行：

```bash
go get github.com/loopopen/gap
go get github.com/loopopen/gap/broker/xrabbitmq
go get github.com/loopopen/gap/storage/xgorm
```

其他可用适配器包括 `broker/xkafka`、`broker/xconfluent`、`storage/xmysql` 和 `storage/xpostgres`。

内置 broker 和 storage 模块通过核心模块的内部契约协作。应用代码只需导入核心模块及实际使用的适配器。

## 快速开始

下面是一个使用 RabbitMQ 和 MySQL（GORM）的完整示例：

### 1. 定义事件

```go
package event

import "github.com/google/uuid"

type OrderCreated struct {
    SN string
}

func NewOrderCreated() *OrderCreated {
    return &OrderCreated{
        SN: uuid.New().String(),
    }
}

func (e OrderCreated) Topic() string {
    return "order.created"
}
```

### 2. 设置发布者和订阅者

```go
package main

import (
    "context"
    "fmt"
    "log/slog"
    "os/signal"
    "syscall"
    "time"

    "github.com/loopopen/gap"
    "github.com/loopopen/gap/broker/xrabbitmq"
    "github.com/loopopen/gap/storage/xgorm"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

func main() {
    dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
    url := "amqp://guest:guest@localhost:5672/example"

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    db, _ := gorm.Open(mysql.Open(dsn))

    // 创建应用级 Gap 实例
    g := gap.New(
        gap.ServiceName("my-service.worker"),
        xgorm.UseGorm(
            xgorm.DSN(dsn, mysql.Open),
        ),
    )

    // 创建 publisher
    pub := g.NewEventPublisher(
        xrabbitmq.UseRabbitMQ(
            xrabbitmq.URL(url),
        ),
    )

    // 设置 subscriber
    g.Subscribe(
        gap.From(pub),
        gap.Inject(/*handler_dependencies*/),
    )

    // Publish messages in a transaction
    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()

        for range ticker.C {
            slog.Info("Publishing message...")
            err := xgorm.DoInTx(ctx, func(ctx context.Context, txer gap.Txer) error {
                // Your business logic here...

                // Bind the publisher so the message and business data are
                // committed in the same transaction.
                txPub, err := pub.Bind(txer)
                if err != nil {
                    return err
                }
                return txPub.Publish(ctx, event.NewOrderCreated())
            }, db)
            if err != nil {
                slog.Error("failed to create order", slog.Any("err", err))
            }
        }
    }()

    <-ctx.Done()

    drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := g.Shutdown(drainCtx); err != nil {
        slog.Warn("gap shutdown failed", slog.Any("err", err))
    }
}

//go:generate go run github.com/loopopen/gap/cmd/gapc -file=$GOFILE

// @subscribe
func handle(/*dependency-list*/) gap.Handler[*event.OrderCreated] {
    return func(ctx context.Context, msg *event.OrderCreated, headers map[string]string) error {
        slog.Info(fmt.Sprintf("received event: %s, %s", msg.Topic(), msg.SN))
        return nil
    }
}
```

### 3. 生成处理器

运行代码生成：

```bash
go generate ./...
```

这将自动生成所需的处理器代码。

### 4. 优雅关机

该库通过显式调用 Runtime 统一管理的关闭流程支持优雅关机。

- `g.Shutdown(ctx context.Context) error` 停止接收新任务、排空已接收任务，并关闭 Runtime 持有的资源
- 取消应用的信号上下文不会关闭 Gap；应使用独立的排空上下文显式调用 `g.Shutdown`

## 投递语义

GAP 采用 **at-least-once（至少一次）** 的消息处理语义，不保证 Handler 只执行一次。同一条逻辑消息可能多次到达 Handler，例如 Broker 重新投递、租约过期，或者业务操作已经成功但消息状态尚未持久化时进程退出。

因此，Handler 必须保证幂等。建议使用订单 ID、事件 ID 等稳定的业务键，并在 Handler 的业务事务中同时完成幂等校验，例如使用唯一约束或记录已处理的消息。不要根据消息内容是否相同来去重，因为业务可能有意再次发布相同内容。

## 消息保留与清理

GAP 会分批自动删除终态消息记录，`Pending` 消息永远不会被自动删除。默认情况下，成功消息保留 24 小时，失败消息保留 15 天；清理器每 5 分钟运行一次，每轮最多运行 30 秒。存储支持时，多实例通过行锁协调清理。

可以使用 `gap.SucceededRetentionSeconds`、`gap.FailedRetentionSeconds`、`gap.CleanupIntervalSeconds`、`gap.CleanupBatchSize` 和 `gap.CleanupTimeoutSeconds` 调整保留及清理策略。如果清理由外部任务负责，可传入 `gap.CleanupEnabled(false)`。

## 路由

一个 Runtime 只领取自己注册过的路由。

`published` 行只会被 `broker_name` 与本进程某个 publisher 相同的实例领取。`received` 行只会被 `group` 和 `broker_name` 都与本进程 `Subscribe` 注册的 handler 相同的实例领取。broker name 使用 `gap.BrokerName` 传入的值；没有传入时，使用 broker 插件名，例如 `RabbitMQ` 或 `Kafka`。没有填写 group 的 handler，包括 `HandleTopic`，使用 `gap.DefaultGroup`；除非改过，否则是 `default`。`HandleTopicWithinGroup` 以及已经写了 group 的生成代码保留那个 group。同一个 broker name 不能同时注册两套不同的连接配置。

GAP 没有全局路由目录。没有任何正在运行的实例注册该 broker name，或该 group 与 broker name 时，行保持 `Pending`，不会因为路由不存在而写成 `Failed`。行被领取之后，如果进程里已经没有对应 broker name 的 sender，或没有对应 broker name 与 group 的 handler，Runtime 把它释放回 `Pending`。group 已经注册，但其中没有这个 topic 的 handler，这是不可重试错误，行会变成 `Failed`。

领取还要求行上的 `version` 等于 Runtime 的消息版本。消息版本默认是 `v1`，用 `gap.Version` 修改。它用来隔离已持久化的消息，不是模块版本。

## 租约、重试和状态

插入的行一开始是 `Pending`。租约在这条 `INSERT` 里按数据库时钟计算：`lease_until` 是数据库当前时间加上 `LeaseTTL`，默认 180 秒。计时因此从调用方的事务内部开始。事务打开的时间超过 `LeaseTTL` 时，提交的那一刻租约可能已经过期。快路径仍可能投递它正持有的消息，轮询也可能领取同一行。这会多投递一次，语义仍然是 at-least-once。之后的状态更新必须带上租约 token。token 已经不是该行的当前持有者时返回 `ErrLeaseLost`，不能覆盖新持有者写入的状态。v1 保持从 INSERT 起算租约。

消息在内存中等待发送或处理，或者正在发送或处理时，Runtime 在大约半个 `LeaseTTL` 后续租。新的截止时间使用数据库时钟。存储拒绝续租时，这次尝试被跳过，不写状态。

领取选择到期的 `Pending` 行：租约为空或已过期，`retries` 仍小于 `MaxRetries`（默认 30），并且路由属于本进程。领取时 `retries` 加一，并装上新租约。轮询间隔是 `PumpInterval`，默认 1 秒。

- Broker 接受了消息，或 Handler 返回 nil：行变成 `Succeeded`，租约清除。
- 发送或 Handler 返回普通错误：行保持 `Pending`，租约清除，`next_attempt_at` 变成数据库时间加上 `RetryInterval`（默认 10 秒）。
- 错误用 `gap.Permanent` 包装、Handler panic、payload 不是合法 JSON，或已注册的订阅者没有这个 topic 的 handler：行变成 `Failed`。
- 记录结果时 `retries` 已经达到 `MaxRetries`，或之后的领取发现到期且没有有效租约的行已经达到 `MaxRetries`：行变成 `Failed`。
- sender 或 group handler 已经不再注册：行回到 `Pending`。

`Succeeded` 和 `Failed` 是终态。清理不会删除 `Pending`。

## Schema

`storage/xmysql`、`storage/xpostgres` 和 `storage/xgorm` 打开时执行的 `CREATE` 语句就是 v1 初始 schema。DDL 测试锁住其中的列和领取索引。打开 storage 会补齐缺少的表和索引，不会修改已经存在的表。

v1.0.0 之前的版本不提供 migration。由这些语句创建的数据库就是 v1 基线。

v1.0.0 之后，这些表的结构变更由拥有该表的 storage 模块随发布提供 migration。应用在升级该模块时执行这份 migration。根模块和 broker 模块不迁移这些表。`CREATE ... IF NOT EXISTS` 也不会修改已有的表。

## 版本

一次 GAP 发布用同一个版本给根模块和所有附带的 broker、storage 模块打 tag。这些模块会 require 同一个根模块版本。应用里的 `github.com/loopopen/gap`，以及它 import 的每个 `github.com/loopopen/gap/broker/...` 或 `github.com/loopopen/gap/storage/...`，使用同一个版本。根模块配上另一次发布的 broker 或 storage 模块不受支持，因为这些模块共用内部契约。

在整个 v1 期间，应用 API、上面的状态规则和这份 schema 保持兼容。schema 变更带有 storage 模块的 migration。不兼容的应用 API 变更属于新的主版本。

## 使用示例

完整可运行示例请参见 [`./examples/rabbitmq-gorm-mysql-example`](./examples/rabbitmq-gorm-mysql-example)。

运行示例：

1. 启动 MySQL 和 RabbitMQ 服务
2. 创建名为 `example` 的数据库
3. 运行示例：

```bash
cd examples/rabbitmq-gorm-mysql-example
go run .
```

## API 参考

这里列出的类型、函数和 option 函数是 v1 应用 API。`gap.Permanent` 表示这个错误不能重试。`gap.KeysMessageID`、`gap.KeysMessageType`、`gap.KeysGroup`、`gap.KeysCorrelationID`、`gap.KeysBrokerName` 和 `gap.KeysBrokerType` 是 GAP 保存并投递的 header 名。

应用自己的事务以 `storage.TxerBase` 为基座。`storage.Storage` 的方法和 `broker` 的驱动接口由附带的模块实现，在同一次 GAP 发布内保持兼容。`gapc` 生成的代码与同一次发布里的 `gapc` 兼容。升级之后需要重新生成。

### 发布者

- `gap.SetWorkerID(id) error`：设置本进程用于生成消息 ID 的 Snowflake 节点。最后一次成功调用为准。未调用时，第一次 `gap.New` 用 MAC 地址推导节点；没有可用 MAC 时随机生成，并且只生成一次
- `gap.New(opts...) *Gap`：创建应用级 Gap 实例
- `gap.NewPublisher[T](g, opts...) Publisher[T]`（Go 1.24+）：创建类型化发布者
- `g.NewPublisher[T](opts...) Publisher[T]`（Go 1.27+）：从 Gap 实例创建类型化发布者
- `g.NewEventPublisher(opts...) EventPublisher`：创建事件发布者
- `publisher.Bind(tx Txer) (Publisher[T], error)`：将发布者绑定到事务以实现 Outbox 模式

### 订阅者

- `g.Subscribe(opts...)`：使用 broker、handler 与依赖选项设置订阅者
- `g.DashboardHandler()`：创建 Dashboard HTTP handler
- `g.Shutdown(ctx)`：停止接收新任务、排空已接收任务，并关闭 Runtime 持有的资源

### 选项

- 传给 `gap.New` 的运行时 `Options`：Dashboard、消息保留、清理，以及 storage 配置，例如 `gap.UseDashboard`、`gap.CleanupBatchSize` 和 `xmysql.UseMySQL`
- 传给 publisher 构造函数的 `BrokerOptions`：`gap.BrokerName`，以及 `xrabbitmq.UseRabbitMQ` 等 broker 配置
- 传给 `g.Subscribe` 的订阅配置：`gap.ServiceName` 和 `gap.Inject`

## 代码生成

使用 `gapc` 工具生成处理器模板：

```bash
go run github.com/loopopen/gap/cmd/gapc -file=main.go
```

向处理器函数添加注解：（topic;group）

```go
// @subscribe: topic="my.topic"
func MyHandler(/*dependency-list*/) gap.Handler[MyMessage] {
    // handler implementation
}
```

如果 `MyMessage` 实现了 `gap.Event` 接口，则 topic 可以省略：

```go
// @subscribe
func MyHandler(/*dependency-list*/) gap.Handler[MyEvent] {
    // handler implementation
}
```

也可以使用接收者作为依赖项：

```go
// @subscribe: topic="my.topic"
func (svc *MySvc) MyHandler(/*other-dependencies*/) gap.Handler[MyMessage] {
    // handler implementation
}
```

## 贡献

欢迎贡献！请随时提交 issue 和 pull request。

## 许可协议

详情请参阅 [LICENSE](./LICENSE) 文件。

# 启发项目

* [CAP](https://github.com/dotnetcore/CAP)
