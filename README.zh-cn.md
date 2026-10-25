# gap

[![Language](https://img.shields.io/badge/language-英文-blue.svg)](https://github.com/loopopen/gap/blob/main/README.md)

GAP 是一个轻量、事件驱动的 Go 消息库。它采用 Outbox 模式，将消息与业务数据写入同一个数据库事务，并在事务提交后投递消息。目前支持 RabbitMQ 和 Kafka，也可以将消息存储在 MySQL 或 GORM 支持的任意数据库中。未来还会支持更多消息代理和数据库；GAP 的扩展机制让你无需大幅改动现有代码即可接入它们。

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
        gap.Context(ctx),
        xgorm.UseGorm(
            xgorm.DB(db),
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
        pub,
        gap.ServiceName("my-service.worker"),
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

该库通过上下文取消和 Runtime 统一管理的关闭流程支持优雅关机。

- `g.Shutdown(ctx context.Context) error` 停止接收新任务、排空已接收任务，并关闭 Runtime 持有的资源

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

### 发布者

- `gap.New(opts...) *Gap`：创建应用级 Gap 实例
- `gap.NewPublisher[T](g, opts...) Publisher[T]`（Go 1.24+）：创建类型化发布者
- `g.NewPublisher[T](opts...) Publisher[T]`（Go 1.27+）：从 Gap 实例创建类型化发布者
- `g.NewEventPublisher(opts...) EventPublisher`：创建事件发布者
- `publisher.Bind(tx Txer) (Publisher[T], error)`：将发布者绑定到事务以实现 Outbox 模式

### 订阅者

- `g.Subscribe(publisher, opts...)`：设置带处理器的订阅者
- `g.DashboardHandler()`：创建 Dashboard HTTP handler
- `g.Shutdown(ctx)`：停止接收新任务、排空已接收任务，并关闭 Runtime 持有的资源

### 选项

- 传给 `gap.New` 的运行时 `Options`：`gap.Context`、`gap.UseDashboard`，以及 `xmysql.UseMySQL` 等 storage 配置
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
