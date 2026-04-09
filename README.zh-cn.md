# gap

[![Language](https://img.shields.io/badge/language-英文-blue.svg)](https://github.com/lopolopen/gap/blob/main/README.md)

一个轻量、事件驱动的 Go 消息库。它实现了 Outbox 模式，并支持 RabbitMQ、Kafka 和 MySQL（或基于 GORM 的存储），同时设计上可扩展以支持更多的 broker 和数据库。

## 功能

- **Outbox Pattern**：通过数据库事务保证可靠的消息发布
- **Multiple Brokers**：支持 RabbitMQ、Kafka，且可扩展到其他 broker
- **Storage Backends**：与 GORM 集成，支持 MySQL、PostgreSQL 等
- **Code Generation**：使用 `gapc` 自动生成处理器代码
- **Type Safety**：基于泛型的 API 提供类型安全的消息处理
- **Dependency Injection**：内置处理器依赖注入容器

## 安装

```bash
go get github.com/lopolopen/gap
```

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

    "github.com/lopolopen/gap"
    "github.com/lopolopen/gap/storage/xgorm"
    "github.com/lopolopen/gap/broker/xrabbitmq"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"
)

func main() {
    dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
    url := "amqp://guest:guest@localhost:5672/example"

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    db, _ := gorm.Open(mysql.Open(dsn))

    // Create publisher
    pub := gap.NewEventPublisher(
        gap.WithDrain(ctx, 5),
        xrabbitmq.UseRabbitMQ(
            xrabbitmq.URL(url),
        ),
        xgorm.UseGorm(
            xgorm.DB(db),
        ),
    )

    // Set up subscriber
    gap.Subscribe(
        gap.From(pub),
        gap.ServiceName("my-service.worker"),
        gap.Inject(/*handler_dependencies*/),
    )

    // Publish messages in a transaction
    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()

        for range ticker.C {
            slog.Info("Publishing message...")
            db.Transaction(func(tx *gorm.DB) error {
                // Your business logic here...

                // Publish message (outbox pattern ensures reliability)
                return pub.Publish(ctx, event.NewOrderCreated())
            })
        }
    }()

    <-ctx.Done()
    stop()
}

//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

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

该库通过上下文取消和内置的 drain 辅助函数支持优雅关机。

- `gap.WaitDrain(ctx context.Context)`  在退出前为发布者/订阅者留出处理未完成消息的时间

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

- `gap.NewPublisher[T](opts...) Publisher[T]`：创建一个类型化发布者
- `gap.NewEventPublisher(opts...) EventPublisher`：创建事件发布者
- `publisher.Bind(tx Txer) Publisher[T]`：将发布者绑定到事务以实现 Outbox 模式

### 订阅者

- `gap.Subscribe(opts...)`: 设置带处理器的订阅者

### 选项

- `gap.WithDrain(ctx context.Context, timeoutSeconds int)`: 启用优雅关机的 drain 模式
- `gap.ServiceName(name string)`: 设置服务名称
- `gap.Inject(deps...)`: 向处理器注入依赖项

## 代码生成

使用 `gapc` 工具生成处理器模板：

```bash
go run github.com/lopolopen/gap/cmd/gapc -file=main.go
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
