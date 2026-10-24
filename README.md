# gap

[![Language](https://img.shields.io/badge/language-中文-blue.svg)](https://github.com/loopopen/gap/blob/main/README.zh-cn.md)

GAP is a lightweight, event-driven messaging library for Go. It uses the outbox pattern, so a message is written in the same database transaction as your data, then delivered once that transaction commits. It speaks RabbitMQ and Kafka today, and it can store messages in MySQL or in any database GORM already supports. More brokers and databases are part of the plan. The library is shaped so those can be added without tearing up what you already have.

## Operational Dashboard

Monitor message delivery at a glance with the built-in operational dashboard. See publishing and consumption health, success rates, in-flight work, failures, backlog age, and active runtime components from one place.

[![GAP operational dashboard](./docs/images/dashboard-overview.jpg)](./docs/images/dashboard-overview.jpg)

## Features

- **Outbox Pattern**: Reliable message publishing with database transactions
- **Multiple Brokers**: Support for RabbitMQ, Kafka (and extensible for others)
- **Storage Backends**: GORM integration for MySQL, PostgreSQL, etc.
- **Code Generation**: Automatic handler generation with `gapc`
- **Type Safety**: Generic-based API for type-safe message handling
- **Dependency Injection**: Built-in DI container for handlers

## Installation

GAP's broker and storage integrations are separate Go modules. Install the core module together with the integrations your application uses. For the RabbitMQ and GORM setup used below:

```bash
go get github.com/loopopen/gap
go get github.com/loopopen/gap/broker/xrabbitmq
go get github.com/loopopen/gap/storage/xgorm
```

Other available integrations include `broker/xkafka`, `broker/xconfluent`, `storage/xmysql`, and `storage/xpostgres`.

The bundled broker and storage modules share internal contracts with the core module. Application code should import only the core module and the integrations it uses.

## Quick Start

Here's a complete example using RabbitMQ and MySQL with GORM:

### 1. Define Events

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

### 2. Set up Publisher and Subscriber

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

    // Create the application-scoped Gap instance
    g := gap.New(
        gap.WithDrain(ctx, 5),
        xgorm.UseGorm(
            xgorm.DB(db),
        ),
    )

    // Create publisher
    pub := g.NewEventPublisher(
        xrabbitmq.UseRabbitMQ(
            xrabbitmq.URL(url),
        ),
    )

    // Set up subscriber
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
    if err := g.WaitDrain(drainCtx); err != nil {
        slog.Warn("gap failed to drain", slog.Any("err", err))
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

### 3. Generate Handlers

Run the code generation:

```bash
go generate ./...
```

This will generate the necessary handler code automatically.

### 4. Graceful Shutdown

The library supports graceful shutdown through context cancellation and the built-in drain helper.

- `g.WaitDrain(ctx context.Context) error` gives the publisher/subscriber time to finish in-flight messages before exiting

## Usage Examples

For a complete working example, see [`./examples/rabbitmq-gorm-mysql-example`](./examples/rabbitmq-gorm-mysql-example).

To run the example:

1. Start MySQL and RabbitMQ services
2. Create a database named `example`
3. Run the example:

```bash
cd examples/rabbitmq-gorm-mysql-example
go run .
```

## API Reference

### Publishers

- `gap.New(opts...) *Gap`: Create an application-scoped Gap instance
- `gap.NewPublisher[T](g, opts...) Publisher[T]`: Create a typed publisher
- `g.NewEventPublisher(opts...) EventPublisher`: Create an event publisher
- `publisher.Bind(tx Txer) (Publisher[T], error)`: Bind a publisher to a transaction for the outbox pattern

### Subscribers

- `g.Subscribe(publisher, opts...)`: Set up subscribers with handlers
- `g.DashboardHandler()`: Create the dashboard HTTP handler
- `g.WaitDrain(ctx)`: Wait for in-flight work during shutdown

### Options

- Runtime `Options` are passed to `gap.New`: `gap.WithDrain`, `gap.UseDashboard`, and storage options such as `xmysql.UseMySQL`
- `BrokerOptions` are passed to publisher constructors: `gap.BrokerName` and broker options such as `xrabbitmq.UseRabbitMQ`
- Subscriber options are passed to `g.Subscribe`: `gap.ServiceName` and `gap.Inject`

## Code Generation

Use the `gapc` tool to generate handler boilerplate:

```bash
go run github.com/loopopen/gap/cmd/gapc -file=main.go
```

Add annotations to your handler functions: (topic;group)

```go
// @subscribe: topic="my.topic"
func MyHandler(/*dependency-list*/) gap.Handler[MyMessage] {
    // handler implementation
}
```

If `MyMessage` implements the `gap.Event` interface, the topic can be omitted:

```go
// @subscribe
func MyHandler(/*dependency-list*/) gap.Handler[MyEvent] {
    // handler implementation
}
```

Or use receiver as a dependency:

```go
// @subscribe: topic="my.topic"
func (svc *MySvc) MyHandler(/*other-dependencies*/) gap.Handler[MyMessage] {
    // handler implementation
}
```

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

See [LICENSE](./LICENSE) file for details.

# Inspiring projects

* [CAP](https://github.com/dotnetcore/CAP)
