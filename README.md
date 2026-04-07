# gap

A lightweight, event-driven messaging library for Go. It provides outbox pattern implementation with support for RabbitMQ, Kafka and MySQL (or GORM-based storage), and is designed to support additional brokers and databases in the future.

## Features

- **Outbox Pattern**: Reliable message publishing with database transactions
- **Multiple Brokers**: Support for RabbitMQ, Kafka (and extensible for others)
- **Storage Backends**: GORM integration for MySQL, PostgreSQL, etc.
- **Code Generation**: Automatic handler generation with `gapc`
- **Type Safety**: Generic-based API for type-safe message handling
- **Dependency Injection**: Built-in DI container for handlers

## Installation

```bash
go get github.com/lopolopen/gap
```

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

### Graceful Shutdown

The library supports graceful shutdown through context cancellation and the built-in drain helper.

- `signal.NotifyContext` watches `SIGINT` / `SIGTERM`
- `gap.WithDrain(ctx, seconds)` gives the publisher/subscriber time to finish in-flight messages before exiting

Example:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

pub := gap.NewEventPublisher(
    gap.WithDrain(ctx, 5),
    xrabbitmq.UseRabbitMQ(xrabbitmq.URL(url)),
    xgorm.UseGorm(xgorm.DB(db)),
)

gap.Subscribe(
    gap.From(pub),
    gap.ServiceName("my-service.worker"),
    gap.Inject(/* handler deps */),
)

<-ctx.Done()
```


//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

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

- `gap.NewPublisher[T](opts...) Publisher[T]`: Create a typed publisher
- `gap.NewEventPublisher(opts...) EventPublisher`: Create an event publisher
- `publisher.Bind(tx Txer) Publisher[T]`: Bind publisher to a transaction for outbox pattern

### Subscribers

- `gap.Subscribe(opts...)`: Set up subscribers with handlers

### Options

#### RabbitMQ Options
- `xrabbitmq.URL(url string)`: Set RabbitMQ connection URL

#### GORM Options
- `xgorm.DB(db *gorm.DB)`: Use existing GORM DB instance
- `xgorm.MySQL(conf *MySQLConf)`: Configure MySQL connection
- `xgorm.LogLevel(level logger.LogLevel)`: Set GORM log level

#### General Options
- `gap.WithDrain(ctx context.Context, timeoutSeconds int)`: Enable drain mode for graceful shutdown
- `gap.ServiceName(name string)`: Set service name
- `gap.Inject(deps...)`: Inject dependencies into handlers

## Code Generation

Use the `gapc` tool to generate handler boilerplate:

```bash
go run github.com/lopolopen/gap/cmd/gapc -file=main.go
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