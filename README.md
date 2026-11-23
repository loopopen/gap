# gap

[![Language](https://img.shields.io/badge/language-中文-blue.svg)](https://github.com/loopopen/gap/blob/main/README.zh-cn.md) [![CI](https://github.com/loopopen/gap/actions/workflows/ci.yml/badge.svg)](https://github.com/loopopen/gap/actions/workflows/ci.yml)

GAP is a lightweight, event-driven messaging library for Go. It uses the outbox pattern, so a message is written in the same database transaction as your data, then delivered once that transaction commits. It speaks RabbitMQ and Kafka today, and it can store messages in MySQL or in any database GORM already supports. More brokers and databases are part of the plan. The library is shaped so those can be added without tearing up what you already have.

v1.0.0 is the stable release. Changes are listed in [CHANGELOG.md](CHANGELOG.md). Upgrading from v1.0.0-beta.7 is described in [UPGRADING.md](UPGRADING.md).

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
        gap.ServiceName("my-service.worker"),
        xgorm.UseGorm(
            xgorm.DSN(dsn, mysql.Open),
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

### 3. Generate Handlers

Run the code generation:

```bash
go generate ./...
```

This will generate the necessary handler code automatically.

### 4. Graceful Shutdown

The library supports graceful shutdown through an explicit, runtime-owned shutdown sequence.

- `g.Shutdown(ctx context.Context) error` stops intake, drains accepted work, and closes runtime-owned resources
- Canceling the application's signal context does not shut down Gap; call `g.Shutdown` explicitly with a dedicated drain context

## Delivery Semantics

GAP uses **at-least-once** message processing and does not guarantee exactly-once execution. The same logical message may reach a handler more than once, for example after broker redelivery, lease expiration, or a process failure after a business side effect succeeds but before the message status is persisted.

Handlers must therefore be idempotent. Use a stable business key such as an order ID or event ID, and enforce idempotency in the same transaction as the handler's business changes—for example with a unique constraint or a processed-message record. Do not rely on message payload equality for deduplication, because publishing the same payload again may be intentional.

## Message Retention

GAP automatically removes terminal message records in bounded batches. Pending
messages are never deleted. Successful messages are retained for 24 hours by
default, failed messages for 15 days, and cleanup runs every 5 minutes with a
30-second time budget. Multiple instances coordinate cleanup with row locking
where supported by the storage provider.

Use `gap.SucceededRetentionSeconds`, `gap.FailedRetentionSeconds`,
`gap.CleanupIntervalSeconds`, `gap.CleanupBatchSize`, and
`gap.CleanupTimeoutSeconds` to tune retention and cleanup. Pass
`gap.CleanupEnabled(false)` when cleanup is managed externally.

## Routing

A runtime claims only routes it has registered.

A published row is claimed only when its `broker_name` matches a publisher in that process. A received row is claimed only when its `group` and `broker_name` match a handler registered through `Subscribe` in that process. The broker name is the value passed to `gap.BrokerName`. When that option is omitted, the name is the broker plugin name, such as `RabbitMQ` or `Kafka`. A handler that does not set a group, including `HandleTopic`, uses `gap.DefaultGroup`, which is `default` unless you change it. `HandleTopicWithinGroup`, and generated code that already names a group, keep that group. One broker name cannot be registered with two different connection settings.

GAP has no global route directory. A row whose broker name, or whose group and broker name, is not registered by a running instance stays `Pending`. A missing route is not written as `Failed`. After a row is claimed, a process that no longer has the sender for that broker name, or the handler for that broker name and group, releases the row back to `Pending`. A subscriber that is registered for the group but has no handler for the topic is a permanent error, and that row becomes `Failed`.

A claim also requires the row's `version` to equal the runtime's message version. That version defaults to `v1` and is changed with `gap.Version`. It isolates persisted messages. It is not the module version.

## Lease, retry, and status

An inserted row starts as `Pending`. The lease is calculated in that `INSERT` from the database clock: `lease_until` is the database's current timestamp plus `LeaseTTL`, which is 180 seconds by default. The clock therefore starts inside the caller's transaction. If the transaction stays open longer than `LeaseTTL`, the lease can already be expired when it commits. The fast path can still deliver the message it holds, and a poller can claim the same row. That is one extra delivery, and it remains at-least-once. A later status update must present the lease token. A token that no longer owns the row returns `ErrLeaseLost` and does not overwrite the newer owner. v1 keeps this insert-time lease.

While a message is waiting in memory or being sent or handled, the runtime renews its lease around half of `LeaseTTL`. The database clock supplies the new deadline. If storage rejects the renewal, that attempt is skipped and writes no status.

A claim takes due `Pending` rows whose lease is empty or expired, whose `retries` are still below `MaxRetries` (30 by default), and whose route this process owns. The claim adds one to `retries` and installs a new lease. The poll runs every `PumpInterval`, which is 1 second by default.

- The broker accepts the message, or the handler returns nil: the row becomes `Succeeded` and the lease is cleared.
- The send or the handler returns an ordinary error: the row stays `Pending`, the lease is cleared, and `next_attempt_at` becomes the database time plus `RetryInterval` (10 seconds by default).
- The error is wrapped with `gap.Permanent`, the handler panics, the payload is not valid JSON, or the registered subscriber has no handler for the topic: the row becomes `Failed`.
- `retries` has reached `MaxRetries` when that result is recorded, or a later claim finds a due, unleased row that has reached `MaxRetries`: the row becomes `Failed`.
- The sender or the group handler is no longer registered: the row returns to `Pending`.

`Succeeded` and `Failed` are terminal. Cleanup never deletes `Pending`.

## Schema

The `CREATE` statements run by `storage/xmysql`, `storage/xpostgres`, and `storage/xgorm` when they open are the v1 initial schema. Their DDL tests lock the columns and claim indexes. Opening storage creates missing tables and indexes. It does not change a table that already exists.

Releases before v1.0.0 do not include a migration. A database created by those statements is the v1 baseline.

After v1.0.0, a change to these tables is shipped by the storage module that owns them. The application runs that migration as part of upgrading the module. The core module and the broker modules do not migrate these tables, and `CREATE ... IF NOT EXISTS` does not alter an existing table.

## Versions

One GAP release tags the core module and every bundled broker and storage module with the same version. Each of those modules requires that core version. Require the same version of `github.com/loopopen/gap` and of every `github.com/loopopen/gap/broker/...` or `github.com/loopopen/gap/storage/...` module the application imports. Combining a core module with a broker or storage module from another release is not supported, because those modules share internal contracts.

Throughout v1, the application API, the status rules above, and this schema stay compatible. A schema change comes with the storage module's migration. An incompatible change to the application API is a new major version.

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

The types, functions, and option functions listed here are the v1 application API. `gap.Permanent` marks an error that must not be retried. `gap.KeysMessageID`, `gap.KeysMessageType`, `gap.KeysGroup`, `gap.KeysCorrelationID`, `gap.KeysBrokerName`, and `gap.KeysBrokerType` are the header names GAP stores and delivers.

`storage.TxerBase` is the supported base for an application transaction. The `storage.Storage` methods and the `broker` driver interfaces are the contract implemented by the bundled modules; they stay compatible within one GAP release. Code generated by `gapc` is compatible with the `gapc` from that same release. Regenerate it after upgrading.

### Publishers

- `gap.SetWorkerID(id) error`: Set the process-wide Snowflake node used for message IDs. The latest successful call wins. When unset, the first `gap.New` derives the node once from a MAC address, or at random when no MAC address is available
- `gap.New(opts...) *Gap`: Create an application-scoped Gap instance
- `gap.NewPublisher[T](g, opts...) Publisher[T]` (Go 1.24+): Create a typed publisher
- `g.NewPublisher[T](opts...) Publisher[T]` (Go 1.27+): Create a typed publisher from a Gap instance
- `g.NewEventPublisher(opts...) EventPublisher`: Create an event publisher
- `publisher.Bind(tx Txer) (Publisher[T], error)`: Bind a publisher to a transaction for the outbox pattern

### Subscribers

- `g.Subscribe(opts...)`: Set up subscribers with broker, handler, and dependency options
- `g.DashboardHandler()`: Create the dashboard HTTP handler
- `g.Shutdown(ctx)`: Stop intake, drain accepted work, and close runtime-owned resources

### Options

- Runtime `Options` are passed to `gap.New`: dashboard, retention, cleanup, and storage options such as `gap.UseDashboard`, `gap.CleanupBatchSize`, and `xmysql.UseMySQL`
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
