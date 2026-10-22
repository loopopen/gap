package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/loopopen/gap"
	"github.com/loopopen/gap/broker/xrabbitmq"
	"github.com/loopopen/gap/storage/xmysql"
	"github.com/loopopen/gap/storage/xsql"
)

func main() {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
	url := "amqp://guest:guest@localhost:5672/example"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pub := gap.NewPublisher[time.Time](
		gap.WithDrain(ctx, 5),
		xrabbitmq.UseRabbitMQ(
			xrabbitmq.URL(url),
		),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)

	gap.Subscribe(
		gap.From(pub),
		gap.ServiceName("rabbitmq-mysql-example.worker"),
	)

	db := must(sql.Open("mysql", dsn))
	go runjob(ctx, db, pub)

	<-ctx.Done()
	stop()
}

func runjob(ctx context.Context, db *sql.DB, pub gap.Publisher[time.Time]) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		slog.Info("running job...")

		func() {
			xsql.DoInTx(ctx, func(ctx context.Context, txer gap.Txer) error {
				pub := must(pub.Bind(txer))

				//do biz db change...

				err := pub.Publish(ctx, "topic.time.now", time.Now())
				if err != nil {
					return err
				}

				return nil
			}, db, nil)
		}()
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

//go:generate go run github.com/loopopen/gap/cmd/gapc -file=$GOFILE

// @subscribe: topic="topic.time.now"
func handle() gap.Handler[time.Time] {
	return func(ctx context.Context, msg time.Time, headers map[string]string) error {
		slog.Info(fmt.Sprintf("received message: %v", msg))
		for k, v := range headers {
			slog.Info("headers", slog.String(k, v))
		}
		return nil
	}
}
