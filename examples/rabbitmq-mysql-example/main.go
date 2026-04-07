package main

import (
	"context"
	"database/sql"
	"examples/rabbitmq-mysql-example/sqltx"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/broker/xrabbitmq"
	"github.com/lopolopen/gap/storage/xmysql"
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

		_ = func() (err error) {
			var tx *sql.Tx
			tx, err = db.Begin()
			if err != nil {
				return nil
			}
			defer func() {
				if err != nil {
					tx.Rollback()
				} else {
					err = tx.Commit()
				}
			}()
			pub := must(pub.Bind(sqltx.New(tx)))

			//do biz db change...

			err = pub.Publish(ctx, "topic.time.now", time.Now())
			if err != nil {
				return err
			}
			return nil
		}()
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

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
