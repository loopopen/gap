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
	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/broker/xkafka"
	"github.com/lopolopen/gap/storage/xmysql"
	"github.com/lopolopen/gap/storage/xsql"
)

func main() {
	// slog.SetLogLoggerLevel(slog.LevelDebug)

	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
	brokers := []string{
		"127.0.0.1:9092",
		"127.0.0.1:9094",
		"127.0.0.1:9095",
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pub := gap.NewPublisher[time.Time](
		gap.WithDrain(ctx, 5),
		xkafka.UseKafka(
			xkafka.Brokers(brokers),
			xkafka.ConfigTopic(
				xkafka.NumPartitions(4),
				xkafka.ReplicationFactor(3),
			),
		),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)

	gap.Subscribe(
		gap.From(pub),
		gap.ServiceName("kafka-mysql-example.worker"),
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

		err := xsql.DoInTx(ctx, func(ctx context.Context, txer gap.Txer) error {
			pub := must(pub.Bind(txer))

			//do biz db change...

			err := pub.Publish(ctx, "topic.time.now", time.Now())
			if err != nil {
				return err
			}

			return nil
		}, db)
		if err != nil {
			slog.Error("job failed", slog.Any("err", err))
		}
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
		slog.Info(fmt.Sprintf("📌 received message: %v", msg))
		for k, v := range headers {
			slog.Info("headers", slog.String(k, v))
		}
		return nil
	}
}
