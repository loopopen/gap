package main

import (
	"context"
	"examples/rabbitmq-mysql-example-worker/event"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/loopopen/gap"
	"github.com/loopopen/gap/broker/xrabbitmq"
	"github.com/loopopen/gap/storage/xmysql"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
	url := "amqp://guest:guest@127.0.0.1:5672/example"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g := gap.New(
		gap.ServiceName("rabbitmq-mysql-example.worker"),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
			xmysql.Log(true),
		),
	)

	g.Subscribe(
		xrabbitmq.UseRabbitMQ(
			xrabbitmq.URL(url),
		),
	)

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := g.Shutdown(shutdownCtx); err != nil {
		slog.Warn("gap shutdown failed", slog.Any("err", err))
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

//go:generate go run github.com/loopopen/gap/cmd/gapc -file=$GOFILE

// @subscribe
func handle() gap.Handler[event.Hello] {
	return func(ctx context.Context, msg event.Hello, headers map[string]string) error {
		slog.Info(fmt.Sprintf("received message: %v", msg))
		for k, v := range headers {
			slog.Info("headers", slog.String(k, v))
		}
		slog.Info(fmt.Sprintf("📌 [worker]Hello, %s!", msg.Name))
		return nil
	}
}
