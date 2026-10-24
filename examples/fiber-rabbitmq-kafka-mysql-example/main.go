package main

import (
	"context"
	"database/sql"
	"examples/fiber-rabbitmq-kafka-mysql-example/handlers"
	"examples/fiber-rabbitmq-kafka-mysql-example/service"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/loopopen/gap"
	"github.com/loopopen/gap/broker/xconfluent"
	"github.com/loopopen/gap/broker/xrabbitmq"
	"github.com/loopopen/gap/storage/xmysql"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	app := fiber.New(fiber.Config{})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, svcCtx := initSvc(ctx)
	app.All("/dashboard/*", adaptor.HTTPHandler(g.DashboardHandler()))
	app.Get("/api/greet", handlers.Greet(svcCtx.GreetSvc))

	go func() {
		err := app.Listen(":8084")
		if err != nil {
			slog.Error("server stop listening with error", slog.Any("err", err))
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully, press Ctrl+C again to force")

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		slog.Warn("forced shutdown by user")
		os.Exit(1)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownGroup, ctx := errgroup.WithContext(ctx)

	shutdownGroup.Go(func() error {
		err := g.Shutdown(ctx)
		if err != nil {
			slog.Warn("gap failed to drain", slog.Any("err", err))
		}
		return err
	})

	shutdownGroup.Go(func() error {
		err := app.ShutdownWithContext(ctx)
		if err != nil {
			slog.Warn("server shutdown with error", slog.Any("err", err))
		}
		return err
	})

	if err := shutdownGroup.Wait(); err == nil {
		slog.Info("server shutdown successfully")
	}
}

func initSvc(ctx context.Context) (*gap.Gap, *service.SvcHub) {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
	db := must(sql.Open("mysql", dsn))

	g := gap.New(
		gap.Context(ctx),
		gap.UseDashboard(),
		xmysql.UseMySQL(xmysql.DSN(dsn)),
	)

	pubR := g.NewEventPublisher(
		gap.BrokerName("greeting-rabbitmq"),
		xrabbitmq.UseRabbitMQ(),
	)

	pubK := g.NewEventPublisher(
		gap.BrokerName("greeting-kafka"),
		xconfluent.UseKafka(
			xconfluent.UserName("kafka"),
			xconfluent.Password("kafka"),
		),
	)

	svcHub := service.NewSvcHub(db, pubR, pubK)
	svcHub.Init()

	g.Subscribe(
		pubR,
		gap.Inject(svcHub.GreetSvc),
	)
	g.Subscribe(
		pubK,
		gap.Inject(svcHub.GreetSvc),
	)

	return g, svcHub
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
