package main

import (
	"context"
	"examples/fiber-rabbitmq-mysql-example/handlers"
	"examples/fiber-rabbitmq-mysql-example/service"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/loopopen/gap/v2"
	"github.com/loopopen/gap/broker/xrabbitmq/v2"
	"github.com/loopopen/gap/storage/xmysql/v2"
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
		err := g.WaitDrain(ctx)
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

func initSvc(ctx context.Context) (*gap.Gap, *service.SvcContext) {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"

	g := gap.New(
		gap.WithDrain(ctx, 5),
		gap.UseDashboard(),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)
	pub := g.NewEventPublisher(xrabbitmq.UseRabbitMQ())

	svcCtx := service.NewSvcContext(pub)
	svcCtx.Init()

	g.Subscribe(
		pub,
		gap.Inject(svcCtx.GreetSvc),
	)

	return g, svcCtx
}
