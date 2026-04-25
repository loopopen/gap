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
	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/broker/xrabbitmq"
	"github.com/lopolopen/gap/storage/xmysql"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	app := fiber.New(fiber.Config{})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svcCtx := initSvc(ctx)
	app.All("dashboard/*", adaptor.HTTPHandler(gap.NewDashboardHandler(svcCtx.Pub)))
	app.Get("api/greet", handlers.Greet(svcCtx.GreetSvc))

	go func() {
		err := app.Listen(":8080")
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
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := gap.WaitDrain(ctx)
		if err != nil {
			slog.Warn("gap failed to drain", slog.Any("err", err))
		}
		return err
	})

	g.Go(func() error {
		err := app.ShutdownWithContext(ctx)
		if err != nil {
			slog.Warn("server shutdown with error", slog.Any("err", err))
		}
		return err
	})

	if err := g.Wait(); err == nil {
		slog.Info("server shutdown successfully")
	}
}

func initSvc(ctx context.Context) *service.SvcContext {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"

	pub := gap.NewEventPublisher(
		gap.WithDrain(ctx, 5),
		gap.UseDashboard(),
		xrabbitmq.UseRabbitMQ(),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)

	svcCtx := service.NewSvcContext(pub)
	svcCtx.Init()

	gap.Subscribe(
		gap.From(pub),
		gap.Inject(svcCtx.GreetSvc),
	)

	return svcCtx
}
