package main

import (
	"context"
	"examples/redis-mysql-example-fiber/handlers"
	"examples/redis-mysql-example-fiber/service"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/loopopen/gap"
	"github.com/loopopen/gap/broker/xredisstreams"
	"github.com/loopopen/gap/storage/xmysql"
	"github.com/valyala/fasthttp"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	app := fiber.New(fiber.Config{
		ReadTimeout: 3 * time.Second,
	})
	app.Server().ConnState = func(conn net.Conn, state fasthttp.ConnState) {
		slog.Debug("http connection state changed",
			slog.String("remote", conn.RemoteAddr().String()),
			slog.String("state", state.String()),
		)
	}
	app.Use(func(c *fiber.Ctx) (err error) {
		started := time.Now()
		connID := c.Context().ConnID()
		method := c.Method()
		path := c.Path()
		slog.Debug("http request started",
			slog.Uint64("conn_id", connID),
			slog.String("method", method),
			slog.String("path", path),
		)
		defer func() {
			attrs := []any{
				slog.Uint64("conn_id", connID),
				slog.String("method", method),
				slog.String("path", path),
				slog.Duration("duration", time.Since(started)),
			}
			if err != nil {
				attrs = append(attrs, slog.Any("err", err))
			}
			slog.Debug("http request finished", attrs...)
		}()
		return c.Next()
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, svcHub := initSvc()
	app.Hooks().OnShutdown(func() error {
		drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := g.Shutdown(drainCtx)
		if err != nil {
			slog.Error("gap shutdown with error", slog.Any("err", err))
		}
		return err
	})
	app.All("/dashboard/*", adaptor.HTTPHandler(g.DashboardHandler()))
	app.Get("/api/greet", handlers.Greet(svcHub.GreetSvc))

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	shutdownFinished := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shutdownFinished:
				return
			case <-ticker.C:
				slog.Debug("waiting for fiber shutdown",
					slog.Int64("open_connections", int64(app.Server().GetOpenConnectionsCount())),
					slog.Uint64("current_concurrency", uint64(app.Server().GetCurrentConcurrency())),
				)
			}
		}
	}()
	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		slog.Warn("server shutdown with error", slog.Any("err", err))
	} else {
		slog.Info("server shutdown successfully")
	}
	close(shutdownFinished)
}

func initSvc() (*gap.Gap, *service.SvcHub) {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"

	g := gap.New(
		gap.UseDashboard(),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)
	pub := g.NewEventPublisher(
		xredisstreams.UseRedisStreams(
			xredisstreams.Addrs([]string{"127.0.0.1:6379"}),
		),
	)

	svcHub := service.NewSvcHub(pub)
	svcHub.Init()

	g.Subscribe(
		gap.From(pub),
		gap.Inject(svcHub.GreetSvc),
	)

	return g, svcHub
}
