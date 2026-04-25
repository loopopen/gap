package main

import (
	"context"
	"examples/gin-kafka-mysql-example/handlers"
	"examples/gin-kafka-mysql-example/service"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/broker/xkafka"
	"github.com/lopolopen/gap/dashboard"
	"github.com/lopolopen/gap/storage/xmysql"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	// gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svcCtx := initSvc(ctx)
	r.Any("/gap/*any", func(c *gin.Context) {
		gap.NewDashboardHandler(svcCtx.Pub).ServeHTTP(c.Writer, c.Request)
	})
	r.GET("/api/say", handlers.Say(svcCtx.SaySvc))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		err := srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("failed to listen", slog.Any("err", err))
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
		err := srv.Shutdown(ctx)
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
	brokers := []string{"127.0.0.1:9092"}

	pub := gap.NewEventPublisher(
		gap.WithDrain(ctx, 5),
		gap.UseDashboard(
			dashboard.PathPrefix("/gap"),
		),
		xkafka.UseKafka(
			xkafka.Brokers(brokers),
			// xkafka.ConfigTopic(
			// 	xkafka.NumPartitions(2),
			// ),
			xkafka.StartOffset(kafka.FirstOffset),
		),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)

	svcCtx := service.NewSvcContext(pub)
	svcCtx.Init()

	gap.Subscribe(
		gap.From(pub),
		gap.Inject(svcCtx.SaySvc),
	)

	return svcCtx
}
