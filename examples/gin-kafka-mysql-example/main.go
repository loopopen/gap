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
	"github.com/loopopen/gap/v2"
	"github.com/loopopen/gap/broker/xkafka/v2"
	"github.com/loopopen/gap/v2/dashboard"
	"github.com/loopopen/gap/storage/xmysql/v2"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	// gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, svcCtx := initSvc(ctx)
	dashboardHandler := g.DashboardHandler()
	r.Any("/gap/*any", func(c *gin.Context) {
		dashboardHandler.ServeHTTP(c.Writer, c.Request)
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
	shutdownGroup, ctx := errgroup.WithContext(ctx)

	shutdownGroup.Go(func() error {
		err := g.WaitDrain(ctx)
		if err != nil {
			slog.Warn("gap failed to drain", slog.Any("err", err))
		}
		return err
	})

	shutdownGroup.Go(func() error {
		err := srv.Shutdown(ctx)
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
	brokers := []string{"127.0.0.1:9092"}

	g := gap.New(
		gap.WithDrain(ctx, 5),
		gap.UseDashboard(
			dashboard.PathPrefix("/gap"),
		),
		xmysql.UseMySQL(
			xmysql.DSN(dsn),
		),
	)
	pub := g.NewEventPublisher(
		xkafka.UseKafka(
			xkafka.UserName("kafka"),
			xkafka.Password("kafka"),
			xkafka.Brokers(brokers),
			// xkafka.ConfigTopic(
			// 	xkafka.NumPartitions(2),
			// ),
			xkafka.StartOffset(kafka.FirstOffset),
		),
	)

	svcCtx := service.NewSvcContext(pub)
	svcCtx.Init()

	g.Subscribe(
		pub,
		gap.Inject(svcCtx.SaySvc),
	)

	return g, svcCtx
}
