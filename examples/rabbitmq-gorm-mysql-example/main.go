package main

import (
	"context"
	"examples/rabbitmq-gorm-mysql-example/service"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/broker/xrabbitmq"
	"github.com/lopolopen/gap/storage/xgorm"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	dsn := "root:root@tcp(127.0.0.1:3306)/example?charset=utf8mb4&parseTime=True&loc=Local"
	url := "amqp://guest:guest@localhost:5672/example"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db := must(gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	}))

	pub := gap.NewEventPublisher(
		gap.WithDrain(ctx, 5),
		xrabbitmq.UseRabbitMQ(
			xrabbitmq.URL(url),
		),
		xgorm.UseGorm(
			xgorm.DB(db),
		),
	)

	mySvc := service.NewMySvc(db, pub)

	gap.Subscribe(
		gap.From(pub),
		gap.Inject(mySvc),
	)

	go runjob(mySvc)

	<-ctx.Done()
	stop()
}

func runjob(mySvc *service.MySvc) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		slog.Info("running job...")

		mySvc.CreateOrder()
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
