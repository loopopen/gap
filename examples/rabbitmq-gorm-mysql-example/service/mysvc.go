package service

import (
	"context"
	"examples/rabbitmq-gorm-mysql-example/event"
	"examples/rabbitmq-gorm-mysql-example/repo"
	"fmt"
	"log/slog"

	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/storage/xgorm"
	"gorm.io/gorm"
)

//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

type MySvc struct {
	db        *gorm.DB
	pub       gap.EventPublisher
	orderRepo repo.OrderRepo
}

func NewMySvc(db *gorm.DB, pub gap.EventPublisher) *MySvc {
	return &MySvc{
		db:        db,
		pub:       pub,
		orderRepo: nil,
	}
}

func (svc *MySvc) CreateOrder() {
	ctx := context.Background()
	xgorm.DoInTx(ctx, func(ctx context.Context, txer gap.Txer) error {
		var err error
		pub := must(svc.pub.Bind(txer))
		// orderRepo := must(s.orderRepo.Bind(txer))

		// err = orderRepo.Create("...")
		// if err != nil {
		// 	return err
		// }

		err = pub.Publish(ctx, event.NewOrderCreated())
		if err != nil {
			return err
		}

		return nil
	}, svc.db)
}

// @subscribe
func (svc *MySvc) HandleOrderCreatedEvent() gap.Handler[*event.OrderCreated] {
	return func(ctx context.Context, msg *event.OrderCreated, headers map[string]string) error {
		slog.Info(fmt.Sprintf("event received: order.sn = %s", msg.SN))
		for k, v := range headers {
			slog.Info("headers", slog.String(k, v))
		}
		return nil
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
