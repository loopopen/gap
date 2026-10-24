package service

import (
	"context"
	"database/sql"
	"examples/fiber-rabbitmq-kafka-mysql-example/event"
	"fmt"
	"log/slog"

	"github.com/loopopen/gap/v2"
	"github.com/loopopen/gap/v2/storage/xsql"
)

type GreetSvc struct {
	db   *sql.DB
	pubR gap.EventPublisher
	pubK gap.EventPublisher
}

func NewGreetSvc(db *sql.DB, pubR, pubK gap.EventPublisher) *GreetSvc {
	return &GreetSvc{
		db:   db,
		pubR: pubR,
		pubK: pubK,
	}
}

func (svc *GreetSvc) Greet(ctx context.Context, name string) error {
	err := xsql.DoInTx(ctx, func(ctx context.Context, txer gap.Txer) error {
		pubR := must(svc.pubR.Bind(txer))
		pubK := must(svc.pubK.Bind(txer))

		tx := txer.Tx().(*sql.Tx)
		if tx != nil {
			fmt.Println("do biz work...")
		}

		err := pubR.Publish(ctx, event.Hello{
			Name: name,
			By:   "rabbitmq",
		})
		if err != nil {
			return err
		}

		err = pubK.Publish(ctx, event.Hello{
			Name: name,
			By:   "kafka",
		})
		if err != nil {
			return err
		}

		return nil
	}, svc.db, nil)
	return err
}

//go:generate go run github.com/loopopen/gap/v2/cmd/gapc -file=$GOFILE

// @subscribe
func (svc *GreetSvc) HandleHello() gap.Handler[event.Hello] {
	return func(ctx context.Context, msg event.Hello, headers map[string]string) error {
		slog.Info(fmt.Sprintf("📌 Hello, %s (%s)!", msg.Name, msg.By))
		return nil
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
