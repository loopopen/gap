package service

import (
	"context"
	"examples/fiber-rabbitmq-mysql-example/event"
	"fmt"
	"log/slog"

	"github.com/lopolopen/gap"
)

type GreetSvc struct {
	pub gap.EventPublisher
}

func NewGreetSvc(pub gap.EventPublisher) *GreetSvc {
	return &GreetSvc{
		pub: pub,
	}
}

func (svc *GreetSvc) Greet(ctx context.Context, name string) error {
	e := event.Hello{
		Name: name,
	}
	err := svc.pub.Publish(ctx, e)
	return err
}

//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

// @subscribe
func (svc *GreetSvc) HandleHello() gap.Handler[event.Hello] {
	return func(ctx context.Context, msg event.Hello, headers map[string]string) error {
		slog.Info(fmt.Sprintf("📌 Hello, %s!", msg.Name))
		return nil
	}
}
