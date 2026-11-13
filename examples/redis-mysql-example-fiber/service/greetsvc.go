package service

import (
	"context"
	"examples/redis-mysql-example-fiber/event"
	"fmt"
	"log/slog"
	"time"

	"github.com/loopopen/gap"
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}
	err := svc.pub.Publish(ctx, e)
	return err
}

//go:generate go run github.com/loopopen/gap/cmd/gapc -file=$GOFILE

// @subscribe
func (svc *GreetSvc) HandleHello() gap.Handler[event.Hello] {
	return func(ctx context.Context, msg event.Hello, headers map[string]string) error {
		slog.Info(fmt.Sprintf("📌 Hello, %s!", msg.Name))
		return nil
	}
}
