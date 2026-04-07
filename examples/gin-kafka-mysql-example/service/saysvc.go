package service

import (
	"context"
	"errors"
	"examples/gin-kafka-mysql-example/event"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lopolopen/gap"
)

type SaySvc struct {
	pub gap.EventPublisher
}

func NewSaySvc(pub gap.EventPublisher) *SaySvc {
	return &SaySvc{
		pub: pub,
	}
}

func (svc *SaySvc) Say(ctx context.Context, name string) error {
	err := svc.pub.Publish(ctx, event.SomethingSaid{
		Words: fmt.Sprintf("My name is %s.", name),
	})
	return err
}

//go:generate go run github.com/lopolopen/gap/cmd/gapc -file=$GOFILE

// @subscribe
func (svc *SaySvc) HandleSomethingSaid() gap.Handler[event.SomethingSaid] {
	return func(ctx context.Context, msg event.SomethingSaid, headers map[string]string) error {

		if strings.Contains(msg.Words, "error") {
			time.Sleep(3 * time.Second)
			return errors.New("test err")
		}

		slog.Info(fmt.Sprintf("📌 He said: %s", msg.Words))
		return nil
	}
}

//todo:
// @subscribe: concurrency=2
