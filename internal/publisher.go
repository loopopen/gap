package internal

import (
	"context"

	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
)

type Publisher[T any] interface {
	Bind(txer txer.Txer) (Publisher[T], error)

	Publish(ctx context.Context, topic string, msg T, args ...any) error

	OptionsGetter
}

type EventPublisher interface {
	Bind(txer txer.Txer) (EventPublisher, error)

	Publish(ctx context.Context, event Event, args ...any) error

	OptionsGetter
}

type OptionsGetter interface {
	Options() *gap.Options
}
