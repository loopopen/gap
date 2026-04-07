package internal

import (
	"context"

	"github.com/lopolopen/gap/internal/txer"
	"github.com/lopolopen/gap/options/gap"
)

type Publisher[T any] interface {
	Bind(txer txer.Txer) (Publisher[T], error)

	Publish(ctx context.Context, topic string, msg T, args ...any) error

	OptsHolder
}

type EventPublisher interface {
	Bind(txer txer.Txer) (EventPublisher, error)

	Publish(ctx context.Context, event Event, args ...any) error

	OptsHolder
}

type OptsHolder interface {
	Options() *gap.Options
}
