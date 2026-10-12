package broker

import (
	"context"

	"github.com/loopopen/gap/internal/entity"
)

type Producer interface {
	Produce(ctx context.Context, envelope *entity.Envelope) error
}

type Consumer interface {
	Subscribe(ctx context.Context, topic string) error

	Consume(ctx context.Context) (<-chan *entity.Envelope, error)

	Commit(tag any) error

	Reject(tag any) error
}
