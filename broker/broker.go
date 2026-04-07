package broker

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
)

type Writer interface {
	Write(ctx context.Context, envelope *entity.Envelope) error
}

type Reader interface {
	Subscribe(ctx context.Context, topic string) error

	Read(ctx context.Context) (<-chan *entity.Envelope, error)

	Commit(tag any) error

	Reject(tag any) error
}
