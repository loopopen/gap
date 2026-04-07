package pump

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
)

type Sender interface {
	SendAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}

type Handler interface {
	HandleAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}
