package txer

import (
	"github.com/lopolopen/gap/internal/entity"
)

type Txer interface {
	Tx() any

	SetFlushHandler(handler func(envelope *entity.Envelope))

	Append(envelope *entity.Envelope)

	Flush()
}
