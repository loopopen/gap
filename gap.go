package gap

import (
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/tx"
	"github.com/lopolopen/gap/options/gap"
)

const version = "v0.0.2-alpha.5"

const (
	KeysMessageID     = internal.KeysMessageID
	KeysMessageType   = internal.KeysMessageType
	KeysGroup         = internal.KeysGroup
	KeysCorrelationID = internal.KeysCorrelationID
)

var Pair = internal.Pair

type Tx = tx.Tx

type Txer = tx.Txer

type Publisher[T any] = internal.Publisher[T]

type Event = internal.Event

type EventPublisher = internal.EventPublisher

type Handler[T any] = gap.Handler[T]

type Options = gap.Options

func init() {
	dashboard.AddMeta(enum.Self, 0, version)
}
