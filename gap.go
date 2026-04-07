package gap

import (
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/pump"
	"github.com/lopolopen/gap/internal/txer"
	"github.com/lopolopen/shoot"
)

const version = "v0.1.0-beta.1"

const (
	KeysMessageID     = internal.KeysMessageID
	KeysMessageType   = internal.KeysMessageType
	KeysGroup         = internal.KeysGroup
	KeysCorrelationID = internal.KeysCorrelationID
)

var Pair = internal.Pair

type Txer = txer.Txer

type Publisher[T any] = internal.Publisher[T]

type Event = internal.Event

type EventPublisher = internal.EventPublisher

type Handler[T any] = gap.Handler[T]

type Options = gap.Options

var WaitDrain = pump.WaitDrain

func From(pub internal.OptsHolder) shoot.Option[Options, *Options] {
	return func(o *Options) {
		opts := pub.Options()
		*o = *opts
	}
}

func init() {
	dashboard.AddMeta(enum.Self, 0, version)
}
