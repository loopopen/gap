package gap

import (
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/pump"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/shoot"
)

const version = "v0.2.0-alpha.1"

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

type OptionsGetter internal.OptionsGetter

type Handler[T any] = gap.Handler[T]

type Options = gap.Options

var WaitDrain = pump.WaitDrain

func From(opts OptionsGetter) shoot.Option[Options, *Options] {
	return func(o *Options) {
		*o = *opts.Options()
	}
}

func init() {
	dashboard.AddMeta(enum.PluginKindSelf, 0, version)
}
