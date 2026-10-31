package gap

import (
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
)

const version = "v1.0.0-beta.2"

const (
	KeysMessageID     = internal.KeysMessageID
	KeysMessageType   = internal.KeysMessageType
	KeysGroup         = internal.KeysGroup
	KeysCorrelationID = internal.KeysCorrelationID
	KeysBrokerName    = internal.KeysBrokerName
	KeysBrokerType    = internal.KeysBrokerType
)

var Pair = internal.Pair

type Txer = txer.Txer

type Publisher[T any] = internal.Publisher[T]

type Event = internal.Event

type EventPublisher = internal.EventPublisher

type OptionsGetter internal.OptionsGetter

type BrokerOptionsGetter internal.BrokerOptionsGetter

type Handler[T any] = ingap.Handler[T]

type Options = ingap.Options

type BrokerOptions = ingap.BrokerOptions

func init() {
	dashboard.AddMeta(enum.PluginKindGap, 0, version)
}
