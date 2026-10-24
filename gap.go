package gap

import (
	"github.com/loopopen/gap/v2/internal"
	"github.com/loopopen/gap/v2/internal/dashboard"
	"github.com/loopopen/gap/v2/internal/enum"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
	"github.com/loopopen/gap/v2/internal/txer"
)

const version = "v2.0.0-beta.0"

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

type Handler[T any] = internalgap.Handler[T]

type Options = internalgap.Options

type BrokerOptions = internalgap.BrokerOptions

func init() {
	dashboard.AddMeta(enum.PluginKindSelf, 0, version)
}
