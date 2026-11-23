package gap

import (
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
)

const version = "v1.0.0-beta.7"

// Header names written by GAP. These strings are part of the v1 message format.
const (
	KeysMessageID     = internal.KeysMessageID
	KeysMessageType   = internal.KeysMessageType
	KeysGroup         = internal.KeysGroup
	KeysCorrelationID = internal.KeysCorrelationID
	KeysBrokerName    = internal.KeysBrokerName
	KeysBrokerType    = internal.KeysBrokerType
)

// Pair returns a two-value tuple for APIs that accept paired values.
var Pair = internal.Pair

type Txer = txer.Txer

type Publisher[T any] = internal.Publisher[T]

type Event = internal.Event

type EventPublisher = internal.EventPublisher

// BrokerOptionsGetter is implemented by a publisher. [From] copies its broker
// configuration. This is part of the v1 application API.
type BrokerOptionsGetter internal.BrokerOptionsGetter

type Handler[T any] = ingap.Handler[T]

type Options = ingap.Options

type BrokerOptions = ingap.BrokerOptions

func init() {
	dashboard.AddMeta(enum.PluginKindGap, 0, version)
}

// Permanent marks a handler error that retrying the same message cannot repair.
// Gap records that received message as Failed instead of scheduling another attempt.
func Permanent(err error) error {
	return errx.Permanent(err)
}
