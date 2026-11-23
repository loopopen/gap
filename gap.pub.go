package gap

import (
	"log/slog"
	"math/rand"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/internal/workerid"
	"github.com/loopopen/shoot"
)

// NewPublisher creates a typed publisher owned by g using the supplied broker options.
func NewPublisher[T any](g *Gap, opts ...shoot.Option[BrokerOptions, *BrokerOptions]) Publisher[T] {
	done, err := g.opts.BeginOperation()
	if err != nil {
		panic(err)
	}
	defer done()

	brokerOpts := new(BrokerOptions).With(opts...)
	producer := plugin.MustGetProducer(g.opts, brokerOpts)
	if producer == nil {
		panic("producer broker must not be nil")
	}
	pub := internal.NewPub[T](g.opts, brokerOpts, producer, g.storage, g.pump)
	return pub
}

// NewEventPublisher creates an event publisher owned by g using the supplied broker options.
func NewEventPublisher(g *Gap, opts ...shoot.Option[BrokerOptions, *BrokerOptions]) EventPublisher {
	pub := &internal.EventPub{
		Pub: NewPublisher[Event](g, opts...).(*internal.Pub[Event]),
	}
	return pub
}

// NewEventPublisher creates an event publisher owned by g using the supplied broker options.
func (g *Gap) NewEventPublisher(opts ...shoot.Option[BrokerOptions, *BrokerOptions]) EventPublisher {
	return NewEventPublisher(g, opts...)
}

// SetWorkerID sets the process-wide Snowflake node used to generate message IDs.
// The latest successful call wins, including a call made after New. When it is
// never called, the first New derives the node once from a MAC address, or at
// random when no MAC address is available. An invalid id returns an error and
// leaves the current node unchanged.
func SetWorkerID(id int64) error {
	return entity.InitSnowflake(id)
}

func ensureSnowflake() error {
	return entity.EnsureSnowflake(func() (int64, error) {
		node, err := workerid.GenOnMAC()
		if err != nil {
			slog.Warn("failed to generate worker id on MAC, falling back to random number")
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			return int64(r.Intn(1 << snowflake.NodeBits)), nil
		}
		return node, nil
	})
}
