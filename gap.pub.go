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

func NewPublisher[T any](g *Gap, opts ...shoot.Option[BrokerOptions, *BrokerOptions]) Publisher[T] {
	brokerOpts := g.publisherOptions(opts...)
	producer := plugin.MustGetProducer(g.opts, brokerOpts)
	if producer == nil {
		panic("producer broker must not be nil")
	}
	pub := internal.NewPub[T](g.opts, brokerOpts, producer, g.storage, g.pump)
	return pub
}

func (g *Gap) NewEventPublisher(opts ...shoot.Option[BrokerOptions, *BrokerOptions]) EventPublisher {
	pub := &internal.EventPub{
		Pub: NewPublisher[Event](g, opts...).(*internal.Pub[Event]),
	}
	return pub
}

func initSnowflake(node int64) {
	if node < 0 {
		var err error
		node, err = workerid.GenOnMAC()
		if err != nil {
			slog.Warn("failed to generate worker id on MAC, falling back to random number")
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			node = int64(r.Intn(1 << snowflake.NodeBits))
		}
	}
	entity.MustInitSnowflake(node)
}
