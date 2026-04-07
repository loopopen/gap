package gap

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/registry"
	"github.com/lopolopen/gap/internal/workerid"
	"github.com/lopolopen/shoot"
)

var fixPubOnce sync.Once

func NewPublisher[T any](opts ...shoot.Option[Options, *Options]) Publisher[T] {
	gapOpts := new(Options).With(opts...)

	brok := registry.MustGetWBroker(gapOpts)
	if brok == nil {
		panic("writer broker must not be nil")
	}
	stor := registry.MustGetStorage(gapOpts)
	if stor != nil {
		fixPubOnce.Do(func() {
			err := stor.UpdateStatusPublished(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed)
			if err != nil {
				panic(err)
			}
		})
	}

	initSnowflake(gapOpts.WorkerID)
	initDashboard(gapOpts)

	pub := internal.NewPub[T](gapOpts, brok, stor)
	return pub
}

func NewEventPublisher(opts ...shoot.Option[Options, *Options]) EventPublisher {
	pub := &internal.EventPub{
		Pub: NewPublisher[Event](opts...).(*internal.Pub[Event]),
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
