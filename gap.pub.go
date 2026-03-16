package gap

import (
	"log/slog"
	"math/rand"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/workerid"
	"github.com/lopolopen/shoot"
)

func NewPublisher[T any](opts ...shoot.Option[Options, *Options]) Publisher[T] {
	gapOpts := new(Options).With(opts...)

	brok := internal.MustGetBroker(gapOpts)
	if brok == nil {
		panic("broker must not be nil")
	}
	stor := internal.MustGetStorage(gapOpts)

	//only publisher with storage can have a pump
	if stor != nil {
		pump := internal.NewPump(gapOpts, stor, brok)
		pump.PollingSend()
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

// func storageAndBroker(gapOpts *Options) (storage.Storage, broker.Broker) {
// 	var stor storage.Storage
// 	var brok broker.Broker
// 	if gapOpts.Gorm() != nil {
// 		f := internal.MustGet[storage.Factory]("gorm")
// 		stor, _ = f.CreateStorage(gapOpts)
// 	}
// 	if gapOpts.MySQL() != nil {
// 		f := internal.MustGet[storage.Factory]("mysql")
// 		stor, _ = f.CreateStorage(gapOpts)
// 	}
// 	if gapOpts.RabbitMQ() != nil {
// 		brok = rabbitmq.NewBroker(gapOpts)
// 	}
// 	if gapOpts.Kafka() != nil {
// 		brok = kafka.NewBroker(gapOpts)
// 	}
// 	return stor, brok
// }
