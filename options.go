package gap

import (
	"context"
	"time"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	Version               = ingap.Version
	ServiceName           = ingap.ServiceName
	DefaultGroup          = ingap.DefaultGroup
	WorkerID              = ingap.WorkerID
	ClaimBatchSize        = ingap.ClaimBatchSize
	MaxRetries            = ingap.MaxRetries
	LookbackSeconds       = ingap.LookbackSeconds
	PumpIntervalSeconds   = ingap.PumpIntervalSeconds
	MaxPublishConcurrency = ingap.MaxPublishConcurrency
	PublishBufferSize     = ingap.PublishBufferSize
	WorkConcurrencyFactor = ingap.WorkConcurrencyFactor
	UseDashboard          = ingap.UseDashboard
	Inject                = ingap.Inject
	BrokerName            = ingap.BrokerName
)

func WithDrain(ctx context.Context, timeoutSeconds int) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.Context = ctx
		drainCtx, cancel := context.WithCancel(context.Background())
		context.AfterFunc(o.Context, func() {
			time.AfterFunc(time.Duration(timeoutSeconds)*time.Second, func() {
				cancel()
			})
		})
		o.DrainContext = drainCtx
	}
}

func HandleTopic[T any](handler ingap.Handler[T], topic string) shoot.Option[Options, *Options] {
	return ingap.HandleTopicWithinGroup(handler, topic, "")
}

func HandleTopicWithinGroup[T any](group, topic string, handler ingap.Handler[T]) shoot.Option[Options, *Options] {
	return ingap.HandleTopicWithinGroup(handler, topic, group)
}
