package gap

import (
	"context"
	"time"

	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

var (
	UseDashboard          = gap.UseDashboard
	Version               = gap.Version
	ServiceName           = gap.ServiceName
	DefaultGroup          = gap.DefaultGroup
	WorkerID              = gap.WorkerID
	ClaimBatchSize        = gap.ClaimBatchSize
	MaxRetries            = gap.MaxRetries
	LookbackSeconds       = gap.LookbackSeconds
	PumpIntervalSeconds   = gap.PumpIntervalSeconds
	MaxPublishConcurrency = gap.MaxPublishConcurrency
	PublishBufferSize     = gap.PublishBufferSize
	WorkConcurrencyFactor = gap.WorkConcurrencyFactor

	Inject = gap.Inject
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

func HandleTopic[T any](handler gap.Handler[T], topic string) shoot.Option[Options, *Options] {
	return gap.HandleTopicWithinGroup(handler, topic, "")
}

func HandleTopicWithinGroup[T any](group, topic string, handler gap.Handler[T]) shoot.Option[Options, *Options] {
	return gap.HandleTopicWithinGroup(handler, topic, group)
}
