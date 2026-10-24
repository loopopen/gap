package gap

import (
	"context"
	"time"

	internalgap "github.com/loopopen/gap/v2/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	Version               = internalgap.Version
	ServiceName           = internalgap.ServiceName
	DefaultGroup          = internalgap.DefaultGroup
	WorkerID              = internalgap.WorkerID
	ClaimBatchSize        = internalgap.ClaimBatchSize
	MaxRetries            = internalgap.MaxRetries
	LookbackSeconds       = internalgap.LookbackSeconds
	PumpIntervalSeconds   = internalgap.PumpIntervalSeconds
	MaxPublishConcurrency = internalgap.MaxPublishConcurrency
	PublishBufferSize     = internalgap.PublishBufferSize
	WorkConcurrencyFactor = internalgap.WorkConcurrencyFactor
	UseDashboard          = internalgap.UseDashboard
	Inject                = internalgap.Inject
	BrokerName            = internalgap.BrokerName
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

func HandleTopic[T any](handler internalgap.Handler[T], topic string) shoot.Option[Options, *Options] {
	return internalgap.HandleTopicWithinGroup(handler, topic, "")
}

func HandleTopicWithinGroup[T any](group, topic string, handler internalgap.Handler[T]) shoot.Option[Options, *Options] {
	return internalgap.HandleTopicWithinGroup(handler, topic, group)
}
