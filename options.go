package gap

import (
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	Context               = ingap.Context
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
	ErrRuntimeStopping    = ingap.ErrRuntimeStopping
)

func HandleTopic[T any](handler ingap.Handler[T], topic string) shoot.Option[Options, *Options] {
	return ingap.HandleTopicWithinGroup(handler, topic, "")
}

func HandleTopicWithinGroup[T any](group, topic string, handler ingap.Handler[T]) shoot.Option[Options, *Options] {
	return ingap.HandleTopicWithinGroup(handler, topic, group)
}
