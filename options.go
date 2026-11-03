package gap

import (
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
	LeaseTTLSeconds       = ingap.LeaseTTLSeconds
	RetryIntervalSeconds  = ingap.RetryIntervalSeconds
	PumpIntervalSeconds   = ingap.PumpIntervalSeconds
	MaxPublishConcurrency = ingap.MaxPublishConcurrency
	PublishBufferSize     = ingap.PublishBufferSize
	WorkConcurrencyFactor = ingap.WorkConcurrencyFactor
	UseDashboard          = ingap.UseDashboard
	Inject                = ingap.Inject
	BrokerName            = ingap.BrokerName
	ErrRuntimeStopping    = ingap.ErrRuntimeStopping
)

func HandleTopic[T any](handler ingap.Handler[T], topic string) shoot.Option[BrokerOptions, *BrokerOptions] {
	return ingap.HandleTopicWithinGroup(handler, topic, "")
}

func HandleTopicWithinGroup[T any](group, topic string, handler ingap.Handler[T]) shoot.Option[BrokerOptions, *BrokerOptions] {
	return ingap.HandleTopicWithinGroup(handler, topic, group)
}

// From copies a publisher's broker configuration into a broker option. It lets
// a subscriber reuse the configuration owned by that publisher.
func From(source BrokerOptionsGetter) shoot.Option[BrokerOptions, *BrokerOptions] {
	return func(target *BrokerOptions) {
		if source == nil {
			return
		}
		brokerOpts := source.BrokerOptions()
		if brokerOpts == nil {
			return
		}
		*target = *brokerOpts
		target.HandlerOptsLst = append([]ingap.HandlerOptions(nil), brokerOpts.HandlerOptsLst...)
		target.DependencyOptsLst = append([]ingap.DependencyOptions(nil), brokerOpts.DependencyOptsLst...)
		target.Dependencies = append([]any(nil), brokerOpts.Dependencies...)
	}
}
