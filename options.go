package gap

import (
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	// Version sets the message version used to isolate persisted messages.
	Version = ingap.Version
	// ServiceName identifies the application service that owns the runtime.
	ServiceName = ingap.ServiceName
	// DefaultGroup sets the consumer group used when a handler does not specify one.
	DefaultGroup = ingap.DefaultGroup
	// WorkerID sets the Snowflake worker identifier used to generate message IDs.
	WorkerID = ingap.WorkerID
	// ClaimBatchSize sets the maximum number of messages claimed in one polling batch.
	ClaimBatchSize = ingap.ClaimBatchSize
	// MaxRetries sets the maximum number of delivery attempts before a message is failed.
	MaxRetries = ingap.MaxRetries
	// LeaseTTLSeconds sets how long a claimed message remains leased to a worker.
	LeaseTTLSeconds = ingap.LeaseTTLSeconds
	// RetryIntervalSeconds sets the delay before a pending message becomes eligible for retry.
	RetryIntervalSeconds = ingap.RetryIntervalSeconds
	// PumpIntervalSeconds sets how often the runtime polls storage for due messages.
	PumpIntervalSeconds = ingap.PumpIntervalSeconds
	// CleanupEnabled enables automatic deletion of expired terminal messages.
	CleanupEnabled = ingap.CleanupEnabled
	// SucceededRetentionSeconds sets how long succeeded messages remain in storage.
	SucceededRetentionSeconds = ingap.SucceededRetentionSeconds
	// FailedRetentionSeconds sets how long failed messages remain in storage.
	FailedRetentionSeconds = ingap.FailedRetentionSeconds
	// CleanupIntervalSeconds sets the base interval between cleanup cycles.
	CleanupIntervalSeconds = ingap.CleanupIntervalSeconds
	// CleanupBatchSize sets the maximum number of messages deleted per cleanup batch.
	CleanupBatchSize = ingap.CleanupBatchSize
	// CleanupTimeoutSeconds limits the duration of one cleanup cycle.
	CleanupTimeoutSeconds = ingap.CleanupTimeoutSeconds
	// MaxPublishConcurrency limits concurrent message publishing.
	MaxPublishConcurrency = ingap.MaxPublishConcurrency
	// PublishBufferSize sets the capacity of the in-memory publishing queue.
	PublishBufferSize = ingap.PublishBufferSize
	// WorkConcurrencyFactor scales handler concurrency from the process CPU count.
	WorkConcurrencyFactor = ingap.WorkConcurrencyFactor
	// UseDashboard enables the operational dashboard with optional configuration.
	UseDashboard = ingap.UseDashboard
	// Inject registers dependency values used to construct generated handlers.
	Inject = ingap.Inject
	// BrokerName sets the stable logical identity of a broker configuration.
	BrokerName = ingap.BrokerName
	// ErrRuntimeStopping is returned when new work is submitted during shutdown.
	ErrRuntimeStopping = ingap.ErrRuntimeStopping
)

// HandleTopic registers a typed handler for a topic in the default consumer group.
func HandleTopic[T any](handler ingap.Handler[T], topic string) shoot.Option[BrokerOptions, *BrokerOptions] {
	return ingap.HandleTopicWithinGroup(handler, topic, "")
}

// HandleTopicWithinGroup registers a typed handler for a topic and consumer group.
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
