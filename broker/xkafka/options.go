package xkafka

import (
	"github.com/loopopen/gap/broker/xkafka/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
)

const version = "v1.0.0-beta.5"

type Options = internal.Options

var (
	// UseKafka configures GAP to use segmentio/kafka-go.
	UseKafka = internal.UseKafka
	// Password sets the broker authentication password.
	Password = internal.Password
	// UserName sets the broker authentication username.
	UserName = internal.UserName
	// Brokers sets the Kafka bootstrap broker addresses.
	Brokers = internal.Brokers
	// TopicOpts configures topic provisioning.
	TopicOpts = internal.TopicOpts
	// StartOffset sets the initial consumer offset.
	StartOffset = internal.StartOffset
	// TLSConfig sets the TLS configuration used by broker connections.
	TLSConfig = internal.TLSConfig
)

var (
	// ConfigTopic configures topic provisioning options.
	ConfigTopic = internal.ConfigTopic
	// NumPartitions sets the number of partitions created for a topic.
	NumPartitions = internal.NumPartitions
	// ReplicationFactor sets the replication factor created for a topic.
	ReplicationFactor = internal.ReplicationFactor
)

func init() {
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginKafka, version)
}
