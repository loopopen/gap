package xconfluent

import (
	"github.com/loopopen/gap/broker/xconfluent/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
)

const version = "v1.0.0-beta.6"

type Options = internal.Options

var (
	// UseKafka configures GAP to use the Confluent Kafka client.
	UseKafka = internal.UseKafka
	// UseConfluent configures GAP to use the Confluent Kafka client.
	UseConfluent = internal.UseKafka
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
	// ConfigMap applies a Confluent client configuration map.
	ConfigMap = internal.ClientConfig
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
