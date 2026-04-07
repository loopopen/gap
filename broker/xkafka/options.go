package xkafka

import (
	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/broker/xkafka/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/registry"
)

const version = "v0.0.1-alpha.5"

var (
	UseKafka    = internal.UseKafka
	Password    = internal.Password
	UserName    = internal.UserName
	Brokers     = internal.Brokers
	TopicOpts   = internal.TopicOpts
	StartOffset = internal.StartOffset
)

var (
	ConfigTopic       = internal.ConfigTopic
	NumPartitions     = internal.NumPartitions
	ReplicationFactor = internal.ReplicationFactor
)

func init() {
	registry.Register[broker.FactoryIface](enum.Kafka, broker.NewFactory(&internal.Factory{}))
	dashboard.AddMeta(enum.Broker, enum.Kafka, version)
}
