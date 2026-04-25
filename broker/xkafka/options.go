package xkafka

import (
	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/broker/xkafka/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/plugin"
)

const version = "v0.1.0-beta.2"

type Options = internal.Options

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
	plugin.Register[broker.FactoryIface](enum.PluginKafka, broker.NewFactory(&internal.Factory{}))
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginKafka, version)
}
