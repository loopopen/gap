package xkafka

import (
	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/broker/xkafka/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/plugin"
)

const version = "v0.2.0-beta.1"

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
