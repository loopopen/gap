package xconfluent

import (
	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/broker/xconfluent/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/plugin"
)

const version = "v0.3.0-beta.3"

type Options = internal.Options

var (
	UseKafka     = internal.UseKafka
	UseConfluent = internal.UseKafka
	Password     = internal.Password
	UserName     = internal.UserName
	Brokers      = internal.Brokers
	TopicOpts    = internal.TopicOpts
	StartOffset  = internal.StartOffset
	ConfigMap    = internal.ClientConfig
)

var (
	ConfigTopic       = internal.ConfigTopic
	NumPartitions     = internal.NumPartitions
	ReplicationFactor = internal.ReplicationFactor
)

func init() {
	plugin.Register[broker.FactoryIface](enum.PluginConfluent, broker.NewFactory(&internal.Factory{}))
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginConfluent, version)
}
