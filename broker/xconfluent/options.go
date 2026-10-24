package xconfluent

import (
	"github.com/loopopen/gap/broker/xconfluent/v2/internal"
	"github.com/loopopen/gap/v2/spi/dashboard"
	"github.com/loopopen/gap/v2/spi/enum"
)

const version = "v2.0.0-beta.0"

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
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginKafka, version)
}
