package xkafka

import (
	"github.com/loopopen/gap/broker/xkafka/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
)

const version = "v1.0.0-beta.1"

type Options = internal.Options

var (
	UseKafka    = internal.UseKafka
	Password    = internal.Password
	UserName    = internal.UserName
	Brokers     = internal.Brokers
	TopicOpts   = internal.TopicOpts
	StartOffset = internal.StartOffset
	TLSConfig   = internal.TLSConfig
)

var (
	ConfigTopic       = internal.ConfigTopic
	NumPartitions     = internal.NumPartitions
	ReplicationFactor = internal.ReplicationFactor
)

func init() {
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginKafka, version)
}
