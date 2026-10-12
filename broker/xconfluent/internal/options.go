package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,TopicOptions

type Options struct {
	Password string `yaml:"password"`
	UserName string `yaml:"username"`

	//@def=[]string{"localhost:9092"}
	Brokers []string `yaml:"brokers"`

	//@def=new(TopicOptions).With()
	TopicOpts *TopicOptions `yaml:"topicOpts"`

	//@def=kafka.OffsetEnd
	StartOffset kafka.Offset `yaml:"startOffset"`

	// Config contains additional librdkafka properties. Named Options fields
	// override the corresponding entries when both are provided.
	Config kafka.ConfigMap `yaml:"-"`
}

type TopicOptions struct {
	//@def=-1
	NumPartitions int `yaml:"numPartitions"`

	//@def=-1
	ReplicationFactor int `yaml:"replicationFactor"`
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginConfluent
}

func ClientConfig(config kafka.ConfigMap) shoot.Option[Options, *Options] {
	return func(o *Options) { o.Config = config }
}

func ConfigTopic(opts ...shoot.Option[TopicOptions, *TopicOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) { o.TopicOpts = new(TopicOptions).With(opts...) }
}

func UseKafka(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) { o.BrokerOptions = new(Options).With(opts...) }
}
