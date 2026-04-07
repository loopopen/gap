package internal

import (
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,TopicOptions

type Options struct {
	//shoot: def=""
	Password string

	//shoot: def=""
	UserName string

	//shoot: def=[]string{"localhost:9092"}
	Brokers []string

	//shoot: def=new(TopicOptions).With()
	TopicOpts *TopicOptions

	//shoot: def=kafka.LastOffset
	StartOffset int64
}

type TopicOptions struct {
	//shoot: def=-1
	NumPartitions int

	//shoot: def=-1
	ReplicationFactor int
}

func (o *Options) PluginType() enum.PluginType {
	return enum.Kafka
}

func ConfigTopic(opts ...shoot.Option[TopicOptions, *TopicOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(TopicOptions).With(opts...)
		o.TopicOpts = options
	}
}

func UseKafka(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.BrokerOptions = options
	}
}
