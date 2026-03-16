package xkafka

import (
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

const version = "v0.0.1-alpha.5"

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
}

type TopicOptions struct {
	//shoot: def=-1
	NumPartitions int

	//shoot: def=-1
	ReplicationFactor int
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
		o.BrokerPlugin = options
	}
}
