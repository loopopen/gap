package internal

import (
	"crypto/tls"
	"log/slog"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,TopicOptions

var _ gap.BrokerPlugin = (*Options)(nil)

type Options struct {
	//@def=""
	Password string `yaml:"password"`

	//@def=""
	UserName string `yaml:"username"`

	//@def=[]string{"localhost:9092"}
	Brokers []string `yaml:"brokers"`

	//@def=new(TopicOptions).With()
	TopicOpts *TopicOptions `yaml:"topicOpts"`

	//@def=kafka.LastOffset
	StartOffset int64 `yaml:"startOffset"`

	//@def=nil
	TLSConfig *tls.Config `yaml:"-"`
}

type TopicOptions struct {
	//@def=-1
	NumPartitions int `yaml:"numPartitions"`

	//@def=-1
	ReplicationFactor int `yaml:"replicationFactor"`
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginKafka
}

func ConfigTopic(opts ...shoot.Option[TopicOptions, *TopicOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(TopicOptions).With(opts...)
		o.TopicOpts = options
	}
}

func UseKafka(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		o.BrokerOptions = new(Options).With(opts...)
	}
}

var clients = broker.NewClientCache()

func (o *Options) OpenProducer(gapOpts *gap.Options) (broker.Producer, error) {
	return clients.Producer(func() (broker.Producer, error) {
		producer := NewProducer(gapOpts, o)
		if err := producer.init(); err != nil {
			slog.Error("failed to init producer", slog.Any("err", err))
			return nil, err
		}
		return producer, nil
	})
}

func (o *Options) OpenConsumer(gapOpts *gap.Options, group string) (broker.Consumer, error) {
	return clients.Consumer(group, func() (broker.Consumer, error) {
		consumer := NewConsumer(gapOpts, o, group)
		if err := consumer.init(); err != nil {
			slog.Error("failed to init consumer", slog.Any("err", err))
			return nil, err
		}
		return consumer, nil
	})
}
