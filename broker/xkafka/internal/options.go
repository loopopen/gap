package internal

import (
	"crypto/tls"
	"log/slog"

	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/spi/enum"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,TopicOptions

var _ internalgap.BrokerPlugin = (*Options)(nil)

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

func UseKafka(opts ...shoot.Option[Options, *Options]) shoot.Option[internalgap.BrokerOptions, *internalgap.BrokerOptions] {
	return func(o *internalgap.BrokerOptions) {
		o.Plugin = new(Options).With(opts...)
	}
}

var clients = broker.NewClientCache()

func (o *Options) OpenProducer(gapOpts *internalgap.Options) (broker.Producer, error) {
	return clients.Producer(o.producerKey(), func() (broker.Producer, error) {
		producer := NewProducer(gapOpts, o)
		if err := producer.init(); err != nil {
			slog.Error("failed to init producer", slog.Any("err", err))
			return nil, err
		}
		return producer, nil
	})
}

func (o *Options) OpenConsumer(gapOpts *internalgap.Options, group string) (broker.Consumer, error) {
	return clients.Consumer(o.consumerKey(gapOpts.Version, group), func() (broker.Consumer, error) {
		consumer := NewConsumer(gapOpts, o, group)
		if err := consumer.init(); err != nil {
			slog.Error("failed to init consumer", slog.Any("err", err))
			return nil, err
		}
		return consumer, nil
	})
}
