package internal

import (
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,TopicOptions

var _ ingap.BrokerPlugin = (*Options)(nil)

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
	return enum.PluginKafka
}

func ClientConfig(config kafka.ConfigMap) shoot.Option[Options, *Options] {
	return func(o *Options) { o.Config = config }
}

func ConfigTopic(opts ...shoot.Option[TopicOptions, *TopicOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) { o.TopicOpts = new(TopicOptions).With(opts...) }
}

func UseKafka(opts ...shoot.Option[Options, *Options]) shoot.Option[ingap.BrokerOptions, *ingap.BrokerOptions] {
	return func(o *ingap.BrokerOptions) {
		o.Plugin = new(Options).With(opts...)
	}
}

var clients = broker.NewClientCache()

func (o *Options) OpenProducer(gapOpts *ingap.Options) (broker.Producer, error) {
	return clients.Producer(o.producerKey(), func() (broker.Producer, error) {
		producer, err := NewProducer(gapOpts, o)
		if err != nil {
			return nil, err
		}
		if err = producer.init(); err != nil {
			slog.Error("failed to init confluent producer", slog.Any("err", err))
			return nil, err
		}
		return producer, nil
	})
}

func (o *Options) OpenConsumer(gapOpts *ingap.Options, group string) (broker.Consumer, error) {
	return clients.Consumer(o.consumerKey(gapOpts.Version, group), func() (broker.Consumer, error) {
		consumer, err := NewConsumer(gapOpts, o, group)
		if err != nil {
			return nil, err
		}
		if err = consumer.init(); err != nil {
			slog.Error("failed to init confluent consumer", slog.Any("err", err))
			return nil, err
		}
		return consumer, nil
	})
}
