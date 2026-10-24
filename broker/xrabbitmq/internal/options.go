package internal

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,QueueOptions

var _ ingap.BrokerPlugin = (*Options)(nil)

const ExchangeKind = "topic"

type Options struct {
	//@def="guest"
	Password string `yaml:"password"`

	//@def="guest"
	UserName string `yaml:"username"`

	//@def="/"
	VirtualHost string `yaml:"virtual_host"`

	//@def="default"
	Exchange string `yaml:"exchange"`

	//@def="localhost:5672"
	Endpoint string `yaml:"endpoint"`

	URL string `yaml:"url"`

	PublisherConfirms bool `yaml:"publisher_confirms"`

	// Mandatory returns messages that cannot be routed to any queue. Enabling it
	// also enables publisher confirms internally so returns can be observed
	// before a publish is reported as successful.
	Mandatory bool `yaml:"mandatory"`

	//@def=runtime.GOMAXPROCS(0)*10
	PrefetchCount int `yaml:"prefetch_count"`

	//@def=new(QueueOptions).With()
	QueueOpts *QueueOptions
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginRabbitMQ
}

type QueueOptions struct {
	//@def=true
	Durable bool

	Exclusive bool

	AutoDelete bool
}

func (o *Options) AmqpURL() string {
	if o.URL != "" {
		return o.URL
	}
	return fmt.Sprintf("amqp://%s:%s@%s%s", o.UserName, o.Password, o.Endpoint, o.VirtualHost)
}

func (o *Options) connectionKey() string {
	return o.AmqpURL()
}

func (o *Options) producerKey() string {
	return strings.Join([]string{
		o.connectionKey(),
		o.Exchange,
		strconv.FormatBool(o.PublisherConfirms || o.Mandatory),
	}, "\x00")
}

func (o *Options) ConfigIdentity() string {
	return broker.ConfigIdentity("xrabbitmq", o.producerKey())
}

func ConfigQueue(opts ...shoot.Option[QueueOptions, *QueueOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(QueueOptions).With(opts...)
		o.QueueOpts = options
	}
}

func UseRabbitMQ(opts ...shoot.Option[Options, *Options]) shoot.Option[ingap.BrokerOptions, *ingap.BrokerOptions] {
	return func(o *ingap.BrokerOptions) {
		o.Plugin = new(Options).With(opts...)
	}
}

func (o *Options) OpenProducer(gapOpts *ingap.Options) (broker.Producer, error) {
	producer := NewProducer(gapOpts, o)
	if err := producer.init(); err != nil {
		_ = producer.Close()
		slog.Error("failed to init producer", slog.Any("err", err))
		return nil, err
	}
	return producer, nil
}

func (o *Options) OpenConsumer(gapOpts *ingap.Options, group string) (broker.Consumer, error) {
	consumer := NewConsumer(gapOpts, o, group)
	if err := consumer.init(); err != nil {
		_ = consumer.Close()
		slog.Error("failed to init consumer", slog.Any("err", err))
		return nil, err
	}
	return consumer, nil
}
