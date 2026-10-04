package internal

import (
	"fmt"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,QueueOptions

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

func ConfigQueue(opts ...shoot.Option[QueueOptions, *QueueOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(QueueOptions).With(opts...)
		o.QueueOpts = options
	}
}

func UseRabbitMQ(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.BrokerOptions = options
	}
}
