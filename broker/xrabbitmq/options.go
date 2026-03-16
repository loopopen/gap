package xrabbitmq

import (
	"fmt"

	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

const version = "v0.0.1-alpha.3"

//go:generate go tool shoot new -opt -short -type=Options,QueueOptions

const ExchangeKind = "topic"

type Options struct {
	//shoot: def="guest"
	Password string `yaml:"password"`

	//shoot: def="guest"
	UserName string `yaml:"username"`

	//shoot: def="/"
	VirtualHost string `yaml:"virtual_host"`

	//shoot: def="default"
	Exchange string `yaml:"exchange"`

	//shoot: def="localhost:5672"
	Endpoint string `yaml:"endpoint"`

	URL string `yaml:"url"`

	ConfirmMode bool `yaml:"confirm_mode"`

	//shoot: def=new(QueueOptions).With()
	QueueOpts *QueueOptions
}

func (o *Options) PluginType() enum.PluginType {
	return enum.RabbitMQ
}

type QueueOptions struct {
	//shoot: def=true
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

// func (o *Options) QueueOptions() *QueueOptions {
// 	return o._queueOpts
// }

func ConfigQueue(opts ...shoot.Option[QueueOptions, *QueueOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(QueueOptions).With(opts...)
		o.QueueOpts = options
	}
}

func UseRabbitMQ(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.BrokerPlugin = options
	}
}
