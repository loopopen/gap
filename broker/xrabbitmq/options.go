package xrabbitmq

import (
	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/broker/xrabbitmq/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/plugin"
)

const version = "v0.1.0-beta.2"

var (
	UseRabbitMQ       = internal.UseRabbitMQ
	Password          = internal.Password
	UserName          = internal.UserName
	VirtualHost       = internal.VirtualHost
	Exchange          = internal.Exchange
	Endpoint          = internal.Endpoint
	URL               = internal.URL
	PublisherConfirms = internal.PublisherConfirms
	PrefetchCount     = internal.PrefetchCount
)

var (
	ConfigQueue = internal.ConfigQueue
	Durable     = internal.Durable
	Exclusive   = internal.Exclusive
	AutoDelete  = internal.AutoDelete
)

func init() {
	plugin.Register[broker.FactoryIface](enum.PluginRabbitMQ, broker.NewFactory(&internal.Factory{}))
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginRabbitMQ, version)
}
