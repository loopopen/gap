package xrabbitmq

import (
	"github.com/loopopen/gap/broker/xrabbitmq/v2/internal"
	"github.com/loopopen/gap/v2/spi/dashboard"
	"github.com/loopopen/gap/v2/spi/enum"
)

const version = "v2.0.0-beta.0"

var (
	UseRabbitMQ       = internal.UseRabbitMQ
	Password          = internal.Password
	UserName          = internal.UserName
	VirtualHost       = internal.VirtualHost
	Exchange          = internal.Exchange
	Endpoint          = internal.Endpoint
	URL               = internal.URL
	PublisherConfirms = internal.PublisherConfirms
	Mandatory         = internal.Mandatory
	PrefetchCount     = internal.PrefetchCount
)

var (
	ConfigQueue = internal.ConfigQueue
	Durable     = internal.Durable
	Exclusive   = internal.Exclusive
	AutoDelete  = internal.AutoDelete
)

func init() {
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginRabbitMQ, version)
}
