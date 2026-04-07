package xrabbitmq

import (
	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/broker/xrabbitmq/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/registry"
)

const version = "v0.0.1-alpha.3"

var (
	UseRabbitMQ   = internal.UseRabbitMQ
	Password      = internal.Password
	UserName      = internal.UserName
	VirtualHost   = internal.VirtualHost
	Exchange      = internal.Exchange
	Endpoint      = internal.Endpoint
	URL           = internal.URL
	ConfirmMode   = internal.ConfirmMode
	PrefetchCount = internal.PrefetchCount
)

var (
	ConfigQueue = internal.ConfigQueue
	Durable     = internal.Durable
	Exclusive   = internal.Exclusive
	AutoDelete  = internal.AutoDelete
)

func init() {
	registry.Register[broker.FactoryIface](enum.RabbitMQ, broker.NewFactory(&internal.Factory{}))
	dashboard.AddMeta(enum.Broker, enum.RabbitMQ, version)
}
