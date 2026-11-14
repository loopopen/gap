package xrabbitmq

import (
	"github.com/loopopen/gap/broker/xrabbitmq/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
)

const version = "v1.0.0-beta.6"

var (
	// UseRabbitMQ configures GAP to use RabbitMQ.
	UseRabbitMQ = internal.UseRabbitMQ
	// Password sets the broker authentication password.
	Password = internal.Password
	// UserName sets the broker authentication username.
	UserName = internal.UserName
	// VirtualHost sets the RabbitMQ virtual host.
	VirtualHost = internal.VirtualHost
	// Exchange sets the RabbitMQ exchange name.
	Exchange = internal.Exchange
	// Endpoint sets the RabbitMQ network endpoint.
	Endpoint = internal.Endpoint
	// URL sets the complete RabbitMQ connection URL.
	URL = internal.URL
	// PublisherConfirms enables RabbitMQ publisher confirmations.
	PublisherConfirms = internal.PublisherConfirms
	// Mandatory sets the mandatory flag on published messages.
	Mandatory = internal.Mandatory
	// PrefetchCount sets the consumer QoS prefetch count.
	PrefetchCount = internal.PrefetchCount
)

var (
	// ConfigQueue configures RabbitMQ queue declaration options.
	ConfigQueue = internal.ConfigQueue
	// Durable controls whether the queue survives broker restarts.
	Durable = internal.Durable
	// Exclusive controls whether the queue is scoped to one connection.
	Exclusive = internal.Exclusive
	// AutoDelete controls whether the queue is deleted after its consumers leave.
	AutoDelete = internal.AutoDelete
)

func init() {
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginRabbitMQ, version)
}
