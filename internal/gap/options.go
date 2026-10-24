package gap

import (
	"context"
	"time"

	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/dashboard"
	"github.com/loopopen/gap/v2/internal/enum"
	"github.com/loopopen/gap/v2/storage"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,BrokerOptions

type Options struct {
	//@def=context.Background()
	Context context.Context

	//@def=context.Background()
	DrainContext context.Context

	ServiceName string

	//@def="v1"
	Version string

	//@def="default"
	DefaultGroup string

	//@def=200
	ClaimBatchSize int

	//@def=30
	MaxRetries int

	//@def=180
	LookbackSeconds int

	//@def=1
	PumpIntervalSeconds int

	MaxPublishConcurrency int

	//@def=-1
	WorkerID int64

	//@def=runtime.GOMAXPROCS(0)*512
	PublishBufferSize int

	//@def=1
	WorkConcurrencyFactor int

	DashboardOptions  *dashboard.Options  `new:"-"`
	StorageOptions    StoragePlugin       `new:"-"`
	HandlerOptsLst    []HandlerOptions    `new:"-"`
	DependencyOptsLst []DependencyOptions `new:"-"`
	Dependencies      []any               `new:"-"`
}

type BrokerOptions struct {
	// BrokerName is the stable logical name used to route persisted messages.
	// It must not contain connection details such as hosts or credentials.
	BrokerName string

	Plugin BrokerPlugin `new:"-"`
}

// StoragePlugin is the selected storage driver. The concrete options open the
// database themselves, so dispatch does not go through a plugin registry.
type StoragePlugin interface {
	PluginType() enum.Plugin
	OpenStorage(opts *Options, dashboard bool) (storage.Storage, error)
}

// BrokerPlugin is the selected broker driver.
type BrokerPlugin interface {
	PluginType() enum.Plugin
	OpenProducer(opts *Options) (broker.Producer, error)
	OpenConsumer(opts *Options, group string) (broker.Consumer, error)
}

type HandlerOptions struct {
	GroupOptions
	Topic   string
	Handler Handler[[]byte]
}

type GroupOptions struct {
	Group             string
	IngestConcurrency int
}

func (o *Options) Lookback() time.Duration {
	return time.Second * (time.Duration(o.LookbackSeconds))
}

func (o *Options) PumpInterval() time.Duration {
	return time.Second * (time.Duration(o.PumpIntervalSeconds))
}

func UseDashboard(opts ...shoot.Option[dashboard.Options, *dashboard.Options]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(dashboard.Options).With(opts...)
		o.DashboardOptions = options
	}
}

func Inject(values ...any) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.Dependencies = append(o.Dependencies, values...)
	}
}
