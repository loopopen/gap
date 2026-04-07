package gap

import (
	"context"
	"time"

	"github.com/lopolopen/gap/options/dashboard"
	"github.com/lopolopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def=context.Background()
	Context context.Context

	//shoot: def=context.Background()
	DrainContext context.Context

	ServiceName string

	//shoot: def="v1"
	Version string

	//shoot: def="default"
	DefaultGroup string

	//shoot: def=200
	ClaimBatchSize int

	//shoot: def=30
	MaxRetries int

	//shoot: def=180
	LookbackSeconds int

	//shoot: def=1
	PumpIntervalSeconds int

	MaxPublishConcurrency int

	//shoot: def=-1
	WorkerID int64

	StoragePlugin PluginOptions

	BrokerPlugin PluginOptions

	//shoot: def=runtime.GOMAXPROCS(0)*512
	PublishBufferSize int

	//shoot: def=1
	WorkConcurrencyFactor int

	_dashboard           *dashboard.Options
	_handlers            []HandlerOptions
	_dependencies        []HandlerDepsOptions
	_values              []any
	_registerHandlerOnly bool
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

func (o *Options) Dashboard() *dashboard.Options {
	return o._dashboard
}

func (o *Options) Handlers() []HandlerOptions {
	return o._handlers
}

func (o *Options) Dependencies() []HandlerDepsOptions {
	return o._dependencies
}

func (o *Options) Values() []any {
	return o._values
}

func UseDashboard(opts ...shoot.Option[dashboard.Options, *dashboard.Options]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(dashboard.Options).With(opts...)
		o._dashboard = options
	}
}

func Inject(values ...any) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o._values = append(o._values, values...)
	}
}

func GoGenerated() shoot.Option[Options, *Options] {
	return func(o *Options) {
		o._registerHandlerOnly = true
	}
}
