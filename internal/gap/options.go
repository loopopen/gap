package gap

import (
	"context"
	"time"

	"github.com/loopopen/gap/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//@ def=context.Background()
	Context context.Context

	//@ def=context.Background()
	DrainContext context.Context

	ServiceName string

	//@ def="v1"
	Version string

	//@ def="default"
	DefaultGroup string

	//@ def=200
	ClaimBatchSize int

	//@ def=30
	MaxRetries int

	//@ def=180
	LookbackSeconds int

	//@ def=1
	PumpIntervalSeconds int

	MaxPublishConcurrency int

	//@ def=-1
	WorkerID int64

	//@ def=runtime.GOMAXPROCS(0)*512
	PublishBufferSize int

	//@ def=1
	WorkConcurrencyFactor int

	DashboardOptions     *dashboard.Options
	StorageOptions       PluginOptions
	BrokerOptions        PluginOptions
	HandlerOptsLst       []HandlerOptions
	DependencyOptsLst    []DependencyOptions
	Dependencies         []any
	_registerHandlerOnly bool
}

type PluginOptions interface {
	PluginType() enum.Plugin
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

func GoGenerated() shoot.Option[Options, *Options] {
	return func(o *Options) {
		o._registerHandlerOnly = true
	}
}
