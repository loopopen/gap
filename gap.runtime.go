package gap

import (
	"context"

	"github.com/loopopen/gap/v2/internal"
	"github.com/loopopen/gap/v2/internal/enum"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
	"github.com/loopopen/gap/v2/internal/plugin"
	"github.com/loopopen/gap/v2/internal/pump"
	"github.com/loopopen/gap/v2/storage"
	"github.com/loopopen/shoot"
)

// Gap owns the shared storage, pump, dashboard, subscriptions, and lifecycle
// of an application. Broker-specific configuration belongs to publishers.
type Gap struct {
	opts    *Options
	storage storage.Storage
	pump    *pump.Pump
	subs    *internal.GroupedSubs
}

// New creates an application-scoped Gap instance.
func New(opts ...shoot.Option[Options, *Options]) *Gap {
	gapOpts := new(Options).With(opts...)
	stor := plugin.MustGetStorage(gapOpts)
	if stor != nil {
		if err := stor.UpdateStatusPublished(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed); err != nil {
			panic(err)
		}
	}

	initSnowflake(gapOpts.WorkerID)

	g := &Gap{
		opts:    gapOpts,
		storage: stor,
		subs:    internal.NewGroupedSubs(),
	}
	g.subs.AddDependencyOtps(registered.DependencyOtpsLst)
	g.subs.AddHandlerOtps(registered.HandlerOtpsLst)
	if stor != nil {
		g.pump = pump.New(gapOpts, stor)
	}
	return g
}

func (g *Gap) publisherOptions(opts ...shoot.Option[BrokerOptions, *BrokerOptions]) *BrokerOptions {
	return new(BrokerOptions).With(opts...)
}

func (g *Gap) subscriberOptions(opts ...shoot.Option[Options, *Options]) *Options {
	result := cloneOptions(g.opts)
	for _, opt := range opts {
		opt(result)
	}
	return result
}

func cloneOptions(source *Options) *Options {
	result := *source
	result.HandlerOptsLst = append([]internalgap.HandlerOptions(nil), source.HandlerOptsLst...)
	result.DependencyOptsLst = append([]internalgap.DependencyOptions(nil), source.DependencyOptsLst...)
	result.Dependencies = append([]any(nil), source.Dependencies...)
	return &result
}

// WaitDrain waits for in-flight persistent message work to finish.
func (g *Gap) WaitDrain(ctx context.Context) error {
	if g.pump == nil {
		return nil
	}
	return g.pump.WaitDrain(ctx)
}
