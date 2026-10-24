package gap

import (
	"context"
	"errors"
	"sync"

	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/internal/pump"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/shoot"
)

// Gap owns the shared storage, pump, dashboard, subscriptions, and lifecycle
// of an application. Broker-specific configuration belongs to publishers.
type Gap struct {
	opts    *Options
	storage storage.Storage
	pump    *pump.Pump
	subs    *internal.GroupedSubs

	shutdownMu      sync.Mutex
	shutdownStarted bool
	shutdownDone    chan struct{}
	shutdownErr     error
}

// New creates an application-scoped Gap instance.
func New(opts ...shoot.Option[Options, *Options]) *Gap {
	gapOpts := new(Options).With(opts...)
	gapOpts.RuntimeIdentity()
	gapOpts.InitializeRuntime()
	stor := plugin.MustGetStorage(gapOpts)
	if stor != nil {
		if err := stor.UpdateStatusPublished(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed); err != nil {
			panic(err)
		}
	}

	initSnowflake(gapOpts.WorkerID)

	g := &Gap{
		opts:         gapOpts,
		storage:      stor,
		subs:         internal.NewGroupedSubs(),
		shutdownDone: make(chan struct{}),
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
	result.HandlerOptsLst = append([]ingap.HandlerOptions(nil), source.HandlerOptsLst...)
	result.DependencyOptsLst = append([]ingap.DependencyOptions(nil), source.DependencyOptsLst...)
	result.Dependencies = append([]any(nil), source.Dependencies...)
	return &result
}

// Shutdown stops intake, drains accepted work, cancels work that exceeds ctx,
// and closes runtime-owned resources. It is safe to call more than once.
func (g *Gap) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	g.shutdownMu.Lock()
	if g.shutdownStarted {
		done := g.shutdownDone
		g.shutdownMu.Unlock()
		select {
		case <-done:
			g.shutdownMu.Lock()
			err := g.shutdownErr
			g.shutdownMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	g.shutdownStarted = true
	g.shutdownMu.Unlock()

	err := g.shutdown(ctx)
	g.shutdownMu.Lock()
	g.shutdownErr = err
	close(g.shutdownDone)
	g.shutdownMu.Unlock()
	return err
}

func (g *Gap) shutdown(ctx context.Context) (err error) {
	g.opts.StopAccepting()
	defer func() {
		if g.pump != nil {
			g.pump.Seal()
		}
		g.opts.CancelWork()
		err = errors.Join(err, g.opts.CloseResources())
	}()

	// All public operations admitted before shutdown must finish before their
	// dispatch channels can be sealed.
	if err = g.opts.WaitOperations(ctx); err != nil {
		return err
	}
	if g.subs != nil {
		if err = g.subs.Stop(ctx); err != nil {
			return err
		}
	}
	if g.pump == nil {
		return nil
	}
	if err = g.pump.StopSources(ctx); err != nil {
		return err
	}
	g.pump.Seal()
	return g.pump.Drain(ctx)
}
