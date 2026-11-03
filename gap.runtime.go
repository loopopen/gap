package gap

import (
	"context"
	"errors"
	"sync"

	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/internal/pump"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/shoot"
)

// Gap owns the shared storage, pump, dashboard, subscriptions, and lifecycle
// of an application. Publishers and subscribers supply their broker-specific
// configuration independently.
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
	if err := gapOpts.Validate(); err != nil {
		panic(err)
	}
	gapOpts.RuntimeIdentity()
	gapOpts.InitRuntime()
	stor := plugin.MustGetStorage(gapOpts)

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
		err = errors.Join(err, g.opts.CloseResources(ctx))
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
