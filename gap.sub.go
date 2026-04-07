package gap

import (
	"sync"

	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/internal/pump"
	"github.com/lopolopen/gap/internal/registry"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

var fixSubOnce sync.Once

type groupedSubs struct {
	subMap      map[string]*internal.Sub
	handlerOtps []gap.HandlerOptions
	diOtps      []gap.HandlerDepsOptions
}

var subs *groupedSubs

func Subscribe(opts ...shoot.Option[Options, *Options]) {
	gapOpts := new(Options).With(opts...)

	dps := gapOpts.Dependencies()
	if subs == nil {
		subs = &groupedSubs{}
	}
	subs.diOtps = append(subs.diOtps, dps...)
	hs := gapOpts.Handlers()
	subs.handlerOtps = append(subs.handlerOtps, hs...)

	if gap.RegisterHandlerOnly(gapOpts) {
		return
	}

	initSnowflake(gapOpts.WorkerID)
	initDashboard(gapOpts)

	for _, dep := range subs.diOtps {
		dep.Resolve(gapOpts.Values())
	}

	err := subs.subscribeAll(gapOpts)
	if err != nil {
		panic(err)
	}

	err = subs.listeningAll()
	if err != nil {
		panic(err)
	}

	pump.StartHandler()
}

func (g *groupedSubs) subscribeAll(gapOpts *Options) error {
	//todo: IngestConcurrency
	for _, o := range g.handlerOtps {
		if o.Handler == nil {
			return errx.ErrNilHandler
		}
		if o.Topic == "" {
			return errx.ErrEmptyTopic
		}
		err := g.subscribeOne(gapOpts, &o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *groupedSubs) subscribeOne(gapOpts *Options, opts *gap.HandlerOptions) error {
	group := opts.Group

	if g.subMap == nil {
		g.subMap = make(map[string]*internal.Sub)
	}
	sub, ok := g.subMap[group]
	if !ok {
		reader := registry.MustGetRBroker(gapOpts, group)
		if reader == nil {
			panic("reader broker must not be nil")
		}
		stor := registry.MustGetStorage(gapOpts)
		if stor != nil {
			fixSubOnce.Do(func() {
				err := stor.UpdateStatusReceived(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed)
				if err != nil {
					panic(err)
				}
			})
		}

		sub = internal.NewSub(gapOpts, &opts.GroupOptions, reader, stor)
		g.subMap[group] = sub
	}
	err := sub.Subscribe(opts.Topic, opts.Handler)
	if err != nil {
		return err
	}
	return nil
}

func (g *groupedSubs) listeningAll() error {
	for _, sub := range g.subMap {
		err := sub.Listening()
		if err != nil {
			return err
		}
	}
	return nil
}
