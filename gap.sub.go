package gap

import (
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

type groupedSubs struct {
	subMap      map[string]*internal.Sub
	handlerOtps []gap.HandlerOptions
	diOtps      []gap.HandlerDepsOptions
}

var subs *groupedSubs

func Subscribe(opts ...shoot.Option[Options, *Options]) {
	gapOpts := new(Options).With(opts...)

	ds := gapOpts.Dependencies()
	if subs == nil {
		subs = &groupedSubs{}
	}
	subs.diOtps = append(subs.diOtps, ds...)
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

	err := subs.subscribe(gapOpts)
	if err != nil {
		panic(err)
	}

	err = subs.listeningAll()
	if err != nil {
		panic(err)
	}
}

func (g *groupedSubs) subscribe(gapOpts *Options) error {
	for _, o := range g.handlerOtps {
		if o.Handler == nil {
			return errx.ErrNilHandler
		}
		if o.Topic == "" {
			return errx.ErrEmptyTopic
		}
		optsClone := *gapOpts
		err := g.subscribeOne(&optsClone, o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *groupedSubs) subscribeOne(optsClone *Options, hOpts gap.HandlerOptions) error {
	group := hOpts.Group
	if group == "" {
		group = optsClone.DefaultGroup
	}
	optsClone.Group = group

	if g.subMap == nil {
		g.subMap = make(map[string]*internal.Sub)
	}
	sub, ok := g.subMap[group]
	if !ok {
		brok := internal.MustGetBroker(optsClone)
		if brok == nil {
			panic("broker must not be nil")
		}
		stor := internal.MustGetStorage(optsClone)

		sub = internal.NewSub(optsClone, brok, stor)
		g.subMap[group] = sub
	}
	err := sub.Subscribe(hOpts.Topic, hOpts.Handler)
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
