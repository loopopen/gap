package gap

import (
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/shoot"
)

var registered = internal.NewGroupedSubs()

// Register stores generated handler and dependency definitions during package
// initialization. Applications start them with [Gap.Subscribe].
func Register(opts ...shoot.Option[Options, *Options]) {
	gapOpts := new(Options).With(opts...)
	registered.AddDependencyOtps(gapOpts.DependencyOptsLst)
	registered.AddHandlerOtps(gapOpts.HandlerOptsLst)
}

func (g *Gap) Subscribe(publisher BrokerOptionsGetter, opts ...shoot.Option[Options, *Options]) {
	done, beginErr := g.opts.BeginOperation()
	if beginErr != nil {
		panic(beginErr)
	}
	defer done()

	gapOpts := g.subscriberOptions(opts...)
	brokerOpts := publisher.BrokerOptions()

	g.subs.AddDependencyOtps(gapOpts.DependencyOptsLst)
	g.subs.AddHandlerOtps(gapOpts.HandlerOptsLst)

	for _, o := range g.subs.DependencyOtpsLst {
		o.Resolve(gapOpts.Dependencies)
	}

	err := g.subs.SubscribeAll(gapOpts, brokerOpts, g.storage, g.pump)
	if err != nil {
		panic(err)
	}

	if g.pump != nil {
		g.pump.StartHandler()
	}

	err = g.subs.ListeningAll()
	if err != nil {
		panic(err)
	}
}
