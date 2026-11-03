package gap

import (
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/shoot"
)

var registered = internal.NewGroupedSubs()

// Register stores generated handler and dependency definitions during package
// initialization. Applications start them with [Gap.Subscribe].
func Register(opts ...shoot.Option[BrokerOptions, *BrokerOptions]) {
	brokerOpts := new(BrokerOptions).With(opts...)
	registered.AddDependencyOtps(brokerOpts.DependencyOptsLst)
	registered.AddHandlerOtps(brokerOpts.HandlerOptsLst)
}

// Subscribe configures the broker and registers all configured handlers.
func (g *Gap) Subscribe(opts ...shoot.Option[BrokerOptions, *BrokerOptions]) {
	done, beginErr := g.opts.BeginOperation()
	if beginErr != nil {
		panic(beginErr)
	}
	defer done()

	brokerOpts := g.publisherOptions(opts...)

	g.subs.AddDependencyOtps(brokerOpts.DependencyOptsLst)
	g.subs.AddHandlerOtps(brokerOpts.HandlerOptsLst)

	for _, o := range g.subs.DependencyOtpsLst {
		o.Resolve(brokerOpts.Dependencies)
	}

	err := g.subs.SubscribeAll(g.opts, brokerOpts, g.storage, g.pump)
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
