package gap

import (
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/pump"
	"github.com/lopolopen/shoot"
)

func Subscribe(opts ...shoot.Option[Options, *Options]) {
	gapOpts := new(Options).With(opts...)

	subs := internal.SingleGroupedSubs()
	subs.AddDependencyOtps(gapOpts.DependencyOptsLst)
	subs.AddHandlerOtps(gapOpts.HandlerOptsLst)

	if gap.RegisterHandlerOnly(gapOpts) {
		return
	}

	initSnowflake(gapOpts.WorkerID)

	for _, o := range subs.DependencyOtpsLst {
		o.Resolve(gapOpts.Dependencies)
	}

	err := subs.SubscribeAll(gapOpts)
	if err != nil {
		panic(err)
	}

	err = subs.ListeningAll()
	if err != nil {
		panic(err)
	}

	pump.StartHandler()
	dashboard.InitDashboard(gapOpts)
}
