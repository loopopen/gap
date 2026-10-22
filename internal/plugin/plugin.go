package plugin

import (
	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

func MustGetProducer(gapOpts *gap.Options) broker.Producer {
	opts := gapOpts.BrokerOptions
	if opts == nil {
		return nil
	}
	producer, err := opts.OpenProducer(gapOpts)
	if err != nil {
		panic(err)
	}
	return producer
}

func MustGetConsumer(gapOpts *gap.Options, group string) broker.Consumer {
	opts := gapOpts.BrokerOptions
	if opts == nil {
		return nil
	}
	consumer, err := opts.OpenConsumer(gapOpts, group)
	if err != nil {
		panic(err)
	}
	return consumer
}

func MustGetStorage(gapOpts *gap.Options) storage.Storage {
	return mustStorage(gapOpts, false)
}

// MustGetDashboardStorage opens the storage plugin's dashboard read pool.
func MustGetDashboardStorage(gapOpts *gap.Options) storage.Storage {
	return mustStorage(gapOpts, true)
}

func mustStorage(gapOpts *gap.Options, dashboard bool) storage.Storage {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil
	}
	stor, err := sp.OpenStorage(gapOpts, dashboard)
	if err != nil {
		panic(err)
	}
	return stor
}
