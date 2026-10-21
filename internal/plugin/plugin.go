package plugin

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

var registries = make(map[enum.Plugin]any)
var regMu sync.Mutex

func Register[T any](typ enum.Plugin, value T) {
	regMu.Lock()
	defer regMu.Unlock()

	if _, ok := registries[typ]; ok {
		panic("gap plugin conflict: " + typ.String())
	}
	registries[typ] = value
}

func mustGet[T any](typ enum.Plugin) T {
	var zero T
	if typ == enum.PluginNone {
		return zero
	}
	v, _ := registries[typ]
	t, ok := v.(T)
	if !ok {
		elem := reflect.TypeOf((*T)(nil)).Elem()
		panic(fmt.Sprintf("key '%s': got %T, want %s", typ, v, elem.String()))
	}
	return t
}

func MustGetProducer(gapOpts *gap.Options) broker.Producer {
	opts := gapOpts.BrokerOptions
	if opts == nil {
		return nil
	}
	f := mustGet[broker.FactoryIface](opts.PluginType())
	producer, err := f.CreateProducer(gapOpts)
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
	f := mustGet[broker.FactoryIface](opts.PluginType())
	consumer, err := f.CreateConsumer(gapOpts, group)
	if err != nil {
		panic(err)
	}
	return consumer
}

func MustGetStorage(gapOpts *gap.Options) storage.Storage {
	return mustStorage(gapOpts, false)
}

// MustGetDashboardStorage opens the dashboard read pool when the storage
// plugin provides one. Otherwise it uses the same storage as the data plane.
func MustGetDashboardStorage(gapOpts *gap.Options) storage.Storage {
	return mustStorage(gapOpts, true)
}

func mustStorage(gapOpts *gap.Options, dashboard bool) storage.Storage {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil
	}
	f := mustGet[storage.FactoryIface](sp.PluginType())
	var (
		stor storage.Storage
		err  error
	)
	if dashboard {
		if df, ok := f.(storage.DashboardFactory); ok {
			stor, err = df.CreateDashboardStorage(gapOpts)
		} else {
			stor, err = f.CreateStorage(gapOpts)
		}
	} else {
		stor, err = f.CreateStorage(gapOpts)
	}
	if err != nil {
		panic(err)
	}
	return stor
}
