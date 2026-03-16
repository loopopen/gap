package internal

import (
	"fmt"
	"reflect"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/gap/storage"
)

var pluginRegistries = make(map[enum.PluginType]any)

func Register[T any](typ enum.PluginType, value T) {
	pluginRegistries[typ] = value
}

func MustGet[T any](typ enum.PluginType) T {
	var zero T
	if typ == enum.None {
		return zero
	}
	v, _ := pluginRegistries[typ]
	t, ok := v.(T)
	if !ok {
		elem := reflect.TypeOf((*T)(nil)).Elem()
		panic(fmt.Sprintf("key '%s': got %T, want %s", typ, v, elem.String()))
	}
	return t
}

func MustGetBroker(gapOpts *gap.Options) broker.Broker {
	bp := gapOpts.BrokerPlugin
	if bp == nil {
		return nil
	}
	f := MustGet[broker.Factory](bp.PluginType())
	brok, err := f.CreateBroker(gapOpts)
	if err != nil {
		panic(err)
	}
	return brok
}

func MustGetStorage(gapOpts *gap.Options) storage.Storage {
	sp := gapOpts.StoragePlugin
	if sp == nil {
		return nil
	}
	f := MustGet[storage.Factory](sp.PluginType())
	stor, err := f.CreateStorage(gapOpts)
	if err != nil {
		panic(err)
	}
	return stor
}
