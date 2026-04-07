package registry

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/gap/storage"
)

var pluginRegistries = make(map[enum.PluginType]any)
var regMu sync.Mutex

func Register[T any](typ enum.PluginType, value T) {
	regMu.Lock()
	defer regMu.Unlock()

	if _, ok := pluginRegistries[typ]; ok {
		panic("gap plugin conflict: " + typ.String())
	}
	pluginRegistries[typ] = value
}

func mustGet[T any](typ enum.PluginType) T {
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

func MustGetWBroker(gapOpts *gap.Options) broker.Writer {
	bp := gapOpts.BrokerPlugin
	if bp == nil {
		return nil
	}
	f := mustGet[broker.FactoryIface](bp.PluginType())
	writer, err := f.CreateWriter(gapOpts)
	if err != nil {
		panic(err)
	}
	return writer
}

func MustGetRBroker(gapOpts *gap.Options, group string) broker.Reader {
	bp := gapOpts.BrokerPlugin
	if bp == nil {
		return nil
	}
	f := mustGet[broker.FactoryIface](bp.PluginType())
	reader, err := f.CreateReader(gapOpts, group)
	if err != nil {
		panic(err)
	}
	return reader
}

func MustGetStorage(gapOpts *gap.Options) storage.Storage {
	sp := gapOpts.StoragePlugin
	if sp == nil {
		return nil
	}
	f := mustGet[storage.FactoryIface](sp.PluginType())
	stor, err := f.CreateStorage(gapOpts)
	if err != nil {
		panic(err)
	}
	return stor
}
