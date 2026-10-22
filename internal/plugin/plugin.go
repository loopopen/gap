package plugin

import (
	"fmt"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

func BrokerConfigIdentity(gapOpts *gap.Options) string {
	opts := gapOpts.BrokerOptions
	if opts == nil {
		return ""
	}
	if identified, ok := opts.(broker.ConfigIdentifier); ok {
		return identified.ConfigIdentity()
	}
	return fmt.Sprintf("%T:%s", opts, opts.PluginType().String())
}

// BrokerName returns the stable logical broker name persisted with messages.
// A caller-provided name is preferred; the plugin type is a stable default for
// applications that configure only one broker of each type.
func BrokerName(gapOpts *gap.Options) string {
	if gapOpts == nil || gapOpts.BrokerOptions == nil {
		return ""
	}
	if gapOpts.BrokerName != "" {
		return gapOpts.BrokerName
	}
	return gapOpts.BrokerOptions.PluginType().String()
}

func PluginType(gapOpts *gap.Options) string {
	if gapOpts == nil || gapOpts.BrokerOptions == nil {
		return ""
	}
	return gapOpts.BrokerOptions.PluginType().String()
}

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
