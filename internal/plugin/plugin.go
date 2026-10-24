package plugin

import (
	"fmt"

	"github.com/loopopen/gap/v2/broker"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
	"github.com/loopopen/gap/v2/storage"
)

func BrokerConfigIdentity(brokerOpts *internalgap.BrokerOptions) string {
	if brokerOpts == nil {
		return ""
	}
	opts := brokerOpts.Plugin
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
func BrokerName(brokerOpts *internalgap.BrokerOptions) string {
	if brokerOpts == nil || brokerOpts.Plugin == nil {
		return ""
	}
	if brokerOpts.BrokerName != "" {
		return brokerOpts.BrokerName
	}
	return brokerOpts.Plugin.PluginType().String()
}

func BrokerType(brokerOpts *internalgap.BrokerOptions) string {
	if brokerOpts == nil || brokerOpts.Plugin == nil {
		return ""
	}
	return brokerOpts.Plugin.PluginType().String()
}

func MustGetProducer(gapOpts *internalgap.Options, brokerOpts *internalgap.BrokerOptions) broker.Producer {
	if brokerOpts == nil {
		return nil
	}
	opts := brokerOpts.Plugin
	if opts == nil {
		return nil
	}
	producer, err := opts.OpenProducer(gapOpts)
	if err != nil {
		panic(err)
	}
	return producer
}

func MustGetConsumer(gapOpts *internalgap.Options, brokerOpts *internalgap.BrokerOptions, group string) broker.Consumer {
	if brokerOpts == nil {
		return nil
	}
	opts := brokerOpts.Plugin
	if opts == nil {
		return nil
	}
	consumer, err := opts.OpenConsumer(gapOpts, group)
	if err != nil {
		panic(err)
	}
	return consumer
}

func MustGetStorage(gapOpts *internalgap.Options) storage.Storage {
	return mustStorage(gapOpts, false)
}

// MustGetDashboardStorage opens the storage plugin's dashboard read pool.
func MustGetDashboardStorage(gapOpts *internalgap.Options) storage.Storage {
	return mustStorage(gapOpts, true)
}

func mustStorage(gapOpts *internalgap.Options, dashboard bool) storage.Storage {
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
