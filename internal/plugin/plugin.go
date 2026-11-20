package plugin

import (
	"fmt"

	"github.com/loopopen/gap/broker"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

// BrokerConfigID returns the stable identity of a broker configuration.
func BrokerConfigID(brokerOpts *ingap.BrokerOptions) string {
	if brokerOpts == nil {
		return ""
	}
	opts := brokerOpts.Plugin
	if opts == nil {
		return ""
	}
	if identified, ok := opts.(broker.ConfigIdentifier); ok {
		return identified.ConfigID()
	}
	return fmt.Sprintf("%T:%s", opts, opts.PluginType().String())
}

// BrokerName returns the stable logical broker name persisted with messages.
// A caller-provided name is preferred; the plugin type is a stable default for
// applications that configure only one broker of each type.
func BrokerName(brokerOpts *ingap.BrokerOptions) string {
	if brokerOpts == nil || brokerOpts.Plugin == nil {
		return ""
	}
	if brokerOpts.BrokerName != "" {
		return brokerOpts.BrokerName
	}
	return brokerOpts.Plugin.PluginType().String()
}

// BrokerType returns the broker plugin type recorded in the envelope.
func BrokerType(brokerOpts *ingap.BrokerOptions) string {
	if brokerOpts == nil || brokerOpts.Plugin == nil {
		return ""
	}
	return brokerOpts.Plugin.PluginType().String()
}

// MustGetProducer opens a broker producer or panics on failure.
func MustGetProducer(gapOpts *ingap.Options, brokerOpts *ingap.BrokerOptions) broker.Producer {
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
	gapOpts.RegisterCloser(producer)
	return producer
}

// MustGetConsumer opens a broker consumer or panics on failure.
func MustGetConsumer(gapOpts *ingap.Options, brokerOpts *ingap.BrokerOptions, group string) broker.Consumer {
	if brokerOpts == nil {
		return nil
	}
	plugin := brokerOpts.Plugin
	if plugin == nil {
		return nil
	}
	consumer, err := plugin.OpenConsumer(gapOpts, group)
	if err != nil {
		panic(err)
	}
	gapOpts.RegisterCloser(consumer)
	return consumer
}

// MustGetStorage opens the configured storage or panics on failure.
func MustGetStorage(gapOpts *ingap.Options) storage.Storage {
	return mustStorage(gapOpts, storage.RoleData)
}

// MustGetDashboardStorage opens the storage plugin's dashboard read pool.
func MustGetDashboardStorage(gapOpts *ingap.Options) storage.Storage {
	return mustStorage(gapOpts, storage.RoleDashboard)
}

func mustStorage(gapOpts *ingap.Options, role storage.Role) storage.Storage {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil
	}
	stor, err := sp.OpenStorage(gapOpts, role)
	if err != nil {
		panic(err)
	}
	gapOpts.RegisterCloser(stor)
	return stor
}
