package xmysql

import (
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

const version = "v1.0.0-beta.5"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//@def="gap"
	Schema string

	DSN string

	Log bool
}

// PluginType returns the plugin type implemented by the receiver.
func (o *Options) PluginType() enum.Plugin {
	return enum.PluginMySQL
}

var _ ingap.StoragePlugin = (*Options)(nil)

// UseMySQL configures GAP to use MySQL storage.
func UseMySQL(opts ...shoot.Option[Options, *Options]) shoot.Option[ingap.Options, *ingap.Options] {
	return func(o *ingap.Options) {
		o.StorageOptions = new(Options).With(opts...)
	}
}
