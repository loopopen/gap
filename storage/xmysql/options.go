package xmysql

import (
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/shoot"
)

const version = "v0.0.1-alpha.3"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def="gap"
	Schema string

	DSN string
}

func (o *Options) PluginType() enum.PluginType {
	return enum.MySQL
}

func UseMySQL(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.StoragePlugin = options
	}
}
