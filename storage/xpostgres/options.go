package xpostgres

import (
	"github.com/loopopen/gap/v2/spi/enum"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/loopopen/shoot"
)

const version = "v2.0.0-beta.0"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//@def="gap"
	Schema string

	DSN string
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginPostgres
}

var _ internalgap.StoragePlugin = (*Options)(nil)

func UsePostgres(opts ...shoot.Option[Options, *Options]) shoot.Option[internalgap.Options, *internalgap.Options] {
	return func(o *internalgap.Options) {
		o.StorageOptions = new(Options).With(opts...)
	}
}
