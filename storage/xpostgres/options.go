package xpostgres

import (
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

const version = "v0.0.1-beta.1"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//@def="gap"
	Schema string

	DSN string
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginPostgres
}

func UsePostgres(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.StorageOptions = options
	}
}
