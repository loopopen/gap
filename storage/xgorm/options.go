package xgorm

import (
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const version = "v0.3.0-beta.7"

//go:generate go tool shoot new -opt -short -type=Options

type DialectorFactory func(dsn string) gorm.Dialector

type Options struct {
	//@def="gap"
	Schema string

	LogLevel logger.LogLevel

	DB *gorm.DB

	_dsn string

	_dialector DialectorFactory
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginGORM
}

func UseGorm(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.StorageOptions = options
	}
}

func DSN(dsn string, dialector DialectorFactory) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o._dsn = dsn
		o._dialector = dialector
	}
}
