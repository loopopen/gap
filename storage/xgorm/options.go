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

	dsn       string           `new:"-"`
	dialector DialectorFactory `new:"-"`
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginGORM
}

var _ gap.StoragePlugin = (*Options)(nil)

func UseGorm(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		o.StorageOptions = new(Options).With(opts...)
	}
}

func DSN(dsn string, dialector DialectorFactory) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.dsn = dsn
		o.dialector = dialector
	}
}
