package xgorm

import (
	"github.com/loopopen/gap/v2/spi/enum"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/loopopen/shoot"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const version = "v2.0.0-beta.0"

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

var _ internalgap.StoragePlugin = (*Options)(nil)

func UseGorm(opts ...shoot.Option[Options, *Options]) shoot.Option[internalgap.Options, *internalgap.Options] {
	return func(o *internalgap.Options) {
		o.StorageOptions = new(Options).With(opts...)
	}
}

func DSN(dsn string, dialector DialectorFactory) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.dsn = dsn
		o.dialector = dialector
	}
}
