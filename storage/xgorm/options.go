package xgorm

import (
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const version = "v1.0.0-beta.6"

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

// PluginType returns the plugin type implemented by the receiver.
func (o *Options) PluginType() enum.Plugin {
	return enum.PluginGORM
}

var _ ingap.StoragePlugin = (*Options)(nil)

// UseGorm configures GAP to use GORM storage.
func UseGorm(opts ...shoot.Option[Options, *Options]) shoot.Option[ingap.Options, *ingap.Options] {
	return func(o *ingap.Options) {
		o.StorageOptions = new(Options).With(opts...)
	}
}

// DSN configures GORM storage from a DSN and dialector.
func DSN(dsn string, dialector DialectorFactory) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.dsn = dsn
		o.dialector = dialector
	}
}
