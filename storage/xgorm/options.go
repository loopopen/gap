package xgorm

import (
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const version = "v0.2.0-beta.1"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//@ def="gap"
	Schema string

	LogLevel logger.LogLevel

	DB *gorm.DB
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
