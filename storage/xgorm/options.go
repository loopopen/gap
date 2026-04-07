package xgorm

import (
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/shoot"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const version = "v0.1.0-beta.1"

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def="gap"
	Schema string

	LogLevel logger.LogLevel

	DB *gorm.DB

	MySQL *MySQLConf

	PostgreSQL *PostgreSQLConf
}

func (o *Options) PluginType() enum.PluginType {
	return enum.GORM
}

type MySQLConf struct {
	DSN string
}

type PostgreSQLConf struct{}

func UseGorm(opts ...shoot.Option[Options, *Options]) shoot.Option[gap.Options, *gap.Options] {
	return func(o *gap.Options) {
		options := new(Options).With(opts...)
		o.StorageOptions = options
	}
}
