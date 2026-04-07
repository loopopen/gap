package xgorm

import (
	"errors"
	"log/slog"

	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/registry"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/gap/storage"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type factory struct{}

func (f *factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	sp := gapOpts.StoragePlugin
	if sp == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if sp.PluginType() != enum.GORM {
		return nil, errors.New("storage plugin does not match")
	}
	db, err := makeDB(sp.(*Options))
	if err != nil {
		return nil, err
	}

	stor := NewStorage(gapOpts, db)
	err = stor.init()
	if err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func makeDB(opts *Options) (*gorm.DB, error) {
	if opts.DB != nil {
		db := *opts.DB
		return &db, nil
	}
	var dial gorm.Dialector
	if opts.MySQL != nil {
		dial = mysql.Open(opts.MySQL.DSN)
	}

	db, err := gorm.Open(dial)
	if err != nil {
		slog.Error("failed to connect database", slog.Any("err", err))
		panic(err)
	}

	return db, nil
}

func init() {
	registry.Register[storage.FactoryIface](enum.GORM, &factory{})
	dashboard.AddMeta(enum.Storage, enum.GORM, version)
}
