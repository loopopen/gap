package xmysql

import (
	"database/sql"
	"errors"
	"log/slog"

	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/plugin"
	"github.com/lopolopen/gap/storage"
)

type Factory struct{}

func (f Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if sp.PluginType() != enum.MySQL {
		return nil, errors.New("storage plugin does not match")
	}

	opts := sp.(*Options)
	db, err := sql.Open("mysql", opts.DSN)
	if err != nil {
		slog.Error("failed to connect mysql database", slog.Any("err", err))
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

func init() {
	plugin.Register[storage.FactoryIface](enum.MySQL, &Factory{})
	dashboard.AddMeta(enum.Storage, enum.MySQL, version)
}
