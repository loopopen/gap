package xgorm

import (
	"errors"
	"log/slog"

	"github.com/lopolopen/gap/internal/dashboard"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/plugin"
	"github.com/lopolopen/gap/storage"
)

type Factory struct{}

func (f *Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	o := gapOpts.StorageOptions
	if o == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if o.PluginType() != enum.PluginGORM {
		return nil, errors.New("storage plugin does not match")
	}
	opts := o.(*Options)
	if opts.DB == nil {
		return nil, errors.New("gorm db is nil")
	}

	stor := NewStorage(gapOpts, opts.DB)
	err := stor.init()
	if err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func init() {
	plugin.Register[storage.FactoryIface](enum.PluginGORM, &Factory{})
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginGORM, version)
}
