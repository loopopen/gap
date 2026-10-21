package xmysql

import (
	"errors"
	"log/slog"

	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/storage"
)

type Factory struct{}

func (f Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	return f.create(gapOpts, roleData)
}

func (f Factory) CreateDashboardStorage(gapOpts *gap.Options) (storage.Storage, error) {
	return f.create(gapOpts, roleDashboard)
}

func (f Factory) create(gapOpts *gap.Options, role poolRole) (storage.Storage, error) {
	opts, err := mysqlOptions(gapOpts)
	if err != nil {
		return nil, err
	}

	var pool *connPool
	switch role {
	case roleDashboard:
		pool, err = dashboardPool(gapOpts.Context, opts.DSN)
	default:
		pool, err = dataPool(gapOpts.Context, opts.DSN, dataPlaneMaxOpen(gapOpts))
	}
	if err != nil {
		slog.Error("failed to connect mysql database", slog.Any("err", err))
		return nil, err
	}

	stor := NewStorage(gapOpts, pool.db)
	if err = pool.ensureSchema(opts.Schema, stor.init); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func mysqlOptions(gapOpts *gap.Options) (*Options, error) {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if sp.PluginType() != enum.PluginMySQL {
		return nil, errors.New("storage plugin does not match")
	}
	return sp.(*Options), nil
}

func init() {
	plugin.Register[storage.FactoryIface](enum.PluginMySQL, &Factory{})
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginMySQL, version)
}

var (
	_ storage.FactoryIface     = Factory{}
	_ storage.DashboardFactory = Factory{}
)
