package xmysql

import (
	"errors"
	"log/slog"

	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/xsql"
)

type Factory struct{}

func (f Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	return f.create(gapOpts, xsql.RoleData)
}

func (f Factory) CreateDashboardStorage(gapOpts *gap.Options) (storage.Storage, error) {
	return f.create(gapOpts, xsql.RoleDashboard)
}

func (f Factory) create(gapOpts *gap.Options, role xsql.Role) (storage.Storage, error) {
	opts, err := mysqlOptions(gapOpts)
	if err != nil {
		return nil, err
	}

	var pool *xsql.Pool
	switch role {
	case xsql.RoleDashboard:
		pool, err = registry.Dashboard(gapOpts.Context, opts.DSN)
	default:
		pool, err = registry.Data(gapOpts.Context, opts.DSN, xsql.DataPlaneMaxOpen(gapOpts))
	}
	if err != nil {
		slog.Error("failed to connect mysql database", slog.Any("err", err))
		return nil, err
	}

	stor := NewStorage(gapOpts, pool.DB)
	if err = pool.EnsureSchema(opts.Schema, stor.init); err != nil {
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
