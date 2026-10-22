package xpostgres

import (
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/xsql"
)

func (o *Options) OpenStorage(gapOpts *gap.Options, dashboard bool) (storage.Storage, error) {
	role := xsql.RoleData
	if dashboard {
		role = xsql.RoleDashboard
	}
	return o.open(gapOpts, role)
}

func (o *Options) open(gapOpts *gap.Options, role xsql.Role) (storage.Storage, error) {
	var (
		pool *xsql.Pool
		err  error
	)
	switch role {
	case xsql.RoleDashboard:
		pool, err = registry.Dashboard(gapOpts.Context, o.DSN)
	default:
		pool, err = registry.Data(gapOpts.Context, o.DSN, xsql.DataPlaneMaxOpen(gapOpts))
	}
	if err != nil {
		slog.Error("failed to connect postgres database", slog.Any("err", err))
		return nil, err
	}

	stor := NewStorage(gapOpts, o, pool.DB)
	if err = pool.EnsureSchema(o.Schema, stor.init); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func init() {
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginPostgres, version)
}
