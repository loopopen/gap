package xpostgres

import (
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/loopopen/gap/v2/spi/dashboard"
	"github.com/loopopen/gap/v2/spi/enum"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/loopopen/gap/v2/storage"
	"github.com/loopopen/gap/v2/storage/xsql"
)

func (o *Options) OpenStorage(gapOpts *internalgap.Options, role storage.Role) (storage.Storage, error) {
	pool, err := registry.Open(gapOpts.Context, o.DSN, role, xsql.DataPlaneMaxOpen(gapOpts))
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
