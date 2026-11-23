package xpostgres

import (
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/internal/xsql"
)

// OpenStorage opens and initializes the configured storage.
func (o *Options) OpenStorage(gapOpts *ingap.Options, role storage.Role) (storage.Storage, error) {
	pool, err := registry.Open(gapOpts.IntakeContext(), o.DSN, role, xsql.DataPlaneMaxOpen(gapOpts))
	if err != nil {
		slog.Error("failed to connect postgres database", slog.Any("err", err))
		return nil, err
	}

	stor := newStorage(gapOpts, o, pool.DB)
	stor.pool = pool
	if err = pool.EnsureSchema(o.Schema, stor.init); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		_ = stor.Close()
		return nil, err
	}
	return stor, nil
}

// Close returns this storage's borrow of the shared pool.
// The last borrow closes the pool. A second call does nothing.
func (s *storageAdapter) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		err = s.pool.Release()
	})
	return err
}

func init() {
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginPostgres, version)
}
