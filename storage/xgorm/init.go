package xgorm

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"

	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/internal/xsql"
	"gorm.io/gorm"
)

var (
	registry           = xsql.NewRegistry(openGormSQL)
	dialectorFactories sync.Map
)

// OpenStorage opens and initializes the configured storage.
func (o *Options) OpenStorage(gapOpts *ingap.Options, role storage.Role) (storage.Storage, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	dsn := strings.TrimSpace(o.dsn)
	rememberDialector(dsn, o.dialector)

	pool, err := registry.Open(gapOpts.IntakeContext(), dsn, role, xsql.DataPlaneMaxOpen(gapOpts))
	if err != nil {
		slog.Error("failed to connect gorm database", slog.Any("err", err))
		return nil, err
	}
	dialector, err := dialectorFor(dsn)
	if err != nil {
		_ = pool.Release()
		return nil, err
	}
	db, err := openOnPool(dialector, pool.DB)
	if err != nil {
		_ = pool.Release()
		slog.Error("failed to open gorm database", slog.Any("err", err))
		return nil, err
	}

	stor := newStorage(gapOpts, o, db)
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

func (o *Options) validate() error {
	if strings.TrimSpace(o.dsn) == "" || o.dialector == nil {
		return errors.New("gorm dsn and dialector must be configured together")
	}
	return nil
}

func rememberDialector(dsn string, factory DialectorFactory) {
	dialectorFactories.Store(dsn, factory)
}

func dialectorFor(dsn string) (gorm.Dialector, error) {
	value, ok := dialectorFactories.Load(dsn)
	if !ok {
		return nil, errors.New("gorm dialector is not configured")
	}
	dialector := value.(DialectorFactory)(dsn)
	if dialector == nil {
		return nil, errors.New("gorm dialector is nil")
	}
	return dialector, nil
}

func openGormSQL(ctx context.Context, dsn string, _ xsql.Role) (*sql.DB, error) {
	dialector, err := dialectorFor(dsn)
	if err != nil {
		return nil, err
	}
	gdb, err := gorm.Open(dialector)
	if err != nil {
		return nil, err
	}
	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, err
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return sqlDB, nil
}

func openOnPool(dialector gorm.Dialector, db *sql.DB) (*gorm.DB, error) {
	if err := setDialectorConn(dialector, db); err != nil {
		return nil, err
	}
	return gorm.Open(dialector)
}

// setDialectorConn points a mysql or postgres dialector at an existing pool.
// Those drivers read Config.Conn and ignore gorm.Config.ConnPool.
func setDialectorConn(dialector gorm.Dialector, db *sql.DB) error {
	target := connField(reflect.ValueOf(dialector))
	if !target.IsValid() || !target.CanSet() {
		return errors.New("gorm dialector cannot reuse an existing pool")
	}
	target.Set(reflect.ValueOf(db))
	return nil
}

func connField(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return reflect.Value{}
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	if field := v.FieldByName("Config"); field.IsValid() {
		if nested := connField(field); nested.IsValid() && nested.CanSet() {
			return nested
		}
	}
	field := v.FieldByName("Conn")
	if field.IsValid() && field.CanSet() {
		return field
	}
	return reflect.Value{}
}

func init() {
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginGORM, version)
}
