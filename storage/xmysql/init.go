package xmysql

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/storage"
)

const (
	maxOpenConns    = 25
	maxIdleConns    = 25
	connMaxLifetime = 5 * time.Minute
)

type Factory struct{}

func (f Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	sp := gapOpts.StorageOptions
	if sp == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if sp.PluginType() != enum.PluginMySQL {
		return nil, errors.New("storage plugin does not match")
	}

	opts := sp.(*Options)
	db, err := openMySQL(gapOpts.Context, opts.DSN)
	if err != nil {
		slog.Error("failed to connect mysql database", slog.Any("err", err))
		return nil, err
	}
	stor := NewStorage(gapOpts, db)
	if err = stor.init(); err != nil {
		_ = db.Close()
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func openMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("mysql dsn is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	configurePool(db)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func configurePool(db *sql.DB) {
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
}

func init() {
	plugin.Register[storage.FactoryIface](enum.PluginMySQL, &Factory{})
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginMySQL, version)
}
