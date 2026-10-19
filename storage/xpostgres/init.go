package xpostgres

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	if sp.PluginType() != enum.PluginPostgres {
		return nil, errors.New("storage plugin does not match")
	}

	opts := sp.(*Options)
	pool, err := sharedPool(gapOpts.Context, opts.DSN)
	if err != nil {
		slog.Error("failed to connect postgres database", slog.Any("err", err))
		return nil, err
	}
	stor := NewStorage(gapOpts, pool.db)
	if err = pool.ensureSchema(opts.Schema, stor.init); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

// dsnPools keeps one database/sql pool per DSN. CreateStorage runs for the
// publisher, pump, subscribers, and dashboard, so a new pool each time
// exhausts Postgres max_connections.
var dsnPools sync.Map

type dsnPool struct {
	db      *sql.DB
	mu      sync.Mutex
	schemas map[string]struct{}
}

func sharedPool(ctx context.Context, dsn string) (*dsnPool, error) {
	if v, ok := dsnPools.Load(dsn); ok {
		return v.(*dsnPool), nil
	}
	db, err := openPoolDB(ctx, dsn)
	if err != nil {
		return nil, err
	}
	pool := &dsnPool{db: db, schemas: map[string]struct{}{}}
	actual, loaded := dsnPools.LoadOrStore(dsn, pool)
	if loaded {
		_ = db.Close()
		return actual.(*dsnPool), nil
	}
	return pool, nil
}

func (p *dsnPool) ensureSchema(schema string, init func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.schemas[schema]; ok {
		return nil
	}
	if err := init(); err != nil {
		return err
	}
	p.schemas[schema] = struct{}{}
	return nil
}

var openPoolDB = openPostgres

func openPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := sql.Open("pgx", dsn)
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
	plugin.Register[storage.FactoryIface](enum.PluginPostgres, &Factory{})
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginPostgres, version)
}
