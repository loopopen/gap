package xsql

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

const (
	// Published claims, received claims, and expired-message cleanup can each
	// hold one data-plane connection.
	dataPlaneWorkers = 3
	// Spare slots for the one-shot startup repair and brief overlap.
	dataPlaneHeadroom = 2
	dataPlaneMaxIdle  = 4

	dashboardMaxOpen = 4
	dashboardMaxIdle = 2

	// DashboardMaxExecution bounds a dashboard query.
	// MySQL uses max_execution_time, MariaDB uses max_statement_time,
	// Postgres uses statement_timeout.
	DashboardMaxExecution = 10 * time.Second

	connMaxLifetime = 5 * time.Minute
	connMaxIdleTime = time.Minute
)

// Role selects which pool of a DSN a caller borrows.
type Role = storage.Role

const (
	RoleData      = storage.RoleData
	RoleDashboard = storage.RoleDashboard
)

type poolKey struct {
	dsn  string
	role Role
}

// Opener dials one *sql.DB. The registry applies pool limits after it returns.
type Opener func(ctx context.Context, dsn string, role Role) (*sql.DB, error)

// Registry shares one pool per DSN and role. Each Open borrows a reference.
// The last Release closes that pool and drops it, so a later Open dials again.
type Registry struct {
	open  Opener
	mu    sync.Mutex
	pools map[poolKey]*Pool
}

// NewRegistry creates a SQL connection-pool registry.
func NewRegistry(open Opener) *Registry {
	return &Registry{
		open:  open,
		pools: map[poolKey]*Pool{},
	}
}

// Open borrows the pool for a DSN and role. The dashboard pool uses a fixed small limit.
// The data-plane pool uses maxOpen and only raises that limit on later calls.
// Release the returned pool once the caller is done with it.
func (r *Registry) Open(ctx context.Context, dsn string, role Role, maxOpen int) (*Pool, error) {
	maxIdle := dataPlaneMaxIdle
	if role == RoleDashboard {
		maxOpen = dashboardMaxOpen
		maxIdle = dashboardMaxIdle
	}
	return r.load(ctx, poolKey{dsn: dsn, role: role}, maxOpen, maxIdle)
}

// Data borrows the shared data-plane pool, raising its open limit when maxOpen is higher.
func (r *Registry) Data(ctx context.Context, dsn string, maxOpen int) (*Pool, error) {
	return r.Open(ctx, dsn, RoleData, maxOpen)
}

// Dashboard borrows the small read pool for a DSN.
func (r *Registry) Dashboard(ctx context.Context, dsn string) (*Pool, error) {
	return r.Open(ctx, dsn, RoleDashboard, 0)
}

// Reset closes and drops every pool. Tests use it to isolate global registries.
func (r *Registry) Reset() {
	r.mu.Lock()
	pools := r.pools
	r.pools = map[poolKey]*Pool{}
	r.mu.Unlock()
	for _, pool := range pools {
		if pool.DB != nil {
			_ = pool.DB.Close()
		}
	}
}

func (r *Registry) load(ctx context.Context, key poolKey, maxOpen, maxIdle int) (*Pool, error) {
	if maxOpen < 1 {
		maxOpen = 1
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}

	r.mu.Lock()
	if pool := r.pools[key]; pool != nil {
		pool.refs++
		r.mu.Unlock()
		pool.grow(maxOpen)
		return pool, nil
	}
	r.mu.Unlock()

	db, err := r.open(ctx, key.dsn, key.role)
	if err != nil {
		return nil, err
	}
	pool := newPool(db, maxOpen, maxIdle)
	pool.reg = r
	pool.key = key
	pool.refs = 1

	r.mu.Lock()
	if existing := r.pools[key]; existing != nil {
		existing.refs++
		r.mu.Unlock()
		_ = db.Close()
		existing.grow(maxOpen)
		return existing, nil
	}
	r.pools[key] = pool
	r.mu.Unlock()
	return pool, nil
}

func (r *Registry) release(pool *Pool) error {
	if pool == nil {
		return nil
	}
	r.mu.Lock()
	if pool.refs == 0 {
		r.mu.Unlock()
		return nil
	}
	pool.refs--
	closeDB := false
	if pool.refs == 0 {
		if current := r.pools[pool.key]; current == pool {
			delete(r.pools, pool.key)
		}
		closeDB = true
	}
	db := pool.DB
	r.mu.Unlock()
	if !closeDB || db == nil {
		return nil
	}
	return db.Close()
}

// Pool is one database/sql pool plus the schemas it has already initialized.
type Pool struct {
	DB      *sql.DB
	maxOpen int
	refs    int
	reg     *Registry
	key     poolKey

	mu      sync.Mutex
	schemas map[string]struct{}
}

func newPool(db *sql.DB, maxOpen, maxIdle int) *Pool {
	configurePool(db, maxOpen, maxIdle)
	return &Pool{
		DB:      db,
		maxOpen: maxOpen,
		schemas: map[string]struct{}{},
	}
}

// Release returns one borrow. The last borrow closes the pool.
// An extra call after the last borrow does nothing.
func (p *Pool) Release() error {
	if p == nil || p.reg == nil {
		return nil
	}
	return p.reg.release(p)
}

// ConfigureDashboardPool applies the shared dashboard connection limits.
func ConfigureDashboardPool(db *sql.DB) {
	configurePool(db, dashboardMaxOpen, dashboardMaxIdle)
}

// ConfigureDataPool applies the shared data-plane connection limits.
func ConfigureDataPool(db *sql.DB, maxOpen int) {
	if maxOpen < 1 {
		maxOpen = 1
	}
	configurePool(db, maxOpen, dataPlaneMaxIdle)
}

func configurePool(db *sql.DB, maxOpen, maxIdle int) {
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)
}

func (p *Pool) grow(maxOpen int) {
	if maxOpen < 1 || p.DB == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if maxOpen <= p.maxOpen {
		return
	}
	p.maxOpen = maxOpen
	p.DB.SetMaxOpenConns(maxOpen)
}

// EnsureSchema runs init once per schema. A failed init stays retryable.
func (p *Pool) EnsureSchema(schema string, init func() error) error {
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

// DataPlaneMaxOpen sizes the shared pool to the connections the pipeline can
// hold at once: two claim transactions, one cleanup batch, publish updates,
// handler updates, and a little headroom.
// A later subscriber raises the limit; it is never lowered.
func DataPlaneMaxOpen(opts *ingap.Options) int {
	publish := opts.MaxPublishConcurrency
	if publish < 1 {
		publish = 1
	}
	handle := runtime.GOMAXPROCS(0) * opts.WorkConcurrencyFactor
	if handle < 1 {
		handle = 1
	}
	return dataPlaneWorkers + publish + handle + dataPlaneHeadroom
}
