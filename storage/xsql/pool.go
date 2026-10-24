package xsql

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	internalgap "github.com/loopopen/gap/v2/internal/gap"
)

const (
	// Data-plane claim pollers for published and received each hold one connection.
	dataPlaneClaims = 2
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
type Role uint8

const (
	RoleData Role = iota
	RoleDashboard
)

type poolKey struct {
	dsn  string
	role Role
}

// Opener dials one *sql.DB. The registry applies pool limits after it returns.
type Opener func(ctx context.Context, dsn string, role Role) (*sql.DB, error)

// Registry keeps at most two pools per DSN: one for the data plane
// (publisher, pump, subscribers) and one for dashboard reads.
type Registry struct {
	open  Opener
	pools sync.Map
}

func NewRegistry(open Opener) *Registry {
	return &Registry{open: open}
}

// Data returns the shared data-plane pool, raising its open limit when maxOpen is higher.
func (r *Registry) Data(ctx context.Context, dsn string, maxOpen int) (*Pool, error) {
	return r.load(ctx, poolKey{dsn: dsn, role: RoleData}, maxOpen, dataPlaneMaxIdle)
}

// Dashboard returns the small read pool for a DSN.
func (r *Registry) Dashboard(ctx context.Context, dsn string) (*Pool, error) {
	return r.load(ctx, poolKey{dsn: dsn, role: RoleDashboard}, dashboardMaxOpen, dashboardMaxIdle)
}

// Reset closes and drops every pool. Tests use it to isolate global registries.
func (r *Registry) Reset() {
	r.pools.Range(func(key, value any) bool {
		r.pools.Delete(key)
		if pool, ok := value.(*Pool); ok && pool.DB != nil {
			_ = pool.DB.Close()
		}
		return true
	})
}

func (r *Registry) load(ctx context.Context, key poolKey, maxOpen, maxIdle int) (*Pool, error) {
	if maxOpen < 1 {
		maxOpen = 1
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	if v, ok := r.pools.Load(key); ok {
		pool := v.(*Pool)
		pool.grow(maxOpen)
		return pool, nil
	}

	db, err := r.open(ctx, key.dsn, key.role)
	if err != nil {
		return nil, err
	}
	pool := newPool(db, maxOpen, maxIdle)
	actual, loaded := r.pools.LoadOrStore(key, pool)
	if loaded {
		_ = db.Close()
		kept := actual.(*Pool)
		kept.grow(maxOpen)
		return kept, nil
	}
	return pool, nil
}

// Pool is one database/sql pool plus the schemas it has already initialized.
type Pool struct {
	DB      *sql.DB
	maxOpen int

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
	if maxOpen < 1 {
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
// hold at once: two claim transactions, publish updates, handler updates,
// ingest inserts across groups, plus a little headroom.
// A later subscriber raises the limit; it is never lowered.
func DataPlaneMaxOpen(opts *internalgap.Options) int {
	publish := opts.MaxPublishConcurrency
	if publish < 1 {
		publish = 1
	}
	handle := runtime.GOMAXPROCS(0) * opts.WorkConcurrencyFactor
	if handle < 1 {
		handle = 1
	}
	return dataPlaneClaims + publish + handle + ingestSlots(opts) + dataPlaneHeadroom
}

func ingestSlots(opts *internalgap.Options) int {
	perGroup := make(map[string]int)
	for _, h := range opts.HandlerOptsLst {
		n := h.IngestConcurrency
		if n < 1 {
			n = 1
		}
		if cur, ok := perGroup[h.Group]; !ok || n > cur {
			perGroup[h.Group] = n
		}
	}
	total := 0
	for _, n := range perGroup {
		total += n
	}
	return total
}
