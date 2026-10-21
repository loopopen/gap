package xmysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/loopopen/gap/internal/gap"
)

const (
	// Claim pollers for published and received each hold one connection.
	dataPlaneClaims = 2
	// Spare slots for the one-shot startup repair and brief overlap.
	dataPlaneHeadroom = 2
	dataPlaneMaxIdle  = 4

	dashboardMaxOpen = 4
	dashboardMaxIdle = 2

	// max_execution_time applies to SELECT only, which is every dashboard query.
	dashboardMaxExecution = 10 * time.Second

	connMaxLifetime = 5 * time.Minute
	connMaxIdleTime = time.Minute
)

type poolRole uint8

const (
	roleData poolRole = iota
	roleDashboard
)

type poolKey struct {
	dsn  string
	role poolRole
}

// pools holds at most two *sql.DB values per DSN: one for the data plane
// (publisher, pump, subscribers) and one for dashboard reads.
var pools sync.Map

type connPool struct {
	db      *sql.DB
	maxOpen int

	mu      sync.Mutex
	schemas map[string]struct{}
}

var openPoolDB = func(ctx context.Context, dsn string, role poolRole) (*sql.DB, error) {
	if role == roleDashboard {
		return openDashboard(ctx, dsn)
	}
	return openMySQL(ctx, dsn)
}

func dataPool(ctx context.Context, dsn string, maxOpen int) (*connPool, error) {
	return loadOrOpen(ctx, poolKey{dsn: dsn, role: roleData}, maxOpen, dataPlaneMaxIdle)
}

func dashboardPool(ctx context.Context, dsn string) (*connPool, error) {
	return loadOrOpen(ctx, poolKey{dsn: dsn, role: roleDashboard}, dashboardMaxOpen, dashboardMaxIdle)
}

func loadOrOpen(ctx context.Context, key poolKey, maxOpen, maxIdle int) (*connPool, error) {
	if maxOpen < 1 {
		maxOpen = 1
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	if v, ok := pools.Load(key); ok {
		p := v.(*connPool)
		p.grow(maxOpen)
		return p, nil
	}

	db, err := openPoolDB(ctx, key.dsn, key.role)
	if err != nil {
		return nil, err
	}
	p := newConnPool(db, maxOpen, maxIdle)
	actual, loaded := pools.LoadOrStore(key, p)
	if loaded {
		_ = db.Close()
		kept := actual.(*connPool)
		kept.grow(maxOpen)
		return kept, nil
	}
	return p, nil
}

func newConnPool(db *sql.DB, maxOpen, maxIdle int) *connPool {
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)
	return &connPool{
		db:      db,
		maxOpen: maxOpen,
		schemas: map[string]struct{}{},
	}
}

func (p *connPool) grow(maxOpen int) {
	if maxOpen < 1 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if maxOpen <= p.maxOpen {
		return
	}
	p.maxOpen = maxOpen
	p.db.SetMaxOpenConns(maxOpen)
}

func (p *connPool) ensureSchema(schema string, init func() error) error {
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

// dataPlaneMaxOpen sizes the shared pool to the connections the pipeline can
// hold at once: two claim transactions, publish updates, handler updates,
// ingest inserts across groups, plus a little headroom.
// A later subscriber raises the limit; it is never lowered.
func dataPlaneMaxOpen(opts *gap.Options) int {
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

func ingestSlots(opts *gap.Options) int {
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
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openDashboard(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("mysql dsn is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	base, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(timeoutConnector{Connector: base})
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func dashboardLimitSQL() string {
	ms := dashboardMaxExecution / time.Millisecond
	return fmt.Sprintf("SET SESSION max_execution_time = %d", ms)
}

// timeoutConnector sets max_execution_time on every new dashboard connection
// so a full-table stats scan or a payload LIKE cannot hold the connection open.
type timeoutConnector struct {
	driver.Connector
}

func (c timeoutConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("mysql connection cannot set max_execution_time")
	}
	if _, err = execer.ExecContext(ctx, dashboardLimitSQL(), nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
