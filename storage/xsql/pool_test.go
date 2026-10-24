package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ingap "github.com/loopopen/gap/internal/gap"
)

func TestDataPlaneMaxOpen(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	opts := &ingap.Options{
		MaxPublishConcurrency: 4,
		WorkConcurrencyFactor: 2,
		HandlerOptsLst: []ingap.HandlerOptions{
			{GroupOptions: ingap.GroupOptions{Group: "a", IngestConcurrency: 3}},
			{GroupOptions: ingap.GroupOptions{Group: "a", IngestConcurrency: 1}},
			{GroupOptions: ingap.GroupOptions{Group: "b", IngestConcurrency: 0}},
		},
	}
	// group a contributes 3, group b contributes 1 (serial ingest).
	want := dataPlaneClaims + 4 + procs*2 + 4 + dataPlaneHeadroom
	if got := DataPlaneMaxOpen(opts); got != want {
		t.Fatalf("max open = %d, want %d", got, want)
	}
	if dataPlaneMaxIdle >= want {
		t.Fatalf("idle %d is not below max open %d", dataPlaneMaxIdle, want)
	}

	opts.MaxPublishConcurrency = 0
	opts.WorkConcurrencyFactor = 0
	opts.HandlerOptsLst = nil
	want = dataPlaneClaims + 1 + 1 + dataPlaneHeadroom
	if got := DataPlaneMaxOpen(opts); got != want {
		t.Fatalf("serial max open = %d, want %d", got, want)
	}
}

func TestDataPlanePoolSharedAndRaised(t *testing.T) {
	reg := NewRegistry(stubOpener(t))
	ctx := context.Background()
	first, err := reg.Data(ctx, "dsn-a", 6)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reg.Data(ctx, "dsn-a", 12)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.DB != second.DB {
		t.Fatal("same dsn opened two data-plane pools")
	}
	if got := first.DB.Stats().MaxOpenConnections; got != 12 {
		t.Fatalf("max open = %d", got)
	}
	shrunk, err := reg.Data(ctx, "dsn-a", 8)
	if err != nil {
		t.Fatal(err)
	}
	if shrunk != first {
		t.Fatal("limit shrink replaced the pool")
	}
	if got := first.DB.Stats().MaxOpenConnections; got != 12 {
		t.Fatalf("max open after shrink attempt = %d", got)
	}

	other, err := reg.Data(ctx, "dsn-b", 6)
	if err != nil {
		t.Fatal(err)
	}
	if other.DB == first.DB {
		t.Fatal("different dsn shared a pool")
	}
	reg.Reset()
}

func TestDashboardPoolIsSeparateAndSmall(t *testing.T) {
	reg := NewRegistry(stubOpener(t))
	ctx := context.Background()
	data, err := reg.Data(ctx, "dsn-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	board, err := reg.Dashboard(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	if board.DB == data.DB {
		t.Fatal("dashboard shares the data-plane pool")
	}
	if got := board.DB.Stats().MaxOpenConnections; got != dashboardMaxOpen {
		t.Fatalf("dashboard max open = %d", got)
	}
	again, err := reg.Dashboard(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	if again.DB != board.DB {
		t.Fatal("dashboard pool was reopened")
	}
	reg.Reset()
}

func TestPoolOpenErrorIsNotCached(t *testing.T) {
	var opens atomic.Int32
	reg := NewRegistry(func(context.Context, string, Role) (*sql.DB, error) {
		opens.Add(1)
		return nil, errors.New("down")
	})
	for range 2 {
		pool, err := reg.Data(context.Background(), "dsn-down", 6)
		if err == nil || pool != nil {
			t.Fatalf("pool=%v err=%v", pool, err)
		}
	}
	if got := opens.Load(); got != 2 {
		t.Fatalf("opens = %d", got)
	}
}

func TestEnsureSchemaRetriesAfterFailure(t *testing.T) {
	pool := &Pool{schemas: map[string]struct{}{}}
	var calls int
	if err := pool.EnsureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.EnsureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.EnsureSchema("other", func() error {
		calls++
		return errors.New("boom")
	}); err == nil {
		t.Fatal("expected init error")
	}
	if err := pool.EnsureSchema("other", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("init calls = %d", calls)
	}
}

func TestConcurrentPoolOpen(t *testing.T) {
	reg := NewRegistry(func(context.Context, string, Role) (*sql.DB, error) {
		time.Sleep(20 * time.Millisecond)
		return sql.Open("xsqltest", "dsn")
	})
	t.Cleanup(reg.Reset)

	var wg sync.WaitGroup
	got := make([]*Pool, 8)
	errs := make([]error, len(got))
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = reg.Data(context.Background(), "dsn-race", 6)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, pool := range got[1:] {
		if pool != got[0] {
			t.Fatal("concurrent open did not share one pool")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := got[0].DB.PingContext(ctx)
	if err != nil && strings.Contains(err.Error(), "database is closed") {
		t.Fatal(err)
	}
}

func TestOpenEmptyDSN(t *testing.T) {
	db, err := Open(context.Background(), "mysql", " ")
	if err == nil || db != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
}

func TestSetupConnectorSetsSQL(t *testing.T) {
	conn := &fakeConn{}
	got, err := (SetupConnector{Connector: fakeConnector{conn: conn}, SQL: "SET x"}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != conn {
		t.Fatal("returned a different connection")
	}
	if conn.query != "SET x" {
		t.Fatalf("query = %q", conn.query)
	}
	if conn.closed {
		t.Fatal("closed a usable connection")
	}
}

func TestSetupConnectorClosesOnExecError(t *testing.T) {
	conn := &fakeConn{execErr: errors.New("unknown variable")}
	_, err := (SetupConnector{Connector: fakeConnector{conn: conn}, SQL: "SET x"}).Connect(context.Background())
	if err == nil {
		t.Fatal("expected exec error")
	}
	if !conn.closed {
		t.Fatal("connection left open after setup failure")
	}
}

func stubOpener(t *testing.T) Opener {
	t.Helper()
	return func(context.Context, string, Role) (*sql.DB, error) {
		return sql.Open("xsqltest", "dsn")
	}
}

func init() {
	sql.Register("xsqltest", nopDriver{})
}

type nopDriver struct{}

func (nopDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

type fakeConnector struct {
	conn driver.Conn
}

func (f fakeConnector) Connect(context.Context) (driver.Conn, error) { return f.conn, nil }

func (f fakeConnector) Driver() driver.Driver { return nil }

type fakeConn struct {
	query   string
	execErr error
	closed  bool
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("unused") }

func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.query = query
	if c.execErr != nil {
		return nil, c.execErr
	}
	return emptyResult{}, nil
}

type emptyResult struct{}

func (emptyResult) LastInsertId() (int64, error) { return 0, nil }

func (emptyResult) RowsAffected() (int64, error) { return 0, nil }
