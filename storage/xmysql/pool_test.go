package xmysql

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

	"github.com/loopopen/gap/internal/gap"
)

func TestDataPlaneMaxOpen(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	opts := &gap.Options{
		MaxPublishConcurrency: 4,
		WorkConcurrencyFactor: 2,
		HandlerOptsLst: []gap.HandlerOptions{
			{GroupOptions: gap.GroupOptions{Group: "a", IngestConcurrency: 3}},
			{GroupOptions: gap.GroupOptions{Group: "a", IngestConcurrency: 1}},
			{GroupOptions: gap.GroupOptions{Group: "b", IngestConcurrency: 0}},
		},
	}
	// group a contributes 3, group b contributes 1 (serial ingest).
	want := dataPlaneClaims + 4 + procs*2 + 4 + dataPlaneHeadroom
	if got := dataPlaneMaxOpen(opts); got != want {
		t.Fatalf("max open = %d, want %d", got, want)
	}
	if dataPlaneMaxIdle >= want {
		t.Fatalf("idle %d is not below max open %d", dataPlaneMaxIdle, want)
	}

	opts.MaxPublishConcurrency = 0
	opts.WorkConcurrencyFactor = 0
	opts.HandlerOptsLst = nil
	want = dataPlaneClaims + 1 + 1 + dataPlaneHeadroom
	if got := dataPlaneMaxOpen(opts); got != want {
		t.Fatalf("serial max open = %d, want %d", got, want)
	}
}

func TestDataPlanePoolSharedAndRaised(t *testing.T) {
	resetPools(t)
	stubOpen(t)

	ctx := context.Background()
	first, err := dataPool(ctx, "dsn-a", 6)
	if err != nil {
		t.Fatal(err)
	}
	second, err := dataPool(ctx, "dsn-a", 12)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.db != second.db {
		t.Fatal("same dsn opened two data-plane pools")
	}
	if got := first.db.Stats().MaxOpenConnections; got != 12 {
		t.Fatalf("max open = %d", got)
	}
	shrunk, err := dataPool(ctx, "dsn-a", 8)
	if err != nil {
		t.Fatal(err)
	}
	if shrunk != first {
		t.Fatal("limit shrink replaced the pool")
	}
	if got := first.db.Stats().MaxOpenConnections; got != 12 {
		t.Fatalf("max open after shrink attempt = %d", got)
	}

	other, err := dataPool(ctx, "dsn-b", 6)
	if err != nil {
		t.Fatal(err)
	}
	if other.db == first.db {
		t.Fatal("different dsn shared a pool")
	}
}

func TestDashboardPoolIsSeparateAndSmall(t *testing.T) {
	resetPools(t)
	stubOpen(t)

	ctx := context.Background()
	data, err := dataPool(ctx, "dsn-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	board, err := dashboardPool(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	if board.db == data.db {
		t.Fatal("dashboard shares the data-plane pool")
	}
	if got := board.db.Stats().MaxOpenConnections; got != dashboardMaxOpen {
		t.Fatalf("dashboard max open = %d", got)
	}
	again, err := dashboardPool(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	if again.db != board.db {
		t.Fatal("dashboard pool was reopened")
	}
}

func TestPoolOpenErrorIsNotCached(t *testing.T) {
	resetPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	var opens atomic.Int32
	openPoolDB = func(context.Context, string, poolRole) (*sql.DB, error) {
		opens.Add(1)
		return nil, errors.New("down")
	}
	for range 2 {
		db, err := dataPool(context.Background(), "dsn-down", 6)
		if err == nil || db != nil {
			t.Fatalf("pool=%v err=%v", db, err)
		}
	}
	if got := opens.Load(); got != 2 {
		t.Fatalf("opens = %d", got)
	}
}

func TestEnsureSchemaRetriesAfterFailure(t *testing.T) {
	pool := &connPool{schemas: map[string]struct{}{}}
	var calls int
	if err := pool.ensureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.ensureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.ensureSchema("other", func() error {
		calls++
		return errors.New("boom")
	}); err == nil {
		t.Fatal("expected init error")
	}
	if err := pool.ensureSchema("other", func() error {
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
	resetPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	openPoolDB = func(context.Context, string, poolRole) (*sql.DB, error) {
		time.Sleep(20 * time.Millisecond)
		return sql.Open("mysql", "root:@tcp(127.0.0.1:1)/gap")
	}

	var wg sync.WaitGroup
	got := make([]*connPool, 8)
	errs := make([]error, len(got))
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = dataPool(context.Background(), "dsn-race", 6)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range got[1:] {
		if p != got[0] {
			t.Fatal("concurrent open did not share one pool")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := got[0].db.PingContext(ctx)
	if err != nil && strings.Contains(err.Error(), "database is closed") {
		t.Fatal(err)
	}
}

func TestDashboardLimitSQL(t *testing.T) {
	if got, want := dashboardLimitSQL(), "SET SESSION max_execution_time = 10000"; got != want {
		t.Fatalf("sql = %q", got)
	}
}

func TestTimeoutConnectorSetsLimit(t *testing.T) {
	conn := &fakeConn{}
	got, err := (timeoutConnector{Connector: fakeConnector{conn: conn}}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != conn {
		t.Fatal("returned a different connection")
	}
	if conn.query != dashboardLimitSQL() {
		t.Fatalf("query = %q", conn.query)
	}
	if conn.closed {
		t.Fatal("closed a usable connection")
	}
}

func TestTimeoutConnectorClosesOnExecError(t *testing.T) {
	conn := &fakeConn{execErr: errors.New("unknown variable")}
	_, err := (timeoutConnector{Connector: fakeConnector{conn: conn}}).Connect(context.Background())
	if err == nil {
		t.Fatal("expected exec error")
	}
	if !conn.closed {
		t.Fatal("connection left open after setup failure")
	}
}

func TestOpenDashboardEmptyDSN(t *testing.T) {
	db, err := openDashboard(context.Background(), " ")
	if err == nil || db != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
}

func stubOpen(t *testing.T) {
	t.Helper()
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })
	openPoolDB = func(context.Context, string, poolRole) (*sql.DB, error) {
		return sql.Open("mysql", "root:@tcp(127.0.0.1:1)/gap")
	}
}

func resetPools(t *testing.T) {
	t.Helper()
	clearPools()
	t.Cleanup(clearPools)
}

func clearPools() {
	pools.Range(func(key, value any) bool {
		pools.Delete(key)
		if pool, ok := value.(*connPool); ok && pool.db != nil {
			_ = pool.db.Close()
		}
		return true
	})
}

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
