package xgorm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	ingap "github.com/loopopen/gap/internal/gap"
	"gorm.io/gorm"
)

const poolTestDriverName = "xgorm-pool-test"

func TestOpenDashboardDBCreatesIsolatedPool(t *testing.T) {
	dataDB := openPoolTestDB(t, poolTestDialector{})
	dataSQLDB, _ := dataDB.DB()
	defer dataSQLDB.Close()

	dashboardDB, isolated, err := openDashboardDB(&Options{DB: dataDB}, dataDB)
	if err != nil {
		t.Fatal(err)
	}
	if !isolated {
		t.Fatal("dashboard pool was not isolated")
	}
	dashboardSQLDB, err := dashboardDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer dashboardSQLDB.Close()
	if dashboardSQLDB == dataSQLDB {
		t.Fatal("dashboard reused the data-plane sql db")
	}
	if got := dashboardSQLDB.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("dashboard max open = %d, want 4", got)
	}
}

func TestOpenDashboardDBDetectsSharedPool(t *testing.T) {
	shared, err := sql.Open(poolTestDriverName, "shared")
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()

	dataDB := openPoolTestDB(t, poolTestDialector{shared: shared})
	dashboardDB, isolated, err := openDashboardDB(&Options{DB: dataDB}, dataDB)
	if err == nil {
		t.Fatal("expected shared pool error")
	}
	if isolated {
		t.Fatal("shared pool reported as isolated")
	}
	if dashboardDB != dataDB {
		t.Fatal("shared pool did not fall back to the data-plane db")
	}
}

func TestOpenDashboardDBUsesConfiguredDialector(t *testing.T) {
	dataDB := openPoolTestDB(t, poolTestDialector{})
	dataSQLDB, _ := dataDB.DB()
	defer dataSQLDB.Close()

	var gotDSN string
	opts := new(Options).With(
		DB(dataDB),
		DSN("dashboard-dsn", func(dsn string) gorm.Dialector {
			gotDSN = dsn
			return poolTestDialector{}
		}),
	)
	dashboardDB, isolated, err := openDashboardDB(opts, dataDB)
	if err != nil {
		t.Fatal(err)
	}
	if !isolated {
		t.Fatal("configured dialector did not create an isolated pool")
	}
	if gotDSN != "dashboard-dsn" {
		t.Fatalf("dialector dsn = %q", gotDSN)
	}
	dashboardSQLDB, _ := dashboardDB.DB()
	defer dashboardSQLDB.Close()
}

func TestLoadDataFromDSNAndDialector(t *testing.T) {
	var gotDSN string
	opts := new(Options).With(DSN(
		"data-dsn",
		func(dsn string) gorm.Dialector {
			gotDSN = dsn
			return poolTestDialector{}
		},
	))
	db, err := new(gormPools).loadData(&ingap.Options{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if gotDSN != "data-dsn" {
		t.Fatalf("dialector dsn = %q", gotDSN)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if got := sqlDB.Stats().MaxOpenConnections; got < 1 {
		t.Fatalf("data max open = %d", got)
	}
}

func TestPoolsForUsesDSNOrDBIdentity(t *testing.T) {
	resetGormPools(t)

	var calls atomic.Int32
	dial := func(string) gorm.Dialector {
		calls.Add(1)
		return poolTestDialector{}
	}
	sameA := new(Options).With(DSN("same-dsn", dial))
	sameB := new(Options).With(DSN("same-dsn", dial))
	other := new(Options).With(DSN("other-dsn", dial))
	if poolsFor(sameA) != poolsFor(sameB) {
		t.Fatal("same dsn used different pools")
	}
	if poolsFor(sameA) == poolsFor(other) {
		t.Fatal("different dsns shared a pool")
	}

	gapOpts := &ingap.Options{}
	if _, err := poolsFor(sameA).loadData(gapOpts, sameA); err != nil {
		t.Fatal(err)
	}
	if _, err := poolsFor(sameB).loadData(gapOpts, sameB); err != nil {
		t.Fatal(err)
	}
	if _, err := poolsFor(other).loadData(gapOpts, other); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("dialector calls = %d, want 2", got)
	}

	db := openPoolTestDB(t, poolTestDialector{})
	sqlDB, _ := db.DB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	another := openPoolTestDB(t, poolTestDialector{})
	anotherSQL, _ := another.DB()
	t.Cleanup(func() { _ = anotherSQL.Close() })
	if poolsFor(&Options{DB: db}) != poolsFor(&Options{DB: db}) {
		t.Fatal("same gorm db used different pools")
	}
	if poolsFor(&Options{DB: db}) == poolsFor(&Options{DB: another}) {
		t.Fatal("different gorm dbs shared a pool")
	}
}

func resetGormPools(t *testing.T) {
	t.Helper()
	gormPoolCache.Range(func(key, _ any) bool {
		gormPoolCache.Delete(key)
		return true
	})
	t.Cleanup(func() {
		gormPoolCache.Range(func(key, _ any) bool {
			gormPoolCache.Delete(key)
			return true
		})
	})
}

func TestGormOptionsRequiresOneConnectionMode(t *testing.T) {
	db := openPoolTestDB(t, poolTestDialector{})
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	for _, test := range []struct {
		name    string
		opts    *Options
		wantErr bool
	}{
		{name: "db", opts: &Options{DB: db}},
		{name: "managed", opts: new(Options).With(DSN("dsn", func(string) gorm.Dialector { return poolTestDialector{} }))},
		{name: "both", opts: new(Options).With(DB(db), DSN("dsn", func(string) gorm.Dialector { return poolTestDialector{} })), wantErr: true},
		{name: "neither", opts: &Options{}, wantErr: true},
		{name: "partial", opts: &Options{dsn: "dsn"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func openPoolTestDB(t *testing.T, dialector gorm.Dialector) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(dialector, &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

type poolTestDialector struct {
	nopDialector
	shared *sql.DB
}

func (d poolTestDialector) Initialize(db *gorm.DB) error {
	if d.shared != nil {
		db.ConnPool = d.shared
		return nil
	}
	pool, err := sql.Open(poolTestDriverName, "new")
	if err != nil {
		return err
	}
	db.ConnPool = pool
	return nil
}

type poolTestDriver struct{}

func (poolTestDriver) Open(string) (driver.Conn, error) { return poolTestConn{}, nil }

type poolTestConn struct{}

func (poolTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (poolTestConn) Close() error { return nil }

func (poolTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func init() {
	sql.Register(poolTestDriverName, poolTestDriver{})
}
