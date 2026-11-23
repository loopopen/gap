package xgorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"gorm.io/gorm"
)

func TestOpenStorageReleasesPoolWhenInitFails(t *testing.T) {
	resetGormPools(t)
	var mu sync.Mutex
	var opened []*sql.DB
	opts := &Options{
		Schema: "bad-schema",
		dsn:    "gorm-init-fail",
		dialector: func(string) gorm.Dialector {
			return &poolTestDialector{onOpen: func(db *sql.DB) {
				mu.Lock()
				opened = append(opened, db)
				mu.Unlock()
			}}
		},
	}
	gapOpts := &ingap.Options{StorageOptions: opts}
	gapOpts.InitRuntime()
	for range 2 {
		_, err := opts.OpenStorage(gapOpts, storage.RoleData)
		if err == nil {
			t.Fatal("expected init to fail")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != 2 {
		t.Fatalf("opens = %d", len(opened))
	}
	for _, db := range opened {
		err := db.PingContext(context.Background())
		if err == nil || !strings.Contains(err.Error(), "database is closed") {
			t.Fatalf("pool left open: %v", err)
		}
	}
}

func TestValidateRequiresDSNAndDialector(t *testing.T) {
	cases := []*Options{
		{},
		{dsn: "gap"},
		{dialector: func(string) gorm.Dialector { return &poolTestDialector{} }},
	}
	for _, opts := range cases {
		err := opts.validate()
		if err == nil || !strings.Contains(err.Error(), "dsn and dialector") {
			t.Fatal(err)
		}
	}
	err := (&Options{
		dsn:       "gap",
		dialector: func(string) gorm.Dialector { return &poolTestDialector{} },
	}).validate()
	if err != nil {
		t.Fatal(err)
	}
}

func resetGormPools(t *testing.T) {
	t.Helper()
	registry.Reset()
	dialectorFactories.Range(func(key, _ any) bool {
		dialectorFactories.Delete(key)
		return true
	})
	t.Cleanup(func() {
		registry.Reset()
		dialectorFactories.Range(func(key, _ any) bool {
			dialectorFactories.Delete(key)
			return true
		})
	})
}

type poolTestDialector struct {
	nopDialector
	Conn   gorm.ConnPool
	onOpen func(*sql.DB)
}

func (d *poolTestDialector) Initialize(db *gorm.DB) error {
	if d.Conn != nil {
		db.ConnPool = d.Conn
		return nil
	}
	pool, err := sql.Open(poolTestDriverName, "new")
	if err != nil {
		return err
	}
	if d.onOpen != nil {
		d.onOpen(pool)
	}
	db.ConnPool = pool
	return nil
}

const poolTestDriverName = "xgorm-pool-test"

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
