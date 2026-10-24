package xmysql

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage/xsql"
)

func TestOpenMySQLEmptyDSN(t *testing.T) {
	for _, dsn := range []string{"", " ", "\t"} {
		db, err := openMySQL(context.Background(), dsn)
		if err == nil || db != nil {
			t.Fatalf("dsn %q: db=%v err=%v", dsn, db, err)
		}
		if !strings.Contains(err.Error(), "empty") {
			t.Fatal(err)
		}
	}
}

func TestOpenMySQLPingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openMySQL(ctx, "root:@tcp(127.0.0.1:1)/gap?timeout=1s")
	if err == nil || db != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
}

func TestCreateStorageEmptyDSN(t *testing.T) {
	storOpts := &Options{Schema: "gap"}
	_, err := storOpts.OpenStorage(&ingap.Options{
		Context:        context.Background(),
		StorageOptions: storOpts,
	}, xsql.RoleData)
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
}

func resetPools(t *testing.T) {
	t.Helper()
	registry.Reset()
	t.Cleanup(registry.Reset)
}

func TestCreateStorageKeepsPoolWhenInitFails(t *testing.T) {
	resetPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	var opens atomic.Int32
	var opened *sql.DB
	openPoolDB = func(context.Context, string, xsql.Role) (*sql.DB, error) {
		opens.Add(1)
		db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:1)/gap?timeout=1s")
		opened = db
		return db, err
	}

	storOpts := &Options{
		Schema: "gap",
		DSN:    "dsn-init-fail",
	}
	opts := &ingap.Options{
		Context:        context.Background(),
		StorageOptions: storOpts,
	}
	for range 2 {
		_, err := storOpts.OpenStorage(opts, xsql.RoleData)
		if err == nil {
			t.Fatal("expected init to fail")
		}
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("opens = %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := opened.PingContext(ctx)
	if err != nil && strings.Contains(err.Error(), "database is closed") {
		t.Fatal(err)
	}
}
