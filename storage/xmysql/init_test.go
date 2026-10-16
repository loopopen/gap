package xmysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
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
	_, err := Factory{}.CreateStorage(&gap.Options{
		Context: context.Background(),
		StorageOptions: &Options{
			Schema: "gap",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
}

func TestMySQLPoolLimits(t *testing.T) {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:1)/gap")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	configurePool(db)
	if got := db.Stats().MaxOpenConnections; got != maxOpenConns {
		t.Fatalf("max open connections = %d", got)
	}
}

func TestCreateStoragePluginMismatch(t *testing.T) {
	_, err := Factory{}.CreateStorage(&gap.Options{
		StorageOptions: mismatchPlugin{},
	})
	if err == nil {
		t.Fatal("expected plugin mismatch")
	}
}

type mismatchPlugin struct{}

func (mismatchPlugin) PluginType() enum.Plugin { return enum.PluginGORM }
