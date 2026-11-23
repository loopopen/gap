package xpostgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDashboardLimitSQL(t *testing.T) {
	if got, want := dashboardLimitSQL(), "SET statement_timeout = 10000"; got != want {
		t.Fatalf("sql = %q", got)
	}
}

func TestOpenDashboardPingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openDashboard(ctx, "postgres://root@127.0.0.1:1/gap?sslmode=disable&connect_timeout=1")
	if err == nil || db != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
	if strings.Contains(err.Error(), "session setup") {
		t.Fatal(err)
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
