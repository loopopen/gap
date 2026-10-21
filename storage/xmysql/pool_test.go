package xmysql

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDashboardLimitSQL(t *testing.T) {
	if got, want := dashboardLimitSQL("8.0.36"), "SET SESSION max_execution_time = 10000"; got != want {
		t.Fatalf("mysql sql = %q", got)
	}
	if got, want := dashboardLimitSQL("11.8.2-MariaDB-ubu2404"), "SET SESSION max_statement_time = 10"; got != want {
		t.Fatalf("mariadb sql = %q", got)
	}
}

func TestOpenDashboardPingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openDashboard(ctx, "root:@tcp(127.0.0.1:1)/gap?timeout=1s")
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
