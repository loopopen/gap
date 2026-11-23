package xmysql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage/xsql"
)

func TestIntegrationPoolOwnership(t *testing.T) {
	dsn := integrationDSN(t, "GAP_TEST_MYSQL_DSN")
	resetPools(t)

	admin, err := sql.Open(xsql.MySQL, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	drop := "DROP TABLE IF EXISTS `gap_pool_own_published`, `gap_pool_own_received`"
	if _, err := admin.Exec(drop); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(drop) })

	assertPoolOwnership(t, dsn, "gap_pool_own")
}

func integrationDSN(t *testing.T, env string) string {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv(env)); dsn != "" {
		return dsn
	}
	if os.Getenv("GAP_STORAGE_CONTRACT") == "1" {
		t.Fatalf("%s is required when GAP_STORAGE_CONTRACT=1", env)
	}
	t.Skipf("set %s to run the pool ownership test", env)
	return ""
}

func assertPoolOwnership(t *testing.T, dsn, schema string) {
	t.Helper()
	opts := &Options{Schema: schema, DSN: dsn}
	gapOpts := new(ingap.Options).With()
	gapOpts.InitRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := opts.OpenStorage(gapOpts, xsql.RoleData)
	if err != nil {
		t.Fatal(err)
	}
	second, err := opts.OpenStorage(gapOpts, xsql.RoleData)
	if err != nil {
		t.Fatal(err)
	}
	dashboard, err := opts.OpenStorage(gapOpts, xsql.RoleDashboard)
	if err != nil {
		t.Fatal(err)
	}
	s1 := first.(*Storage)
	s2 := second.(*Storage)
	board := dashboard.(*Storage)
	if s1.pool.DB != s2.pool.DB {
		t.Fatal("same dsn opened two data-plane pools")
	}
	if board.pool.DB == s1.pool.DB {
		t.Fatal("dashboard shares the data-plane pool")
	}
	if err := s2.pool.DB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s2.pool.DB.PingContext(ctx); err != nil {
		t.Fatalf("pool closed while another runtime still holds it: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	err = s1.pool.DB.PingContext(ctx)
	if err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("last runtime close: %v", err)
	}

	reopened, err := opts.OpenStorage(gapOpts, xsql.RoleData)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.(*Storage).Close() })
	t.Cleanup(func() { _ = board.Close() })
	again := reopened.(*Storage)
	if again.pool.DB == s1.pool.DB {
		t.Fatal("reopen reused a closed pool")
	}
	if err := again.pool.DB.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}
