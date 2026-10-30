package xpostgres

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage/xsql"
)

func TestQuotedTable(t *testing.T) {
	s := &Storage{opts: &Options{Schema: "gap"}}
	table, err := s.quotedTable("published")
	if err != nil {
		t.Fatal(err)
	}
	if table != `"gap"."published"` {
		t.Fatal(table)
	}
	s.opts.Schema = `gap"; drop schema public; --`
	if _, err := s.quotedTable("published"); err == nil {
		t.Fatal("accepted injected schema")
	}
}

func TestCreateSQL(t *testing.T) {
	published := publishedCreateSQL(`"gap"."published"`, `"gap"."status_enum"`)
	if strings.Contains(published, `"group"`) {
		t.Fatal(published)
	}
	if !strings.Contains(published, `"retries" integer NOT NULL DEFAULT 0`) {
		t.Fatal(published)
	}
	if !strings.Contains(published, `"status" "gap"."status_enum" NOT NULL`) {
		t.Fatal(published)
	}
	for _, column := range []string{`"lease_token" varchar(32)`, `"lease_until" timestamptz`, `"next_attempt_at" timestamptz NOT NULL`} {
		if !strings.Contains(published, column) {
			t.Fatalf("missing %q in %s", column, published)
		}
	}

	received := receivedCreateSQL(`"gap"."received"`, `"gap"."status_enum"`)
	if !strings.Contains(received, `"group" varchar(128) NOT NULL`) {
		t.Fatal(received)
	}
}

func TestClaimIndexSQL(t *testing.T) {
	sql, err := claimIndexSQL(`"gap"."published"`, "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE INDEX IF NOT EXISTS "idx_published_claim" ON "gap"."published" ("version", "status", "next_attempt_at", "lease_until", "retries")`
	if sql != want {
		t.Fatal(sql)
	}
	if _, err := claimIndexSQL(`"gap"."published"`, `idx"; drop`); err == nil {
		t.Fatal("accepted injected index")
	}
}

func TestFailExhaustedSQL(t *testing.T) {
	sql := failExhaustedSQL(`"gap"."published"`, `"gap"."status_enum"`)
	for _, condition := range []string{
		`UPDATE "gap"."published"`,
		`"status" = $1::"gap"."status_enum"`,
		`"lease_token" = NULL`,
		`"lease_until" = NULL`,
		`"version" = $2`,
		`"status" = $3::"gap"."status_enum"`,
		`"next_attempt_at" <= $4`,
		`"retries" >= $5`,
		`"lease_until" IS NULL OR "lease_until" <= $6`,
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("fail exhausted SQL %q does not contain %q", sql, condition)
		}
	}
}

func TestUpdateStatusSQLWritesStatusDirectly(t *testing.T) {
	sql := updateStatusSQL(`"gap"."received"`, `"gap"."status_enum"`)
	if strings.Contains(sql, "CASE") || strings.Contains(sql, "next_attempt_at") || strings.Contains(sql, "retries") {
		t.Fatalf("update status still contains retry semantics: %s", sql)
	}
	if !strings.Contains(sql, `SET "status" = $1::"gap"."status_enum"`) {
		t.Fatalf("update status does not write the supplied status directly: %s", sql)
	}
}

func TestStatusTypeSQL(t *testing.T) {
	sql := statusTypeSQL("gap", `"gap"`)
	if !strings.Contains(sql, `n.nspname = 'gap'`) {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, `CREATE TYPE "gap"."status_enum"`) {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, `AS ENUM ('Pending', 'Succeeded', 'Failed')`) {
		t.Fatal(sql)
	}
}

func TestWherePlaceholders(t *testing.T) {
	h := NewWhereHelper()
	Add(h, `"status" = ?`, enum.StatusFailed)
	Add(h, `"topic" = ?`, "orders")
	Add(h, `"headers" LIKE ? ESCAPE '!'`, `%"tenant-id":"customer-42"%`)
	if h.String() != `WHERE "status" = $1 AND "topic" = $2 AND "headers" LIKE $3 ESCAPE '!'` {
		t.Fatal(h.String())
	}
}

func TestOpenPostgresEmptyDSN(t *testing.T) {
	for _, dsn := range []string{"", " ", "\t"} {
		db, err := openPostgres(context.Background(), dsn)
		if err == nil || db != nil {
			t.Fatalf("dsn %q: db=%v err=%v", dsn, db, err)
		}
		if !strings.Contains(err.Error(), "empty") {
			t.Fatal(err)
		}
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

func TestOpenPostgresPingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openPostgres(ctx, "postgres://root@127.0.0.1:1/gap?sslmode=disable&connect_timeout=1")
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

func TestDashboardLimitSQL(t *testing.T) {
	if got, want := dashboardLimitSQL(), "SET statement_timeout = 10000"; got != want {
		t.Fatalf("sql = %q", got)
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

func TestCreateStorageKeepsPoolWhenInitFails(t *testing.T) {
	resetPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	var opens atomic.Int32
	var opened *sql.DB
	openPoolDB = func(context.Context, string, xsql.Role) (*sql.DB, error) {
		opens.Add(1)
		db, err := sql.Open("pgx", "postgres://postgres@127.0.0.1:1/gap?sslmode=disable&connect_timeout=1")
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

func resetPools(t *testing.T) {
	t.Helper()
	registry.Reset()
	t.Cleanup(registry.Reset)
}

func TestToEntityInvalidHeaders(t *testing.T) {
	p := &Published{ID: 7, Headers: "{"}
	en := p.ToEntity()
	if p.err == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestFromEntityPayloadAndHeaders(t *testing.T) {
	now := time.Now()
	en := &entity.Envelope{
		ID:            4,
		Topic:         "t",
		Message:       map[string]string{"a": "b"},
		Headers:       map[string]string{"h": "v"},
		LeaseUntil:    now.Add(time.Minute),
		NextAttemptAt: now,
	}
	p := new(Published).FromEntity(en)
	if p.err != nil {
		t.Fatal(p.err)
	}
	if p.Payload != `{"a":"b"}` {
		t.Fatalf("payload = %q", p.Payload)
	}
	if p.Headers != `{"h":"v"}` {
		t.Fatalf("headers = %q", p.Headers)
	}
	if p.Status != enum.StatusPending {
		t.Fatalf("status = %s", p.Status)
	}
	if !p.LeaseUntil.Valid || !p.LeaseUntil.V.Equal(en.LeaseUntil) {
		t.Fatalf("lease until = %v, want %v", p.LeaseUntil, en.LeaseUntil)
	}
	if !p.NextAttemptAt.Equal(en.NextAttemptAt) {
		t.Fatalf("next attempt at = %v, want %v", p.NextAttemptAt, en.NextAttemptAt)
	}
}
