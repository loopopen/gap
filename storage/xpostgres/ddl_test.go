package xpostgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/loopopen/gap/internal/enum"
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
	for _, column := range []string{`"broker_name" varchar(128) NOT NULL`, `"lease_token" varchar(32)`, `"lease_until" timestamptz`, `"next_attempt_at" timestamptz NOT NULL`} {
		if !strings.Contains(published, column) {
			t.Fatalf("missing %q in %s", column, published)
		}
	}

	received := receivedCreateSQL(`"gap"."received"`, `"gap"."status_enum"`)
	if !strings.Contains(received, `"group" varchar(128) NOT NULL`) || !strings.Contains(received, `"broker_name" varchar(128) NOT NULL`) {
		t.Fatal(received)
	}
}

func TestClaimIndexSQL(t *testing.T) {
	sql, err := claimIndexSQL(`"gap"."published"`, "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE INDEX IF NOT EXISTS "idx_published_claim" ON "gap"."published" ("version", "status", "broker_name", "next_attempt_at", "lease_until", "retries")`
	if sql != want {
		t.Fatal(sql)
	}
	if _, err := claimIndexSQL(`"gap"."published"`, `idx"; drop`); err == nil {
		t.Fatal("accepted injected index")
	}
}

func TestCleanupIndexSQL(t *testing.T) {
	sql, err := cleanupIndexSQL(`"gap"."published"`, "idx_published_cleanup")
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE INDEX IF NOT EXISTS "idx_published_cleanup" ON "gap"."published" ("expired_at", "id") WHERE "expired_at" IS NOT NULL`
	if sql != want {
		t.Fatal(sql)
	}
}

func TestFailExhaustedSQL(t *testing.T) {
	sql := failExhaustedPublishedSQL(`"gap"."published"`, `"gap"."status_enum"`)
	assertPendingDueWhere(t, sql, 3)
	for _, condition := range []string{
		`UPDATE "gap"."published"`,
		`"status" = $1::"gap"."status_enum"`,
		`"expired_at" = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')`,
		`"lease_token" = NULL`,
		`"lease_until" = NULL`,
		`"retries" >= $6`,
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("fail exhausted SQL %q does not contain %q", sql, condition)
		}
	}
	if strings.Contains(sql, `"retries" <`) {
		t.Fatalf("fail exhausted SQL %q also claims retries", sql)
	}
}

func TestClaimSelectSQL(t *testing.T) {
	published := claimPublishedSQL(`"gap"."published"`, `"gap"."status_enum"`)
	assertPendingDueWhere(t, published, 1)
	for _, condition := range []string{
		`SELECT "id","created_at","version","topic","broker_name","status","headers","payload","retries","expired_at"`,
		`FROM "gap"."published"`,
		`"retries" < $4`,
		"LIMIT $5 FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(published, condition) {
			t.Fatalf("claim SQL %q does not contain %q", published, condition)
		}
	}
	if strings.Contains(published, `"retries" >=`) || strings.Contains(published, `"group"`) {
		t.Fatalf("published claim SQL %q includes the wrong clause", published)
	}

	received := claimReceivedSQL(`"gap"."received"`, `"gap"."status_enum"`)
	assertPendingDueWhere(t, received, 1)
	for _, condition := range []string{
		`"broker_name","status","headers","payload","retries","expired_at","group"`,
		`FROM "gap"."received"`,
		`"group" = $5`,
		"LIMIT $6 FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(received, condition) {
			t.Fatalf("received claim SQL %q does not contain %q", received, condition)
		}
	}

	exhausted := failExhaustedReceivedSQL(`"gap"."received"`, `"gap"."status_enum"`)
	assertPendingDueWhere(t, exhausted, 3)
	for _, condition := range []string{
		`"retries" >= $6`,
		`"group" = $7`,
	} {
		if !strings.Contains(exhausted, condition) {
			t.Fatalf("received exhausted SQL %q does not contain %q", exhausted, condition)
		}
	}
}

func assertPendingDueWhere(t *testing.T, sql string, start int) {
	t.Helper()
	statusType := `"gap"."status_enum"`
	for _, condition := range []string{
		fmt.Sprintf(`"version" = $%d`, start),
		fmt.Sprintf(`"status" = $%d::%s`, start+1, statusType),
		fmt.Sprintf(`"broker_name" = $%d`, start+2),
		`"next_attempt_at" <= CURRENT_TIMESTAMP`,
		`"lease_until" IS NULL OR "lease_until" <= CURRENT_TIMESTAMP`,
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("SQL %q does not contain %q", sql, condition)
		}
	}
}

func TestRetryPublishedSQLKeepsStatus(t *testing.T) {
	sql := retryAttemptSQL(`"gap"."published"`, `"gap"."status_enum"`)
	if strings.Contains(sql, `SET "status"`) {
		t.Fatalf("retry SQL changes status: %s", sql)
	}
	for _, condition := range []string{
		`UPDATE "gap"."published"`,
		`"lease_token" = NULL`,
		`"lease_until" = NULL`,
		`"next_attempt_at" = CURRENT_TIMESTAMP + ($1 * INTERVAL '1 second')`,
		`"id" = $2`,
		`"status" = $3::"gap"."status_enum"`,
		`"lease_token" = $4`,
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("retry SQL %q does not contain %q", sql, condition)
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
	if !strings.Contains(sql, `"expired_at" = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second')`) {
		t.Fatalf("update status does not set expiration: %s", sql)
	}
}

func TestDeleteExpiredSQLOnlyDeletesTerminalRowsInBatches(t *testing.T) {
	sql := deleteExpiredSQL(`"gap"."published"`, `"gap"."status_enum"`)
	for _, condition := range []string{
		`"expired_at" <= CURRENT_TIMESTAMP`,
		`"status" IN ($1::"gap"."status_enum", $2::"gap"."status_enum")`,
		`ORDER BY "expired_at", "id"`,
		`LIMIT $3 FOR UPDATE SKIP LOCKED`,
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("cleanup SQL %q does not contain %q", sql, condition)
		}
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
