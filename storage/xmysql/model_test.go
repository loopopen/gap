package xmysql

import (
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
)

func TestClaimIndexSQL(t *testing.T) {
	sql, err := claimIndexSQL("gap_published", "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "CREATE INDEX `idx_published_claim` ON `gap_published` (`version`, `status`, `next_attempt_at`, `lease_until`, `retries`)" {
		t.Fatal(sql)
	}
	if _, err := claimIndexSQL("gap`published", "idx_published_claim"); err == nil {
		t.Fatal("accepted injected table name")
	}
}

func TestCleanupIndexSQL(t *testing.T) {
	sql, err := cleanupIndexSQL("gap_published", "idx_published_cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "CREATE INDEX `idx_published_cleanup` ON `gap_published` (`expired_at`, `id`)" {
		t.Fatal(sql)
	}
}

func TestFailExhaustedSQL(t *testing.T) {
	sql := failExhaustedSQL("`gap_published`")
	assertPendingDueWhere(t, sql)
	for _, condition := range []string{
		"UPDATE `gap_published`",
		"SET `status` = ?",
		"`expired_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))",
		"`lease_token` = NULL",
		"`lease_until` = NULL",
		"`retries` >= ?",
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("fail exhausted SQL %q does not contain %q", sql, condition)
		}
	}
	if strings.Contains(sql, "`retries` < ?") {
		t.Fatalf("fail exhausted SQL %q also claims retries", sql)
	}
}

func TestClaimSelectSQL(t *testing.T) {
	published := claimPublishedSQL("`gap_published`")
	assertPendingDueWhere(t, published)
	for _, condition := range []string{
		"SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` FROM `gap_published`",
		"`retries` < ?",
		"LIMIT ? FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(published, condition) {
			t.Fatalf("claim SQL %q does not contain %q", published, condition)
		}
	}
	if strings.Contains(published, "`retries` >= ?") || strings.Contains(published, "`group`") {
		t.Fatalf("published claim SQL %q includes the wrong clause", published)
	}

	received := claimReceivedSQL("`gap_received`")
	assertPendingDueWhere(t, received)
	for _, condition := range []string{
		"`expired_at`,`group` FROM `gap_received`",
		"`group` = ?",
	} {
		if !strings.Contains(received, condition) {
			t.Fatalf("received claim SQL %q does not contain %q", received, condition)
		}
	}

	exhausted := failExhaustedReceivedSQL("`gap_received`")
	assertPendingDueWhere(t, exhausted)
	for _, condition := range []string{
		"`retries` >= ?",
		"`group` = ?",
	} {
		if !strings.Contains(exhausted, condition) {
			t.Fatalf("received exhausted SQL %q does not contain %q", exhausted, condition)
		}
	}
}

func assertPendingDueWhere(t *testing.T, sql string) {
	t.Helper()
	for _, condition := range []string{
		"`version` = ?",
		"`status` = ?",
		"`next_attempt_at` <= ?",
		"`lease_until` IS NULL OR `lease_until` <= ?",
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("SQL %q does not contain %q", sql, condition)
		}
	}
}

func TestRetryPublishedSQLKeepsStatus(t *testing.T) {
	sql := retryAttemptSQL("`gap_published`")
	if strings.Contains(sql, "`status` = ?") && strings.Contains(sql, "SET `status`") {
		t.Fatalf("retry SQL changes status: %s", sql)
	}
	for _, condition := range []string{
		"UPDATE `gap_published`",
		"`lease_token` = NULL",
		"`lease_until` = NULL",
		"`next_attempt_at` = ?",
		"`id` = ?",
		"`status` = ?",
		"`lease_token` = ?",
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("retry SQL %q does not contain %q", sql, condition)
		}
	}
}

func TestUpdateStatusSQLWritesStatusDirectly(t *testing.T) {
	sql := updateStatusSQL("`gap_received`")
	if strings.Contains(sql, "IF(") || strings.Contains(sql, "next_attempt_at") || strings.Contains(sql, "retries") {
		t.Fatalf("update status still contains retry semantics: %s", sql)
	}
	if !strings.Contains(sql, "SET `status` = ?") {
		t.Fatalf("update status does not write the supplied status directly: %s", sql)
	}
	if !strings.Contains(sql, "`expired_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))") {
		t.Fatalf("update status does not set expiration: %s", sql)
	}
}

func TestDeleteExpiredSQLOnlyDeletesTerminalRowsInBatches(t *testing.T) {
	sql := deleteExpiredSQL("`gap_published`")
	for _, condition := range []string{
		"`expired_at` <= CURRENT_TIMESTAMP(3)",
		"`status` IN (?, ?)",
		"ORDER BY `expired_at`, `id`",
		"LIMIT ? FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("cleanup SQL %q does not contain %q", sql, condition)
		}
	}
}

func TestPublishedCreateSQLIncludesLeaseColumns(t *testing.T) {
	sql := publishedCreateSQL("`gap_published`")
	if !strings.Contains(sql, "`status` "+statusColumnType+" NOT NULL") {
		t.Fatalf("unexpected status column in %s", sql)
	}
	for _, column := range []string{"`lease_token` char(32) NULL", "`lease_until` datetime(3) NULL", "`next_attempt_at` datetime(3) NOT NULL"} {
		if !strings.Contains(sql, column) {
			t.Fatalf("missing %q in %s", column, sql)
		}
	}
}

func TestQuotedTable(t *testing.T) {
	s := &Storage{opts: &Options{Schema: "gap"}}
	table, err := s.quotedTable("published")
	if err != nil {
		t.Fatal(err)
	}
	if table != "`gap_published`" {
		t.Fatal(table)
	}
	s.opts.Schema = "gap`; drop table x; --"
	if _, err := s.quotedTable("published"); err == nil {
		t.Fatal("accepted injected schema")
	}
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

	r := &Received{ID: 8, Headers: "{", Group: "g"}
	en = r.ToEntity()
	if r.err == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestToEntityHeaders(t *testing.T) {
	p := &Published{ID: 7, Headers: `{"k":"v"}`}
	en := p.ToEntity()
	if p.err != nil {
		t.Fatal(p.err)
	}
	if en.Headers["k"] != "v" {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestFromEntityRejectsUnmarshalablePayload(t *testing.T) {
	en := &entity.Envelope{ID: 3, Message: make(chan int)}
	p := new(Published).FromEntity(en)
	if p.err == nil {
		t.Fatal("expected encode error")
	}
	if p.Payload != "" {
		t.Fatalf("payload = %q", p.Payload)
	}
	if !strings.Contains(p.err.Error(), "encode payload") {
		t.Fatal(p.err)
	}

	r := new(Received).FromEntity(en)
	if r.err == nil {
		t.Fatal("expected encode error")
	}
	if r.Payload != "" {
		t.Fatalf("payload = %q", r.Payload)
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
