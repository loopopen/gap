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

func TestFailExhaustedSQL(t *testing.T) {
	sql := failExhaustedSQL("`gap_published`")
	for _, condition := range []string{
		"UPDATE `gap_published`",
		"`status` = ?",
		"`lease_token` = NULL",
		"`lease_until` = NULL",
		"`version` = ?",
		"`status` = ?",
		"`next_attempt_at` <= ?",
		"`retries` >= ?",
		"`lease_until` IS NULL OR `lease_until` <= ?",
	} {
		if !strings.Contains(sql, condition) {
			t.Fatalf("fail exhausted SQL %q does not contain %q", sql, condition)
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
