package xmysql

import (
	"strings"
	"testing"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
)

func TestClaimIndexSQL(t *testing.T) {
	sql, err := claimIndexSQL("gap_published", "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "CREATE INDEX `idx_published_claim` ON `gap_published` (`version`, `status`, `retries`, `created_at`)" {
		t.Fatal(sql)
	}
	if _, err := claimIndexSQL("gap`published", "idx_published_claim"); err == nil {
		t.Fatal("accepted injected table name")
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
	en := &entity.Envelope{
		ID:      4,
		Topic:   "t",
		Message: map[string]string{"a": "b"},
		Headers: map[string]string{"h": "v"},
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
}
