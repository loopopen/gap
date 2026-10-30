package xgorm

import (
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
)

func TestToEntityInvalidHeaders(t *testing.T) {
	p := &Published{Model: Model{ID: 7, Headers: "{"}}
	en := p.ToEntity()
	if p.err == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}

	r := &Received{Model: Model{ID: 8, Headers: "{"}, Group: "g"}
	en = r.ToEntity()
	if r.err == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestToEntityHeaders(t *testing.T) {
	p := &Published{Model: Model{ID: 7, Headers: `{"k":"v"}`}}
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
	if p.Status != Status(enum.StatusPending) {
		t.Fatalf("status = %s", p.Status)
	}
	if !p.LeaseUntil.Valid || !p.LeaseUntil.V.Equal(en.LeaseUntil) {
		t.Fatalf("lease until = %v, want %v", p.LeaseUntil, en.LeaseUntil)
	}
	if !p.NextAttemptAt.Equal(en.NextAttemptAt) {
		t.Fatalf("next attempt at = %v, want %v", p.NextAttemptAt, en.NextAttemptAt)
	}
}

func TestStatusGormDBDataType(t *testing.T) {
	if mysqlStatusType != "ENUM('Pending','Succeeded','Failed')" {
		t.Fatalf("mysql status type = %q", mysqlStatusType)
	}
	db, _ := openStorage(t)
	if got := (Status(0)).GormDBDataType(db, nil); got != mysqlStatusType {
		t.Fatalf("mysql status type = %q, want %q", got, mysqlStatusType)
	}
}

func TestStatusSQLRoundTrip(t *testing.T) {
	value, err := Status(enum.StatusPending).Value()
	if err != nil {
		t.Fatal(err)
	}
	if value != "Pending" {
		t.Fatalf("value = %#v", value)
	}

	var status Status
	if err := status.Scan([]byte("Succeeded")); err != nil {
		t.Fatal(err)
	}
	if status != Status(enum.StatusSucceeded) {
		t.Fatalf("status = %s", status)
	}
}
