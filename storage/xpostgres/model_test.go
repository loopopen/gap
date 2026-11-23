package xpostgres

import (
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	dbmodel "github.com/loopopen/gap/storage/xpostgres/internal/model"
)

func TestToEntityInvalidHeaders(t *testing.T) {
	p := &dbmodel.Published{ID: 7, Headers: "{"}
	en := p.ToEntity()
	if p.Err() == nil {
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
		BrokerName:    "orders",
		Message:       map[string]string{"a": "b"},
		Headers:       map[string]string{"h": "v"},
		LeaseUntil:    now.Add(time.Minute),
		NextAttemptAt: now,
	}
	p := new(dbmodel.Published).FromEntity(en)
	if p.Err() != nil {
		t.Fatal(p.Err())
	}
	if p.Payload != `{"a":"b"}` {
		t.Fatalf("payload = %q", p.Payload)
	}
	if p.Headers != `{"h":"v"}` {
		t.Fatalf("headers = %q", p.Headers)
	}
	if p.BrokerName != "orders" {
		t.Fatalf("broker name = %q", p.BrokerName)
	}
	if got := p.ToEntity().BrokerName; got != "orders" {
		t.Fatalf("broker name = %q", got)
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
