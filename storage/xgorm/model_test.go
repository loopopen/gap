package xgorm

import (
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage/internal/xsql"
	dbmodel "github.com/loopopen/gap/storage/xgorm/internal/model"
	"gorm.io/gorm"
)

func TestToEntityInvalidHeaders(t *testing.T) {
	p := &dbmodel.Published{Model: dbmodel.Model{ID: 7, Headers: "{"}}
	en := p.ToEntity()
	if p.Err() == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}

	r := &dbmodel.Received{Model: dbmodel.Model{ID: 8, Headers: "{"}, Group: "g"}
	en = r.ToEntity()
	if r.Err() == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestToEntityHeaders(t *testing.T) {
	p := &dbmodel.Published{Model: dbmodel.Model{ID: 7, Headers: `{"k":"v"}`}}
	en := p.ToEntity()
	if p.Err() != nil {
		t.Fatal(p.Err())
	}
	if en.Headers["k"] != "v" {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestFromEntityRejectsUnmarshalablePayload(t *testing.T) {
	en := &entity.Envelope{ID: 3, Message: make(chan int)}
	p := new(dbmodel.Published).FromEntity(en)
	if p.Err() == nil {
		t.Fatal("expected encode error")
	}
	if p.Payload != "" {
		t.Fatalf("payload = %q", p.Payload)
	}
	if !strings.Contains(p.Err().Error(), "encode payload") {
		t.Fatal(p.Err())
	}

	r := new(dbmodel.Received).FromEntity(en)
	if r.Err() == nil {
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
	if p.Status != dbmodel.Status(enum.StatusPending) {
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
	db, _ := openStorage(t)
	if got := (dbmodel.Status(0)).GormDBDataType(db, nil); got != "ENUM('Pending','Succeeded','Failed')" {
		t.Fatalf("mysql status type = %q", got)
	}
}

func TestPayloadGormDBDataType(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: xsql.MySQL, want: "LONGTEXT"},
		{name: xsql.Postgres, want: "TEXT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := &gorm.DB{Config: &gorm.Config{Dialector: namedDialector{name: test.name}}}
			if got := (dbmodel.Payload("")).GormDBDataType(db, nil); got != test.want {
				t.Fatalf("payload type = %q, want %q", got, test.want)
			}
		})
	}
}

type namedDialector struct {
	nopDialector
	name string
}

func (d namedDialector) Name() string { return d.name }

func TestStatusSQLRoundTrip(t *testing.T) {
	value, err := dbmodel.Status(enum.StatusPending).Value()
	if err != nil {
		t.Fatal(err)
	}
	if value != "Pending" {
		t.Fatalf("value = %#v", value)
	}

	var got dbmodel.Status
	if err := got.Scan([]byte("Succeeded")); err != nil {
		t.Fatal(err)
	}
	if got != dbmodel.Status(enum.StatusSucceeded) {
		t.Fatalf("status = %s", got)
	}
}
