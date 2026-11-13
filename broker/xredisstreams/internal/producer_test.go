package internal

import (
	"strings"
	"testing"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
)

func testProducer(fake *fakeStreams, opts *Options) *Producer {
	if opts == nil {
		opts = &Options{}
	}
	return &Producer{
		gapOpts: &ingap.Options{Version: "v-test"},
		opts:    opts,
		client:  fake,
	}
}

func TestProduceWritesHeadersAndBody(t *testing.T) {
	entity.MustInitSnowflake(1)
	fake := newFakeStreams()
	producer := testProducer(fake, &Options{MaxLen: 1000})
	envelope := entity.NewEnvelope("v-test", "orders", nil).WithPayload([]byte("payload"))
	envelope.AddHeader("gap-msg-id", "7")

	if err := producer.Produce(t.Context(), envelope); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.adds) != 1 {
		t.Fatalf("adds = %d, want 1", len(fake.adds))
	}
	added := fake.adds[0]
	if added.stream != "orders" || added.maxLen != 1000 {
		t.Fatalf("add = %+v", added)
	}
	headers, body, err := entryMessage(added.values)
	if err != nil {
		t.Fatalf("entryMessage: %v", err)
	}
	if headers["gap-msg-id"] != "7" || string(body) != "payload" {
		t.Fatalf("headers=%v body=%q", headers, body)
	}
}

func TestProduceRejectsInvalidEnvelopes(t *testing.T) {
	entity.MustInitSnowflake(1)
	producer := testProducer(newFakeStreams(), nil)
	if err := producer.Produce(t.Context(), nil); !errx.IsPermanent(err) {
		t.Fatalf("nil envelope error = %v", err)
	}
	if err := producer.Produce(t.Context(), entity.NewEnvelope("v-test", "", nil)); !errx.IsPermanent(err) {
		t.Fatalf("empty topic error = %v", err)
	}
	if err := producer.Produce(t.Context(), entity.NewEnvelope("v-test", "orders", nil)); !errx.IsPermanent(err) {
		t.Fatalf("empty payload error = %v", err)
	}
}

func TestConfigIDHidesCredentialsAndFollowsConnectionIdentity(t *testing.T) {
	sameA := &Options{Addrs: []string{"b:6379", "a:6379"}, Password: "secret", DB: 2}
	sameB := &Options{Addrs: []string{"a:6379", "b:6379"}, Password: "secret", DB: 2}
	otherPassword := &Options{Addrs: []string{"a:6379", "b:6379"}, Password: "other", DB: 2}
	otherDB := &Options{Addrs: []string{"a:6379", "b:6379"}, Password: "secret", DB: 3}

	if sameA.ConfigID() != sameB.ConfigID() {
		t.Fatal("address order changed the config id")
	}
	if sameA.ConfigID() == otherPassword.ConfigID() || sameA.ConfigID() == otherDB.ConfigID() {
		t.Fatal("different connection settings shared a config id")
	}
	if sameA.ConfigID() == "" || strings.Contains(sameA.ConfigID(), "secret") {
		t.Fatalf("config id = %q", sameA.ConfigID())
	}
}
