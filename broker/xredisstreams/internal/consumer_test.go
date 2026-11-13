package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	ingap "github.com/loopopen/gap/internal/gap"
)

func testGapOpts() *ingap.Options {
	entity.MustInitSnowflake(1)
	opts := &ingap.Options{Version: "v-test"}
	opts.InitRuntime()
	return opts
}

func entry(id, payload string) streamEntry {
	values, err := streamValues(map[string]string{"gap-msg-id": id}, []byte(payload))
	if err != nil {
		panic(err)
	}
	return streamEntry{Stream: "orders", ID: id, Values: values}
}

func testConsumer(fake *fakeStreams) *Consumer {
	return &Consumer{
		gapOpts:          testGapOpts(),
		opts:             &Options{},
		client:           fake,
		group:            "workers",
		groupID:          "gap.v-test.g.workers",
		consumer:         "consumer-1",
		startID:          streamStartNew,
		state:            consumerSubscribed,
		topics:           []string{"orders"},
		readRetryDelay:   time.Millisecond,
		rejectRetryDelay: time.Millisecond,
		commitRetryMin:   time.Millisecond,
		commitRetryMax:   2 * time.Millisecond,
		claimMinIdle:     time.Hour,
		blockTimeout:     30 * time.Millisecond,
		prefetch:         10,
	}
}

func receiveEnvelope(t *testing.T, envelopes <-chan *entity.Envelope) *entity.Envelope {
	t.Helper()
	select {
	case envelope, ok := <-envelopes:
		if !ok {
			t.Fatal("envelope channel closed unexpectedly")
		}
		return envelope
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for envelope")
		return nil
	}
}

func waitClosed(t *testing.T, envelopes <-chan *entity.Envelope) {
	t.Helper()
	select {
	case _, ok := <-envelopes:
		if ok {
			t.Fatal("received unexpected envelope while waiting for close")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for envelope channel to close")
	}
}

func TestRejectRedeliversWithoutAck(t *testing.T) {
	fake := newFakeStreams()
	fake.fresh = []streamEntry{entry("10-0", "payload")}
	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	first := receiveEnvelope(t, envelopes)
	firstTag := first.Tag
	if err = consumer.Reject(firstTag); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	second := receiveEnvelope(t, envelopes)
	if second == first {
		t.Fatal("redelivery should use a new envelope")
	}
	if second.ID != first.ID || string(second.Payload) != "payload" || second.Headers["gap-msg-id"] != "10-0" {
		t.Fatalf("redelivery = id %d payload %q headers %v", second.ID, second.Payload, second.Headers)
	}
	if second.Tag == firstTag {
		t.Fatal("redelivery should use a new delivery tag")
	}
	if err = consumer.Commit(firstTag); !errors.Is(err, errDeliverySettled) {
		t.Fatalf("late Commit error = %v, want %v", err, errDeliverySettled)
	}
	if err = consumer.Commit(second.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reads, acks, _ := fake.snapshot()
	if len(acks) != 1 || acks[0] != "10-0" {
		t.Fatalf("acks = %v, want one ack for 10-0", acks)
	}
	if len(reads) < 1 || reads[0] != readPending {
		t.Fatalf("reads = %v, want pending read first", reads)
	}

	cancel()
	waitClosed(t, envelopes)
}

func TestPendingIsDeliveredBeforeNew(t *testing.T) {
	fake := newFakeStreams()
	fake.pending = []streamEntry{entry("1-0", "pending")}
	fake.fresh = []streamEntry{entry("2-0", "fresh")}
	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	pending := receiveEnvelope(t, envelopes)
	if string(pending.Payload) != "pending" {
		t.Fatalf("first payload = %q, want pending", pending.Payload)
	}
	reads, _, _ := fake.snapshot()
	for _, id := range reads {
		if id == readNew {
			t.Fatal("new entries were read before the pending entry was settled")
		}
	}
	if err = consumer.Commit(pending.Tag); err != nil {
		t.Fatalf("Commit pending: %v", err)
	}

	fresh := receiveEnvelope(t, envelopes)
	if string(fresh.Payload) != "fresh" {
		t.Fatalf("second payload = %q, want fresh", fresh.Payload)
	}
	if err = consumer.Commit(fresh.Tag); err != nil {
		t.Fatalf("Commit fresh: %v", err)
	}
	_, acks, _ := fake.snapshot()
	if len(acks) != 2 || acks[0] != "1-0" || acks[1] != "2-0" {
		t.Fatalf("acks = %v", acks)
	}
}

func TestCommitRetriesWithoutLosingTheDelivery(t *testing.T) {
	fake := newFakeStreams()
	fake.fresh = []streamEntry{entry("4-0", "payload")}
	fake.ackErrs = []error{errors.New("temporary ack failure")}
	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, acks, _ := fake.snapshot()
	if len(acks) != 2 || acks[0] != "4-0" || acks[1] != "4-0" {
		t.Fatalf("acks = %v, want two attempts", acks)
	}
	cancel()
	waitClosed(t, envelopes)
}

func TestCommitAfterIntakeStops(t *testing.T) {
	fake := newFakeStreams()
	fake.fresh = []streamEntry{entry("5-0", "payload")}
	consumer := testConsumer(fake)
	intakeCtx, stopIntake := context.WithCancel(context.Background())
	envelopes, err := consumer.Consume(intakeCtx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	stopIntake()
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit after intake stopped: %v", err)
	}
	_, acks, _ := fake.snapshot()
	if len(acks) != 1 || acks[0] != "5-0" {
		t.Fatalf("acks = %v", acks)
	}
	waitClosed(t, envelopes)
}

func TestRejectsInvalidAndDuplicateOperations(t *testing.T) {
	consumer := testConsumer(newFakeStreams())
	if err := consumer.Commit("10-0"); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Commit error = %v, want %v", err, errInvalidDeliveryTag)
	}
	if err := consumer.Reject(nil); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Reject error = %v, want %v", err, errInvalidDeliveryTag)
	}
}

func TestConsumeMayOnlyStartOnce(t *testing.T) {
	consumer := testConsumer(newFakeStreams())
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("first Consume: %v", err)
	}
	if _, err = consumer.Consume(ctx); !errors.Is(err, errConsumerAlreadyConsuming) {
		t.Fatalf("second Consume error = %v, want %v", err, errConsumerAlreadyConsuming)
	}
	if err = consumer.Subscribe(ctx, "other"); !errors.Is(err, errSubscribeStarted) {
		t.Fatalf("Subscribe after Consume error = %v, want %v", err, errSubscribeStarted)
	}
	cancel()
	waitClosed(t, envelopes)
}

func TestSubscribeRecordsGroupStart(t *testing.T) {
	fake := newFakeStreams()
	consumer := testConsumer(fake)
	consumer.state = consumerCreated
	consumer.topics = nil
	consumer.startID = streamStartBeginning
	if err := consumer.Subscribe(t.Context(), "orders"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := consumer.Subscribe(t.Context(), "orders"); err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	_, _, groups := fake.snapshot()
	if groups["orders\x00"+consumer.groupID] != streamStartBeginning {
		t.Fatalf("groups = %v", groups)
	}
	if len(consumer.topics) != 1 {
		t.Fatalf("topics = %v", consumer.topics)
	}
}

func TestConsumeRequiresSubscription(t *testing.T) {
	consumer := testConsumer(newFakeStreams())
	consumer.topics = nil
	consumer.state = consumerCreated
	if _, err := consumer.Consume(t.Context()); !errors.Is(err, errNoTopics) {
		t.Fatalf("Consume error = %v, want %v", err, errNoTopics)
	}
}

func TestSeveralDeliveriesCanBeUnsettled(t *testing.T) {
	fake := newFakeStreams()
	fake.fresh = []streamEntry{entry("1-0", "one"), entry("2-0", "two")}
	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	first := receiveEnvelope(t, envelopes)
	second := receiveEnvelope(t, envelopes)
	if string(first.Payload) == string(second.Payload) {
		t.Fatalf("payloads = %q and %q", first.Payload, second.Payload)
	}
	if err = consumer.Commit(second.Tag); err != nil {
		t.Fatalf("Commit second: %v", err)
	}
	if err = consumer.Commit(first.Tag); err != nil {
		t.Fatalf("Commit first: %v", err)
	}
	_, acks, _ := fake.snapshot()
	if len(acks) != 2 {
		t.Fatalf("acks = %v, want both deliveries acknowledged", acks)
	}
	cancel()
	waitClosed(t, envelopes)
}

func TestClaimDeliversIdleEntry(t *testing.T) {
	fake := newFakeStreams()
	fake.claims = []streamEntry{entry("8-0", "claimed")}
	consumer := testConsumer(fake)
	consumer.claimMinIdle = time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if string(envelope.Payload) != "claimed" {
		t.Fatalf("payload = %q", envelope.Payload)
	}
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_, acks, _ := fake.snapshot()
	if len(acks) != 1 || acks[0] != "8-0" {
		t.Fatalf("acks = %v", acks)
	}
	cancel()
	waitClosed(t, envelopes)
}

func TestUndecodableEntryIsAcknowledged(t *testing.T) {
	fake := newFakeStreams()
	fake.fresh = []streamEntry{
		{Stream: "orders", ID: "9-0", Values: map[string]any{fieldBody: "raw"}},
		entry("10-0", "ok"),
	}
	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if string(envelope.Payload) != "ok" {
		t.Fatalf("payload = %q, want the decodable entry", envelope.Payload)
	}
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	var acks []string
	for {
		_, acks, _ = fake.snapshot()
		if containsAck(acks, "9-0") && containsAck(acks, "10-0") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acks = %v, want the undecodable entry and the payload acknowledged", acks)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	waitClosed(t, envelopes)
}

func containsAck(acks []string, id string) bool {
	for _, ack := range acks {
		if ack == id {
			return true
		}
	}
	return false
}

func TestClaimUnsupportedDisablesFurtherClaims(t *testing.T) {
	fake := newFakeStreams()
	fake.claimErr = errors.New("ERR unknown command 'XAUTOCLAIM'")
	fake.fresh = []streamEntry{entry("3-0", "payload")}
	consumer := testConsumer(fake)
	consumer.claimMinIdle = time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	envelope := receiveEnvelope(t, envelopes)
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if consumer.claimMinIdle != 0 {
		t.Fatal("unsupported claim should be disabled")
	}
	cancel()
	waitClosed(t, envelopes)
}
