package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/segmentio/kafka-go"
)

type fetchResult struct {
	message kafka.Message
	err     error
}

type fakeKafkaReader struct {
	fetches chan fetchResult

	mu           sync.Mutex
	commits      []kafka.Message
	commitErrors []error
	closeCount   int
}

func newFakeKafkaReader() *fakeKafkaReader {
	return &fakeKafkaReader{fetches: make(chan fetchResult, 1)}
}

func (f *fakeKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case result := <-f.fetches:
		return result.message, result.err
	}
}

func (f *fakeKafkaReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, messages...)
	if len(f.commitErrors) == 0 {
		return nil
	}
	err := f.commitErrors[0]
	f.commitErrors = f.commitErrors[1:]
	return err
}

func (f *fakeKafkaReader) Close() error {
	f.mu.Lock()
	f.closeCount++
	f.mu.Unlock()
	return nil
}

func (f *fakeKafkaReader) snapshot() (commits []kafka.Message, closeCount int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.Message(nil), f.commits...), f.closeCount
}

func testConsumer(fake *fakeKafkaReader) *Consumer {
	entity.MustInitSnowflake(1)
	return &Consumer{
		gapOpts:          &ingap.Options{Version: "v-test"},
		group:            "workers",
		groupID:          "gap.v-test.g.workers",
		state:            consumerSubscribed,
		topics:           []string{"orders"},
		newReader:        func([]string) kafkaReader { return fake },
		readRetryDelay:   time.Millisecond,
		rejectRetryDelay: time.Millisecond,
		commitRetryMin:   time.Millisecond,
		commitRetryMax:   2 * time.Millisecond,
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

func TestReaderRejectRedeliversWithoutReplacingReader(t *testing.T) {
	fake := newFakeKafkaReader()
	fake.fetches <- fetchResult{message: kafka.Message{
		Topic:     "orders",
		Partition: 2,
		Offset:    10,
		Value:     []byte("payload"),
	}}
	r := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := r.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	first := receiveEnvelope(t, envelopes)
	firstTag := first.Tag
	if err := r.Reject(firstTag); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	second := receiveEnvelope(t, envelopes)
	if second == first {
		t.Fatal("redelivery should use an immutable attempt envelope")
	}
	if second.ID != first.ID || string(second.Payload) != string(first.Payload) {
		t.Fatal("reject should preserve the logical envelope identity and payload")
	}
	if second.Tag == firstTag {
		t.Fatal("redelivery should use a new delivery tag")
	}
	if err := r.Commit(firstTag); !errors.Is(err, errDeliverySettled) {
		t.Fatalf("late Commit error = %v, want %v", err, errDeliverySettled)
	}

	if err := r.Commit(second.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	commits, closeCount := fake.snapshot()
	if len(commits) != 1 || commits[0].Offset != 10 {
		t.Fatalf("commits = %+v, want one commit for offset 10", commits)
	}
	if closeCount != 0 {
		t.Fatalf("reader closed %d times before shutdown; reject must not close it", closeCount)
	}

	cancel()
	waitClosed(t, envelopes)
	_, closeCount = fake.snapshot()
	if closeCount != 1 {
		t.Fatalf("reader close count = %d, want 1", closeCount)
	}
}

func TestReaderCommitRetriesWithoutFetchingNextMessage(t *testing.T) {
	fake := newFakeKafkaReader()
	fake.fetches <- fetchResult{message: kafka.Message{Topic: "orders", Partition: 0, Offset: 4}}
	fake.commitErrors = []error{errors.New("temporary commit failure"), nil}
	r := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := r.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := receiveEnvelope(t, envelopes)
	if err := r.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	commits, _ := fake.snapshot()
	if len(commits) != 2 {
		t.Fatalf("commit attempts = %d, want 2", len(commits))
	}

	cancel()
	waitClosed(t, envelopes)
}

func TestReaderRejectsInvalidAndDuplicateOperations(t *testing.T) {
	r := testConsumer(newFakeKafkaReader())
	if err := r.Commit(kafka.Message{}); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Commit error = %v, want %v", err, errInvalidDeliveryTag)
	}
	if err := r.Reject(nil); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Reject error = %v, want %v", err, errInvalidDeliveryTag)
	}
}

func TestReaderMayOnlyStartOnce(t *testing.T) {
	fake := newFakeKafkaReader()
	r := testConsumer(fake)
	ctx, cancel := context.WithCancel(t.Context())
	envelopes, err := r.Consume(ctx)
	if err != nil {
		t.Fatalf("first Consume: %v", err)
	}
	if _, err := r.Consume(ctx); !errors.Is(err, errConsumerAlreadyConsuming) {
		t.Fatalf("second Consume error = %v, want %v", err, errConsumerAlreadyConsuming)
	}
	if err := r.Subscribe(ctx, "other-topic"); !errors.Is(err, errSubscribeStarted) {
		t.Fatalf("Subscribe after Consume error = %v, want %v", err, errSubscribeStarted)
	}
	cancel()
	waitClosed(t, envelopes)
}
