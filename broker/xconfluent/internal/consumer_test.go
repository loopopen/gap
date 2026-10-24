package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/gap"
)

type fakeConsumer struct {
	messages chan *kafka.Message
	closed   chan struct{}

	mu          sync.Mutex
	assignment  []kafka.TopicPartition
	assigned    []kafka.TopicPartition
	commitCount int
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{
		messages: make(chan *kafka.Message, 1),
		closed:   make(chan struct{}),
	}
}

func (f *fakeConsumer) SubscribeTopics([]string, kafka.RebalanceCb) error { return nil }

func (f *fakeConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case message := <-f.messages:
		return message, nil
	case <-timer.C:
		return nil, kafka.NewError(kafka.ErrTimedOut, "timed out", false)
	}
}

func (f *fakeConsumer) CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error) {
	f.mu.Lock()
	f.commitCount++
	f.mu.Unlock()
	return nil, nil
}

func (f *fakeConsumer) Assignment() ([]kafka.TopicPartition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.TopicPartition(nil), f.assignment...), nil
}

func (f *fakeConsumer) Assign(partitions []kafka.TopicPartition) error {
	f.mu.Lock()
	f.assigned = append([]kafka.TopicPartition(nil), partitions...)
	f.mu.Unlock()
	return nil
}

func (f *fakeConsumer) Close() error {
	close(f.closed)
	return nil
}

func testConsumer(client consumerClient) *Consumer {
	entity.MustInitSnowflake(1)
	consumer := &Consumer{
		gapOpts:        &gap.Options{Version: "test"},
		group:          "test-group",
		groupID:        "gap.test.g.test-group",
		state:          consumerSubscribed,
		topics:         []string{"test-topic"},
		pollTimeout:    time.Millisecond,
		readRetryDelay: time.Millisecond,
		commitRetryMin: time.Millisecond,
		commitRetryMax: time.Millisecond,
	}
	consumer.newConsumer = func() (consumerClient, error) { return client, nil }
	return consumer
}

func TestRejectReassignsCurrentAssignmentFromStoredOffsets(t *testing.T) {
	fake := newFakeConsumer()
	topic := "test-topic"
	fake.assignment = []kafka.TopicPartition{
		{Topic: &topic, Partition: 0, Offset: 41},
		{Topic: &topic, Partition: 1, Offset: 72},
	}
	fake.messages <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 42},
		Value:          []byte("payload"),
	}

	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(context.Background())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	envelope := <-envelopes
	if err = consumer.Reject(envelope.Tag); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	fake.mu.Lock()
	assigned := append([]kafka.TopicPartition(nil), fake.assigned...)
	commits := fake.commitCount
	fake.mu.Unlock()
	if commits != 0 {
		t.Fatalf("commit count = %d, want 0", commits)
	}
	if len(assigned) != 2 {
		t.Fatalf("assigned partitions = %d, want 2", len(assigned))
	}
	for _, partition := range assigned {
		if partition.Offset != kafka.OffsetStored {
			t.Fatalf("assigned offset = %v, want %v", partition.Offset, kafka.OffsetStored)
		}
	}

	cancel()
	select {
	case <-fake.closed:
	case <-time.After(time.Second):
		t.Fatal("consumer did not close")
	}
}

func TestCommitCommitsDeliveredMessage(t *testing.T) {
	fake := newFakeConsumer()
	topic := "test-topic"
	fake.messages <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 12},
		Value:          []byte("payload"),
	}

	consumer := testConsumer(fake)
	ctx, cancel := context.WithCancel(context.Background())
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	envelope := <-envelopes
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	fake.mu.Lock()
	commits := fake.commitCount
	assigned := len(fake.assigned)
	fake.mu.Unlock()
	if commits != 1 {
		t.Fatalf("commit count = %d, want 1", commits)
	}
	if assigned != 0 {
		t.Fatalf("assignment reset count = %d, want 0", assigned)
	}
	cancel()
	<-fake.closed
}

func TestCommitDeliveredMessageAfterIntakeStops(t *testing.T) {
	fake := newFakeConsumer()
	topic := "test-topic"
	fake.messages <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 13},
	}
	consumer := testConsumer(fake)
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	consumer.gapOpts.DrainContext = workCtx
	intakeCtx, stopIntake := context.WithCancel(context.Background())
	envelopes, err := consumer.Consume(intakeCtx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	envelope := <-envelopes
	stopIntake()
	if err = consumer.Commit(envelope.Tag); err != nil {
		t.Fatalf("Commit after intake stopped: %v", err)
	}
	fake.mu.Lock()
	commits := fake.commitCount
	fake.mu.Unlock()
	if commits != 1 {
		t.Fatalf("commit count = %d, want 1", commits)
	}
	select {
	case <-fake.closed:
	case <-time.After(time.Second):
		t.Fatal("consumer did not close after intake stopped")
	}
}

func TestDeliveryTagValidation(t *testing.T) {
	consumer := &Consumer{}
	if err := consumer.Commit("invalid"); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Commit error = %v, want %v", err, errInvalidDeliveryTag)
	}
	other := &Consumer{}
	tag := newDeliveryTag(context.Background(), other)
	if err := consumer.Reject(tag); !errors.Is(err, errInvalidDeliveryTag) {
		t.Fatalf("Reject error = %v, want %v", err, errInvalidDeliveryTag)
	}
}
