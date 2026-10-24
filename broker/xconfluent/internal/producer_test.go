package internal

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/internal/entity"
)

type fakeProducer struct {
	message *kafka.Message
	closes  int
}

func (f *fakeProducer) Produce(message *kafka.Message, delivery chan kafka.Event) error {
	f.message = message
	delivery <- message
	return nil
}

func (f *fakeProducer) Close() { f.closes++ }

func TestProduceWaitsForDeliveryAndPreservesHeaders(t *testing.T) {
	entity.MustInitSnowflake(1)
	client := new(fakeProducer)
	producer := &Producer{
		producer: client,
		ensurer: &Ensurer{
			topicsCache: map[string]bool{"test-topic": true},
			topicCalls:  make(map[string]*topicCall),
		},
	}
	envelope := entity.NewEnvelope("test", "test-topic", nil).
		WithPayload([]byte("payload"))
	envelope.AddHeader("trace", "value")

	if err := producer.Produce(context.Background(), envelope); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if client.message == nil || client.message.TopicPartition.Topic == nil {
		t.Fatal("producer did not receive a message")
	}
	if got := *client.message.TopicPartition.Topic; got != "test-topic" {
		t.Fatalf("topic = %q, want test-topic", got)
	}
	if got := string(client.message.Value); got != "payload" {
		t.Fatalf("payload = %q, want payload", got)
	}
	if len(client.message.Headers) != 1 || client.message.Headers[0].Key != "trace" || string(client.message.Headers[0].Value) != "value" {
		t.Fatalf("headers = %#v", client.message.Headers)
	}
}

func TestProducerCloseIsIdempotent(t *testing.T) {
	client := new(fakeProducer)
	producer := &Producer{producer: client}
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Close(); err != nil {
		t.Fatal(err)
	}
	if client.closes != 1 {
		t.Fatalf("producer client closed %d times, want 1", client.closes)
	}
}
