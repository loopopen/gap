package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	ingap "github.com/loopopen/gap/internal/gap"
)

type consumerStub struct {
	consumeCalls atomic.Int32
}

type controlledConsumer struct {
	envelopes chan *entity.Envelope
	rejected  atomic.Int32
}

func (*controlledConsumer) Subscribe(context.Context, string) error { return nil }
func (c *controlledConsumer) Consume(context.Context) (<-chan *entity.Envelope, error) {
	return c.envelopes, nil
}
func (*controlledConsumer) Commit(any) error { return nil }
func (c *controlledConsumer) Reject(any) error {
	c.rejected.Add(1)
	return nil
}

func (*consumerStub) Subscribe(context.Context, string) error { return nil }

func (c *consumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	c.consumeCalls.Add(1)
	ch := make(chan *entity.Envelope)
	close(ch)
	return ch, nil
}

func (*consumerStub) Commit(any) error { return nil }
func (*consumerStub) Reject(any) error { return nil }

func TestSubListeningStartsConsumerOnce(t *testing.T) {
	consumer := new(consumerStub)
	opts := new(ingap.Options).With()
	opts.InitRuntime()
	group := &ingap.GroupOptions{Group: "group"}
	brokerOpts := &ingap.BrokerOptions{BrokerName: "broker", Plugin: new(brokerPluginStub)}
	sub := NewSub(opts, brokerOpts, group, consumer, nil, nil)
	if sub.brokerType != "RabbitMQ" || sub.brokerName != "broker" {
		t.Fatalf("sub broker identity = %q/%q, want RabbitMQ/broker", sub.brokerType, sub.brokerName)
	}

	if err := sub.Listening(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Listening(); err != nil {
		t.Fatal(err)
	}
	if calls := consumer.consumeCalls.Load(); calls != 1 {
		t.Fatalf("Consume() calls = %d, want 1", calls)
	}
}

func TestSubStopsAcceptingMessagesDuringShutdown(t *testing.T) {
	opts := new(ingap.Options).With()
	opts.InitRuntime()
	consumer := &controlledConsumer{envelopes: make(chan *entity.Envelope, 2)}
	brokerOpts := &ingap.BrokerOptions{BrokerName: "broker", Plugin: new(brokerPluginStub)}
	sub := NewSub(opts, brokerOpts, &ingap.GroupOptions{Group: "group"}, consumer, nil, nil)

	var handled atomic.Int32
	var first sync.Once
	firstHandled := make(chan struct{})
	if err := sub.Subscribe("events", func(context.Context, []byte, map[string]string) error {
		handled.Add(1)
		first.Do(func() { close(firstHandled) })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := sub.Listening(); err != nil {
		t.Fatal(err)
	}

	consumer.envelopes <- entity.NewEnvelope("v1", "events", []byte("first"))
	select {
	case <-firstHandled:
	case <-time.After(time.Second):
		t.Fatal("first message was not handled")
	}

	opts.StopAccepting()
	consumer.envelopes <- entity.NewEnvelope("v1", "events", []byte("second"))
	if err := sub.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("handled messages = %d, want 1", got)
	}
	opts.CancelWork()
}
