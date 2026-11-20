package internal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
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

type receivedStorageStub struct {
	storage.Storage
	id         uint
	leaseToken string
	status     enum.Status
	statusSet  bool
}

func (s *receivedStorageStub) UpdateStatusReceived(_ context.Context, id uint, leaseToken string, status enum.Status) error {
	s.id = id
	s.leaseToken = leaseToken
	s.status = status
	s.statusSet = true
	return nil
}

func TestHandleAndUpdateRetriesHandlerFailure(t *testing.T) {
	stor := new(receivedStorageStub)
	sub := receivedSub(stor, func(context.Context, []byte, map[string]string) error {
		return errors.New("downstream timeout")
	})
	envelope := &entity.Envelope{ID: 7, Topic: "events", LeaseToken: "lease", Retries: 1}

	if err := sub.HandleAndUpdate(context.Background(), envelope); err == nil {
		t.Fatal("expected handler error")
	}
	if !stor.statusSet || stor.status != enum.StatusPending || stor.id != envelope.ID || stor.leaseToken != envelope.LeaseToken {
		t.Fatalf("status update = %+v", stor)
	}
}

func TestHandleAndUpdateFailsPermanentAndExhaustedErrors(t *testing.T) {
	tests := []struct {
		name    string
		retries int
		handler ingap.Handler[[]byte]
		topic   string
	}{
		{
			name:    "permanent",
			retries: 0,
			topic:   "events",
			handler: func(context.Context, []byte, map[string]string) error {
				return errx.Permanent(errors.New("invalid business payload"))
			},
		},
		{
			name:    "exhausted",
			retries: 3,
			topic:   "events",
			handler: func(context.Context, []byte, map[string]string) error {
				return errors.New("downstream timeout")
			},
		},
		{
			name:    "missing handler",
			retries: 0,
			topic:   "missing",
			handler: func(context.Context, []byte, map[string]string) error {
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stor := new(receivedStorageStub)
			sub := receivedSub(stor, test.handler)
			envelope := &entity.Envelope{ID: 7, Topic: test.topic, LeaseToken: "lease", Retries: test.retries}
			if err := sub.HandleAndUpdate(context.Background(), envelope); err == nil {
				t.Fatal("expected handle error")
			}
			if !stor.statusSet || stor.status != enum.StatusFailed {
				t.Fatalf("status update = %+v", stor)
			}
		})
	}
}

func receivedSub(stor storage.Storage, handler ingap.Handler[[]byte]) *Sub {
	return &Sub{
		opts:      &ingap.Options{MaxRetries: 3},
		groupOpts: &ingap.GroupOptions{Group: "billing"},
		storage:   stor,
		handlers: map[string]ingap.Handler[[]byte]{
			"events": handler,
		},
	}
}
