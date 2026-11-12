package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

type producerStub struct {
	envelope *entity.Envelope
	err      error
}

func (p *producerStub) Produce(_ context.Context, envelope *entity.Envelope) error {
	p.envelope = envelope
	return p.err
}

type publishedStorageStub struct {
	storage.Storage
	id         uint
	leaseToken string
	status     enum.Status
	statusSet  bool
}

func (s *publishedStorageStub) UpdateStatusPublished(_ context.Context, id uint, leaseToken string, status enum.Status) error {
	s.id = id
	s.leaseToken = leaseToken
	s.status = status
	s.statusSet = true
	return nil
}

type brokerPluginStub struct{}

func (*brokerPluginStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }
func (*brokerPluginStub) ConfigID() string        { return "config" }
func (*brokerPluginStub) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return nil, nil
}
func (*brokerPluginStub) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	return nil, nil
}

func TestPublishWritesStableBrokerRouteHeaders(t *testing.T) {
	entity.MustInitSnowflake(1)
	producer := new(producerStub)
	opts := &ingap.Options{
		Version: "v1",
	}
	brokerOpts := &ingap.BrokerOptions{
		BrokerName: "orders",
		Plugin:     new(brokerPluginStub),
	}
	pub := NewPub[string](opts, brokerOpts, producer, nil, nil)
	if err := pub.Publish(context.Background(), "created", "payload"); err != nil {
		t.Fatal(err)
	}
	if got := producer.envelope.Headers[KeysBrokerName]; got != "orders" {
		t.Fatalf("broker name header = %q, want orders", got)
	}
	if got := producer.envelope.Headers[KeysBrokerType]; got != "RabbitMQ" {
		t.Fatalf("broker type header = %q, want RabbitMQ", got)
	}
}

func TestSendAndUpdateSchedulesRetryBeforeRetriesAreExhausted(t *testing.T) {
	stor := new(publishedStorageStub)
	pub := &Pub[string]{
		opts:     &ingap.Options{MaxRetries: 3, RetryIntervalSeconds: 10},
		storage:  stor,
		producer: &producerStub{err: errors.New("rabbitmq: message was returned")},
	}
	envelope := &entity.Envelope{ID: 7, LeaseToken: "lease", Retries: 1}

	err := pub.SendAndUpdate(context.Background(), envelope)
	if err == nil {
		t.Fatal("expected produce error")
	}
	if !stor.statusSet || stor.status != enum.StatusPending || stor.id != envelope.ID || stor.leaseToken != envelope.LeaseToken {
		t.Fatalf("status update = %+v", stor)
	}
}

func TestSendAndUpdateMarksPermanentProduceErrorFailed(t *testing.T) {
	stor := new(publishedStorageStub)
	pub := &Pub[string]{
		opts:     &ingap.Options{MaxRetries: 3},
		storage:  stor,
		producer: &producerStub{err: errx.Permanent(errx.ErrNilPayload)},
	}
	envelope := &entity.Envelope{ID: 7, LeaseToken: "lease", Retries: 0}

	if err := pub.SendAndUpdate(context.Background(), envelope); err == nil {
		t.Fatal("expected produce error")
	}
	if !stor.statusSet || stor.status != enum.StatusFailed || stor.id != envelope.ID || stor.leaseToken != envelope.LeaseToken {
		t.Fatalf("status update = %+v", stor)
	}
}

func TestSendAndUpdateMarksFailedWhenRetriesAreExhausted(t *testing.T) {
	stor := new(publishedStorageStub)
	pub := &Pub[string]{
		opts:     &ingap.Options{MaxRetries: 3},
		storage:  stor,
		producer: &producerStub{err: errors.New("rabbitmq: message was returned")},
	}
	envelope := &entity.Envelope{ID: 7, LeaseToken: "lease", Retries: 3}

	if err := pub.SendAndUpdate(context.Background(), envelope); err == nil {
		t.Fatal("expected produce error")
	}
	if !stor.statusSet || stor.status != enum.StatusFailed || stor.id != envelope.ID || stor.leaseToken != envelope.LeaseToken {
		t.Fatalf("status update = %+v", stor)
	}
}
