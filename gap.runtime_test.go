package gap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

func TestNewRejectsInvalidStateMachineOptions(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		option shoot.Option[Options, *Options]
	}{
		{name: "claim batch", field: "ClaimBatchSize=0", option: ClaimBatchSize(0)},
		{name: "max retries", field: "MaxRetries=0", option: MaxRetries(0)},
		{name: "lease ttl", field: "LeaseTTLSeconds=0", option: LeaseTTLSeconds(0)},
		{name: "pump interval", field: "PumpIntervalSeconds=0", option: PumpIntervalSeconds(0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatal("New() did not panic")
				}
				if got := fmt.Sprint(recovered); !strings.Contains(got, test.field) {
					t.Fatalf("New() panic = %q, want %q", got, test.field)
				}
			}()
			New(test.option)
		})
	}
}

type runtimeProducerStub struct{}

func (*runtimeProducerStub) Produce(context.Context, *entity.Envelope) error { return nil }

type blockingRuntimeProducer struct {
	started    chan struct{}
	release    chan struct{}
	closed     chan struct{}
	closeCount atomic.Int32
	startOnce  sync.Once
	closeOnce  sync.Once
}

func newBlockingRuntimeProducer() *blockingRuntimeProducer {
	return &blockingRuntimeProducer{
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (p *blockingRuntimeProducer) Produce(ctx context.Context, _ *entity.Envelope) error {
	p.startOnce.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *blockingRuntimeProducer) Close() error {
	p.closeOnce.Do(func() {
		p.closeCount.Add(1)
		close(p.closed)
	})
	return nil
}

type blockingRuntimeBroker struct {
	producer *blockingRuntimeProducer
}

func (*blockingRuntimeBroker) PluginType() enum.Plugin { return enum.PluginRabbitMQ }
func (*blockingRuntimeBroker) ConfigIdentity() string  { return "blocking-runtime" }
func (b *blockingRuntimeBroker) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return b.producer, nil
}
func (*blockingRuntimeBroker) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	return new(runtimeConsumerStub), nil
}

func useBlockingRuntimeBroker(producer *blockingRuntimeProducer) shoot.Option[BrokerOptions, *BrokerOptions] {
	return func(o *BrokerOptions) {
		o.Plugin = &blockingRuntimeBroker{producer: producer}
	}
}

type runtimeConsumerStub struct{}

func (*runtimeConsumerStub) Subscribe(context.Context, string) error { return nil }
func (*runtimeConsumerStub) Commit(any) error                        { return nil }
func (*runtimeConsumerStub) Reject(any) error                        { return nil }
func (*runtimeConsumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	ch := make(chan *entity.Envelope)
	close(ch)
	return ch, nil
}

type isolatedRuntimeConsumer struct {
	stopped chan struct{}
	once    sync.Once
}

func (*isolatedRuntimeConsumer) Subscribe(context.Context, string) error { return nil }
func (*isolatedRuntimeConsumer) Commit(any) error                        { return nil }
func (*isolatedRuntimeConsumer) Reject(any) error                        { return nil }
func (c *isolatedRuntimeConsumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	envelopes := make(chan *entity.Envelope)
	go func() {
		<-ctx.Done()
		c.once.Do(func() { close(c.stopped) })
		close(envelopes)
	}()
	return envelopes, nil
}

type isolatedRuntimeBroker struct {
	consumers chan *isolatedRuntimeConsumer
}

func (*isolatedRuntimeBroker) PluginType() enum.Plugin { return enum.PluginKafka }
func (*isolatedRuntimeBroker) ConfigIdentity() string  { return "shared-config" }
func (*isolatedRuntimeBroker) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return new(runtimeProducerStub), nil
}
func (b *isolatedRuntimeBroker) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	consumer := &isolatedRuntimeConsumer{stopped: make(chan struct{})}
	b.consumers <- consumer
	return consumer, nil
}

func runtimeContexts(ctx context.Context) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.Context = ctx
		o.DrainContext = ctx
	}
}

func useIsolatedRuntimeBroker(consumers chan *isolatedRuntimeConsumer) shoot.Option[BrokerOptions, *BrokerOptions] {
	return func(o *BrokerOptions) {
		o.Plugin = &isolatedRuntimeBroker{consumers: consumers}
	}
}

func TestConsumersAreIsolatedByGapRuntime(t *testing.T) {
	consumers := make(chan *isolatedRuntimeConsumer, 2)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()

	first := New(runtimeContexts(firstCtx))
	firstPublisher := first.NewEventPublisher(useIsolatedRuntimeBroker(consumers))
	second := New(runtimeContexts(secondCtx))
	secondPublisher := second.NewEventPublisher(useIsolatedRuntimeBroker(consumers))
	handler := func(context.Context, string, map[string]string) error { return nil }

	first.Subscribe(firstPublisher, HandleTopicWithinGroup("group", "events", handler))
	second.Subscribe(secondPublisher, HandleTopicWithinGroup("group", "events", handler))
	firstConsumer := <-consumers
	secondConsumer := <-consumers
	if firstConsumer == secondConsumer {
		t.Fatal("two runtimes shared the same consumer")
	}

	cancelFirst()
	select {
	case <-firstConsumer.stopped:
	case <-time.After(time.Second):
		t.Fatal("first runtime consumer did not stop")
	}
	select {
	case <-secondConsumer.stopped:
		t.Fatal("stopping the first runtime stopped the second consumer")
	case <-time.After(50 * time.Millisecond):
	}

	cancelSecond()
	select {
	case <-secondConsumer.stopped:
	case <-time.After(time.Second):
		t.Fatal("second runtime consumer did not stop")
	}
}

type runtimeBrokerStub struct {
	identity string
	opened   *atomic.Int32
}

func (*runtimeBrokerStub) PluginType() enum.Plugin  { return enum.PluginRabbitMQ }
func (b *runtimeBrokerStub) ConfigIdentity() string { return b.identity }
func (*runtimeBrokerStub) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return new(runtimeProducerStub), nil
}
func (b *runtimeBrokerStub) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	b.opened.Add(1)
	return new(runtimeConsumerStub), nil
}

func useRuntimeBroker(identity string, opened *atomic.Int32) shoot.Option[BrokerOptions, *BrokerOptions] {
	return func(o *BrokerOptions) {
		o.Plugin = &runtimeBrokerStub{identity: identity, opened: opened}
	}
}

func TestGapOwnsSharedOptionsAndSupportsMultiplePublishers(t *testing.T) {
	g := New()
	opened := new(atomic.Int32)
	rabbit := g.NewEventPublisher(BrokerName("rabbit"), useRuntimeBroker("rabbit-config", opened))
	kafka := g.NewEventPublisher(BrokerName("kafka"), useRuntimeBroker("kafka-config", opened))

	if rabbit.BrokerOptions().BrokerName != "rabbit" || kafka.BrokerOptions().BrokerName != "kafka" {
		t.Fatal("publisher broker options were not isolated")
	}

	handler := func(context.Context, string, map[string]string) error { return nil }
	g.Subscribe(rabbit, HandleTopic(handler, "events"))
	g.Subscribe(kafka)

	if got := len(g.subs.HandlerOtpsLst); got != 1 {
		t.Fatalf("registered handler count = %d, want 1", got)
	}
	if got := opened.Load(); got != 2 {
		t.Fatalf("subscriber count = %d, want 2", got)
	}
}

func TestShutdownRejectsNewPublishAndWaitsForAcceptedPublish(t *testing.T) {
	producer := newBlockingRuntimeProducer()
	g := New()
	pub := NewPublisher[string](g, useBlockingRuntimeBroker(producer))

	published := make(chan error, 1)
	go func() {
		published <- pub.Publish(context.Background(), "events", "first")
	}()
	<-producer.started

	shutdown := make(chan error, 1)
	go func() { shutdown <- g.Shutdown(context.Background()) }()
	select {
	case <-g.opts.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not stop runtime intake")
	}
	if err := pub.Publish(context.Background(), "events", "second"); !errors.Is(err, ErrRuntimeStopping) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrRuntimeStopping)
	}
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned before accepted publish completed: %v", err)
	default:
	}

	close(producer.release)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	select {
	case <-producer.closed:
	default:
		t.Fatal("producer was not closed after drain")
	}
}

func TestShutdownTimeoutCancelsAcceptedWork(t *testing.T) {
	producer := newBlockingRuntimeProducer()
	g := New()
	pub := NewPublisher[string](g, useBlockingRuntimeBroker(producer))

	published := make(chan error, 1)
	go func() {
		published <- pub.Publish(context.Background(), "events", "message")
	}()
	<-producer.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline exceeded", err)
	}
	select {
	case err := <-published:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed-out shutdown did not cancel accepted publish")
	}
	select {
	case <-producer.closed:
	default:
		t.Fatal("producer was not closed after shutdown timeout")
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	producer := newBlockingRuntimeProducer()
	g := New()
	NewPublisher[string](g, useBlockingRuntimeBroker(producer))

	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := producer.closeCount.Load(); got != 1 {
		t.Fatalf("producer close count = %d, want 1", got)
	}
}
