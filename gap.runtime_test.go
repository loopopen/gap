package gap

import (
	"context"
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

type runtimeProducerStub struct{}

func (*runtimeProducerStub) Produce(context.Context, *entity.Envelope) error { return nil }

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
