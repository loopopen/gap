package gap

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/internal/entity"
	"github.com/loopopen/gap/v2/internal/enum"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
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

type runtimeBrokerStub struct {
	identity string
	opened   *atomic.Int32
}

func (*runtimeBrokerStub) PluginType() enum.Plugin  { return enum.PluginRabbitMQ }
func (b *runtimeBrokerStub) ConfigIdentity() string { return b.identity }
func (*runtimeBrokerStub) OpenProducer(*internalgap.Options) (broker.Producer, error) {
	return new(runtimeProducerStub), nil
}
func (b *runtimeBrokerStub) OpenConsumer(*internalgap.Options, string) (broker.Consumer, error) {
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
