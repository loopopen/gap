package plugin

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
)

type brokerOptionsStub struct {
	endpoint string
}

type closableProducerStub struct {
	closed chan struct{}
	once   sync.Once
}

func (*closableProducerStub) Produce(context.Context, *entity.Envelope) error { return nil }
func (p *closableProducerStub) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

type closableConsumerStub struct {
	closed chan struct{}
	once   sync.Once
}

func (*closableConsumerStub) Subscribe(context.Context, string) error { return nil }
func (*closableConsumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	return nil, nil
}
func (*closableConsumerStub) Commit(any) error { return nil }
func (*closableConsumerStub) Reject(any) error { return nil }
func (c *closableConsumerStub) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

type lifecycleBrokerOptionsStub struct {
	producer *closableProducerStub
	consumer *closableConsumerStub
}

func (*lifecycleBrokerOptionsStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }
func (o *lifecycleBrokerOptionsStub) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return o.producer, nil
}
func (o *lifecycleBrokerOptionsStub) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	return o.consumer, nil
}

func TestBrokerClientsCloseWithRuntimeResources(t *testing.T) {
	producer := &closableProducerStub{closed: make(chan struct{})}
	consumer := &closableConsumerStub{closed: make(chan struct{})}
	plugin := &lifecycleBrokerOptionsStub{producer: producer, consumer: consumer}
	gapOpts := &ingap.Options{Context: context.Background()}
	gapOpts.InitializeRuntime()
	brokerOpts := &ingap.BrokerOptions{Plugin: plugin}

	if got := MustGetProducer(gapOpts, brokerOpts); got != producer {
		t.Fatal("unexpected producer")
	}
	if got := MustGetConsumer(gapOpts, brokerOpts, "group"); got != consumer {
		t.Fatal("unexpected consumer")
	}
	if err := gapOpts.CloseResources(); err != nil {
		t.Fatal(err)
	}

	for name, closed := range map[string]<-chan struct{}{
		"producer": producer.closed,
		"consumer": consumer.closed,
	} {
		select {
		case <-closed:
		default:
			t.Fatalf("%s was not closed", name)
		}
	}
}

func (*brokerOptionsStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }

func (*brokerOptionsStub) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return nil, nil
}

func (*brokerOptionsStub) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	return nil, nil
}

func (o *brokerOptionsStub) ConfigIdentity() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(o.endpoint)))
}

func TestBrokerNameIsIndependentOfBrokerConfiguration(t *testing.T) {
	first := &ingap.BrokerOptions{Plugin: &brokerOptionsStub{endpoint: "rabbit-a:5672"}}
	second := &ingap.BrokerOptions{Plugin: &brokerOptionsStub{endpoint: "rabbit-b:5672"}}

	if got, want := BrokerName(first), BrokerName(second); got != want {
		t.Fatalf("BrokerName changed with endpoint: %q != %q", got, want)
	}
	if BrokerConfigIdentity(first) == BrokerConfigIdentity(second) {
		t.Fatal("config identity should change with endpoint")
	}
}

func TestExplicitBrokerName(t *testing.T) {
	opts := &ingap.BrokerOptions{
		BrokerName: "orders",
		Plugin:     new(brokerOptionsStub),
	}
	if got := BrokerName(opts); got != "orders" {
		t.Fatalf("BrokerName() = %q, want orders", got)
	}
}
