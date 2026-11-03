package plugin

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	gapOpts := new(ingap.Options).With()
	gapOpts.InitRuntime()
	brokerOpts := &ingap.BrokerOptions{BrokerName: "orders", Plugin: plugin}

	if got := MustGetProducer(gapOpts, brokerOpts); got != producer {
		t.Fatal("unexpected producer")
	}
	if got := MustGetConsumer(gapOpts, brokerOpts, "group"); got != consumer {
		t.Fatal("unexpected consumer")
	}
	if err := gapOpts.CloseResources(context.Background()); err != nil {
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

type blockingCloser struct{}

func (blockingCloser) Close() error {
	<-make(chan struct{})
	return nil
}

func TestCloseResourcesReturnsWhenCloseBlocks(t *testing.T) {
	gapOpts := &ingap.Options{}
	gapOpts.InitRuntime()
	gapOpts.RegisterCloser(blockingCloser{})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := gapOpts.CloseResources(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseResources() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("CloseResources blocked for %s", elapsed)
	}
}

func (*brokerOptionsStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }

func (*brokerOptionsStub) OpenProducer(*ingap.Options) (broker.Producer, error) {
	return nil, nil
}

func (*brokerOptionsStub) OpenConsumer(*ingap.Options, string) (broker.Consumer, error) {
	return nil, nil
}

func (o *brokerOptionsStub) ConfigID() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(o.endpoint)))
}

func TestBrokerNameIsIndependentOfBrokerConfiguration(t *testing.T) {
	first := &ingap.BrokerOptions{Plugin: &brokerOptionsStub{endpoint: "rabbit-a:5672"}}
	second := &ingap.BrokerOptions{Plugin: &brokerOptionsStub{endpoint: "rabbit-b:5672"}}

	if got, want := BrokerName(first), BrokerName(second); got != want {
		t.Fatalf("BrokerName changed with endpoint: %q != %q", got, want)
	}
	if BrokerConfigID(first) == BrokerConfigID(second) {
		t.Fatal("config ID should change with endpoint")
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
