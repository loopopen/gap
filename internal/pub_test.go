package internal

import (
	"context"
	"testing"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
)

type producerStub struct {
	envelope *entity.Envelope
}

func (p *producerStub) Produce(_ context.Context, envelope *entity.Envelope) error {
	p.envelope = envelope
	return nil
}

type brokerPluginStub struct{}

func (*brokerPluginStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }
func (*brokerPluginStub) ConfigIdentity() string  { return "config" }
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
