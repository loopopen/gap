package internal

import (
	"context"
	"testing"

	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/internal/entity"
	"github.com/loopopen/gap/v2/internal/enum"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
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
func (*brokerPluginStub) OpenProducer(*internalgap.Options) (broker.Producer, error) {
	return nil, nil
}
func (*brokerPluginStub) OpenConsumer(*internalgap.Options, string) (broker.Consumer, error) {
	return nil, nil
}

func TestPublishWritesStableBrokerRouteHeaders(t *testing.T) {
	entity.MustInitSnowflake(1)
	producer := new(producerStub)
	opts := &internalgap.Options{
		Version: "v1",
	}
	brokerOpts := &internalgap.BrokerOptions{
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
