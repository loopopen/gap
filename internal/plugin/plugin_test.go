package plugin

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	gapopts "github.com/loopopen/gap/internal/gap"
)

type brokerOptionsStub struct {
	endpoint string
}

func (*brokerOptionsStub) PluginType() enum.Plugin { return enum.PluginRabbitMQ }

func (*brokerOptionsStub) OpenProducer(*gapopts.Options) (broker.Producer, error) {
	return nil, nil
}

func (*brokerOptionsStub) OpenConsumer(*gapopts.Options, string) (broker.Consumer, error) {
	return nil, nil
}

func (o *brokerOptionsStub) ConfigIdentity() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(o.endpoint)))
}

func TestBrokerNameIsIndependentOfBrokerConfiguration(t *testing.T) {
	first := &gapopts.Options{BrokerOptions: &brokerOptionsStub{endpoint: "rabbit-a:5672"}}
	second := &gapopts.Options{BrokerOptions: &brokerOptionsStub{endpoint: "rabbit-b:5672"}}

	if got, want := BrokerName(first), BrokerName(second); got != want {
		t.Fatalf("BrokerName changed with endpoint: %q != %q", got, want)
	}
	if BrokerConfigIdentity(first) == BrokerConfigIdentity(second) {
		t.Fatal("config identity should change with endpoint")
	}
}

func TestExplicitBrokerName(t *testing.T) {
	opts := &gapopts.Options{
		BrokerName:    "orders",
		BrokerOptions: new(brokerOptionsStub),
	}
	if got := BrokerName(opts); got != "orders" {
		t.Fatalf("BrokerName() = %q, want orders", got)
	}
}
