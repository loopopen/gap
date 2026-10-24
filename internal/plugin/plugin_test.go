package plugin

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
)

type brokerOptionsStub struct {
	endpoint string
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
