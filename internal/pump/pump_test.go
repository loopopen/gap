package pump

import (
	"context"
	"testing"

	"github.com/loopopen/gap/v2/internal/entity"
)

type senderStub struct {
	ids []uint
}

func (s *senderStub) SendAndUpdate(_ context.Context, envelope *entity.Envelope) error {
	s.ids = append(s.ids, envelope.ID)
	return nil
}

func TestPumpRoutesMessagesToConfiguredSender(t *testing.T) {
	p := &Pump{}
	rabbit := new(senderStub)
	kafka := new(senderStub)
	if err := p.SetSender("RabbitMQ", "events", "rabbit-config", rabbit); err != nil {
		t.Fatal(err)
	}
	if err := p.SetSender("Kafka", "events", "kafka-config", kafka); err != nil {
		t.Fatal(err)
	}

	rabbitMessage := &entity.Envelope{ID: 1, Headers: map[string]string{
		entity.BrokerNameHeader: "events",
		entity.BrokerTypeHeader: "RabbitMQ",
	}}
	kafkaMessage := &entity.Envelope{ID: 2, Headers: map[string]string{
		entity.BrokerNameHeader: "events",
		entity.BrokerTypeHeader: "Kafka",
	}}
	if err := p.send(context.Background(), rabbitMessage); err != nil {
		t.Fatal(err)
	}
	if err := p.send(context.Background(), kafkaMessage); err != nil {
		t.Fatal(err)
	}

	if len(rabbit.ids) != 1 || rabbit.ids[0] != rabbitMessage.ID {
		t.Fatalf("rabbit sender received ids %v", rabbit.ids)
	}
	if len(kafka.ids) != 1 || kafka.ids[0] != kafkaMessage.ID {
		t.Fatalf("kafka sender received ids %v", kafka.ids)
	}
}

func TestPumpRejectsUnknownBrokerIdentity(t *testing.T) {
	p := &Pump{}
	if err := p.SetSender("RabbitMQ", "events", "rabbit-config", new(senderStub)); err != nil {
		t.Fatal(err)
	}

	message := &entity.Envelope{Headers: map[string]string{
		entity.BrokerNameHeader: "events",
		entity.BrokerTypeHeader: "Kafka",
	}}
	if err := p.send(context.Background(), message); err == nil {
		t.Fatal("expected unknown broker identity error")
	}
}

func TestPumpRejectsTwoLiveConfigsForSameBrokerRoute(t *testing.T) {
	p := &Pump{}
	first := new(senderStub)
	if err := p.SetSender("RabbitMQ", "events", "old-config", first); err != nil {
		t.Fatal(err)
	}

	err := p.SetSender("RabbitMQ", "events", "new-config", new(senderStub))
	if err == nil {
		t.Fatal("expected duplicate broker route configuration error")
	}
}
