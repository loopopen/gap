package internal

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/loopopen/gap/v2/internal/entity"
	internalgap "github.com/loopopen/gap/v2/internal/gap"
)

type consumerStub struct {
	consumeCalls atomic.Int32
}

func (*consumerStub) Subscribe(context.Context, string) error { return nil }

func (c *consumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	c.consumeCalls.Add(1)
	ch := make(chan *entity.Envelope)
	close(ch)
	return ch, nil
}

func (*consumerStub) Commit(any) error { return nil }
func (*consumerStub) Reject(any) error { return nil }

func TestSubListeningStartsConsumerOnce(t *testing.T) {
	consumer := new(consumerStub)
	opts := &internalgap.Options{Context: context.Background(), DrainContext: context.Background()}
	group := &internalgap.GroupOptions{Group: "group"}
	sub := NewSub(opts, group, consumer, nil, nil, "RabbitMQ", "broker", "config")

	if err := sub.Listening(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Listening(); err != nil {
		t.Fatal(err)
	}
	if calls := consumer.consumeCalls.Load(); calls != 1 {
		t.Fatalf("Consume() calls = %d, want 1", calls)
	}
}
