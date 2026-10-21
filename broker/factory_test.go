package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/gap"
)

type factoryStub struct {
	producerCalls atomic.Int32
	consumerCalls atomic.Int32
}

func (f *factoryStub) CreateProducer(*gap.Options) (Producer, error) {
	f.producerCalls.Add(1)
	return producerStub{}, nil
}

func (f *factoryStub) CreateConsumer(*gap.Options, string) (Consumer, error) {
	f.consumerCalls.Add(1)
	return consumerStub{}, nil
}

type producerStub struct{}

func (producerStub) Produce(context.Context, *entity.Envelope) error { return nil }

type consumerStub struct{}

func (consumerStub) Subscribe(context.Context, string) error { return nil }
func (consumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	return nil, nil
}
func (consumerStub) Commit(any) error { return nil }
func (consumerStub) Reject(any) error { return nil }

func TestCachedFactoryCreatesProducerOnceConcurrently(t *testing.T) {
	inner := &factoryStub{}
	factory := Cache(inner)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := factory.CreateProducer(&gap.Options{}); err != nil {
				t.Errorf("CreateProducer() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if got := inner.producerCalls.Load(); got != 1 {
		t.Fatalf("CreateProducer() called %d times, want 1", got)
	}
}

func TestCachedFactoryCachesConsumersByGroup(t *testing.T) {
	inner := &factoryStub{}
	factory := Cache(inner)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := factory.CreateConsumer(&gap.Options{}, "group-a"); err != nil {
				t.Errorf("CreateConsumer() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if _, err := factory.CreateConsumer(&gap.Options{}, "group-b"); err != nil {
		t.Fatalf("CreateConsumer() error = %v", err)
	}
	if got := inner.consumerCalls.Load(); got != 2 {
		t.Fatalf("CreateConsumer() called %d times, want 2", got)
	}
}
