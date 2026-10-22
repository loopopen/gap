package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/loopopen/gap/internal/entity"
)

type producerStub struct{}

func (producerStub) Produce(context.Context, *entity.Envelope) error { return nil }

type consumerStub struct{}

func (consumerStub) Subscribe(context.Context, string) error { return nil }
func (consumerStub) Consume(context.Context) (<-chan *entity.Envelope, error) {
	return nil, nil
}
func (consumerStub) Commit(any) error { return nil }
func (consumerStub) Reject(any) error { return nil }

func TestClientCacheCreatesProducerOnceConcurrently(t *testing.T) {
	cache := NewClientCache()
	var calls atomic.Int32

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.Producer(func() (Producer, error) {
				calls.Add(1)
				return producerStub{}, nil
			}); err != nil {
				t.Errorf("Producer() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("Producer() called %d times, want 1", got)
	}
}

func TestClientCacheCachesConsumersByGroup(t *testing.T) {
	cache := NewClientCache()
	var calls atomic.Int32

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.Consumer("group-a", func() (Consumer, error) {
				calls.Add(1)
				return consumerStub{}, nil
			}); err != nil {
				t.Errorf("Consumer() error = %v", err)
			}
		}()
	}
	wg.Wait()

	if _, err := cache.Consumer("group-b", func() (Consumer, error) {
		calls.Add(1)
		return consumerStub{}, nil
	}); err != nil {
		t.Fatalf("Consumer() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Consumer() called %d times, want 2", got)
	}
}

func TestClientCacheRetriesFailedOpen(t *testing.T) {
	cache := NewClientCache()
	_, err := cache.Producer(func() (Producer, error) {
		return nil, errors.New("boom")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if _, err := cache.Producer(func() (Producer, error) {
		return producerStub{}, nil
	}); err != nil {
		t.Fatal(err)
	}
}
