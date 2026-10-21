package broker

import (
	"sync"

	"github.com/loopopen/gap/internal/gap"
)

type Factory interface {
	CreateProducer(opts *gap.Options) (Producer, error)

	CreateConsumer(opts *gap.Options, group string) (Consumer, error)
}

type CachedFactory struct {
	factory   Factory
	mu        sync.Mutex
	producer  Producer
	consumers map[string]Consumer
}

func Cache(factory Factory) Factory {
	return &CachedFactory{
		factory:   factory,
		consumers: make(map[string]Consumer),
	}
}

func (f *CachedFactory) CreateProducer(gapOpts *gap.Options) (Producer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.producer == nil {
		producer, err := f.factory.CreateProducer(gapOpts)
		if err != nil {
			return nil, err
		}
		f.producer = producer
	}

	return f.producer, nil
}

func (f *CachedFactory) CreateConsumer(gapOpts *gap.Options, group string) (Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	consumer, ok := f.consumers[group]
	if !ok {
		var err error
		consumer, err = f.factory.CreateConsumer(gapOpts, group)
		if err != nil {
			return nil, err
		}
		f.consumers[group] = consumer
	}
	return consumer, nil
}
