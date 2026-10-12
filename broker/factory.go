package broker

import (
	"sync"

	"github.com/loopopen/gap/internal/gap"
)

type FactoryIface interface {
	CreateProducer(opts *gap.Options) (Producer, error)

	CreateConsumer(opts *gap.Options, group string) (Consumer, error)
}

type Factory struct {
	factory   FactoryIface
	mu        *sync.Mutex
	producer  Producer
	consumers map[string]Consumer
}

func NewFactory(factory FactoryIface) *Factory {
	return &Factory{
		factory:   factory,
		mu:        &sync.Mutex{},
		consumers: make(map[string]Consumer),
	}
}

func (f *Factory) CreateProducer(gapOpts *gap.Options) (Producer, error) {
	if f.producer == nil {
		producer, err := f.factory.CreateProducer(gapOpts)
		if err != nil {
			return nil, err
		}
		f.producer = producer
	}

	return f.producer, nil
}

func (f *Factory) CreateConsumer(gapOpts *gap.Options, group string) (Consumer, error) {
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
