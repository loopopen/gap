package internal

import (
	"log/slog"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/gap"
)

type Factory struct{}

func (f *Factory) CreateConsumer(gapOpts *gap.Options, group string) (broker.Consumer, error) {
	consumer, err := NewConsumer(gapOpts, group)
	if err != nil {
		return nil, err
	}
	if err = consumer.init(); err != nil {
		slog.Error("failed to init confluent consumer", slog.Any("err", err))
		return nil, err
	}
	return consumer, nil
}

func (f *Factory) CreateProducer(gapOpts *gap.Options) (broker.Producer, error) {
	producer, err := NewProducer(gapOpts)
	if err != nil {
		return nil, err
	}
	if err = producer.init(); err != nil {
		slog.Error("failed to init confluent producer", slog.Any("err", err))
		return nil, err
	}
	return producer, nil
}
