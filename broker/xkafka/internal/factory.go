package internal

import (
	"log/slog"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/gap"
)

type Factory struct{}

func (f *Factory) CreateConsumer(gapOpts *gap.Options, group string) (broker.Consumer, error) {
	consumer := NewConsumer(gapOpts, group)
	err := consumer.init()
	if err != nil {
		slog.Error("failed to init consumer", slog.Any("err", err))
		return nil, err
	}
	return consumer, nil
}

func (f *Factory) CreateProducer(gapOpts *gap.Options) (broker.Producer, error) {
	producer := NewProducer(gapOpts)
	err := producer.init()
	if err != nil {
		slog.Error("failed to init producer", slog.Any("err", err))
		return nil, err
	}
	return producer, nil
}
