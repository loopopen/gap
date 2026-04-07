package internal

import (
	"log/slog"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/gap"
)

type Factory struct{}

func (f *Factory) CreateReader(gapOpts *gap.Options, group string) (broker.Reader, error) {
	reader := NewReader(gapOpts, group)
	err := reader.init()
	if err != nil {
		slog.Error("failed to init reader", slog.Any("err", err))
		return nil, err
	}
	return reader, nil
}

func (f *Factory) CreateWriter(gapOpts *gap.Options) (broker.Writer, error) {
	writer := NewWriter(gapOpts)
	err := writer.init()
	if err != nil {
		slog.Error("failed to init writer", slog.Any("err", err))
		return nil, err
	}
	return writer, nil
}
