package broker

import (
	"sync"

	"github.com/lopolopen/gap/options/gap"
)

type FactoryIface interface {
	CreateWriter(opts *gap.Options) (Writer, error)

	CreateReader(opts *gap.Options, group string) (Reader, error)
}

type Factory struct {
	factory FactoryIface
	mu      *sync.Mutex
	writer  Writer
	readers map[string]Reader
}

func NewFactory(factory FactoryIface) *Factory {
	return &Factory{
		factory: factory,
		mu:      &sync.Mutex{},
		readers: make(map[string]Reader),
	}
}

func (f *Factory) CreateWriter(gapOpts *gap.Options) (Writer, error) {
	if f.writer == nil {
		writer, err := f.factory.CreateWriter(gapOpts)
		if err != nil {
			return nil, err
		}
		f.writer = writer
	}

	return f.writer, nil
}

func (f *Factory) CreateReader(gapOpts *gap.Options, group string) (Reader, error) {
	reader, ok := f.readers[group]
	if !ok {
		var err error
		reader, err = f.factory.CreateReader(gapOpts, group)
		if err != nil {
			return nil, err
		}
		f.readers[group] = reader
	}
	return reader, nil
}
