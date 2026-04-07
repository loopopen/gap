package internal

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/pump"
	"github.com/lopolopen/gap/internal/txer"
	"github.com/lopolopen/gap/storage"
)

type Pub[T any] struct {
	opts    *gap.Options
	storage storage.Storage
	writer  broker.Writer
	pump    *pump.Pump
	txer    txer.Txer
}

func NewPub[T any](opts *gap.Options, writer broker.Writer, storage storage.Storage) *Pub[T] {
	if writer == nil {
		panic(errx.ErrNoBroker)
	}

	pub := &Pub[T]{
		opts:    opts,
		storage: storage,
		writer:  writer,
	}

	if storage != nil {
		pump := pump.Singleton(opts)
		pump.SetSender(pub)
		pub.pump = pump
	} else {
		slog.Debug("pub works on no-persistence mode")
	}

	var _ Publisher[T] = pub
	return pub
}

// Bind implements [Pub].
func (p *Pub[T]) Bind(txer txer.Txer) (Publisher[T], error) {
	pub, err := p.bind(txer)
	return pub, err
}

func (p *Pub[T]) bind(txer txer.Txer) (*Pub[T], error) {
	if p.txer != nil {
		return nil, fmt.Errorf("cannot bind txer: %w", errx.ErrTxMultiBinding)
	}
	if p.storage == nil {
		return nil, fmt.Errorf("cannot bind txer: %v", errx.ErrNoStorage)
	}
	stor, err := p.storage.Bind(txer)
	if err != nil {
		return nil, err
	}
	pub := &Pub[T]{
		opts:    p.opts,
		storage: stor,
		writer:  p.writer,
		pump:    p.pump,
		txer:    txer,
	}
	txer.SetFlushHandler(func(e *entity.Envelope) {
		pub.dispatch(pub.opts.DrainContext, e)
	})
	return pub, nil
}

// Publish implements [Pub].
func (p *Pub[T]) Publish(ctx context.Context, topic string, msg T, args ...any) error {
	var hds Headers
	hds.Add(args...)

	en := entity.NewEnvelope(p.opts.Version, topic, msg)
	idstr := en.IDString()
	typ := reflect.TypeOf(msg)
	hds.Add(
		Pair(KeysMessageID, idstr),
		Pair(KeysCorrelationID, idstr),
		Pair(KeysMessageType, typ.String()),
	)

	headers := hds.Value()
	for k, v := range headers {
		en.AddHeader(k, v)
	}
	if err := en.Verify(); err != nil {
		return err
	}

	if p.storage != nil {
		err := p.storage.CreatePublished(ctx, en)
		if err != nil {
			en.Log().Error("failed to create published", slog.Any("err", err))
			return err
		}

		if p.txer != nil {
			p.txer.Append(en)
		} else {
			p.dispatch(ctx, en)
		}
		return nil
	}

	slog.Warn("send message without persistence")
	err := p.writer.Write(ctx, en)
	if err != nil {
		return err
	}
	return nil
}

func (p *Pub[T]) dispatch(ctx context.Context, envelope *entity.Envelope) {
	err := p.pump.DispatchToSend(ctx, envelope)
	if err != nil {
		envelope.Log().Warn("failed to dispatch envelope to sender, falling back to db polling", slog.Any("err", err))
	}
}

func (p *Pub[T]) SendAndUpdate(ctx context.Context, envelope *entity.Envelope) error {
	err := p.writer.Write(ctx, envelope)
	if err != nil {
		envelope.Log().Debug("failed to send message", slog.Any("err", err))

		if err := p.storage.UpdateStatusPublished(ctx, envelope.ID, 0, enum.StatusFailed); err != nil {
			envelope.Log().Error("falied to set published status to Failed", slog.Any("err", err))
			return err
		}
		return err
	}
	if err := p.storage.UpdateStatusPublished(ctx, envelope.ID, 0, enum.StatusSucceeded); err != nil {
		envelope.Log().Debug("falied to set published status to Succeeded", slog.Any("err", err))
		return err
	}
	return nil
}

func (p *Pub[T]) Options() *gap.Options {
	return p.opts
}

type EventPub struct {
	*Pub[Event]
}

func (e *EventPub) Bind(txer txer.Txer) (EventPublisher, error) {
	p, err := e.Pub.bind(txer)
	if err != nil {
		return nil, err
	}
	return &EventPub{Pub: p}, nil
}

func (p *EventPub) Publish(ctx context.Context, event Event, args ...any) error {
	return p.Pub.Publish(ctx, event.Topic(), event, args...)
}
