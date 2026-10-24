package internal

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/internal/pump"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/storage"
)

type Pub[T any] struct {
	opts           *ingap.Options
	brokerOpts     *ingap.BrokerOptions
	storage        storage.Storage
	producer       broker.Producer
	pump           *pump.Pump
	txer           txer.Txer
	brokerName     string
	brokerType     string
	configIdentity string
}

func NewPub[T any](opts *ingap.Options, brokerOpts *ingap.BrokerOptions, producer broker.Producer, storage storage.Storage, messagePump *pump.Pump) *Pub[T] {
	if producer == nil {
		panic(errx.ErrNoBroker)
	}

	pub := &Pub[T]{
		opts:           opts,
		brokerOpts:     brokerOpts,
		storage:        storage,
		producer:       producer,
		brokerName:     plugin.BrokerName(brokerOpts),
		brokerType:     plugin.BrokerType(brokerOpts),
		configIdentity: plugin.BrokerConfigIdentity(brokerOpts),
	}

	if storage != nil {
		if messagePump == nil {
			panic("message pump must not be nil when storage is configured")
		}
		if err := messagePump.SetSender(pub.brokerType, pub.brokerName, pub.configIdentity, pub); err != nil {
			panic(err)
		}
		pub.pump = messagePump
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
		opts:           p.opts,
		brokerOpts:     p.brokerOpts,
		storage:        stor,
		producer:       p.producer,
		pump:           p.pump,
		txer:           txer,
		brokerName:     p.brokerName,
		brokerType:     p.brokerType,
		configIdentity: p.configIdentity,
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
	en.AddHeader(KeysBrokerName, p.brokerName)
	en.AddHeader(KeysBrokerType, p.brokerType)
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
	err := p.producer.Produce(ctx, en)
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
	err := p.producer.Produce(ctx, envelope)
	if err != nil {
		envelope.Log().Debug("failed to send message", slog.Any("err", err))

		if err := p.storage.UpdateStatusPublished(ctx, envelope.ID, 0, enum.StatusFailed); err != nil {
			envelope.Log().Error("failed to set published status to Failed", slog.Any("err", err))
			return err
		}
		return err
	}
	if err := p.storage.UpdateStatusPublished(ctx, envelope.ID, 0, enum.StatusSucceeded); err != nil {
		envelope.Log().Debug("failed to set published status to Succeeded", slog.Any("err", err))
		return err
	}
	return nil
}

func (p *Pub[T]) Options() *ingap.Options {
	return p.opts
}

func (p *Pub[T]) BrokerOptions() *ingap.BrokerOptions {
	return p.brokerOpts
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
