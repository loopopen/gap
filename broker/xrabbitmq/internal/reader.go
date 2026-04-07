package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/gap"
	amqp "github.com/rabbitmq/amqp091-go"
)

const corrid = internal.KeysCorrelationID

type Reader struct {
	gapOpts *gap.Options
	opts    *Options
	group   string
	chPool  ChanPool
	x       string
	q       string
	ctag    string
}

func NewReader(gapOpts *gap.Options, group string) *Reader {
	if group == "" {
		panic("group should not be empty")
	}

	opts := gapOpts.BrokerOptions.(*Options)

	b := &Reader{
		gapOpts: gapOpts,
		opts:    opts,
		chPool:  NewDefaultPool(gapOpts.DrainContext, opts),
		q:       fmt.Sprintf("gap.%s.q.%s", gapOpts.Version, group),
		group:   group,
	}

	var _ broker.Reader = b
	return b
}

func (r *Reader) init() error {
	ch, err := r.chPool.Rent()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(r.exchange(), ExchangeKind, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return nil
}

func (r *Reader) Subscribe(_ context.Context, topic string) error {
	ch, err := r.chPool.Rent()
	if err != nil {
		return err
	}
	defer r.chPool.Return(ch)

	o := r.opts.QueueOpts
	q, err := ch.QueueDeclare(r.q, o.Durable, o.AutoDelete, o.Exclusive, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.Name, topic, r.exchange(), false, nil)
	if err != nil {
		return err
	}

	slog.Debug("rabbitmq: routing key bound successfully", slog.String("queue", r.q), slog.String("routing_key", topic))
	return nil
}

func (r *Reader) Read(ctx context.Context) (<-chan *entity.Envelope, error) {
	enCh := make(chan *entity.Envelope, r.opts.PrefetchCount)

	go func() {
		defer close(enCh)

		for {
			var deliCh <-chan amqp.Delivery
			ch, err := r.chPool.Rent()
			if err != nil {
				slog.Error("failed to rent a channel", slog.Any("err", err))
				goto RETRY
			}

			deliCh, err = ch.ConsumeWithContext(
				ctx,
				r.q,
				r.consumerTag(),
				false, false, false, false, nil,
			)
			if err != nil {
				slog.Error("consume failed", slog.Any("err", err))
				goto RETRY
			}

			for deli := range deliCh {
				slog.Info(string(deli.Body))
				en := entity.NewEnvelope(r.gapOpts.Version, deli.RoutingKey, nil).
					WithPayload(deli.Body).
					WithGroup(r.group).
					WithTag(deli)

				for k, v := range deli.Headers {
					en.AddHeader(k, fmt.Sprintf("%v", v))
				}

				slog.Debug("rabbitmq: received a message",
					slog.String("topic", en.Topic),
					slog.String("id", en.IDString()),
					slog.String(corrid, en.Headers[corrid]),
				)

				select {
				case <-ctx.Done():
					return
				case enCh <- en:
				}
			}

		RETRY:
			r.chPool.Return(ch)

			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	return enCh, nil
}

func (r *Reader) Commit(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Ack(false)
}

func (r *Reader) Reject(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Nack(false, true)
}

func (r *Reader) exchange() string {
	if r.x == "" {
		r.x = fmt.Sprintf("gap.%s.x.%s", r.gapOpts.Version, r.opts.Exchange)
	}
	return r.x
}

func (r *Reader) consumerTag() string {
	if r.ctag == "" {
		name := r.gapOpts.ServiceName
		if name == "" {
			name = os.Args[0]
		}
		r.ctag = fmt.Sprintf("gap.%s", name)
	}
	return r.ctag
}
