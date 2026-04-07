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
	"github.com/lopolopen/gap/options/gap"
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

	bp := gapOpts.BrokerPlugin
	opts := bp.(*Options)

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

func (b *Reader) init() error {
	ch, err := b.chPool.Rent()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(b.exchange(), ExchangeKind, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return nil
}

func (b *Reader) Subscribe(_ context.Context, topic string) error {
	ch, err := b.chPool.Rent()
	if err != nil {
		return err
	}
	defer b.chPool.Return(ch)

	o := b.opts.QueueOpts
	q, err := ch.QueueDeclare(b.q, o.Durable, o.AutoDelete, o.Exclusive, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.Name, topic, b.exchange(), false, nil)
	if err != nil {
		return err
	}

	slog.Debug("rabbitmq: routing key bound successfully", slog.String("queue", b.q), slog.String("routing_key", topic))
	return nil
}

func (b *Reader) Read(ctx context.Context) (<-chan *entity.Envelope, error) {
	enCh := make(chan *entity.Envelope, b.opts.PrefetchCount)

	go func() {
		defer close(enCh)

		for {
			var deliCh <-chan amqp.Delivery
			ch, err := b.chPool.Rent()
			if err != nil {
				slog.Error("failed to rent a channel", slog.Any("err", err))
				goto RETRY
			}

			deliCh, err = ch.ConsumeWithContext(
				ctx,
				b.q,
				b.consumerTag(),
				false, false, false, false, nil,
			)
			if err != nil {
				slog.Error("consume failed", slog.Any("err", err))
				goto RETRY
			}

			for deli := range deliCh {
				slog.Info(string(deli.Body))
				en := entity.NewEnvelope(b.gapOpts.Version, deli.RoutingKey, nil).
					WithPayload(deli.Body).
					WithGroup(b.group).
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
			b.chPool.Return(ch)

			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	return enCh, nil
}

func (b *Reader) Commit(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Ack(false)
}

func (b *Reader) Reject(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Nack(false, true)
}

func (b *Reader) exchange() string {
	if b.x == "" {
		b.x = fmt.Sprintf("gap.%s.x.%s", b.gapOpts.Version, b.opts.Exchange)
	}
	return b.x
}

func (b *Reader) consumerTag() string {
	if b.ctag == "" {
		name := b.gapOpts.ServiceName
		if name == "" {
			name = os.Args[0]
		}
		b.ctag = fmt.Sprintf("gap.%s", name)
	}
	return b.ctag
}
