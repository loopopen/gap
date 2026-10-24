package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/entity"
	ingap "github.com/loopopen/gap/internal/gap"
	amqp "github.com/rabbitmq/amqp091-go"
)

const corrid = internal.KeysCorrelationID

type Consumer struct {
	gapOpts *ingap.Options
	opts    *Options
	group   string
	chPool  ChanPool
	x       string
	q       string
	ctag    string
}

func NewConsumer(gapOpts *ingap.Options, opts *Options, group string) *Consumer {
	if group == "" {
		panic("group should not be empty")
	}

	consumer := &Consumer{
		gapOpts: gapOpts,
		opts:    opts,
		chPool:  NewDefaultPool(gapOpts.DrainContext, opts),
		q:       fmt.Sprintf("gap.%s.q.%s", gapOpts.Version, group),
		group:   group,
	}

	var _ broker.Consumer = consumer
	return consumer
}

func (c *Consumer) init() error {
	ch, err := c.chPool.Rent()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(c.exchange(), ExchangeKind, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) Subscribe(_ context.Context, topic string) error {
	ch, err := c.chPool.Rent()
	if err != nil {
		return err
	}
	defer c.chPool.Return(ch)

	o := c.opts.QueueOpts
	q, err := ch.QueueDeclare(c.q, o.Durable, o.AutoDelete, o.Exclusive, false, nil)
	if err != nil {
		return err
	}

	err = ch.QueueBind(q.Name, topic, c.exchange(), false, nil)
	if err != nil {
		return err
	}

	slog.Debug("rabbitmq: routing key bound successfully", slog.String("queue", c.q), slog.String("routing_key", topic))
	return nil
}

func (c *Consumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	enCh := make(chan *entity.Envelope, c.opts.PrefetchCount)

	go func() {
		defer close(enCh)

		for {
			var deliCh <-chan amqp.Delivery
			ch, err := c.chPool.Rent()
			if err != nil {
				slog.Error("failed to rent a channel", slog.Any("err", err))
				goto RETRY
			}

			deliCh, err = ch.ConsumeWithContext(
				ctx,
				c.q,
				c.consumerTag(),
				false, false, false, false, nil,
			)
			if err != nil {
				slog.Error("rabbitmq: consume failed", slog.Any("err", err))
				goto RETRY
			}

			for deli := range deliCh {
				en := entity.NewEnvelope(c.gapOpts.Version, deli.RoutingKey, nil).
					WithPayload(deli.Body).
					WithGroup(c.group).
					WithTag(deli)

				for k, v := range deli.Headers {
					en.AddHeader(k, fmt.Sprintf("%v", v))
				}

				en.Log().Debug("rabbitmq: received a message")

				select {
				case <-ctx.Done():
					return
				case enCh <- en:
				}
			}

		RETRY:
			c.chPool.Return(ch)

			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	return enCh, nil
}

func (c *Consumer) Commit(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Ack(false)
}

func (c *Consumer) Reject(tag any) error {
	deli, _ := tag.(amqp.Delivery)
	return deli.Nack(false, true)
}

func (c *Consumer) exchange() string {
	if c.x == "" {
		c.x = fmt.Sprintf("gap.%s.x.%s", c.gapOpts.Version, c.opts.Exchange)
	}
	return c.x
}

func (c *Consumer) consumerTag() string {
	if c.ctag == "" {
		name := c.gapOpts.ServiceName
		if name == "" {
			name = os.Args[0]
		}
		c.ctag = fmt.Sprintf("gap.%s", name)
	}
	return c.ctag
}
