package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
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
	conn    *sharedConn
	release func() error
	mu      sync.Mutex
	ch      *amqp.Channel
	topics  map[string]struct{}
	closed  bool
	x       string
	q       string
	ctag    string
}

func NewConsumer(gapOpts *ingap.Options, opts *Options, group string) *Consumer {
	if group == "" {
		panic("group should not be empty")
	}

	resources, release := opts.acquireRuntime(gapOpts)
	consumer := &Consumer{
		gapOpts: gapOpts,
		opts:    opts,
		conn:    resources.conn,
		release: release,
		topics:  make(map[string]struct{}),
		q:       fmt.Sprintf("gap.%s.q.%s", gapOpts.Version, group),
		group:   group,
	}

	var _ broker.Consumer = consumer
	return consumer
}

func (c *Consumer) init() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.channelLocked()
	return err
}

func (c *Consumer) Subscribe(_ context.Context, topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, err := c.channelLocked()
	if err != nil {
		return err
	}

	o := c.opts.QueueOpts
	q, err := ch.QueueDeclare(c.q, o.Durable, o.AutoDelete, o.Exclusive, false, nil)
	if err != nil {
		c.discardChannelLocked(ch)
		return err
	}

	err = ch.QueueBind(q.Name, topic, c.exchange(), false, nil)
	if err != nil {
		c.discardChannelLocked(ch)
		return err
	}
	c.topics[topic] = struct{}{}

	slog.Debug("rabbitmq: routing key bound successfully", slog.String("queue", c.q), slog.String("routing_key", topic))
	return nil
}

func (c *Consumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	enCh := make(chan *entity.Envelope, c.opts.PrefetchCount)

	go func() {
		defer close(enCh)

		for {
			var deliCh <-chan amqp.Delivery
			ch, err := c.consumerChannel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("failed to open consumer channel", slog.Any("err", err))
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
			c.invalidateChannel(ch)

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

func (c *Consumer) consumerChannel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channelLocked()
}

func (c *Consumer) channelLocked() (*amqp.Channel, error) {
	if c.closed {
		return nil, errConnClosed
	}
	if c.ch != nil && !c.ch.IsClosed() {
		return c.ch, nil
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err = ch.Qos(c.opts.PrefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err = ch.ExchangeDeclare(c.exchange(), ExchangeKind, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, err
	}

	if len(c.topics) > 0 {
		o := c.opts.QueueOpts
		q, declareErr := ch.QueueDeclare(c.q, o.Durable, o.AutoDelete, o.Exclusive, false, nil)
		if declareErr != nil {
			_ = ch.Close()
			return nil, declareErr
		}
		for topic := range c.topics {
			if bindErr := ch.QueueBind(q.Name, topic, c.exchange(), false, nil); bindErr != nil {
				_ = ch.Close()
				return nil, bindErr
			}
		}
	}

	c.ch = ch
	return ch, nil
}

func (c *Consumer) invalidateChannel(ch *amqp.Channel) {
	if ch == nil {
		return
	}
	c.mu.Lock()
	if c.ch == ch {
		c.ch = nil
	}
	c.mu.Unlock()
	if !ch.IsClosed() {
		_ = ch.Close()
	}
}

func (c *Consumer) discardChannelLocked(ch *amqp.Channel) {
	if c.ch == ch {
		c.ch = nil
	}
	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ch := c.ch
	c.ch = nil
	c.mu.Unlock()

	var closeErr error
	if ch != nil && !ch.IsClosed() {
		closeErr = ch.Close()
	}
	if c.release != nil {
		closeErr = errors.Join(closeErr, c.release())
	}
	return closeErr
}

var _ broker.Closer = (*Consumer)(nil)
