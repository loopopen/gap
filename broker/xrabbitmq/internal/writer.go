package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/options/gap"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Writer struct {
	gapOpts *gap.Options
	opts    *Options
	group   string
	chPool  ChanPool
	x       string
	q       string
	ctag    string
}

func NewWriter(gapOpts *gap.Options) *Writer {
	bp := gapOpts.BrokerPlugin
	opts := bp.(*Options)

	b := &Writer{
		gapOpts: gapOpts,
		opts:    opts,
		chPool:  NewDefaultPool(gapOpts.DrainContext, opts),
	}

	var _ broker.Writer = b
	return b
}

func (b *Writer) init() error {
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

// Pub implements [gap.Writer].
func (b *Writer) Write(ctx context.Context, envelope *entity.Envelope) error {
	routingKey := envelope.Topic
	body, err := envelope.PayloadBytes()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return errx.ErrNilPayload
	}
	return b.send(ctx, routingKey, envelope.Headers, envelope.Payload)
}

func (b *Writer) send(ctx context.Context, routingKey string, headers map[string]string, body []byte) error {
	ch, err := b.chPool.Rent()
	if err != nil {
		return err
	}
	defer b.chPool.Return(ch)

	tbl := make(map[string]any)
	for k, v := range headers {
		tbl[k] = v
	}

	// Exchange may not be bound to any queues.
	// In this case:
	// The message will be discarded by the broker.
	// The message in the database will be marked as successfully sent if persistence is enabled.
	if !b.opts.ConfirmMode {
		err := ch.PublishWithContext(ctx, b.exchange(), routingKey, false, false, amqp.Publishing{
			Headers:      tbl,
			MessageId:    headers[internal.KeysMessageID],
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		if err != nil {
			return err
		}
	} else {
		confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, b.exchange(), routingKey, false, false, amqp.Publishing{
			Headers:      tbl,
			MessageId:    headers[internal.KeysMessageID],
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		if err != nil {
			return err
		}
		if confirm == nil {
			return errors.New("rabbitmq: publish confirm is nil")
		}
		acked, err := confirm.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !acked {
			return errors.New("rabbitmq: message was nacked by broker")
		}
	}

	return nil
}

func (b *Writer) exchange() string {
	if b.x == "" {
		b.x = fmt.Sprintf("gap.%s.x.%s", b.gapOpts.Version, b.opts.Exchange)
	}
	return b.x
}
