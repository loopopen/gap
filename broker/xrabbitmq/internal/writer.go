package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	"github.com/loopopen/gap/internal/gap"
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
	opts := gapOpts.BrokerOptions.(*Options)

	w := &Writer{
		gapOpts: gapOpts,
		opts:    opts,
		chPool:  NewDefaultPool(gapOpts.DrainContext, opts),
	}

	var _ broker.Writer = w
	return w
}

func (w *Writer) init() error {
	ch, err := w.chPool.Rent()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(w.exchange(), ExchangeKind, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return nil
}

// Pub implements [gap.Writer].
func (w *Writer) Write(ctx context.Context, envelope *entity.Envelope) error {
	routingKey := envelope.Topic
	body, err := envelope.PayloadBytes()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return errx.ErrNilPayload
	}
	return w.send(ctx, routingKey, envelope.Headers, envelope.Payload)
}

func (w *Writer) send(ctx context.Context, routingKey string, headers map[string]string, body []byte) error {
	ch, err := w.chPool.Rent()
	if err != nil {
		return err
	}
	defer w.chPool.Return(ch)

	tbl := make(map[string]any)
	for k, v := range headers {
		tbl[k] = v
	}

	publishing := amqp.Publishing{
		Headers:      tbl,
		MessageId:    headers[internal.KeysMessageID],
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	if !w.opts.PublisherConfirms && !w.opts.Mandatory {
		err := ch.PublishWithContext(ctx, w.exchange(), routingKey, false, false, publishing)
		if err != nil {
			return err
		}
	} else {
		var returns <-chan amqp.Return
		if w.opts.Mandatory {
			returns = ch.NotifyReturn(make(chan amqp.Return, 1))
		}

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx, w.exchange(), routingKey, w.opts.Mandatory, false, publishing,
		)
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

		// RabbitMQ sends basic.return before basic.ack for an unroutable
		// mandatory message. Since the return channel is buffered, a completed
		// confirmation makes this non-blocking check deterministic.
		if w.opts.Mandatory {
			select {
			case ret, ok := <-returns:
				if ok {
					return fmt.Errorf(
						"rabbitmq: message was returned: code=%d reason=%q exchange=%q routing_key=%q",
						ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey,
					)
				}
			default:
			}
		}
	}

	return nil
}

func (w *Writer) exchange() string {
	if w.x == "" {
		w.x = fmt.Sprintf("gap.%s.x.%s", w.gapOpts.Version, w.opts.Exchange)
	}
	return w.x
}
