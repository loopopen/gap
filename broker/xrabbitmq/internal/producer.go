package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	gapOpts *ingap.Options
	opts    *Options
	group   string
	chPool  ChanPool
	x       string
	q       string
	ctag    string
}

func NewProducer(gapOpts *ingap.Options, opts *Options) *Producer {
	producer := &Producer{
		gapOpts: gapOpts,
		opts:    opts,
		chPool:  NewDefaultPool(opts),
	}

	var _ broker.Producer = producer
	return producer
}

func (p *Producer) init() error {
	ch, err := p.chPool.Rent()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(p.exchange(), ExchangeKind, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return nil
}

// Produce implements [broker.Producer].
func (p *Producer) Produce(ctx context.Context, envelope *entity.Envelope) error {
	routingKey := envelope.Topic
	body, err := envelope.PayloadBytes()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return errx.ErrNilPayload
	}
	return p.send(ctx, routingKey, envelope.Headers, envelope.Payload)
}

func (p *Producer) send(ctx context.Context, routingKey string, headers map[string]string, body []byte) error {
	ch, err := p.chPool.Rent()
	if err != nil {
		return err
	}
	defer p.chPool.Return(ch)

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

	if !p.opts.PublisherConfirms && !p.opts.Mandatory {
		err := ch.PublishWithContext(ctx, p.exchange(), routingKey, false, false, publishing)
		if err != nil {
			return err
		}
	} else {
		var returns <-chan amqp.Return
		if p.opts.Mandatory {
			returns = ch.NotifyReturn(make(chan amqp.Return, 1))
		}

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx, p.exchange(), routingKey, p.opts.Mandatory, false, publishing,
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
		if p.opts.Mandatory {
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

func (p *Producer) exchange() string {
	if p.x == "" {
		p.x = fmt.Sprintf("gap.%s.x.%s", p.gapOpts.Version, p.opts.Exchange)
	}
	return p.x
}

func (p *Producer) Close() error {
	if closer, ok := p.chPool.(broker.Closer); ok {
		return closer.Close()
	}
	return nil
}

var _ broker.Closer = (*Producer)(nil)
