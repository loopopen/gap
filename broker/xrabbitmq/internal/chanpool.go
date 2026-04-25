package internal

import (
	"context"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ChanPool interface {
	Rent() (*amqp.Channel, error)

	Return(ch *amqp.Channel)
}

type DefaultPool struct {
	opts *Options
	conn *amqp.Connection
}

func NewDefaultPool(drainCtx context.Context, opts *Options) *DefaultPool {
	p := &DefaultPool{
		opts: opts,
	}
	go func() {
		<-drainCtx.Done()
		if p.conn != nil {
			_ = p.conn.Close()
			slog.Debug("rabbitmq connection closed")
		}
	}()
	return p
}

func (p *DefaultPool) connection() (*amqp.Connection, error) {
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn, nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	conn, err := amqp.Dial(p.opts.AmqpURL())
	if err != nil {
		slog.Error("failed to connect rabbitmq server", slog.Any("err", err))
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

func (p *DefaultPool) Rent() (*amqp.Channel, error) {
	conn, err := p.connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.Qos(p.opts.PrefetchCount, 0, false)
	if err != nil {
		return nil, err
	}

	if p.opts.PublisherConfirms {
		err := ch.Confirm(false)
		if err != nil {
			return nil, err
		}
	}
	return ch, nil
}

func (p *DefaultPool) Return(ch *amqp.Channel) {
	if ch != nil && !ch.IsClosed() {
		_ = ch.Close()
	}
}
