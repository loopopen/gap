package internal

import (
	"errors"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ChanPool interface {
	Rent() (*amqp.Channel, error)

	Return(ch *amqp.Channel)
}

type DefaultPool struct {
	opts   *Options
	mu     sync.Mutex
	conn   *amqp.Connection
	closed bool
}

var errPoolClosed = errors.New("rabbitmq: channel pool is closed")

func NewDefaultPool(opts *Options) *DefaultPool {
	return &DefaultPool{opts: opts}
}

func (p *DefaultPool) connection() (*amqp.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errPoolClosed
	}
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

	// Mandatory returns are asynchronous. Confirm mode gives the publisher a
	// response boundary: RabbitMQ sends basic.return before the corresponding
	// basic.ack, so the writer can reliably decide whether the message routed.
	if p.opts.PublisherConfirms || p.opts.Mandatory {
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

func (p *DefaultPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	conn := p.conn
	p.conn = nil
	p.mu.Unlock()

	if conn == nil || conn.IsClosed() {
		return nil
	}
	if err := conn.Close(); err != nil {
		return err
	}
	slog.Debug("rabbitmq connection closed")
	return nil
}
