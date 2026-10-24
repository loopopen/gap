package internal

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	errConnClosed = errors.New("rabbitmq: connection is closed")
	errPoolClosed = errors.New("rabbitmq: publisher channel pool is closed")
)

// sharedConn owns the single AMQP connection used by one Gap runtime.
// Publishers and consumers create their channels from this connection, while
// channel ownership remains with the individual publisher pool or consumer.
type sharedConn struct {
	opts *Options

	mu     sync.Mutex
	conn   *amqp.Connection
	closed bool
}

func newSharedConn(opts *Options) *sharedConn {
	return &sharedConn{opts: opts}
}

func (c *sharedConn) connection() (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errConnClosed
	}
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}

	conn, err := amqp.Dial(c.opts.AmqpURL())
	if err != nil {
		slog.Error("failed to connect rabbitmq server", slog.Any("err", err))
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *sharedConn) Channel() (*amqp.Channel, error) {
	conn, err := c.connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err == nil || !conn.IsClosed() {
		return ch, err
	}

	// Let this attempt establish a replacement connection. The pointer
	// comparison prevents an old failed connection from clearing a newer one.
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.mu.Unlock()

	conn, err = c.connection()
	if err != nil {
		return nil, err
	}
	return conn.Channel()
}

func (c *sharedConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn == nil || conn.IsClosed() {
		return nil
	}
	if err := conn.Close(); err != nil {
		return err
	}
	slog.Debug("rabbitmq connection closed")
	return nil
}

type publisherChannel struct {
	channel *amqp.Channel
	returns <-chan amqp.Return
}

// pubChannelPool is a bounded, lazy pool. A rented channel is exclusively
// owned by one publish operation until it is returned or discarded.
type pubChannelPool struct {
	conn       *sharedConn
	opts       *Options
	capacity   int
	idle       chan *publisherChannel
	available  chan struct{}
	closedCh   chan struct{}
	mu         sync.Mutex
	open       int
	closed     bool
	closeOnce  sync.Once
	newChannel func() (*publisherChannel, error)
}

func newPubChannelPool(conn *sharedConn, opts *Options, capacity int) *pubChannelPool {
	if capacity < 1 {
		capacity = 1
	}
	pool := &pubChannelPool{
		conn:      conn,
		opts:      opts,
		capacity:  capacity,
		idle:      make(chan *publisherChannel, capacity),
		available: make(chan struct{}, capacity),
		closedCh:  make(chan struct{}),
	}
	pool.newChannel = pool.openChannel
	return pool
}

func (p *pubChannelPool) Rent(ctx context.Context) (*publisherChannel, error) {
	for {
		select {
		case pooled := <-p.idle:
			if pooled.channel != nil && !pooled.channel.IsClosed() && !p.isClosed() {
				return pooled, nil
			}
			p.discard(pooled)
			continue
		default:
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if p.open < p.capacity {
			p.open++
			p.mu.Unlock()

			pooled, err := p.newChannel()
			if err != nil {
				p.decrementOpen()
				return nil, err
			}
			if p.isClosed() {
				p.discard(pooled)
				return nil, errPoolClosed
			}
			return pooled, nil
		}
		p.mu.Unlock()

		select {
		case pooled := <-p.idle:
			if pooled.channel != nil && !pooled.channel.IsClosed() && !p.isClosed() {
				return pooled, nil
			}
			p.discard(pooled)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.available:
			continue
		case <-p.closedCh:
			return nil, errPoolClosed
		}
	}
}

func (p *pubChannelPool) openChannel() (*publisherChannel, error) {
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, err
	}

	if p.opts.PublisherConfirms || p.opts.Mandatory {
		if err = ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, err
		}
	}

	pooled := &publisherChannel{channel: ch}
	if p.opts.Mandatory {
		// Register once for the lifetime of this channel. Registering on every
		// publish would retain obsolete listeners when a channel is reused.
		pooled.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	}
	return pooled, nil
}

// Return makes a healthy channel available to another publisher. Callers must
// mark a channel unusable if a publish did not reach a complete protocol
// boundary (for example, a context expired while waiting for confirmation).
func (p *pubChannelPool) Return(pooled *publisherChannel, reusable bool) {
	if pooled == nil {
		return
	}
	if !reusable || pooled.channel == nil || pooled.channel.IsClosed() {
		p.discard(pooled)
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.discard(pooled)
		return
	}
	select {
	case p.idle <- pooled:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		// This should only be reachable after misuse (such as a duplicate
		// return), but closing is safer than allowing an unbounded resource.
		p.discard(pooled)
	}
}

func (p *pubChannelPool) discard(pooled *publisherChannel) {
	if pooled != nil && pooled.channel != nil && !pooled.channel.IsClosed() {
		_ = pooled.channel.Close()
	}
	p.decrementOpen()
}

func (p *pubChannelPool) decrementOpen() {
	p.mu.Lock()
	decremented := p.open > 0
	if p.open > 0 {
		p.open--
	}
	p.mu.Unlock()
	if !decremented {
		return
	}
	select {
	case p.available <- struct{}{}:
	default:
	}
}

func (p *pubChannelPool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *pubChannelPool) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.closedCh)
		p.mu.Unlock()

		for {
			select {
			case pooled := <-p.idle:
				if pooled != nil && pooled.channel != nil && !pooled.channel.IsClosed() {
					closeErr = errors.Join(closeErr, pooled.channel.Close())
				}
				p.decrementOpen()
			default:
				return
			}
		}
	})
	return closeErr
}

// runtimeResources contains all AMQP resources shared within one Gap runtime.
// Consumer channels are intentionally not stored here; each Consumer owns one.
type runtimeResources struct {
	conn       *sharedConn
	publishers *pubChannelPool
	closeOnce  sync.Once
	closeErr   error
}

func newRuntimeResources(opts *Options, publisherConcurrency int) *runtimeResources {
	conn := newSharedConn(opts)
	return &runtimeResources{
		conn:       conn,
		publishers: newPubChannelPool(conn, opts, publisherConcurrency),
	}
}

func (r *runtimeResources) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = errors.Join(r.publishers.Close(), r.conn.Close())
	})
	return r.closeErr
}
