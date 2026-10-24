package internal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisherChannelPoolDoesNotReopenAfterClose(t *testing.T) {
	opts := new(Options).With()
	pool := newPubChannelPool(newSharedConn(opts), opts, 1)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := pool.Rent(context.Background()); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Rent() error = %v, want %v", err, errPoolClosed)
	}
}

func TestPublisherChannelPoolBoundsAndReusesChannels(t *testing.T) {
	opts := new(Options).With()
	pool := newPubChannelPool(newSharedConn(opts), opts, 2)
	var opened atomic.Int32
	pool.newChannel = func() (*publisherChannel, error) {
		opened.Add(1)
		return &publisherChannel{channel: new(amqp.Channel)}, nil
	}

	first, err := pool.Rent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.Rent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := opened.Load(); got != 2 {
		t.Fatalf("opened channels = %d, want 2", got)
	}

	rented := make(chan *publisherChannel, 1)
	errCh := make(chan error, 1)
	go func() {
		third, rentErr := pool.Rent(context.Background())
		if rentErr != nil {
			errCh <- rentErr
			return
		}
		rented <- third
	}()

	select {
	case <-rented:
		t.Fatal("rent exceeded the configured channel capacity")
	case err = <-errCh:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}

	pool.Return(first, true)
	select {
	case third := <-rented:
		if third != first {
			t.Fatal("pool did not reuse the returned channel")
		}
	case err = <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("rent did not unblock after a channel was returned")
	}
	if got := opened.Load(); got != 2 {
		t.Fatalf("opened channels after reuse = %d, want 2", got)
	}

	// Do not close these synthetic amqp.Channel values: unlike real channels,
	// they intentionally have no backing connection.
	_ = second
}

func TestPublisherChannelPoolReplacesDiscardedChannel(t *testing.T) {
	opts := new(Options).With()
	pool := newPubChannelPool(newSharedConn(opts), opts, 1)
	var opened atomic.Int32
	pool.newChannel = func() (*publisherChannel, error) {
		opened.Add(1)
		// A nil channel is sufficient here because this test exercises slot
		// accounting only and deliberately discards every synthetic value.
		return new(publisherChannel), nil
	}

	first, err := pool.Rent(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	rented := make(chan *publisherChannel, 1)
	errCh := make(chan error, 1)
	go func() {
		replacement, rentErr := pool.Rent(context.Background())
		if rentErr != nil {
			errCh <- rentErr
			return
		}
		rented <- replacement
	}()

	pool.Return(first, false)
	select {
	case <-rented:
	case err = <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("discarding a channel did not wake a waiting renter")
	}
	if got := opened.Load(); got != 2 {
		t.Fatalf("opened channels = %d, want 2", got)
	}
}
