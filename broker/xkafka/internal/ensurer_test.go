package internal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsureTopicWithDeduplicatesSameTopic(t *testing.T) {
	e := newEnsurer(new(Options), nil)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	ensure := func(context.Context, string) error {
		calls.Add(1)
		close(started)
		<-release
		return nil
	}

	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- e.ensureTopicWith(context.Background(), "orders", ensure)
	}()
	<-started

	waitCtx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- e.ensureTopicWith(waitCtx, "orders", ensure)
	}()
	cancel()

	if err := <-waiterDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting ensure error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("ensure calls = %d, want 1", got)
	}

	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner ensure failed: %v", err)
	}

	if err := e.ensureTopicWith(context.Background(), "orders", ensure); err != nil {
		t.Fatalf("cached ensure failed: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("ensure calls after cache hit = %d, want 1", got)
	}
}

func TestEnsureTopicWithRunsDifferentTopicsConcurrently(t *testing.T) {
	e := newEnsurer(new(Options), nil)
	entered := make(chan string, 2)
	release := make(chan struct{})

	ensure := func(_ context.Context, topic string) error {
		entered <- topic
		<-release
		return nil
	}

	errs := make(chan error, 2)
	go func() { errs <- e.ensureTopicWith(context.Background(), "orders", ensure) }()
	go func() { errs <- e.ensureTopicWith(context.Background(), "payments", ensure) }()

	seen := make(map[string]bool)
	for range 2 {
		select {
		case topic := <-entered:
			seen[topic] = true
		case <-time.After(time.Second):
			t.Fatal("different topics were serialized behind the cache lock")
		}
	}
	close(release)

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("ensure failed: %v", err)
		}
	}
	if !seen["orders"] || !seen["payments"] {
		t.Fatalf("entered topics = %v, want orders and payments", seen)
	}
}

func TestEnsureTopicWithDoesNotCacheFailure(t *testing.T) {
	e := newEnsurer(new(Options), nil)
	wantErr := errors.New("create failed")
	var calls atomic.Int32

	ensure := func(context.Context, string) error {
		calls.Add(1)
		return wantErr
	}

	for range 2 {
		if err := e.ensureTopicWith(context.Background(), "orders", ensure); !errors.Is(err, wantErr) {
			t.Fatalf("ensure error = %v, want %v", err, wantErr)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("ensure calls = %d, want 2", got)
	}
}
