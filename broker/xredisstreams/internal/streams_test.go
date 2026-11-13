package internal

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/redis/go-redis/v9"
)

func TestRedisStreamsRoundTrip(t *testing.T) {
	server := miniredis.RunT(t)
	opts := &Options{
		Addrs:              []string{server.Addr()},
		StreamEntriesCount: 10,
		BlockTimeout:       200 * time.Millisecond,
	}
	gapOpts := testGapOpts()
	producer, err := opts.OpenProducer(gapOpts)
	if err != nil {
		t.Fatalf("OpenProducer: %v", err)
	}
	t.Cleanup(func() { _ = producer.(broker.Closer).Close() })

	consumer, err := opts.OpenConsumer(gapOpts, "workers")
	if err != nil {
		t.Fatalf("OpenConsumer: %v", err)
	}
	concrete := consumer.(*Consumer)
	concrete.claimMinIdle = -1
	concrete.rejectRetryDelay = time.Millisecond
	t.Cleanup(func() { _ = concrete.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err = consumer.Subscribe(ctx, "orders"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	envelopes, err := consumer.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	published := entity.NewEnvelope("v-test", "orders", nil).WithPayload([]byte("hello"))
	published.AddHeader("gap-msg-id", "99")
	if err = producer.Produce(ctx, published); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	received := receiveEnvelope(t, envelopes)
	if string(received.Payload) != "hello" || received.Headers["gap-msg-id"] != "99" || received.Group != "workers" {
		t.Fatalf("received payload=%q headers=%v group=%s", received.Payload, received.Headers, received.Group)
	}
	if err = consumer.Reject(received.Tag); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	again := receiveEnvelope(t, envelopes)
	if string(again.Payload) != "hello" || again.Tag == received.Tag {
		t.Fatalf("redelivery payload=%q sameTag=%v", again.Payload, again.Tag == received.Tag)
	}
	if err = consumer.Commit(again.Tag); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	group := "gap.v-test.g.workers"
	pending, err := redis.NewClient(&redis.Options{Addr: server.Addr()}).XPending(ctx, "orders", group).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d, want 0", pending.Count)
	}
}

func TestGroupStartSkipsHistoryUnlessRequested(t *testing.T) {
	t.Run("new messages", func(t *testing.T) {
		server := miniredis.RunT(t)
		opts := redisOpts(server.Addr())
		gapOpts := testGapOpts()
		produceRaw(t, server.Addr(), "orders", "old")

		consumer := openConsumer(t, opts, gapOpts)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := consumer.Subscribe(ctx, "orders"); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		envelopes, err := consumer.Consume(ctx)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		produceRaw(t, server.Addr(), "orders", "new")
		received := receiveEnvelope(t, envelopes)
		if string(received.Payload) != "new" {
			t.Fatalf("payload = %q, want new", received.Payload)
		}
		if err = consumer.Commit(received.Tag); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})

	t.Run("from beginning", func(t *testing.T) {
		server := miniredis.RunT(t)
		opts := redisOpts(server.Addr())
		opts.ReadFromBeginning = true
		gapOpts := testGapOpts()
		produceRaw(t, server.Addr(), "orders", "old")

		consumer := openConsumer(t, opts, gapOpts)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := consumer.Subscribe(ctx, "orders"); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		envelopes, err := consumer.Consume(ctx)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		received := receiveEnvelope(t, envelopes)
		if string(received.Payload) != "old" {
			t.Fatalf("payload = %q, want old", received.Payload)
		}
		if err = consumer.Commit(received.Tag); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})
}

func TestClientsFollowBrokerIdentity(t *testing.T) {
	sameA := &Options{Addrs: []string{"b:6379", "a:6379"}, UserName: "u", Password: "p", DB: 1}
	sameB := &Options{Addrs: []string{"a:6379", "b:6379"}, UserName: "u", Password: "p", DB: 1, PoolSize: defaultPoolSize}
	otherPassword := &Options{Addrs: []string{"a:6379", "b:6379"}, UserName: "u", Password: "other", DB: 1}

	clientA, err := SingleClient(sameA)
	if err != nil {
		t.Fatalf("SingleClient: %v", err)
	}
	clientB, err := SingleClient(sameB)
	if err != nil {
		t.Fatalf("SingleClient: %v", err)
	}
	clientC, err := SingleClient(otherPassword)
	if err != nil {
		t.Fatalf("SingleClient: %v", err)
	}
	if clientA != clientB {
		t.Fatal("same broker identity opened two clients")
	}
	if clientA == clientC {
		t.Fatal("different passwords shared a client")
	}

	if _, err = SingleClient(&Options{URL: "not a url"}); err == nil {
		t.Fatal("invalid url was accepted")
	}
}

func redisOpts(addr string) *Options {
	return &Options{
		Addrs:              []string{addr},
		StreamEntriesCount: 10,
		BlockTimeout:       200 * time.Millisecond,
	}
}

func openConsumer(t *testing.T, opts *Options, gapOpts *ingap.Options) broker.Consumer {
	t.Helper()
	consumer, err := opts.OpenConsumer(gapOpts, "workers")
	if err != nil {
		t.Fatalf("OpenConsumer: %v", err)
	}
	consumer.(*Consumer).claimMinIdle = -1
	t.Cleanup(func() { _ = consumer.(*Consumer).Close() })
	return consumer
}

func produceRaw(t *testing.T, addr, stream, payload string) {
	t.Helper()
	values, err := streamValues(nil, []byte(payload))
	if err != nil {
		t.Fatalf("streamValues: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err = client.XAdd(t.Context(), &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		t.Fatalf("XADD: %v", err)
	}
}
