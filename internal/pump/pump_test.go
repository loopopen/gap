package pump

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

type senderStub struct {
	ids []uint
}

type handlerStub struct{}

func (*handlerStub) HandleAndUpdate(context.Context, *entity.Envelope) error { return nil }

type statusStorageStub struct {
	storage.Storage
	receivedID         uint
	receivedLeaseToken string
	receivedStatus     enum.Status
}

type cleanupResult struct {
	deleted int64
	err     error
}

type cleanupStorageStub struct {
	storage.Storage
	published []cleanupResult
	received  []cleanupResult
	pubCalls  int
	recCalls  int
}

func (s *cleanupStorageStub) DeleteExpiredPublishedBatch(context.Context, int) (int64, error) {
	s.pubCalls++
	if len(s.published) == 0 {
		return 0, nil
	}
	result := s.published[0]
	s.published = s.published[1:]
	return result.deleted, result.err
}

func (s *cleanupStorageStub) DeleteExpiredReceivedBatch(context.Context, int) (int64, error) {
	s.recCalls++
	if len(s.received) == 0 {
		return 0, nil
	}
	result := s.received[0]
	s.received = s.received[1:]
	return result.deleted, result.err
}

func (s *statusStorageStub) UpdateStatusReceived(_ context.Context, id uint, leaseToken string, status enum.Status) error {
	s.receivedID = id
	s.receivedLeaseToken = leaseToken
	s.receivedStatus = status
	return nil
}

func (s *senderStub) SendAndUpdate(_ context.Context, envelope *entity.Envelope) error {
	s.ids = append(s.ids, envelope.ID)
	return nil
}

func TestPumpRoutesMessagesToConfiguredSender(t *testing.T) {
	p := &Pump{}
	rabbit := new(senderStub)
	kafka := new(senderStub)
	if err := p.SetSender("rabbit-events", "rabbit-config", rabbit); err != nil {
		t.Fatal(err)
	}
	if err := p.SetSender("kafka-events", "kafka-config", kafka); err != nil {
		t.Fatal(err)
	}

	rabbitMessage := &entity.Envelope{ID: 1, Headers: map[string]string{
		entity.BrokerNameHeader: "rabbit-events",
		entity.BrokerTypeHeader: "RabbitMQ",
	}}
	kafkaMessage := &entity.Envelope{ID: 2, Headers: map[string]string{
		entity.BrokerNameHeader: "kafka-events",
		entity.BrokerTypeHeader: "Kafka",
	}}
	if err := p.send(context.Background(), rabbitMessage); err != nil {
		t.Fatal(err)
	}
	if err := p.send(context.Background(), kafkaMessage); err != nil {
		t.Fatal(err)
	}

	if len(rabbit.ids) != 1 || rabbit.ids[0] != rabbitMessage.ID {
		t.Fatalf("rabbit sender received ids %v", rabbit.ids)
	}
	if len(kafka.ids) != 1 || kafka.ids[0] != kafkaMessage.ID {
		t.Fatalf("kafka sender received ids %v", kafka.ids)
	}
}

func TestPumpRejectsUnknownBrokerIdentity(t *testing.T) {
	p := &Pump{}
	if err := p.SetSender("events", "rabbit-config", new(senderStub)); err != nil {
		t.Fatal(err)
	}

	message := &entity.Envelope{Headers: map[string]string{
		entity.BrokerNameHeader: "missing",
		entity.BrokerTypeHeader: "Kafka",
	}}
	if err := p.send(context.Background(), message); err == nil {
		t.Fatal("expected unknown broker identity error")
	}
}

func TestPumpMarksReceivedFailedWhenHandlerRouteIsMissing(t *testing.T) {
	stor := new(statusStorageStub)
	p := &Pump{
		storage:  stor,
		handlers: new(sync.Map),
	}
	envelope := &entity.Envelope{
		ID:         42,
		Group:      "billing",
		LeaseToken: "lease-token",
		Headers: map[string]string{
			entity.BrokerNameHeader: "events",
			entity.BrokerTypeHeader: "RabbitMQ",
		},
	}

	if err := p.handle(context.Background(), envelope); err == nil {
		t.Fatal("expected missing handler error")
	}
	if stor.receivedID != envelope.ID {
		t.Fatalf("updated id = %d, want %d", stor.receivedID, envelope.ID)
	}
	if stor.receivedLeaseToken != envelope.LeaseToken {
		t.Fatalf("updated lease token = %q, want %q", stor.receivedLeaseToken, envelope.LeaseToken)
	}
	if stor.receivedStatus != enum.StatusFailed {
		t.Fatalf("updated status = %v, want %v", stor.receivedStatus, enum.StatusFailed)
	}
}

func TestCleanupCycleAlternatesTablesUntilBothAreDrained(t *testing.T) {
	stor := &cleanupStorageStub{
		published: []cleanupResult{{deleted: 2}, {deleted: 2}, {deleted: 0}},
		received:  []cleanupResult{{deleted: 2}, {deleted: 1}},
	}
	p := &Pump{
		gapOpts: &ingap.Options{CleanupBatchSize: 2},
		storage: stor,
	}

	deleted, err := p.cleanupCycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 7 {
		t.Fatalf("deleted = %d, want 7", deleted)
	}
	if stor.pubCalls != 3 || stor.recCalls != 2 {
		t.Fatalf("cleanup calls = published:%d received:%d, want 3 and 2", stor.pubCalls, stor.recCalls)
	}
}

func TestCleanupCycleContinuesOtherTableAfterError(t *testing.T) {
	boom := errors.New("cleanup failed")
	stor := &cleanupStorageStub{
		published: []cleanupResult{{err: boom}},
		received:  []cleanupResult{{deleted: 1}},
	}
	p := &Pump{
		gapOpts: &ingap.Options{CleanupBatchSize: 2},
		storage: stor,
	}

	deleted, err := p.cleanupCycle(context.Background())
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("cleanup error = %v, want %v", err, boom)
	}
	if stor.recCalls != 1 {
		t.Fatalf("received cleanup calls = %d, want 1", stor.recCalls)
	}
}

func TestCleanupCycleStopsOnContextCancellation(t *testing.T) {
	stor := new(cleanupStorageStub)
	p := &Pump{
		gapOpts: &ingap.Options{CleanupBatchSize: 2},
		storage: stor,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deleted, err := p.cleanupCycle(ctx)
	if deleted != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup = deleted:%d err:%v, want 0 and context canceled", deleted, err)
	}
	if stor.pubCalls != 0 || stor.recCalls != 0 {
		t.Fatalf("cleanup called storage after cancellation: published:%d received:%d", stor.pubCalls, stor.recCalls)
	}
}

func TestCleanupDelayStaysWithinTenPercent(t *testing.T) {
	interval := 5 * time.Minute
	for range 100 {
		delay := cleanupDelay(interval)
		if delay < 9*interval/10 || delay > 11*interval/10 {
			t.Fatalf("cleanup delay = %v, want within 10%% of %v", delay, interval)
		}
	}
}

func TestPumpRejectsTwoLiveConfigsForSameBrokerRoute(t *testing.T) {
	p := &Pump{}
	first := new(senderStub)
	if err := p.SetSender("events", "old-config", first); err != nil {
		t.Fatal(err)
	}

	err := p.SetSender("events", "new-config", new(senderStub))
	if err == nil {
		t.Fatal("expected duplicate broker route configuration error")
	}
	if got, want := err.Error(), `broker name "events" has multiple configurations; set a unique BrokerName`; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestPumpRejectsSameBrokerNameWithDifferentConfigs(t *testing.T) {
	p := &Pump{}
	if err := p.SetSender("events", "rabbit-config", new(senderStub)); err != nil {
		t.Fatal(err)
	}
	if err := p.SetSender("events", "kafka-config", new(senderStub)); err == nil {
		t.Fatal("expected duplicate broker name configuration error")
	}
}

func TestNextClaimGroupRotatesRegisteredNames(t *testing.T) {
	p := &Pump{handlers: new(sync.Map)}
	p.handlers.Store(handlerKey("rabbit", "shipping"), new(handlerStub))
	p.handlers.Store(handlerKey("rabbit", "billing"), new(handlerStub))
	p.handlers.Store(handlerKey("kafka", "billing"), new(handlerStub))
	p.handlers.Store(handlerKey("rabbit", ""), new(handlerStub))

	var got []string
	for i := 0; i < 4; i++ {
		group, ok := p.nextClaimGroup()
		if !ok {
			t.Fatal("expected a group")
		}
		got = append(got, group)
	}
	if !reflect.DeepEqual(got, []string{"", "billing", "shipping", ""}) {
		t.Fatalf("rotation = %#v", got)
	}
}

func TestNextClaimGroupFollowsNameWhenSetChanges(t *testing.T) {
	p := &Pump{handlers: new(sync.Map)}
	p.handlers.Store(handlerKey("rabbit", "b"), new(handlerStub))
	p.handlers.Store(handlerKey("rabbit", "d"), new(handlerStub))
	if group, ok := p.nextClaimGroup(); !ok || group != "b" {
		t.Fatalf("first group = %q ok=%v", group, ok)
	}
	p.handlers.Store(handlerKey("rabbit", "a"), new(handlerStub))
	if group, _ := p.nextClaimGroup(); group != "d" {
		t.Fatalf("next group = %q, want d", group)
	}
}

func TestNextClaimGroupRestartsWhenCursorLeaves(t *testing.T) {
	p := &Pump{
		handlers:       new(sync.Map),
		claimCursor:    "gone",
		claimCursorSet: true,
	}
	p.handlers.Store(handlerKey("rabbit", "keep"), new(handlerStub))
	group, ok := p.nextClaimGroup()
	if !ok || group != "keep" {
		t.Fatalf("group = %q ok=%v", group, ok)
	}
}

func TestNextClaimGroupEmpty(t *testing.T) {
	p := &Pump{handlers: new(sync.Map)}
	if _, ok := p.nextClaimGroup(); ok {
		t.Fatal("expected no group")
	}
}

func TestPumpRejectsSameHandlerBrokerNameWithDifferentConfigs(t *testing.T) {
	p := &Pump{}
	if err := p.SetHandler("events", "rabbit-config", "billing", new(handlerStub)); err != nil {
		t.Fatal(err)
	}
	err := p.SetHandler("events", "kafka-config", "billing", new(handlerStub))
	if err == nil {
		t.Fatal("expected duplicate handler broker name configuration error")
	}
	if got, want := err.Error(), `broker name "events" has multiple configurations; set a unique BrokerName`; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

type blockingSender struct {
	started chan uint
	release chan struct{}
}

func newBlockingSender() *blockingSender {
	return &blockingSender{
		started: make(chan uint, 10),
		release: make(chan struct{}, 10),
	}
}

func (s *blockingSender) SendAndUpdate(_ context.Context, envelope *entity.Envelope) error {
	s.started <- envelope.ID
	<-s.release
	return nil
}

func newDispatchTestPump(sender Sender, concurrency, queueSize int) *Pump {
	p := &Pump{
		sendCh:         make(chan *entity.Envelope, queueSize),
		senders:        map[string]Sender{routeKey("events"): sender},
		senderConfigs:  make(map[string]string),
		handlers:       new(sync.Map),
		handlerConfigs: new(sync.Map),
	}
	p.dispatcherWG.Add(1)
	go func() {
		defer p.dispatcherWG.Done()
		p.dispatchingSend(context.Background(), concurrency)
	}()
	return p
}

func dispatchTestEnvelope(id uint) *entity.Envelope {
	return &entity.Envelope{ID: id, Headers: map[string]string{
		entity.BrokerNameHeader: "events",
		entity.BrokerTypeHeader: "RabbitMQ",
	}}
}

func TestDrainWaitsForQueuedAndLateStartedTasks(t *testing.T) {
	sender := newBlockingSender()
	p := newDispatchTestPump(sender, 1, 2)
	if err := p.DispatchToSend(context.Background(), dispatchTestEnvelope(1)); err != nil {
		t.Fatal(err)
	}
	if err := p.DispatchToSend(context.Background(), dispatchTestEnvelope(2)); err != nil {
		t.Fatal(err)
	}
	p.Seal()

	drained := make(chan error, 1)
	go func() { drained <- p.Drain(context.Background()) }()

	if id := <-sender.started; id != 1 {
		t.Fatalf("first started message = %d, want 1", id)
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned while work was running: %v", err)
	default:
	}
	sender.release <- struct{}{}

	if id := <-sender.started; id != 2 {
		t.Fatalf("second started message = %d, want 2", id)
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned with a queued task remaining: %v", err)
	default:
	}
	sender.release <- struct{}{}

	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Drain did not finish after queued work completed")
	}
}

func TestDrainWaitsWhenTaskCountChangesFromZero(t *testing.T) {
	sender := newBlockingSender()
	startDispatcher := make(chan struct{})
	p := &Pump{
		sendCh:         make(chan *entity.Envelope, 1),
		senders:        map[string]Sender{routeKey("events"): sender},
		senderConfigs:  make(map[string]string),
		handlers:       new(sync.Map),
		handlerConfigs: new(sync.Map),
	}
	p.dispatcherWG.Add(1)
	go func() {
		defer p.dispatcherWG.Done()
		<-startDispatcher
		p.dispatchingSend(context.Background(), 1)
	}()
	if err := p.DispatchToSend(context.Background(), dispatchTestEnvelope(1)); err != nil {
		t.Fatal(err)
	}
	p.Seal()

	drained := make(chan error, 1)
	go func() { drained <- p.Drain(context.Background()) }()
	select {
	case err := <-drained:
		t.Fatalf("Drain returned while task count could still increase: %v", err)
	default:
	}

	close(startDispatcher)
	if id := <-sender.started; id != 1 {
		t.Fatalf("started message = %d, want 1", id)
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned while the late task was running: %v", err)
	default:
	}
	sender.release <- struct{}{}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}

func TestDrainTimeout(t *testing.T) {
	sender := newBlockingSender()
	p := newDispatchTestPump(sender, 1, 1)
	if err := p.DispatchToSend(context.Background(), dispatchTestEnvelope(1)); err != nil {
		t.Fatal(err)
	}
	p.Seal()
	<-sender.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	sender.release <- struct{}{}
}

func TestDispatchRejectedAfterSeal(t *testing.T) {
	p := newDispatchTestPump(new(senderStub), 1, 1)
	p.Seal()
	if err := p.DispatchToSend(context.Background(), dispatchTestEnvelope(1)); !errors.Is(err, ingap.ErrRuntimeStopping) {
		t.Fatalf("DispatchToSend() error = %v, want %v", err, ingap.ErrRuntimeStopping)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
