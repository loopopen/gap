package pump

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/storage"
)

type renewStorage struct {
	storage.Storage
	mu      sync.Mutex
	calls   int
	until   time.Time
	leases  []storage.Lease
	renewed []uint
	err     error
}

func (s *renewStorage) RenewPublishedLeases(_ context.Context, until time.Time, leases []storage.Lease) ([]uint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.until = until
	s.leases = append([]storage.Lease(nil), leases...)
	if s.err != nil {
		return nil, s.err
	}
	if s.renewed != nil {
		return append([]uint(nil), s.renewed...), nil
	}
	ids := make([]uint, len(leases))
	for i, lease := range leases {
		ids[i] = lease.ID
	}
	return ids, nil
}

func (s *renewStorage) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestSendLeaseMountsAndDropsOnlyMatchingToken(t *testing.T) {
	p := &Pump{renewals: newPublishedRenewals(nil, time.Minute)}
	envelope := &entity.Envelope{ID: 7, LeaseToken: "owned", LeaseUntil: time.Now().Add(time.Minute)}
	p.sendLease(envelope, true)
	p.sendLease(&entity.Envelope{ID: 7, LeaseToken: "other"}, false)

	p.renewals.mu.Lock()
	_, held := p.renewals.items[envelope.ID]
	p.renewals.mu.Unlock()
	if !held {
		t.Fatal("lease dropped for a different token")
	}

	p.sendLease(envelope, false)
	p.renewals.mu.Lock()
	_, held = p.renewals.items[envelope.ID]
	p.renewals.mu.Unlock()
	if held {
		t.Fatal("lease still held after drop")
	}
}

func TestRenewDueWaitsUntilHalfLeaseAndDropsLostLease(t *testing.T) {
	stor := &renewStorage{}
	ttl := 2 * time.Minute
	renewals := newPublishedRenewals(stor, ttl)
	now := time.Now()
	kept := &entity.Envelope{ID: 1, LeaseToken: "keep", LeaseUntil: now.Add(ttl)}
	lost := &entity.Envelope{ID: 2, LeaseToken: "lose", LeaseUntil: now.Add(ttl / 4)}
	renewals.attach(kept)
	renewals.attach(lost)
	stor.renewed = []uint{kept.ID}

	if err := renewals.renewDue(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if stor.callCount() != 1 {
		t.Fatalf("renew calls = %d, want 1", stor.callCount())
	}
	stor.mu.Lock()
	got := append([]storage.Lease(nil), stor.leases...)
	stor.mu.Unlock()
	if len(got) != 1 || got[0].ID != lost.ID || got[0].LeaseToken != lost.LeaseToken {
		t.Fatalf("renewed leases = %#v, want the half-elapsed lease", got)
	}

	renewals.mu.Lock()
	defer renewals.mu.Unlock()
	if _, ok := renewals.items[lost.ID]; ok {
		t.Fatal("lost lease is still tracked")
	}
	item, ok := renewals.items[kept.ID]
	if !ok {
		t.Fatal("lease that was not due was dropped")
	}
	if !item.leaseUntil.Equal(kept.LeaseUntil) {
		t.Fatalf("untouched lease until = %v, want %v", item.leaseUntil, kept.LeaseUntil)
	}
}

func TestRenewDueIncludesLeasesInsideTheCoalesceWindow(t *testing.T) {
	stor := &renewStorage{}
	ttl := 2 * time.Minute
	renewals := newPublishedRenewals(stor, ttl)
	now := time.Now()
	inside := &entity.Envelope{ID: 1, LeaseToken: "inside", LeaseUntil: now.Add(ttl/2 + renewalWindow)}
	outside := &entity.Envelope{ID: 2, LeaseToken: "outside", LeaseUntil: now.Add(ttl/2 + renewalWindow + time.Nanosecond)}
	due := &entity.Envelope{ID: 3, LeaseToken: "due", LeaseUntil: now.Add(ttl / 2)}
	renewals.attach(inside)
	renewals.attach(outside)
	renewals.attach(due)

	if err := renewals.renewDue(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	stor.mu.Lock()
	got := make(map[uint]struct{}, len(stor.leases))
	for _, lease := range stor.leases {
		got[lease.ID] = struct{}{}
	}
	stor.mu.Unlock()
	if _, ok := got[due.ID]; !ok {
		t.Fatal("due lease was not renewed")
	}
	if _, ok := got[inside.ID]; !ok {
		t.Fatal("lease inside the window was not renewed")
	}
	if _, ok := got[outside.ID]; ok {
		t.Fatal("lease past the window was renewed")
	}
}

func TestRenewDueExtendsDueLease(t *testing.T) {
	stor := &renewStorage{}
	ttl := time.Minute
	renewals := newPublishedRenewals(stor, ttl)
	now := time.Now()
	envelope := &entity.Envelope{ID: 4, LeaseToken: "tok", LeaseUntil: now.Add(ttl / 2)}
	renewals.attach(envelope)

	if err := renewals.renewDue(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	wantUntil := now.Add(ttl)
	renewals.mu.Lock()
	item := renewals.items[envelope.ID]
	renewals.mu.Unlock()
	if !item.leaseUntil.Equal(wantUntil) {
		t.Fatalf("lease until = %v, want %v", item.leaseUntil, wantUntil)
	}
}

func TestPublishedRenewalLoopRenewsAfterHalfLease(t *testing.T) {
	stor := &renewStorage{}
	ttl := 40 * time.Millisecond
	renewals := newPublishedRenewals(stor, ttl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		renewals.run(ctx)
		close(done)
	}()
	renewals.attach(&entity.Envelope{ID: 9, LeaseToken: "tok", LeaseUntil: time.Now().Add(ttl)})

	deadline := time.Now().Add(time.Second)
	for stor.callCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("renewal did not run after half the lease")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("renewal loop did not stop")
	}
}

func TestDispatchDropsLeaseWhenSendBufferIsFull(t *testing.T) {
	p := &Pump{
		sendCh:   make(chan *entity.Envelope, 1),
		renewals: newPublishedRenewals(&renewStorage{}, time.Minute),
	}
	queued := leasedEnvelope(1)
	rejected := leasedEnvelope(2)
	if err := p.DispatchToSend(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	if err := p.DispatchToSend(context.Background(), rejected); err == nil {
		t.Fatal("expected send buffer overflow")
	}

	p.renewals.mu.Lock()
	_, queuedHeld := p.renewals.items[queued.ID]
	_, rejectedHeld := p.renewals.items[rejected.ID]
	p.renewals.mu.Unlock()
	if !queuedHeld {
		t.Fatal("queued message was dropped before send")
	}
	if rejectedHeld {
		t.Fatal("rejected message stayed on the renewal list")
	}
}

func TestDispatchDropsLeaseWhenSendReturns(t *testing.T) {
	sender := newBlockingSender()
	p := newDispatchTestPump(sender, 1, 1)
	p.renewals = newPublishedRenewals(&renewStorage{}, time.Minute)
	envelope := leasedEnvelope(1)
	if err := p.DispatchToSend(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if id := <-sender.started; id != envelope.ID {
		t.Fatalf("started message = %d, want %d", id, envelope.ID)
	}

	p.renewals.mu.Lock()
	_, held := p.renewals.items[envelope.ID]
	p.renewals.mu.Unlock()
	if !held {
		t.Fatal("lease dropped while send was still running")
	}

	sender.release <- struct{}{}
	waitUntilDropped(t, p, envelope.ID)
	p.Seal()
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func leasedEnvelope(id uint) *entity.Envelope {
	return &entity.Envelope{
		ID:         id,
		BrokerName: "events",
		LeaseToken: "lease",
		LeaseUntil: time.Now().Add(time.Minute),
	}
}

func waitUntilDropped(t *testing.T, p *Pump, id uint) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		p.renewals.mu.Lock()
		_, held := p.renewals.items[id]
		p.renewals.mu.Unlock()
		if !held {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("message %d is still on the renewal list", id)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSendClaimedRenewsUntilEachSendReturns(t *testing.T) {
	sender := newBlockingSender()
	p := &Pump{
		senders:  map[string]Sender{routeKey("events"): sender},
		renewals: newPublishedRenewals(&renewStorage{}, time.Minute),
	}
	first := leasedEnvelope(1)
	second := leasedEnvelope(2)
	done := make(chan struct{})
	go func() {
		p.sendClaimed(context.Background(), context.Background(), []*entity.Envelope{first, second})
		close(done)
	}()
	if id := <-sender.started; id != first.ID {
		t.Fatalf("started message = %d, want %d", id, first.ID)
	}
	if !p.leaseHeld(first) || !p.leaseHeld(second) {
		t.Fatal("claimed messages were dropped before their send returned")
	}

	sender.release <- struct{}{}
	if id := <-sender.started; id != second.ID {
		t.Fatalf("started message = %d, want %d", id, second.ID)
	}
	if p.leaseHeld(first) {
		t.Fatal("finished message stayed on the renewal list")
	}
	if !p.leaseHeld(second) {
		t.Fatal("message was dropped while its send was running")
	}
	sender.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sendClaimed did not finish")
	}
	if p.leaseHeld(second) {
		t.Fatal("last message stayed on the renewal list")
	}
}

func TestSendClaimedSkipsMessageAfterLeaseLoss(t *testing.T) {
	sender := newBlockingSender()
	p := &Pump{
		senders:  map[string]Sender{routeKey("events"): sender},
		renewals: newPublishedRenewals(&renewStorage{}, time.Minute),
	}
	first := leasedEnvelope(1)
	lost := leasedEnvelope(2)
	done := make(chan struct{})
	go func() {
		p.sendClaimed(context.Background(), context.Background(), []*entity.Envelope{first, lost})
		close(done)
	}()
	if id := <-sender.started; id != first.ID {
		t.Fatalf("started message = %d, want %d", id, first.ID)
	}
	p.renewals.detach(lost.ID, lost.LeaseToken)
	sender.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sendClaimed did not finish")
	}
	select {
	case id := <-sender.started:
		t.Fatalf("sent message %d after its lease was lost", id)
	default:
	}
}

func TestRenewDueKeepsLeasesWhenStorageFails(t *testing.T) {
	stor := &renewStorage{err: errors.New("db down")}
	renewals := newPublishedRenewals(stor, time.Minute)
	envelope := &entity.Envelope{ID: 5, LeaseToken: "tok", LeaseUntil: time.Now()}
	renewals.attach(envelope)
	if err := renewals.renewDue(context.Background(), time.Now()); err == nil {
		t.Fatal("expected storage error")
	}
	renewals.mu.Lock()
	_, held := renewals.items[envelope.ID]
	renewals.mu.Unlock()
	if !held {
		t.Fatal("lease dropped after a failed renewal")
	}
}
