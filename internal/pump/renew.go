package pump

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/storage"
)

type msgLease struct {
	token      string
	leaseUntil time.Time
}

// renewalWindow coalesces leases whose half point falls this soon after the
// earliest one. They are renewed together, slightly before their own half point.
const renewalWindow = time.Second

// publishedRenewals lazily extends leases for messages accepted onto the fast
// send path. The earliest half-elapsed lease opens a renewal. Leases whose half
// point falls within renewalWindow are included in that same renewal.
type publishedRenewals struct {
	storage    storage.Storage
	ttl        time.Duration
	retryDelay time.Duration

	mu    sync.Mutex
	items map[uint]msgLease
	wake  chan struct{}
}

func (p *Pump) startPublishedRenewals() {
	if p.storage == nil || p.gapOpts == nil {
		return
	}
	ttl := p.gapOpts.LeaseTTL()
	if ttl <= 0 {
		return
	}
	p.renewals = newPublishedRenewals(p.storage, ttl)
	p.renewWG.Add(1)
	go func() {
		defer p.renewWG.Done()
		p.renewals.run(p.gapOpts.WorkContext())
	}()
}

func newPublishedRenewals(stor storage.Storage, ttl time.Duration) *publishedRenewals {
	return &publishedRenewals{
		storage:    stor,
		ttl:        ttl,
		retryDelay: time.Second,
		items:      make(map[uint]msgLease),
		wake:       make(chan struct{}, 1),
	}
}

func (r *publishedRenewals) attach(envelope *entity.Envelope) {
	if envelope == nil || envelope.LeaseToken == "" || envelope.LeaseUntil.IsZero() {
		return
	}
	r.mu.Lock()
	r.items[envelope.ID] = msgLease{
		token:      envelope.LeaseToken,
		leaseUntil: envelope.LeaseUntil,
	}
	r.mu.Unlock()
	r.kick()
}

func (r *publishedRenewals) detach(id uint, token string) {
	r.mu.Lock()
	item, ok := r.items[id]
	deleted := ok && item.token == token
	if deleted {
		delete(r.items, id)
	}
	r.mu.Unlock()
	if deleted {
		r.kick()
	}
}

func (r *publishedRenewals) kick() {
	select {
	case r.wake <- struct{}{}:
	default: //drop
	}
}

func (r *publishedRenewals) run(ctx context.Context) {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	for {
		delay, waiting := r.nextDelay(time.Now())
		if !waiting {
			select {
			case <-ctx.Done():
				return
			case <-r.wake:
			}
			continue
		}
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-r.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			if err := r.renewDue(ctx, time.Now()); err != nil {
				slog.Error("failed to renew published leases", slog.Any("err", err))
				if !waitCtx(ctx, r.retryDelay) {
					return
				}
			}
		}
	}
}

func (r *publishedRenewals) nextDelay(now time.Time) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var earliest time.Time
	for _, item := range r.items {
		half := item.leaseUntil.Add(-r.ttl / 2)
		if earliest.IsZero() || half.Before(earliest) {
			earliest = half
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	delay := earliest.Sub(now)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func (r *publishedRenewals) renewDue(ctx context.Context, now time.Time) error {
	deadline := now.Add(r.ttl/2 + renewalWindow)
	r.mu.Lock()
	due := make([]storage.Lease, 0)
	for id, item := range r.items {
		if item.leaseUntil.After(deadline) {
			continue
		}
		due = append(due, storage.Lease{ID: id, LeaseToken: item.token})
	}
	r.mu.Unlock()
	if len(due) == 0 {
		return nil
	}

	until := now.Add(r.ttl)
	renewed, err := r.storage.RenewPublishedLeases(ctx, until, due)
	if err != nil {
		return err
	}
	renewedSet := make(map[uint]struct{}, len(renewed))
	for _, id := range renewed {
		renewedSet[id] = struct{}{}
	}

	lost := 0
	r.mu.Lock()
	for _, lease := range due {
		item, ok := r.items[lease.ID]
		if !ok || item.token != lease.LeaseToken {
			continue
		}
		if _, ok := renewedSet[lease.ID]; ok {
			item.leaseUntil = until
			r.items[lease.ID] = item
			continue
		}
		delete(r.items, lease.ID)
		lost++
	}
	r.mu.Unlock()
	if lost > 0 {
		slog.Warn("published lease lost during renewal", slog.Int("lost", lost))
	}
	return nil
}

func waitCtx(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *publishedRenewals) holds(id uint, token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.items[id]
	return ok && item.token == token
}

// sendLease is the only path that mounts or drops a published send lease.
// hold mounts the envelope before it enters sendCh, or before a claimed batch
// waits to send. The matching drop runs when sendCh refuses the envelope, or
// when send returns.
func (p *Pump) sendLease(envelope *entity.Envelope, hold bool) {
	if p == nil || p.renewals == nil || envelope == nil {
		return
	}
	if hold {
		p.renewals.attach(envelope)
		return
	}
	p.renewals.detach(envelope.ID, envelope.LeaseToken)
}

// leaseHeld reports whether a tracked published lease is still owned.
// Messages outside renewal tracking are treated as held so sending proceeds.
func (p *Pump) leaseHeld(envelope *entity.Envelope) bool {
	if p == nil || p.renewals == nil || envelope == nil {
		return true
	}
	if envelope.LeaseToken == "" || envelope.LeaseUntil.IsZero() {
		return true
	}
	return p.renewals.holds(envelope.ID, envelope.LeaseToken)
}

// sendClaimed mounts every claimed published message, then sends the ones whose
// lease is still held. A lease dropped by renewal is not produced again.
func (p *Pump) sendClaimed(ctx, workCtx context.Context, envelopes []*entity.Envelope) {
	for _, envelope := range envelopes {
		p.sendLease(envelope, true)
	}
	for _, envelope := range envelopes {
		if !p.leaseHeld(envelope) {
			envelope.Log().Warn("skipping claimed published message after lease loss")
			continue
		}
		if err := p.send(workCtx, envelope); err != nil && ctx.Err() == nil {
			wait(ctx, 10*time.Second)
		}
	}
}
