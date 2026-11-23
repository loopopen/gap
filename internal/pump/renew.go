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

type leaseRenewFunc func(ctx context.Context, leases []storage.Lease) ([]uint, error)

// leaseRenewer lazily extends leases for one message table. The earliest
// half-elapsed lease opens a renewal. Leases whose half point falls within
// renewalWindow are included in that same renewal.
type leaseRenewer struct {
	name       string
	renew      leaseRenewFunc
	ttl        time.Duration
	retryDelay time.Duration

	mu    sync.Mutex
	items map[uint]msgLease
	wake  chan struct{}
}

func (p *Pump) startRenewals() {
	if p.storage == nil || p.gapOpts == nil {
		return
	}
	ttl := p.gapOpts.LeaseTTL()
	if ttl <= 0 {
		return
	}
	p.publishedRenewals = newLeaseRenewer("published", ttl, p.storage.RenewPublishedLeases)
	p.receivedRenewals = newLeaseRenewer("received", ttl, p.storage.RenewReceivedLeases)
	ctx := p.gapOpts.WorkContext()
	for _, renewer := range []*leaseRenewer{p.publishedRenewals, p.receivedRenewals} {
		p.renewWG.Add(1)
		go func(renewer *leaseRenewer) {
			defer p.renewWG.Done()
			renewer.run(ctx)
		}(renewer)
	}
}

func newPublishedRenewals(stor storage.Storage, ttl time.Duration) *leaseRenewer {
	return newLeaseRenewer("published", ttl, func(ctx context.Context, leases []storage.Lease) ([]uint, error) {
		if stor == nil {
			return nil, nil
		}
		return stor.RenewPublishedLeases(ctx, leases)
	})
}

func newReceivedRenewals(stor storage.Storage, ttl time.Duration) *leaseRenewer {
	return newLeaseRenewer("received", ttl, func(ctx context.Context, leases []storage.Lease) ([]uint, error) {
		if stor == nil {
			return nil, nil
		}
		return stor.RenewReceivedLeases(ctx, leases)
	})
}

func newLeaseRenewer(name string, ttl time.Duration, renew leaseRenewFunc) *leaseRenewer {
	return &leaseRenewer{
		name:       name,
		renew:      renew,
		ttl:        ttl,
		retryDelay: time.Second,
		items:      make(map[uint]msgLease),
		wake:       make(chan struct{}, 1),
	}
}

func (r *leaseRenewer) attach(envelope *entity.Envelope) {
	if r == nil || envelope == nil || envelope.LeaseToken == "" || envelope.LeaseUntil.IsZero() {
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

func (r *leaseRenewer) detach(id uint, token string) {
	if r == nil {
		return
	}
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

func (r *leaseRenewer) kick() {
	select {
	case r.wake <- struct{}{}:
	default: //drop
	}
}

func (r *leaseRenewer) run(ctx context.Context) {
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
				slog.Error("failed to renew leases", slog.String("table", r.name), slog.Any("err", err))
				if !waitCtx(ctx, r.retryDelay) {
					return
				}
			}
		}
	}
}

func (r *leaseRenewer) nextDelay(now time.Time) (time.Duration, bool) {
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

func (r *leaseRenewer) renewDue(ctx context.Context, now time.Time) error {
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

	renewed, err := r.renew(ctx, due)
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
			item.leaseUntil = now.Add(r.ttl)
			r.items[lease.ID] = item
			continue
		}
		delete(r.items, lease.ID)
		lost++
	}
	r.mu.Unlock()
	if lost > 0 {
		slog.Warn("lease lost during renewal", slog.String("table", r.name), slog.Int("lost", lost))
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

func (r *leaseRenewer) holds(id uint, token string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item, ok := r.items[id]
	return ok && item.token == token
}

// trackLease is the only path that mounts or drops a message lease.
// hold mounts the envelope before it enters a dispatch channel, or before a
// claimed batch waits to be processed. The matching drop runs when that channel
// refuses the envelope, or when processing returns.
func trackLease(renewals *leaseRenewer, envelope *entity.Envelope, hold bool) {
	if renewals == nil || envelope == nil {
		return
	}
	if hold {
		renewals.attach(envelope)
		return
	}
	renewals.detach(envelope.ID, envelope.LeaseToken)
}

// leaseTracked reports whether a tracked lease is still owned.
// Messages outside renewal tracking are treated as held so processing proceeds.
func leaseTracked(renewals *leaseRenewer, envelope *entity.Envelope) bool {
	if renewals == nil || envelope == nil {
		return true
	}
	if envelope.LeaseToken == "" || envelope.LeaseUntil.IsZero() {
		return true
	}
	return renewals.holds(envelope.ID, envelope.LeaseToken)
}

func (p *Pump) publishedLease(envelope *entity.Envelope, hold bool) {
	if p == nil {
		return
	}
	trackLease(p.publishedRenewals, envelope, hold)
}

func (p *Pump) receivedLease(envelope *entity.Envelope, hold bool) {
	if p == nil {
		return
	}
	trackLease(p.receivedRenewals, envelope, hold)
}

func (p *Pump) publishedLeaseHeld(envelope *entity.Envelope) bool {
	if p == nil {
		return true
	}
	return leaseTracked(p.publishedRenewals, envelope)
}

func (p *Pump) receivedLeaseHeld(envelope *entity.Envelope) bool {
	if p == nil {
		return true
	}
	return leaseTracked(p.receivedRenewals, envelope)
}

func (p *Pump) renewClaimed(workCtx context.Context, renewals *leaseRenewer, envelopes []*entity.Envelope, run func(context.Context, *entity.Envelope) error) {
	for _, envelope := range envelopes {
		trackLease(renewals, envelope, true)
	}
	for _, envelope := range envelopes {
		if !leaseTracked(renewals, envelope) {
			envelope.Log().Warn("skipping claimed message after lease loss")
			continue
		}
		_ = run(workCtx, envelope)
	}
}

// publishedClaimed mounts every claimed published message, then sends the ones whose
// lease is still held. A lease dropped by renewal is not produced again.
func (p *Pump) publishedClaimed(workCtx context.Context, envelopes []*entity.Envelope) {
	p.renewClaimed(workCtx, p.publishedRenewals, envelopes, p.send)
}

// receivedClaimed mounts every claimed received message, then handles the ones
// whose lease is still held. A lease dropped by renewal is not handled again.
func (p *Pump) receivedClaimed(workCtx context.Context, envelopes []*entity.Envelope) {
	p.renewClaimed(workCtx, p.receivedRenewals, envelopes, p.handle)
}
