package storagetest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

const (
	leaseWait      = 1500 * time.Millisecond
	eventuallyWait = 4 * time.Second
)

// Harness supplies one initialized storage and its transaction boundary to the
// shared storage contract. Each contract case gets a fresh Harness.
type Harness struct {
	Storage  storage.Storage
	Transact func(context.Context, func(storage.Storage) error) error
}

// Factory creates an isolated Harness for one contract case.
type Factory func(t *testing.T, opts *ingap.Options) Harness

var (
	nextID     atomic.Uint64
	nextSchema atomic.Uint64
)

// Run executes the storage contract against fresh storage state for each case.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	if factory == nil {
		t.Fatal("storage contract factory is nil")
	}

	run := func(name string, test func(*testing.T, Harness, *ingap.Options)) {
		t.Run(name, func(t *testing.T) {
			opts := contractOptions()
			h := factory(t, opts)
			if h.Storage == nil {
				t.Fatal("storage contract harness has nil Storage")
			}
			if h.Transact == nil {
				t.Fatal("storage contract harness has nil Transact")
			}
			test(t, h, opts)
		})
	}

	run("published routing", publishedRoutingContract)
	run("received routing", receivedRoutingContract)
	run("published lease renewal and fencing", publishedLeaseContract)
	run("received lease renewal and fencing", receivedLeaseContract)
	run("retry and exhaustion", retryContract)
	run("terminal retention and cleanup", cleanupContract)
	run("transaction binding", transactionContract)
}

// RequireDSN returns an integration-test DSN. Ordinary unit-test runs skip the
// contract when it is absent; explicit contract runs fail instead of silently
// skipping it.
func RequireDSN(t *testing.T, env string) string {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv(env)); dsn != "" {
		return dsn
	}
	if os.Getenv("GAP_STORAGE_CONTRACT") == "1" {
		t.Fatalf("%s is required when GAP_STORAGE_CONTRACT=1", env)
	}
	t.Skipf("set %s to run the storage contract", env)
	return ""
}

// Schema returns a process-unique, database-safe schema or table prefix.
func Schema(prefix string) string {
	prefix = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, strings.ToLower(prefix))
	return fmt.Sprintf("gap_contract_%s_%d_%d", prefix, os.Getpid(), nextSchema.Add(1))
}

func contractOptions() *ingap.Options {
	opts := new(ingap.Options).With(
		ingap.Version("contract-v1"),
		ingap.ClaimBatchSize(16),
		ingap.MaxRetries(2),
		ingap.LeaseTTLSeconds(1),
		ingap.RetryIntervalSeconds(1),
		ingap.SucceededRetentionSeconds(1),
		ingap.FailedRetentionSeconds(1),
		ingap.CleanupBatchSize(1),
	)
	opts.InitRuntime()
	return opts
}

func publishedRoutingContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	first := newEnvelope(opts, "route-a", "")
	second := newEnvelope(opts, "route-b", "")
	must(t, h.Storage.CreatePublished(ctx, first))
	must(t, h.Storage.CreatePublished(ctx, second))

	waitForInitialLease()
	assertNoPublished(t, ctx, h.Storage, "missing-route")

	claimedA := claimPublished(t, ctx, h.Storage, "route-a")
	assertEnvelope(t, claimedA, first)
	claimedB := claimPublished(t, ctx, h.Storage, "route-b")
	assertEnvelope(t, claimedB, second)
	must(t, h.Storage.UpdateStatusPublished(ctx, claimedA.ID, claimedA.LeaseToken, enum.StatusSucceeded))
	must(t, h.Storage.UpdateStatusPublished(ctx, claimedB.ID, claimedB.LeaseToken, enum.StatusSucceeded))
	assertNoPublished(t, ctx, h.Storage, "route-a")
}

func receivedRoutingContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	first := newEnvelope(opts, "route-a", "billing")
	second := newEnvelope(opts, "route-a", "shipping")
	third := newEnvelope(opts, "route-b", "billing")
	for _, envelope := range []*entity.Envelope{first, second, third} {
		must(t, h.Storage.CreateReceived(ctx, envelope))
	}

	waitForInitialLease()
	assertNoReceived(t, ctx, h.Storage, "missing", "route-a")
	assertNoReceived(t, ctx, h.Storage, "billing", "missing-route")

	claimed := []*entity.Envelope{
		claimReceived(t, ctx, h.Storage, "billing", "route-a"),
		claimReceived(t, ctx, h.Storage, "shipping", "route-a"),
		claimReceived(t, ctx, h.Storage, "billing", "route-b"),
	}
	for i, want := range []*entity.Envelope{first, second, third} {
		assertEnvelope(t, claimed[i], want)
		must(t, h.Storage.UpdateStatusReceived(ctx, claimed[i].ID, claimed[i].LeaseToken, enum.StatusSucceeded))
	}
}

func publishedLeaseContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	envelope := newEnvelope(opts, "lease-published", "")
	must(t, h.Storage.CreatePublished(ctx, envelope))
	first := claimPublished(t, ctx, h.Storage, envelope.BrokerName)

	renewed, err := h.Storage.RenewPublishedLeases(ctx, []storage.Lease{{ID: first.ID, LeaseToken: "not-owned"}})
	must(t, err)
	if len(renewed) != 0 {
		t.Fatalf("renewed published lease with stale token: %v", renewed)
	}
	renewed, err = h.Storage.RenewPublishedLeases(ctx, []storage.Lease{{ID: first.ID, LeaseToken: first.LeaseToken}})
	must(t, err)
	assertRenewed(t, renewed, first.ID)
	assertNoPublished(t, ctx, h.Storage, envelope.BrokerName)

	second := claimPublished(t, ctx, h.Storage, envelope.BrokerName)
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("reclaimed published message kept the old lease token")
	}
	if err := h.Storage.UpdateStatusPublished(ctx, first.ID, first.LeaseToken, enum.StatusSucceeded); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old published lease update error = %v, want %v", err, storage.ErrLeaseLost)
	}
	must(t, h.Storage.UpdateStatusPublished(ctx, second.ID, second.LeaseToken, enum.StatusSucceeded))
}

func receivedLeaseContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	envelope := newEnvelope(opts, "lease-received", "billing")
	must(t, h.Storage.CreateReceived(ctx, envelope))
	first := claimReceived(t, ctx, h.Storage, envelope.Group, envelope.BrokerName)

	renewed, err := h.Storage.RenewReceivedLeases(ctx, []storage.Lease{{ID: first.ID, LeaseToken: "not-owned"}})
	must(t, err)
	if len(renewed) != 0 {
		t.Fatalf("renewed received lease with stale token: %v", renewed)
	}
	renewed, err = h.Storage.RenewReceivedLeases(ctx, []storage.Lease{{ID: first.ID, LeaseToken: first.LeaseToken}})
	must(t, err)
	assertRenewed(t, renewed, first.ID)
	assertNoReceived(t, ctx, h.Storage, envelope.Group, envelope.BrokerName)

	second := claimReceived(t, ctx, h.Storage, envelope.Group, envelope.BrokerName)
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("reclaimed received message kept the old lease token")
	}
	if err := h.Storage.UpdateStatusReceived(ctx, first.ID, first.LeaseToken, enum.StatusSucceeded); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old received lease update error = %v, want %v", err, storage.ErrLeaseLost)
	}
	must(t, h.Storage.UpdateStatusReceived(ctx, second.ID, second.LeaseToken, enum.StatusSucceeded))
}

func retryContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	retried := newEnvelope(opts, "retry", "")
	must(t, h.Storage.CreatePublished(ctx, retried))
	first := claimPublished(t, ctx, h.Storage, retried.BrokerName)
	if first.Retries != 1 {
		t.Fatalf("first claim retries = %d, want 1", first.Retries)
	}
	must(t, h.Storage.UpdateStatusPublished(ctx, first.ID, first.LeaseToken, enum.StatusPending))
	assertNoPublished(t, ctx, h.Storage, retried.BrokerName)
	second := claimPublished(t, ctx, h.Storage, retried.BrokerName)
	if second.Retries != 2 {
		t.Fatalf("second claim retries = %d, want 2", second.Retries)
	}
	must(t, h.Storage.UpdateStatusPublished(ctx, second.ID, second.LeaseToken, enum.StatusSucceeded))

	exhausted := newEnvelope(opts, "exhausted", "")
	must(t, h.Storage.CreatePublished(ctx, exhausted))
	exhaustedFirst := claimPublished(t, ctx, h.Storage, exhausted.BrokerName)
	must(t, h.Storage.UpdateStatusPublished(ctx, exhaustedFirst.ID, exhaustedFirst.LeaseToken, enum.StatusPending))
	exhaustedSecond := claimPublished(t, ctx, h.Storage, exhausted.BrokerName)
	waitForStatus(t, ctx, h.Storage, exhaustedSecond, enum.StatusFailed)
}

func cleanupContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	succeeded := newEnvelope(opts, "cleanup-succeeded", "")
	failed := newEnvelope(opts, "cleanup-failed", "")
	pending := newEnvelope(opts, "cleanup-pending", "")
	for _, envelope := range []*entity.Envelope{succeeded, failed, pending} {
		must(t, h.Storage.CreatePublished(ctx, envelope))
	}
	must(t, h.Storage.UpdateStatusPublished(ctx, succeeded.ID, succeeded.LeaseToken, enum.StatusSucceeded))
	must(t, h.Storage.UpdateStatusPublished(ctx, failed.ID, failed.LeaseToken, enum.StatusFailed))

	waitForDeleted(t, ctx, h.Storage.DeleteExpiredPublishedBatch, 1)
	deleted, err := h.Storage.DeleteExpiredPublishedBatch(ctx, 1)
	must(t, err)
	if deleted != 1 {
		t.Fatalf("second cleanup deleted %d rows, want 1", deleted)
	}
	for _, id := range []uint{succeeded.ID, failed.ID} {
		got, err := h.Storage.GetPublishedByID(ctx, id)
		must(t, err)
		if got != nil {
			t.Fatalf("terminal message %d still exists after cleanup", id)
		}
	}
	got, err := h.Storage.GetPublishedByID(ctx, pending.ID)
	must(t, err)
	if got == nil || got.Status != enum.StatusPending {
		t.Fatalf("pending message was removed by cleanup: %#v", got)
	}

	received := newEnvelope(opts, "cleanup-received", "billing")
	must(t, h.Storage.CreateReceived(ctx, received))
	must(t, h.Storage.UpdateStatusReceived(ctx, received.ID, received.LeaseToken, enum.StatusSucceeded))
	waitForDeleted(t, ctx, h.Storage.DeleteExpiredReceivedBatch, 1)
}

func transactionContract(t *testing.T, h Harness, opts *ingap.Options) {
	ctx := testContext(t)
	committed := newEnvelope(opts, "transaction-commit", "")
	err := h.Transact(ctx, func(bound storage.Storage) error {
		return bound.CreatePublished(ctx, committed)
	})
	must(t, err)
	got, err := h.Storage.GetPublishedByID(ctx, committed.ID)
	must(t, err)
	if got == nil {
		t.Fatal("committed transaction message is not visible")
	}

	rolledBack := newEnvelope(opts, "transaction-rollback", "")
	rollback := errors.New("contract rollback")
	err = h.Transact(ctx, func(bound storage.Storage) error {
		if err := bound.CreatePublished(ctx, rolledBack); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("rollback transaction error = %v, want %v", err, rollback)
	}
	got, err = h.Storage.GetPublishedByID(ctx, rolledBack.ID)
	must(t, err)
	if got != nil {
		t.Fatal("rolled-back transaction message is visible")
	}
}

func newEnvelope(opts *ingap.Options, brokerName, group string) *entity.Envelope {
	return &entity.Envelope{
		ID:         uint(nextID.Add(1)),
		CreatedAt:  time.Now().UTC(),
		Version:    opts.Version,
		Topic:      "contract.topic",
		BrokerName: brokerName,
		Group:      group,
		Headers:    map[string]string{"contract": "true"},
		Payload:    []byte(`{"contract":true}`),
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitForInitialLease() {
	time.Sleep(leaseWait)
}

func claimPublished(t *testing.T, ctx context.Context, stor storage.Storage, brokerName string) *entity.Envelope {
	t.Helper()
	return eventuallyClaim(t, func() ([]*entity.Envelope, error) {
		return stor.ClaimPublishedBatch(ctx, 16, brokerName)
	})
}

func claimReceived(t *testing.T, ctx context.Context, stor storage.Storage, group, brokerName string) *entity.Envelope {
	t.Helper()
	return eventuallyClaim(t, func() ([]*entity.Envelope, error) {
		return stor.ClaimReceivedBatch(ctx, 16, group, brokerName)
	})
}

func eventuallyClaim(t *testing.T, claim func() ([]*entity.Envelope, error)) *entity.Envelope {
	t.Helper()
	deadline := time.Now().Add(eventuallyWait)
	for {
		envelopes, err := claim()
		must(t, err)
		if len(envelopes) > 0 {
			if len(envelopes) != 1 {
				t.Fatalf("claim returned %d messages, want 1", len(envelopes))
			}
			return envelopes[0]
		}
		if time.Now().After(deadline) {
			t.Fatal("message did not become claimable")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func assertNoPublished(t *testing.T, ctx context.Context, stor storage.Storage, brokerName string) {
	t.Helper()
	envelopes, err := stor.ClaimPublishedBatch(ctx, 16, brokerName)
	must(t, err)
	if len(envelopes) != 0 {
		t.Fatalf("claimed %d unexpected published messages for broker %q", len(envelopes), brokerName)
	}
}

func assertNoReceived(t *testing.T, ctx context.Context, stor storage.Storage, group, brokerName string) {
	t.Helper()
	envelopes, err := stor.ClaimReceivedBatch(ctx, 16, group, brokerName)
	must(t, err)
	if len(envelopes) != 0 {
		t.Fatalf("claimed %d unexpected received messages for group %q broker %q", len(envelopes), group, brokerName)
	}
}

func assertEnvelope(t *testing.T, got, want *entity.Envelope) {
	t.Helper()
	if got.ID != want.ID || got.BrokerName != want.BrokerName || got.Group != want.Group {
		t.Fatalf("claimed envelope = {id:%d broker:%q group:%q}, want {id:%d broker:%q group:%q}",
			got.ID, got.BrokerName, got.Group, want.ID, want.BrokerName, want.Group)
	}
	if string(got.Payload) != string(want.Payload) || got.Headers["contract"] != "true" {
		t.Fatalf("claimed envelope %d did not preserve payload and headers", got.ID)
	}
}

func assertRenewed(t *testing.T, renewed []uint, want uint) {
	t.Helper()
	if len(renewed) != 1 || renewed[0] != want {
		t.Fatalf("renewed ids = %v, want [%d]", renewed, want)
	}
}

func waitForStatus(t *testing.T, ctx context.Context, stor storage.Storage, envelope *entity.Envelope, want enum.Status) {
	t.Helper()
	deadline := time.Now().Add(eventuallyWait)
	for {
		_, err := stor.ClaimPublishedBatch(ctx, 16, envelope.BrokerName)
		must(t, err)
		got, err := stor.GetPublishedByID(ctx, envelope.ID)
		must(t, err)
		if got != nil && got.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("message %d status did not become %s; got %#v", envelope.ID, want.String(), got)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForDeleted(t *testing.T, ctx context.Context, deleteBatch func(context.Context, int) (int64, error), batchSize int) {
	t.Helper()
	deadline := time.Now().Add(eventuallyWait)
	for {
		deleted, err := deleteBatch(ctx, batchSize)
		must(t, err)
		if deleted > 0 {
			if deleted != int64(batchSize) {
				t.Fatalf("cleanup deleted %d rows, want %d", deleted, batchSize)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal message did not become eligible for cleanup")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
