package entity

import (
	"testing"
	"time"

	"github.com/loopopen/gap/internal/enum"
)

func TestWithLease(t *testing.T) {
	ttl := 3 * time.Minute
	before := time.Now()
	envelope := new(Envelope)
	if got := envelope.WithLease(ttl); got != envelope {
		t.Fatal("WithLease returned a different envelope")
	}
	after := time.Now()

	if len(envelope.LeaseToken) != 32 {
		t.Fatalf("token length = %d, want 32", len(envelope.LeaseToken))
	}
	if envelope.Status != enum.StatusPending {
		t.Fatalf("status = %v, want %v", envelope.Status, enum.StatusPending)
	}
	if envelope.NextAttemptAt.Before(before) || envelope.NextAttemptAt.After(after) {
		t.Fatalf("next attempt at = %v, want between %v and %v", envelope.NextAttemptAt, before, after)
	}
	if want := envelope.NextAttemptAt.Add(ttl); !envelope.LeaseUntil.Equal(want) {
		t.Fatalf("lease until = %v, want %v", envelope.LeaseUntil, want)
	}

	firstToken := envelope.LeaseToken
	envelope.WithLease(ttl)
	if envelope.LeaseToken == firstToken {
		t.Fatal("consecutive lease tokens are equal")
	}
}
