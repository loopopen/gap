package gap

import (
	"testing"
	"time"
)

func TestLeaseAndRetryDefaults(t *testing.T) {
	opts := new(Options).With()
	if got, want := opts.LeaseTTL(), 180*time.Second; got != want {
		t.Fatalf("LeaseTTL() = %v, want %v", got, want)
	}
	if got, want := opts.RetryInterval(), 10*time.Second; got != want {
		t.Fatalf("RetryInterval() = %v, want %v", got, want)
	}
}
