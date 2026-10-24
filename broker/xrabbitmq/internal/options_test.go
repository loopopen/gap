package internal

import (
	"testing"

	ingap "github.com/loopopen/gap/internal/gap"
)

func TestMandatoryOption(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		opts := new(Options).With()
		if opts.Mandatory {
			t.Fatal("Mandatory should be disabled by default")
		}
	})

	t.Run("can be enabled", func(t *testing.T) {
		opts := new(Options).With(Mandatory(true))
		if !opts.Mandatory {
			t.Fatal("Mandatory(true) did not enable mandatory publishing")
		}
	})
}

func TestRuntimeResourcesAreSharedByOptionSnapshots(t *testing.T) {
	opts := new(Options).With()
	runtimeOpts := new(ingap.Options).With(ingap.MaxPublishConcurrency(4))
	runtimeOpts.RuntimeIdentity()
	clonedOpts := *runtimeOpts

	first, releaseFirst := opts.acquireRuntime(runtimeOpts)
	second, releaseSecond := opts.acquireRuntime(&clonedOpts)
	if first != second {
		t.Fatal("options from the same Gap runtime did not share RabbitMQ resources")
	}
	if first.publishers.capacity != 4 {
		t.Fatalf("publisher pool capacity = %d, want 4", first.publishers.capacity)
	}

	if err := releaseFirst(); err != nil {
		t.Fatal(err)
	}
	if first.publishers.isClosed() {
		t.Fatal("releasing one client closed resources still used by another client")
	}
	if err := releaseSecond(); err != nil {
		t.Fatal(err)
	}
	if !first.publishers.isClosed() {
		t.Fatal("releasing the last client did not close the publisher pool")
	}
}

func TestRuntimeResourcesAreIsolated(t *testing.T) {
	opts := new(Options).With()
	firstOpts := new(ingap.Options).With()
	secondOpts := new(ingap.Options).With()

	first, releaseFirst := opts.acquireRuntime(firstOpts)
	second, releaseSecond := opts.acquireRuntime(secondOpts)
	t.Cleanup(func() {
		_ = releaseFirst()
		_ = releaseSecond()
	})

	if first == second || first.conn == second.conn || first.publishers == second.publishers {
		t.Fatal("independent Gap runtimes shared RabbitMQ resources")
	}
}
