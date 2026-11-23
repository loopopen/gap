package entity

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/snowflake"
)

func TestInitSnowflakeRejectsNodeOutsideSnowflakeRange(t *testing.T) {
	resetSnowflake(t)

	if err := InitSnowflake(4); err != nil {
		t.Fatal(err)
	}
	if err := InitSnowflake(1 << snowflake.NodeBits); err == nil {
		t.Fatal("InitSnowflake() error = nil, want a range error")
	}
	if got := generatedNode(); got != 4 {
		t.Fatalf("generated node = %d, want 4", got)
	}
}

func TestEnsureSnowflakeResolvesOnce(t *testing.T) {
	resetSnowflake(t)

	var calls atomic.Int32
	resolve := func() (int64, error) {
		calls.Add(1)
		return 8, nil
	}
	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := EnsureSnowflake(resolve); err != nil {
				errc <- err
			}
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1", got)
	}
	if got := generatedNode(); got != 8 {
		t.Fatalf("generated node = %d, want 8", got)
	}
	if err := EnsureSnowflake(func() (int64, error) {
		t.Fatal("resolve ran after the node was initialized")
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func generatedNode() int64 {
	return snowflake.ParseInt64(int64(NewEnvelope("v", "topic", "msg").ID)).Node()
}

func resetSnowflake(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		sfMu.Lock()
		sfNode.Store(nil)
		sfMu.Unlock()
	})
	sfMu.Lock()
	sfNode.Store(nil)
	sfMu.Unlock()
}
