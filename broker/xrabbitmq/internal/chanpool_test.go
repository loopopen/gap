package internal

import (
	"errors"
	"testing"
)

func TestDefaultPoolDoesNotReopenAfterClose(t *testing.T) {
	pool := NewDefaultPool(new(Options).With())
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := pool.Rent(); !errors.Is(err, errPoolClosed) {
		t.Fatalf("Rent() error = %v, want %v", err, errPoolClosed)
	}
}
