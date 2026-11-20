package errx

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) returned an error")
	}
	base := errors.New("bad payload")
	err := fmt.Errorf("handle: %w", Permanent(base))
	if !IsPermanent(err) {
		t.Fatal("wrapped permanent error was not recognized")
	}
	if !errors.Is(err, base) {
		t.Fatal("permanent error hid the wrapped cause")
	}
	if IsPermanent(base) {
		t.Fatal("plain error was treated as permanent")
	}
}
