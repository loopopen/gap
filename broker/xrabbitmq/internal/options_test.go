package internal

import "testing"

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
