package gap

import (
	"strconv"
	"strings"
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

func TestValidateStateMachineOptions(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		value  int
		mutate func(*Options, int)
	}{
		{name: "claim batch zero", field: "ClaimBatchSize", value: 0, mutate: func(o *Options, v int) { o.ClaimBatchSize = v }},
		{name: "claim batch negative", field: "ClaimBatchSize", value: -1, mutate: func(o *Options, v int) { o.ClaimBatchSize = v }},
		{name: "max retries zero", field: "MaxRetries", value: 0, mutate: func(o *Options, v int) { o.MaxRetries = v }},
		{name: "max retries negative", field: "MaxRetries", value: -1, mutate: func(o *Options, v int) { o.MaxRetries = v }},
		{name: "lease ttl zero", field: "LeaseTTLSeconds", value: 0, mutate: func(o *Options, v int) { o.LeaseTTLSeconds = v }},
		{name: "lease ttl negative", field: "LeaseTTLSeconds", value: -1, mutate: func(o *Options, v int) { o.LeaseTTLSeconds = v }},
		{name: "pump interval zero", field: "PumpIntervalSeconds", value: 0, mutate: func(o *Options, v int) { o.PumpIntervalSeconds = v }},
		{name: "pump interval negative", field: "PumpIntervalSeconds", value: -1, mutate: func(o *Options, v int) { o.PumpIntervalSeconds = v }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := new(Options).With()
			test.mutate(opts, test.value)
			err := opts.Validate()
			if err == nil {
				t.Fatal("Validate() returned nil")
			}
			want := test.field + "="
			if !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "="+strconv.Itoa(test.value)) {
				t.Fatalf("Validate() error = %q, want field and value", err)
			}
		})
	}
}

func TestValidateAcceptsReasonableUpperValues(t *testing.T) {
	opts := new(Options).With()
	opts.ClaimBatchSize = 10_000
	opts.MaxRetries = 1_000_000
	opts.LeaseTTLSeconds = 30 * 24 * 60 * 60
	opts.PumpIntervalSeconds = 60 * 60
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsDurationOverflow(t *testing.T) {
	maxInt := int64(^uint(0) >> 1)
	if maxInt <= maxDurationSeconds {
		t.Skip("int cannot represent a duration-overflowing seconds value")
	}

	for _, field := range []string{"LeaseTTLSeconds", "PumpIntervalSeconds"} {
		t.Run(field, func(t *testing.T) {
			opts := new(Options).With()
			value := int(maxDurationSeconds + 1)
			if field == "LeaseTTLSeconds" {
				opts.LeaseTTLSeconds = value
			} else {
				opts.PumpIntervalSeconds = value
			}
			err := opts.Validate()
			if err == nil || !strings.Contains(err.Error(), field+"=") {
				t.Fatalf("Validate() error = %v, want %s overflow", err, field)
			}
		})
	}
}
