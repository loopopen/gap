package gap

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
)

func TestTypedHandlerMarksInvalidPayloadPermanent(t *testing.T) {
	opts := new(BrokerOptions)
	HandleTopicWithinGroup(func(context.Context, string, map[string]string) error {
		return nil
	}, "events", "group")(opts)
	if len(opts.HandlerOptsLst) != 1 {
		t.Fatalf("handlers = %d, want 1", len(opts.HandlerOptsLst))
	}
	err := opts.HandlerOptsLst[0].Handler(context.Background(), []byte("{"), nil)
	if !errx.IsPermanent(err) {
		t.Fatalf("unmarshal error = %v, want permanent", err)
	}
}

func TestLeaseAndRetryDefaults(t *testing.T) {
	opts := new(Options).With()
	if got, want := opts.LeaseTTL(), 180*time.Second; got != want {
		t.Fatalf("LeaseTTL() = %v, want %v", got, want)
	}
	if got, want := opts.RetryInterval(), 10*time.Second; got != want {
		t.Fatalf("RetryInterval() = %v, want %v", got, want)
	}
	if !opts.CleanupEnabled {
		t.Fatal("CleanupEnabled = false, want true")
	}
	if got, want := opts.Retention(enum.StatusSucceeded), 24*time.Hour; got != want {
		t.Fatalf("successful retention = %v, want %v", got, want)
	}
	if got, want := opts.Retention(enum.StatusFailed), 15*24*time.Hour; got != want {
		t.Fatalf("failed retention = %v, want %v", got, want)
	}
	if got, want := opts.CleanupInterval(), 5*time.Minute; got != want {
		t.Fatalf("cleanup interval = %v, want %v", got, want)
	}
	if got, want := opts.CleanupTimeout(), 30*time.Second; got != want {
		t.Fatalf("cleanup timeout = %v, want %v", got, want)
	}
	if got, want := opts.CleanupBatchSize, 1000; got != want {
		t.Fatalf("cleanup batch size = %d, want %d", got, want)
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
		{name: "retry interval zero", field: "RetryIntervalSeconds", value: 0, mutate: func(o *Options, v int) { o.RetryIntervalSeconds = v }},
		{name: "retry interval negative", field: "RetryIntervalSeconds", value: -1, mutate: func(o *Options, v int) { o.RetryIntervalSeconds = v }},
		{name: "pump interval zero", field: "PumpIntervalSeconds", value: 0, mutate: func(o *Options, v int) { o.PumpIntervalSeconds = v }},
		{name: "pump interval negative", field: "PumpIntervalSeconds", value: -1, mutate: func(o *Options, v int) { o.PumpIntervalSeconds = v }},
		{name: "succeeded retention zero", field: "SucceededRetentionSeconds", value: 0, mutate: func(o *Options, v int) { o.SucceededRetentionSeconds = v }},
		{name: "failed retention negative", field: "FailedRetentionSeconds", value: -1, mutate: func(o *Options, v int) { o.FailedRetentionSeconds = v }},
		{name: "cleanup interval zero", field: "CleanupIntervalSeconds", value: 0, mutate: func(o *Options, v int) { o.CleanupIntervalSeconds = v }},
		{name: "cleanup batch negative", field: "CleanupBatchSize", value: -1, mutate: func(o *Options, v int) { o.CleanupBatchSize = v }},
		{name: "cleanup timeout zero", field: "CleanupTimeoutSeconds", value: 0, mutate: func(o *Options, v int) { o.CleanupTimeoutSeconds = v }},
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

func TestValidateAllowsDisabledCleanupWithZeroValues(t *testing.T) {
	opts := new(Options).With()
	opts.CleanupEnabled = false
	opts.SucceededRetentionSeconds = 0
	opts.FailedRetentionSeconds = 0
	opts.CleanupIntervalSeconds = 0
	opts.CleanupBatchSize = 0
	opts.CleanupTimeoutSeconds = 0
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateAcceptsReasonableUpperValues(t *testing.T) {
	opts := new(Options).With()
	opts.ClaimBatchSize = 10_000
	opts.MaxRetries = 1_000_000
	opts.LeaseTTLSeconds = 30 * 24 * 60 * 60
	opts.RetryIntervalSeconds = 60
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

	for _, field := range []string{"LeaseTTLSeconds", "RetryIntervalSeconds", "PumpIntervalSeconds"} {
		t.Run(field, func(t *testing.T) {
			opts := new(Options).With()
			value := int(maxDurationSeconds + 1)
			switch field {
			case "LeaseTTLSeconds":
				opts.LeaseTTLSeconds = value
			case "RetryIntervalSeconds":
				opts.RetryIntervalSeconds = value
			default:
				opts.PumpIntervalSeconds = value
			}
			err := opts.Validate()
			if err == nil || !strings.Contains(err.Error(), field+"=") {
				t.Fatalf("Validate() error = %v, want %s overflow", err, field)
			}
		})
	}
}
