package dashboard

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/enum"
)

func TestMessageFilterFromRequest(t *testing.T) {
	values := url.Values{
		"status":       {"Pending"},
		"topic":        {"orders.created"},
		"group":        {"notifications"},
		"created_from": {"2026-10-17T09:30:00+09:00"},
		"created_to":   {"2026-10-17T10:30:00+09:00"},
		"payload":      {"customer-42"},
	}
	request := httptest.NewRequest("GET", "/?"+values.Encode(), nil)

	filter, err := messageFilterFromRequest(request, true)
	if err != nil {
		t.Fatal(err)
	}
	if filter.Status != enum.StatusPending || filter.Topic != "orders.created" || filter.Group != "notifications" || filter.Payload != "customer-42" {
		t.Fatalf("unexpected filter: %#v", filter)
	}
	if filter.CreatedFrom == nil || filter.CreatedTo == nil {
		t.Fatalf("missing time boundaries: %#v", filter)
	}
	if got := filter.CreatedFrom.Format(time.RFC3339); got != "2026-10-17T09:30:00+09:00" {
		t.Fatalf("created_from = %q", got)
	}
	if got := filter.CreatedTo.Format(time.RFC3339); got != "2026-10-17T10:30:00+09:00" {
		t.Fatalf("created_to = %q", got)
	}
}

func TestMessageFilterFromRequestRejectsInvalidRange(t *testing.T) {
	request := httptest.NewRequest("GET", "/?created_from=2026-10-17T11%3A00&created_to=2026-10-17T10%3A00", nil)
	if _, err := messageFilterFromRequest(request, false); err == nil {
		t.Fatal("expected an invalid time range error")
	}
}
