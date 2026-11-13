package internal

import (
	"errors"
	"testing"
)

func TestStreamValuesRoundTrip(t *testing.T) {
	values, err := streamValues(map[string]string{"gap-msg-id": "42", "empty": ""}, []byte("payload"))
	if err != nil {
		t.Fatalf("streamValues: %v", err)
	}
	headers, body, err := entryMessage(values)
	if err != nil {
		t.Fatalf("entryMessage: %v", err)
	}
	if headers["gap-msg-id"] != "42" || headers["empty"] != "" || string(body) != "payload" {
		t.Fatalf("headers=%v body=%q", headers, body)
	}
}

func TestStreamValuesNilHeaders(t *testing.T) {
	values, err := streamValues(nil, []byte("payload"))
	if err != nil {
		t.Fatalf("streamValues: %v", err)
	}
	headers, body, err := entryMessage(values)
	if err != nil {
		t.Fatalf("entryMessage: %v", err)
	}
	if len(headers) != 0 || string(body) != "payload" {
		t.Fatalf("headers=%v body=%q", headers, body)
	}
}

func TestEntryMessageRequiresHeadersAndBody(t *testing.T) {
	_, _, err := entryMessage(map[string]any{fieldBody: "payload"})
	if !errors.Is(err, errMissingHeaders) {
		t.Fatalf("error = %v, want %v", err, errMissingHeaders)
	}
	_, _, err = entryMessage(map[string]any{fieldHeaders: "{"})
	if !errors.Is(err, errInvalidHeaders) {
		t.Fatalf("error = %v, want %v", err, errInvalidHeaders)
	}
	_, _, err = entryMessage(map[string]any{fieldHeaders: "{}"})
	if !errors.Is(err, errMissingBody) {
		t.Fatalf("error = %v, want %v", err, errMissingBody)
	}
}
