package entity

import "testing"

func TestMessageFilterHeaderLikePattern(t *testing.T) {
	tests := []struct {
		name   string
		filter *MessageFilter
		want   string
	}{
		{name: "nil filter"},
		{name: "empty", filter: &MessageFilter{}},
		{name: "key only", filter: &MessageFilter{HeaderKey: "tenant-id"}, want: `%"tenant-id":%`},
		{name: "value only", filter: &MessageFilter{HeaderValue: "customer-42"}, want: `%:"customer-42"%`},
		{name: "key and value", filter: &MessageFilter{HeaderKey: "tenant-id", HeaderValue: "customer-42"}, want: `%"tenant-id":"customer-42"%`},
		{name: "JSON and LIKE escaping", filter: &MessageFilter{HeaderKey: `a_%!`, HeaderValue: `b"c`}, want: `%"a!_!%!!":"b\"c"%`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.filter.HeaderLikePattern(); got != tt.want {
				t.Fatalf("HeaderLikePattern() = %q, want %q", got, tt.want)
			}
		})
	}
}
