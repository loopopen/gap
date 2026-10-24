package xpostgres

import (
	"testing"

	"github.com/loopopen/gap/internal/entity"
)

func TestMessageOrder(t *testing.T) {
	tests := []struct {
		name   string
		filter *entity.MessageFilter
		want   string
	}{
		{name: "nil filter", want: "ASC"},
		{name: "ascending", filter: &entity.MessageFilter{}, want: "ASC"},
		{name: "descending", filter: &entity.MessageFilter{Descending: true}, want: "DESC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := messageOrder(tt.filter); got != tt.want {
				t.Fatalf("messageOrder() = %q, want %q", got, tt.want)
			}
		})
	}
}
