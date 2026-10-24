package xmysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
)

func TestAddMessageFilters(t *testing.T) {
	from := time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	id := uint(1844674407370955)
	h := NewWhereHelper()
	addMessageFilters(h, &entity.MessageFilter{
		ID:          &id,
		Status:      enum.StatusPending,
		Topic:       "orders.created",
		Group:       "notifications",
		CreatedFrom: &from,
		CreatedTo:   &to,
		HeaderKey:   "tenant-id",
		HeaderValue: "customer-42",
		Payload:     "customer-42",
	}, true)

	wantSQL := "WHERE `id` = ? AND `status` = ? AND `topic` = ? AND `group` = ? AND `created_at` >= ? AND `created_at` <= ? AND `headers` LIKE ? ESCAPE '!' AND `payload` LIKE ?"
	if got := h.String(); got != wantSQL {
		t.Fatalf("where = %q, want %q", got, wantSQL)
	}
	wantParams := []any{id, enum.StatusPending, "orders.created", "notifications", from, to, `%"tenant-id":"customer-42"%`, "%customer-42%"}
	if got := h.Params(); !reflect.DeepEqual(got, wantParams) {
		t.Fatalf("params = %#v, want %#v", got, wantParams)
	}
}

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
