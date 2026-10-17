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
	h := NewWhereHelper()
	addMessageFilters(h, &entity.MessageFilter{
		Status:      enum.StatusPending,
		Topic:       "orders.created",
		Group:       "notifications",
		CreatedFrom: &from,
		CreatedTo:   &to,
		Payload:     "customer-42",
	}, true)

	wantSQL := "WHERE `status` = ? AND `topic` = ? AND `group` = ? AND `created_at` >= ? AND `created_at` <= ? AND `payload` LIKE ?"
	if got := h.String(); got != wantSQL {
		t.Fatalf("where = %q, want %q", got, wantSQL)
	}
	wantParams := []any{enum.StatusPending, "orders.created", "notifications", from, to, "%customer-42%"}
	if got := h.Params(); !reflect.DeepEqual(got, wantParams) {
		t.Fatalf("params = %#v, want %#v", got, wantParams)
	}
}
