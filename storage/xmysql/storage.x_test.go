package xmysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/loopopen/gap/v2/spi/entity"
	"github.com/loopopen/gap/v2/spi/enum"
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
		Payload:     "customer-42",
	}, true)

	wantSQL := "WHERE `id` = ? AND `status` = ? AND `topic` = ? AND `group` = ? AND `created_at` >= ? AND `created_at` <= ? AND `payload` LIKE ?"
	if got := h.String(); got != wantSQL {
		t.Fatalf("where = %q, want %q", got, wantSQL)
	}
	wantParams := []any{id, enum.StatusPending, "orders.created", "notifications", from, to, "%customer-42%"}
	if got := h.Params(); !reflect.DeepEqual(got, wantParams) {
		t.Fatalf("params = %#v, want %#v", got, wantParams)
	}
}
