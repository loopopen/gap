package xpostgres

import (
	"strings"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

func TestRenewPublishedSQLExtendsLeaseWithoutChangingRetries(t *testing.T) {
	until := time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	query, args := renewPublishedSQL(`"gap"."published"`, `"gap"."status_enum"`, until, enum.StatusPending, []storage.Lease{
		{ID: 7, LeaseToken: "token-a"},
		{ID: 8, LeaseToken: "token-b"},
	})
	for _, part := range []string{
		`UPDATE "gap"."published" AS p SET "lease_until" = $1`,
		`FROM (VALUES ($2::bigint, $3::varchar), ($4::bigint, $5::varchar)) AS v(id, token)`,
		`p."id" = v.id`,
		`p."lease_token" = v.token`,
		`p."status" = $6::"gap"."status_enum"`,
		`RETURNING p."id"`,
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("renew SQL %q does not contain %q", query, part)
		}
	}
	if strings.Contains(query, "retries") {
		t.Fatalf("renew SQL changes retries: %s", query)
	}
	if len(args) != 6 || args[0] != until || args[1] != uint(7) || args[2] != "token-a" || args[3] != uint(8) || args[4] != "token-b" || args[5] != enum.StatusPending {
		t.Fatalf("renew args = %#v", args)
	}
}
