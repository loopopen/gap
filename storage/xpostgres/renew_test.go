package xpostgres

import (
	"strings"
	"testing"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

func TestRenewPublishedSQLExtendsLeaseWithoutChangingRetries(t *testing.T) {
	query, args := renewLeaseSQL(`"gap"."published"`, `"gap"."status_enum"`, 180, enum.StatusPending, []storage.Lease{
		{ID: 7, LeaseToken: "token-a"},
		{ID: 8, LeaseToken: "token-b"},
	})
	for _, part := range []string{
		`UPDATE "gap"."published" AS m SET "lease_until" = CURRENT_TIMESTAMP + ($1 * INTERVAL '1 second')`,
		`FROM (VALUES ($2::bigint, $3::varchar), ($4::bigint, $5::varchar)) AS v(id, token)`,
		`m."id" = v.id`,
		`m."lease_token" = v.token`,
		`m."status" = $6::"gap"."status_enum"`,
		`RETURNING m."id"`,
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("renew SQL %q does not contain %q", query, part)
		}
	}
	if strings.Contains(query, "retries") {
		t.Fatalf("renew SQL changes retries: %s", query)
	}
	if len(args) != 6 || args[0] != int64(180) || args[1] != uint(7) || args[2] != "token-a" || args[3] != uint(8) || args[4] != "token-b" || args[5] != enum.StatusPending {
		t.Fatalf("renew args = %#v", args)
	}
}
