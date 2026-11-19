package xmysql

import (
	"strings"
	"testing"
)

func TestRenewPublishedSQLExtendsLeaseWithoutChangingRetries(t *testing.T) {
	query := renewLeaseSQL("`gap_published`")
	for _, part := range []string{
		"UPDATE `gap_published`",
		"SET `lease_until` = ?",
		"WHERE `id` = ?",
		"AND `status` = ?",
		"AND `lease_token` = ?",
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("renew SQL %q does not contain %q", query, part)
		}
	}
	if strings.Contains(query, "retries") {
		t.Fatalf("renew SQL changes retries: %s", query)
	}
}
