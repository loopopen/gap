package xmysql

import (
	"context"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

// RenewPublishedLeases implements [storage.Storage].
func (s *Storage) RenewPublishedLeases(ctx context.Context, until time.Time, leases []storage.Lease) ([]uint, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	table, err := s.quotedTable("published")
	if err != nil {
		return nil, err
	}
	query := renewPublishedSQL(table)
	renewed := make([]uint, 0, len(leases))
	for _, lease := range leases {
		if lease.ID == 0 || lease.LeaseToken == "" {
			continue
		}
		result, err := s.execer().ExecContext(ctx, query, until, lease.ID, enum.StatusPending, lease.LeaseToken)
		if err != nil {
			return nil, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n > 0 {
			renewed = append(renewed, lease.ID)
		}
	}
	return renewed, nil
}

func renewPublishedSQL(table string) string {
	return "UPDATE " + table + " SET `lease_until` = ? WHERE `id` = ? AND `status` = ? AND `lease_token` = ?"
}
