package xmysql

import (
	"context"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

// RenewPublishedLeases implements [storage.Storage].
func (s *Storage) RenewPublishedLeases(ctx context.Context, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, "published", leases)
}

// RenewReceivedLeases implements [storage.Storage].
func (s *Storage) RenewReceivedLeases(ctx context.Context, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, "received", leases)
}

func (s *Storage) renewLeases(ctx context.Context, base string, leases []storage.Lease) ([]uint, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	table, err := s.quotedTable(base)
	if err != nil {
		return nil, err
	}
	query := renewLeaseSQL(table)
	ttlSeconds := int64(s.gapOpts.LeaseTTL() / time.Second)
	renewed := make([]uint, 0, len(leases))
	for _, lease := range leases {
		if lease.ID == 0 || lease.LeaseToken == "" {
			continue
		}
		result, err := s.execer().ExecContext(ctx, query, ttlSeconds, lease.ID, enum.StatusPending, lease.LeaseToken)
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

func renewLeaseSQL(table string) string {
	return "UPDATE " + table + " SET `lease_until` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)) WHERE `id` = ? AND `status` = ? AND `lease_token` = ?"
}
