package xgorm

import (
	"context"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

// RenewPublishedLeases extends the lease of pending published rows that are
// still owned by the given tokens.
func (s *Storage) RenewPublishedLeases(ctx context.Context, until time.Time, leases []storage.Lease) ([]uint, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	renewed := make([]uint, 0, len(leases))
	for _, lease := range leases {
		if lease.ID == 0 || lease.LeaseToken == "" {
			continue
		}
		result := s.db.WithContext(ctx).
			Model(&Published{}).
			Where("id = ? AND status = ? AND lease_token = ?", lease.ID, enum.StatusPending, lease.LeaseToken).
			Update("lease_until", until)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected > 0 {
			renewed = append(renewed, lease.ID)
		}
	}
	return renewed, nil
}
