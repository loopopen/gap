package xgorm

import (
	"context"
	"time"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
	dbmodel "github.com/loopopen/gap/storage/xgorm/internal/model"
)

// RenewPublishedLeases extends the lease of pending published rows that are
// still owned by the given tokens.
func (s *storageAdapter) RenewPublishedLeases(ctx context.Context, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, &dbmodel.Published{}, leases)
}

// RenewReceivedLeases extends the lease of pending received rows that are
// still owned by the given tokens.
func (s *storageAdapter) RenewReceivedLeases(ctx context.Context, leases []storage.Lease) ([]uint, error) {
	return s.renewLeases(ctx, &dbmodel.Received{}, leases)
}

func (s *storageAdapter) renewLeases(ctx context.Context, model any, leases []storage.Lease) ([]uint, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	deadline := s.databaseAfter(int64(s.gapOpts.LeaseTTL() / time.Second))
	renewed := make([]uint, 0, len(leases))
	for _, lease := range leases {
		if lease.ID == 0 || lease.LeaseToken == "" {
			continue
		}
		result := s.db.WithContext(ctx).
			Model(model).
			Where("id = ? AND status = ? AND lease_token = ?", lease.ID, enum.StatusPending, lease.LeaseToken).
			Update("lease_until", deadline)
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected > 0 {
			renewed = append(renewed, lease.ID)
		}
	}
	return renewed, nil
}
