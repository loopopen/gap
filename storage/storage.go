package storage

import (
	"context"
	"errors"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/txer"
)

// Lease identifies one pending message row owned by a worker.
type Lease struct {
	ID         uint
	LeaseToken string
}

var ErrLeaseLost = errors.New("gap: message lease lost")

// Role selects the data-plane pool or the dashboard read pool.
type Role uint8

const (
	RoleData Role = iota
	RoleDashboard
)

type Storage interface {
	// Bind returns a Storage that uses the caller's transaction.
	Bind(txer txer.Txer) (Storage, error)

	// CreatePublished inserts a pending envelope and holds its lease for LeaseTTL.
	CreatePublished(ctx context.Context, envelope *entity.Envelope) error

	// UpdateStatusPublished releases the current lease.
	// Succeeded and Failed are written as given. Pending keeps that status and
	// schedules the next attempt after RetryInterval.
	UpdateStatusPublished(ctx context.Context, id uint, leaseToken string, status enum.Status) error

	// RenewPublishedLeases extends lease_until by LeaseTTL for pending published
	// rows still owned by the given lease tokens. The database clock supplies the
	// new deadline. It does not change retries. Returned ids are the rows whose
	// lease was extended.
	RenewPublishedLeases(ctx context.Context, leases []Lease) ([]uint, error)

	// ClaimPublishedBatch marks exhausted rows Failed for brokerName, then locks
	// up to batchSize due rows for that broker name with retries below
	// MaxRetries. The claim increments retries and sets a lease. Rows leased by
	// another worker stay with that worker. The caller rotates broker names.
	ClaimPublishedBatch(ctx context.Context, batchSize int, brokerName string) ([]*entity.Envelope, error)

	// CreateReceived inserts a pending envelope for its consumer group and holds
	// its lease for LeaseTTL.
	CreateReceived(ctx context.Context, envelope *entity.Envelope) error

	// ClaimReceivedBatch claims due rows for one group and one broker name.
	// The caller rotates groups and broker names. Exhausted rows of that group
	// and broker name are marked Failed. The claim increments retries and sets
	// a lease.
	ClaimReceivedBatch(ctx context.Context, batchSize int, group, brokerName string) ([]*entity.Envelope, error)

	// UpdateStatusReceived releases the current lease.
	// Succeeded and Failed are written as given. Pending keeps that status and
	// schedules the next attempt after RetryInterval.
	UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error

	// RenewReceivedLeases extends lease_until by LeaseTTL for pending received
	// rows still owned by the given lease tokens. The database clock supplies the
	// new deadline. It does not change retries. Returned ids are the rows whose
	// lease was extended.
	RenewReceivedLeases(ctx context.Context, leases []Lease) ([]uint, error)

	// DeleteExpiredPublishedBatch removes at most batchSize terminal published
	// messages whose retention period has elapsed.
	DeleteExpiredPublishedBatch(ctx context.Context, batchSize int) (int64, error)

	// DeleteExpiredReceivedBatch removes at most batchSize terminal received
	// messages whose retention period has elapsed.
	DeleteExpiredReceivedBatch(ctx context.Context, batchSize int) (int64, error)

	StorageX
}

type StorageX interface {
	// GetPublishedByID returns the published envelope for id.
	// A missing id returns nil, nil.
	GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error)

	// DashboardStats returns published and received counts, and received counts by group.
	DashboardStats(ctx context.Context) (*entity.DashboardStats, error)

	// QueryPublished returns one page of published envelopes that match filter.
	// The returned pagination carries the total number of matches.
	QueryPublished(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)

	// QueryReceived returns one page of received envelopes that match filter.
	// Group is part of the filter. The returned pagination carries the total.
	QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)
}
