package storage

import (
	"context"
	"errors"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/txer"
)

var ErrLeaseLost = errors.New("gap: message lease lost")

// Role selects the data-plane pool or the dashboard read pool.
type Role uint8

const (
	RoleData Role = iota
	RoleDashboard
)

type Storage interface {
	Bind(txer txer.Txer) (Storage, error)

	CreatePublished(ctx context.Context, envelope *entity.Envelope) error

	// UpdateStatusPublished writes status verbatim for the current lease and releases it.
	UpdateStatusPublished(ctx context.Context, id uint, leaseToken string, status enum.Status) error

	ClaimPublishedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	CreateReceived(ctx context.Context, envelope *entity.Envelope) error

	ClaimReceivedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	// UpdateStatusReceived writes status verbatim for the current lease and releases it.
	UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error

	StorageX
}

type StorageX interface {
	GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error)

	DashboardStats(ctx context.Context) (*entity.DashboardStats, error)

	QueryPublished(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)

	QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)
}
