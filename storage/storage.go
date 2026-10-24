package storage

import (
	"context"

	"github.com/loopopen/gap/v2/internal/entity"
	"github.com/loopopen/gap/v2/internal/enum"
	"github.com/loopopen/gap/v2/internal/txer"
)

type Storage interface {
	Bind(txer txer.Txer) (Storage, error)

	CreatePublished(ctx context.Context, envelope *entity.Envelope) error

	UpdateStatusPublished(ctx context.Context, id uint, src enum.Status, status enum.Status) error

	ClaimPublishedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	CreateReceived(ctx context.Context, envelope *entity.Envelope) error

	ClaimReceivedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	UpdateStatusReceived(ctx context.Context, id uint, src enum.Status, status enum.Status) error

	StorageX
}

type StorageX interface {
	GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error)

	DashboardStats(ctx context.Context) (*entity.DashboardStats, error)

	QueryPublished(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)

	QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)
}
