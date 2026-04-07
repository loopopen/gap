package storage

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/txer"
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

	QueryPublished(ctx context.Context, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)

	QueryReceived(ctx context.Context, status enum.Status, topic string, group string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)
}

type FactoryIface interface {
	CreateStorage(opts *gap.Options) (Storage, error)
}
