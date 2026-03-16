package storage

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/tx"
	"github.com/lopolopen/gap/options/gap"
)

type Storage interface {
	Bind(txer tx.Txer) (Storage, error)

	CreatePublished(ctx context.Context, envelope *entity.Envelope) error

	UpdateStatusPublished(ctx context.Context, id uint, status enum.Status) error

	ClaimPublishedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	CreateReceived(ctx context.Context, envelope *entity.Envelope) error

	ClaimReceivedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error)

	UpdateStatusReceived(ctx context.Context, id uint, status enum.Status) error

	StorageX
}

type StorageX interface {
	QueryPublished(ctx context.Context, ids []uint, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)

	QueryReceived(ctx context.Context, ids []uint, status enum.Status, topic string, group string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error)
}

type Factory interface {
	CreateStorage(opts *gap.Options) (Storage, error)
}
