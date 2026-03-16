package xmysql

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/storage"
)

var _ storage.StorageX = (*Storage)(nil)

// QueryPublished implements [storage.StorageX].
func (s *Storage) QueryPublished(ctx context.Context, ids []uint, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	panic("unimplemented")
}

// QueryReceived implements [storage.StorageX].
func (s *Storage) QueryReceived(ctx context.Context, ids []uint, status enum.Status, topic string, group string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	panic("unimplemented")
}
