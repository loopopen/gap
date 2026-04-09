package xgorm

import (
	"context"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"gorm.io/gorm"
)

const (
	MaxPageSize int = 200
	DefPageSize int = 20
)

func (s *Storage) GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error) {
	var pub *Published
	err := s.db.WithContext(ctx).Take(&pub, id).Error
	if err != nil {
		return nil, err
	}
	return pub.ToEntity(), nil
}

func (s *Storage) QueryPublished(ctx context.Context, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var pubs []*Published
	db := s.db.WithContext(ctx).Model(&Published{})
	if status != 0 {
		db.Where("`status` = ?", status)
	}
	if topic != "" {
		db.Where("`topic` = ?", topic)
	}

	var count int64
	err := db.Count(&count).Error
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	err = db.Order("`id`").
		Scopes(paginate(pg)).
		Scan(&pubs).Error
	if err != nil {
		return nil, nil, err
	}

	var es []*entity.Envelope
	for _, p := range pubs {
		es = append(es, p.ToEntity())
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

func (s *Storage) QueryReceived(ctx context.Context, status enum.Status, topic string, group string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var recs []*Received
	db := s.db.WithContext(ctx).Model(&Received{})
	if status != 0 {
		db.Where("`status` = ?", status)
	}
	if topic != "" {
		db.Where("`topic` = ?", topic)
	}
	if group != "" {
		db.Where("`group` = ?", group)
	}

	var count int64
	err := db.Count(&count).Error
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	err = db.Order("`id`").
		Scopes(paginate(pg)).
		Scan(&recs).Error
	if err != nil {
		return nil, nil, err
	}

	var es []*entity.Envelope
	for _, r := range recs {
		es = append(es, r.ToEntity())
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

func paginate(page entity.Pagination) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		offset := (page.Page - 1) * page.PerPage
		return db.Offset(offset).Limit(page.PerPage)
	}
}
