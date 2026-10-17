package xgorm

import (
	"context"
	"errors"

	"github.com/loopopen/gap/internal/entity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MaxPageSize int = 200
	DefPageSize int = 20
)

func (s *Storage) GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error) {
	var pub *Published
	err := s.db.WithContext(ctx).Take(&pub, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	en := pub.ToEntity()
	if pub.err != nil {
		return nil, pub.err
	}
	return en, nil
}

func (s *Storage) QueryPublished(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var pubs []*Published
	db := s.db.WithContext(ctx).Model(&Published{})
	db = applyMessageFilters(db, filter, false)

	var count int64
	err := db.Count(&count).Error
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	err = db.Order(clause.OrderByColumn{Column: clause.Column{Name: "id"}}).
		Scopes(paginate(pg)).
		Scan(&pubs).Error
	if err != nil {
		return nil, nil, err
	}

	es, err := pubsToEntities(pubs)
	if err != nil {
		return nil, nil, err
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

func (s *Storage) QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var recs []*Received
	db := s.db.WithContext(ctx).Model(&Received{})
	db = applyMessageFilters(db, filter, true)

	var count int64
	err := db.Count(&count).Error
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	err = db.Order(clause.OrderByColumn{Column: clause.Column{Name: "id"}}).
		Scopes(paginate(pg)).
		Scan(&recs).Error
	if err != nil {
		return nil, nil, err
	}

	es, err := recsToEntities(recs)
	if err != nil {
		return nil, nil, err
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

func applyMessageFilters(db *gorm.DB, filter *entity.MessageFilter, includeGroup bool) *gorm.DB {
	if filter == nil {
		return db
	}
	if filter.Status != 0 {
		db = db.Where(clause.Eq{Column: clause.Column{Name: "status"}, Value: filter.Status})
	}
	if filter.Topic != "" {
		db = db.Where(clause.Eq{Column: clause.Column{Name: "topic"}, Value: filter.Topic})
	}
	if includeGroup && filter.Group != "" {
		db = db.Where(clause.Eq{Column: clause.Column{Name: "group"}, Value: filter.Group})
	}
	if filter.CreatedFrom != nil {
		db = db.Where(clause.Gte{Column: clause.Column{Name: "created_at"}, Value: *filter.CreatedFrom})
	}
	if filter.CreatedTo != nil {
		db = db.Where(clause.Lte{Column: clause.Column{Name: "created_at"}, Value: *filter.CreatedTo})
	}
	if filter.Payload != "" {
		db = db.Where(clause.Like{Column: clause.Column{Name: "payload"}, Value: "%" + filter.Payload + "%"})
	}
	return db
}

func paginate(page entity.Pagination) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		offset := (page.Page - 1) * page.PerPage
		return db.Offset(offset).Limit(page.PerPage)
	}
}
