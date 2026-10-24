package xgorm

import (
	"context"
	"errors"
	"time"

	"github.com/loopopen/gap/v2/spi/entity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	MaxPageSize int = 200
	DefPageSize int = 20
)

func (s *Storage) DashboardStats(ctx context.Context) (*entity.DashboardStats, error) {
	published, err := messageStats(s.db.WithContext(ctx).Model(&Published{}))
	if err != nil {
		return nil, err
	}
	received, err := messageStats(s.db.WithContext(ctx).Model(&Received{}))
	if err != nil {
		return nil, err
	}
	return &entity.DashboardStats{Published: published, Received: received}, nil
}

func messageStats(db *gorm.DB) (entity.MessageStats, error) {
	type statsRow struct {
		Total         int64
		Pending       int64
		Processing    int64
		Succeeded     int64
		Failed        int64
		Retries       int64
		LastActivity  *time.Time
		OldestBacklog *time.Time
	}
	var row statsRow
	err := db.Select(`
		COUNT(*) AS total,
		COALESCE(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END), 0) AS pending,
		COALESCE(SUM(CASE WHEN status = 'Processing' THEN 1 ELSE 0 END), 0) AS processing,
		COALESCE(SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END), 0) AS succeeded,
		COALESCE(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END), 0) AS failed,
		COALESCE(SUM(retries), 0) AS retries,
		MAX(created_at) AS last_activity,
		MIN(CASE WHEN status IN ('Pending', 'Processing') THEN created_at END) AS oldest_backlog`,
	).Scan(&row).Error
	if err != nil {
		return entity.MessageStats{}, err
	}
	return entity.MessageStats{
		Total:         row.Total,
		Pending:       row.Pending,
		Processing:    row.Processing,
		Succeeded:     row.Succeeded,
		Failed:        row.Failed,
		Retries:       row.Retries,
		LastActivity:  row.LastActivity,
		OldestBacklog: row.OldestBacklog,
	}, nil
}

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
	if filter.ID != nil {
		db = db.Where(clause.Eq{Column: clause.Column{Name: "id"}, Value: *filter.ID})
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
