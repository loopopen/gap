package xpostgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/storage"
)

var _ storage.StorageX = (*Storage)(nil)

func (s *Storage) DashboardStats(ctx context.Context) (*entity.DashboardStats, error) {
	published, err := s.messageStats(ctx, "published")
	if err != nil {
		return nil, err
	}
	received, err := s.messageStats(ctx, "received")
	if err != nil {
		return nil, err
	}
	return &entity.DashboardStats{Published: published, Received: received}, nil
}

func (s *Storage) messageStats(ctx context.Context, name string) (entity.MessageStats, error) {
	table, err := s.quotedTable(name)
	if err != nil {
		return entity.MessageStats{}, err
	}
	script := `SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'Processing' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(retries), 0),
		MAX(created_at),
		MIN(CASE WHEN status IN ('Pending', 'Processing') THEN created_at END)
		FROM ` + table

	var stats entity.MessageStats
	var lastActivity sql.NullTime
	var oldestBacklog sql.NullTime
	err = s.db.QueryRowContext(ctx, script).Scan(
		&stats.Total,
		&stats.Pending,
		&stats.Processing,
		&stats.Succeeded,
		&stats.Failed,
		&stats.Retries,
		&lastActivity,
		&oldestBacklog,
	)
	if err != nil {
		return entity.MessageStats{}, err
	}
	if lastActivity.Valid {
		stats.LastActivity = &lastActivity.Time
	}
	if oldestBacklog.Valid {
		stats.OldestBacklog = &oldestBacklog.Time
	}
	return stats, nil
}

func (s *Storage) GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error) {
	table, err := s.quotedTable("published")
	if err != nil {
		return nil, err
	}
	script := `SELECT "id","created_at","version","topic","status","headers","payload","retries","expired_at" FROM ` +
		table + ` WHERE "id" = $1`
	var pub Published
	err = s.db.QueryRowContext(ctx, script, id).Scan(
		&pub.ID,
		&pub.CreatedAt,
		&pub.Version,
		&pub.Topic,
		&pub.Status,
		&pub.Headers,
		&pub.Payload,
		&pub.Retries,
		&pub.ExpiredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
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

// QueryPublished implements [storage.StorageX].
func (s *Storage) QueryPublished(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	wh := NewWhereHelper()
	if err := s.addMessageFilters(wh, filter, false); err != nil {
		return nil, nil, err
	}
	return s.queryPublished(ctx, wh, filter, page)
}

func (s *Storage) queryPublished(ctx context.Context, wh *WhereHelper, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	table, err := s.quotedTable("published")
	if err != nil {
		return nil, nil, err
	}
	var count int64
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" "+wh.String(), wh.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	script := `SELECT "id","created_at","version","topic","status","headers","payload","retries","expired_at" FROM ` +
		table + " " + wh.String() + ` ORDER BY "id" ` + messageOrder(filter) + " " + paginate(pg)
	rows, err := s.db.QueryContext(ctx, script, wh.Params()...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var pubs []*Published
	for rows.Next() {
		var pub Published
		err = rows.Scan(
			&pub.ID,
			&pub.CreatedAt,
			&pub.Version,
			&pub.Topic,
			&pub.Status,
			&pub.Headers,
			&pub.Payload,
			&pub.Retries,
			&pub.ExpiredAt,
		)
		if err != nil {
			return nil, nil, err
		}
		pubs = append(pubs, &pub)
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	es, err := pubsToEntities(pubs)
	if err != nil {
		return nil, nil, err
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

// QueryReceived implements [storage.StorageX].
func (s *Storage) QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	h := NewWhereHelper()
	if err := s.addMessageFilters(h, filter, true); err != nil {
		return nil, nil, err
	}

	table, err := s.quotedTable("received")
	if err != nil {
		return nil, nil, err
	}
	var count int64
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" "+h.String(), h.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	pg := page.Normalize()
	script := `SELECT "id","created_at","version","topic","status","headers","payload","retries","expired_at","group" FROM ` +
		table + " " + h.String() + ` ORDER BY "id" ` + messageOrder(filter) + " " + paginate(pg)
	rows, err := s.db.QueryContext(ctx, script, h.Params()...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var recs []*Received
	for rows.Next() {
		var rec Received
		err = rows.Scan(
			&rec.ID,
			&rec.CreatedAt,
			&rec.Version,
			&rec.Topic,
			&rec.Status,
			&rec.Headers,
			&rec.Payload,
			&rec.Retries,
			&rec.ExpiredAt,
			&rec.Group,
		)
		if err != nil {
			return nil, nil, err
		}
		recs = append(recs, &rec)
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	es, err := recsToEntities(recs)
	if err != nil {
		return nil, nil, err
	}
	pg.SetTotal(int(count))
	return es, &pg, nil
}

func messageOrder(filter *entity.MessageFilter) string {
	if filter != nil && filter.Descending {
		return "DESC"
	}
	return "ASC"
}

func (s *Storage) addMessageFilters(h *WhereHelper, filter *entity.MessageFilter, includeGroup bool) error {
	if filter == nil {
		return nil
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	if filter.ID != nil {
		Add(h, `"id" = ?`, *filter.ID)
	}
	if filter.Status != 0 {
		Add(h, `"status" = ?::`+statusType, filter.Status)
	}
	AddIfNotZero(h, `"topic" = ?`, filter.Topic)
	if includeGroup {
		AddIfNotZero(h, `"group" = ?`, filter.Group)
	}
	if filter.CreatedFrom != nil {
		Add(h, `"created_at" >= ?`, *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		Add(h, `"created_at" <= ?`, *filter.CreatedTo)
	}
	if pattern := filter.HeaderLikePattern(); pattern != "" {
		Add(h, `"headers" LIKE ? ESCAPE '!'`, pattern)
	}
	if filter.Payload != "" {
		Add(h, `"payload" LIKE ?`, "%"+filter.Payload+"%")
	}
	return nil
}

func paginate(page entity.Pagination) string {
	var buff bytes.Buffer
	fmt.Fprintf(&buff, "LIMIT %d ", page.PerPage)
	if page.Page > 1 {
		fmt.Fprintf(&buff, "OFFSET %d", (page.Page-1)*page.PerPage)
	}
	return buff.String()
}
