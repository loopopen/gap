package xmysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

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
	const script = `SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'Succeeded' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(retries), 0),
		MAX(created_at),
		MIN(CASE WHEN status = 'Pending' THEN created_at END)
		FROM %s`

	var stats entity.MessageStats
	var lastActivity sql.NullTime
	var oldestBacklog sql.NullTime
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(script, table)).Scan(
		&stats.Total,
		&stats.Pending,
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
	const script = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` " +
		"FROM %s WHERE `id` = ?"
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(script, table), id)
	if row == nil {
		return nil, nil
	}
	var pub Published
	err = row.Scan(
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
	var pubs []*Published
	wh := NewWhereHelper()
	addMessageFilters(wh, filter, false)

	table, err := s.quotedTable("published")
	if err != nil {
		return nil, nil, err
	}
	const script2 = "SELECT COUNT(*) FROM %s %s"

	var count int64
	script := fmt.Sprintf(script2, table, wh.String())
	params := wh.Params()
	slog.Info(script, slog.Any("args", params))
	err = s.db.QueryRowContext(ctx, script, params...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` " +
		"FROM %s %s ORDER BY `id` %s %s"

	pg := page.Normalize()
	script = fmt.Sprintf(script1, table, wh.String(), messageOrder(filter), paginate(pg))
	slog.Info(script, slog.Any("args", params))
	rows, err := s.db.QueryContext(ctx, script, params...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
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

// func (s *Storage)

// QueryReceived implements [storage.StorageX].
func (s *Storage) QueryReceived(ctx context.Context, filter *entity.MessageFilter, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var recs []*Received
	h := NewWhereHelper()
	addMessageFilters(h, filter, true)

	table, err := s.quotedTable("received")
	if err != nil {
		return nil, nil, err
	}
	const script2 = "SELECT COUNT(*) FROM %s %s"

	var count int64
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(script2, table, h.String()), h.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group` " +
		"FROM %s %s ORDER BY `id` %s %s"

	pg := page.Normalize()
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(script1, table, h.String(), messageOrder(filter), paginate(pg)), h.Params()...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
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

func addMessageFilters(h *WhereHelper, filter *entity.MessageFilter, includeGroup bool) {
	if filter == nil {
		return
	}
	if filter.ID != nil {
		Add(h, "`id` = ?", *filter.ID)
	}
	AddIfNotZero(h, "`status` = ?", filter.Status)
	AddIfNotZero(h, "`topic` = ?", filter.Topic)
	if includeGroup {
		AddIfNotZero(h, "`group` = ?", filter.Group)
	}
	if filter.CreatedFrom != nil {
		Add(h, "`created_at` >= ?", *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		Add(h, "`created_at` <= ?", *filter.CreatedTo)
	}
	if pattern := filter.HeaderLikePattern(); pattern != "" {
		Add(h, "`headers` LIKE ? ESCAPE '!'", pattern)
	}
	if filter.Payload != "" {
		Add(h, "`payload` LIKE ?", "%"+filter.Payload+"%")
	}
}

func paginate(page entity.Pagination) string {
	var buff bytes.Buffer
	buff.WriteString(fmt.Sprintf("LIMIT %d ", page.PerPage))
	if page.Page > 1 {
		offset := (page.Page - 1) * page.PerPage
		buff.WriteString(fmt.Sprintf("OFFSET %d", offset))
	}
	return buff.String()
}
