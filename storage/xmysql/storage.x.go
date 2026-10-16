package xmysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
)

var _ storage.StorageX = (*Storage)(nil)

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
func (s *Storage) QueryPublished(ctx context.Context, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var pubs []*Published
	wh := NewWhereHelper()
	AddIfNotZero(wh, "`status` = ?", status)
	AddIfNotZero(wh, "`topic` = ?", topic)

	table, err := s.quotedTable("published")
	if err != nil {
		return nil, nil, err
	}
	const script2 = "SELECT COUNT(*) FROM %s %s"

	var count int64
	script := fmt.Sprintf(script2, table, wh.String())
	slog.Info(script)
	err = s.db.QueryRowContext(ctx, script, wh.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` " +
		"FROM %s %s ORDER BY `id` %s"

	pg := page.Normalize()
	script = fmt.Sprintf(script1, table, wh.String(), paginate(pg))
	slog.Info(script)
	rows, err := s.db.QueryContext(ctx, script, wh.Params()...)
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
func (s *Storage) QueryReceived(ctx context.Context, status enum.Status, topic string, group string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var recs []*Received
	h := NewWhereHelper()
	AddIfNotZero(h, "`status` = ?", status)
	AddIfNotZero(h, "`topic` = ?", topic)
	AddIfNotZero(h, "`group` = ?", group)

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
		"FROM %s %s ORDER BY `id` %s"

	pg := page.Normalize()
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(script1, table, h.String(), paginate(pg)), h.Params()...)
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

func paginate(page entity.Pagination) string {
	var buff bytes.Buffer
	buff.WriteString(fmt.Sprintf("LIMIT %d ", page.PerPage))
	if page.Page > 1 {
		offset := (page.Page - 1) * page.PerPage
		buff.WriteString(fmt.Sprintf("OFFSET %d", offset))
	}
	return buff.String()
}
