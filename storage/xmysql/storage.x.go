package xmysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/storage"
)

var _ storage.StorageX = (*Storage)(nil)

func (s *Storage) GetPublishedByID(ctx context.Context, id uint) (*entity.Envelope, error) {
	const script = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` " +
		"FROM `%s_published` WHERE `id` = ?"
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(script, s.opts.Schema), id)
	if row == nil {
		return nil, nil
	}
	var pub Published
	err := row.Scan(
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
	return pub.ToEntity(), nil
}

// QueryPublished implements [storage.StorageX].
func (s *Storage) QueryPublished(ctx context.Context, status enum.Status, topic string, page *entity.Pagination) ([]*entity.Envelope, *entity.Pagination, error) {
	var pubs []*Published
	wh := NewWhereHelper()
	AddIfNotZero(wh, "`status` = ?", status)
	AddIfNotZero(wh, "`topic` = ?", topic)

	const script2 = "SELECT COUNT(*) FROM `%s_published` %s"

	var count int64
	script := fmt.Sprintf(script2, s.opts.Schema, wh.String())
	slog.Info(script)
	err := s.db.QueryRowContext(ctx, script, wh.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` " +
		"FROM `%s_published` %s %s"

	pg := page.Normalize()
	script = fmt.Sprintf(script1, s.opts.Schema, wh.String(), paginate(pg))
	slog.Info(script)
	rows, err := s.db.QueryContext(ctx, script, wh.Params()...)
	if err != nil {
		return nil, nil, err
	}
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

	var es []*entity.Envelope
	for _, p := range pubs {
		es = append(es, p.ToEntity())
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

	const script2 = "SELECT COUNT(*) FROM `%s_received` %s"

	var count int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(script2, s.opts.Schema, h.String()), h.Params()...).Scan(&count)
	if err != nil {
		return nil, nil, err
	}

	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group` " +
		"FROM `%s_received` %s %s"

	pg := page.Normalize()
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(script1, s.opts.Schema, h.String(), paginate(pg)), h.Params()...)
	if err != nil {
		return nil, nil, err
	}
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

	var es []*entity.Envelope
	for _, r := range recs {
		es = append(es, r.ToEntity())
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
