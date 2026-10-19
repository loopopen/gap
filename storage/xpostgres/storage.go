package xpostgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/storage"
)

type Storage struct {
	gapOpts *gap.Options
	opts    *Options
	db      *sql.DB
	tx      *sql.Tx
}

func (s *Storage) execer() Execer {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

func (s *Storage) beginClaim(ctx context.Context) (tx *sql.Tx, own bool, err error) {
	if s.tx != nil {
		return s.tx, false, nil
	}
	tx, err = s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	return tx, true, nil
}

// Bind implements [storage.Storage].
func (s *Storage) Bind(txer txer.Txer) (storage.Storage, error) {
	tx, ok := txer.Tx().(*sql.Tx)
	if !ok {
		return nil, errx.ErrInvalidSQLTx
	}
	newS := Storage{
		gapOpts: s.gapOpts,
		opts:    s.opts,
		db:      s.db,
		tx:      tx,
	}
	return &newS, nil
}

func (s *Storage) InTx() bool {
	return s.tx != nil
}

// ClaimPublishedBatch implements [storage.Storage].
func (s *Storage) ClaimPublishedBatch(ctx context.Context, batchSize int) (es []*entity.Envelope, err error) {
	return s.claimBatch(ctx, batchSize, "published", false)
}

// ClaimReceivedBatch implements [storage.Storage].
func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int) (es []*entity.Envelope, err error) {
	return s.claimBatch(ctx, batchSize, "received", true)
}

func (s *Storage) claimBatch(ctx context.Context, batchSize int, base string, received bool) (es []*entity.Envelope, err error) {
	o := s.gapOpts
	minutesAgo := time.Now().Add(-o.Lookback())
	table, err := s.quotedTable(base)
	if err != nil {
		return nil, err
	}
	statusType, err := s.statusType()
	if err != nil {
		return nil, err
	}

	columns := `"id","created_at","version","topic","status","headers","payload","retries","expired_at"`
	if received {
		columns += `,"group"`
	}
	script1 := "SELECT " + columns + " FROM " + table + " " +
		`WHERE "version" = $1 ` +
		`AND "status" IN ($2::` + statusType + `, $3::` + statusType + `) ` +
		`AND "retries" < $4 ` +
		`AND "created_at" < $5 ` +
		`LIMIT $6 FOR UPDATE SKIP LOCKED`

	tx, own, err := s.beginClaim(ctx)
	if err != nil {
		return nil, err
	}
	if own {
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}()
	}
	rows, err := tx.QueryContext(ctx, script1,
		o.Version,
		enum.StatusPending,
		enum.StatusFailed,
		o.MaxRetries,
		minutesAgo,
		batchSize,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pubs []*Published
	var recs []*Received
	for rows.Next() {
		if received {
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
				return nil, err
			}
			recs = append(recs, &rec)
			continue
		}
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
			return nil, err
		}
		pubs = append(pubs, &pub)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	ids := make([]any, 0)
	if received {
		if len(recs) == 0 {
			return nil, nil
		}
		for _, rec := range recs {
			ids = append(ids, rec.ID)
			rec.Retries++
		}
	} else {
		if len(pubs) == 0 {
			return nil, nil
		}
		for _, pub := range pubs {
			ids = append(ids, pub.ID)
			pub.Retries++
		}
	}

	placeholders := make([]string, len(ids))
	params := make([]any, 0, len(ids)+1)
	params = append(params, enum.StatusProcessing)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		params = append(params, id)
	}
	script2 := "UPDATE " + table + ` SET "status" = $1::` + statusType + `, "retries" = "retries" + 1 WHERE "id" IN (` + strings.Join(placeholders, ", ") + ")"
	if _, err = tx.ExecContext(ctx, script2, params...); err != nil {
		return nil, err
	}
	if received {
		return recsToEntities(recs)
	}
	return pubsToEntities(pubs)
}

// CreatePublished implements [storage.Storage].
func (s *Storage) CreatePublished(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}
	pub := new(Published).FromEntity(envelope)
	if pub.err != nil {
		return pub.err
	}
	table, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	script := "INSERT INTO " + table + " (" +
		`"id","created_at","version","topic","status","headers","payload","retries","expired_at"` +
		") VALUES($1, $2, $3, $4, $5::" + statusType + ", $6, $7, $8, $9)"
	_, err = s.execer().ExecContext(ctx, script,
		pub.ID,
		pub.CreatedAt,
		pub.Version,
		pub.Topic,
		pub.Status,
		pub.Headers,
		pub.Payload,
		pub.Retries,
		pub.ExpiredAt,
	)
	if err != nil {
		return err
	}
	envelope.Log().Debug("created published envelope")
	return nil
}

// CreateReceived implements [storage.Storage].
func (s *Storage) CreateReceived(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}
	rec := new(Received).FromEntity(envelope)
	if rec.err != nil {
		return rec.err
	}
	table, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	script := "INSERT INTO " + table + " (" +
		`"id","created_at","version","topic","status","headers","payload","retries","expired_at","group"` +
		") VALUES($1, $2, $3, $4, $5::" + statusType + ", $6, $7, $8, $9, $10)"
	_, err = s.execer().ExecContext(ctx, script,
		rec.ID,
		rec.CreatedAt,
		rec.Version,
		rec.Topic,
		rec.Status,
		rec.Headers,
		rec.Payload,
		rec.Retries,
		rec.ExpiredAt,
		rec.Group,
	)
	if err != nil {
		return err
	}
	envelope.Log().Debug("created received envelope")
	return nil
}

// UpdateStatusPublished implements [storage.Storage].
func (s *Storage) UpdateStatusPublished(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	return s.updateStatus(ctx, "published", id, src, status)
}

// UpdateStatusReceived implements [storage.Storage].
func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	return s.updateStatus(ctx, "received", id, src, status)
}

func (s *Storage) updateStatus(ctx context.Context, base string, id uint, src enum.Status, status enum.Status) error {
	table, err := s.quotedTable(base)
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	script := "UPDATE " + table + ` SET "status" = $1::` + statusType + ` WHERE `
	args := []any{status}
	if id != 0 {
		script += `"id" = $2`
		args = append(args, id)
	} else {
		script += `"version" = $2 AND "status" = $3::` + statusType
		args = append(args, s.gapOpts.Version, src)
	}
	if _, err = s.execer().ExecContext(ctx, script, args...); err != nil {
		return err
	}
	if id != 0 {
		slog.Debug("updated status", slog.String("table", base), slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func NewStorage(gapOpts *gap.Options, db *sql.DB) *Storage {
	sp := gapOpts.StorageOptions
	s := &Storage{
		gapOpts: gapOpts,
		opts:    sp.(*Options),
		db:      db,
	}
	var _ storage.Storage = s
	return s
}

func (s *Storage) init() error {
	quotedSchema, err := quoteIdent(s.opts.Schema)
	if err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if _, err = s.db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quotedSchema)); err != nil {
		return err
	}
	if _, err = s.db.Exec(statusTypeSQL(s.opts.Schema, quotedSchema)); err != nil {
		return err
	}

	published, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	received, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	if _, err = s.db.Exec(publishedCreateSQL(published, statusType)); err != nil {
		return err
	}
	if _, err = s.db.Exec(receivedCreateSQL(received, statusType)); err != nil {
		return err
	}

	publishedIndex, err := claimIndexSQL(published, "idx_published_claim")
	if err != nil {
		return err
	}
	receivedIndex, err := claimIndexSQL(received, "idx_received_claim")
	if err != nil {
		return err
	}
	if _, err = s.db.Exec(publishedIndex); err != nil {
		return err
	}
	_, err = s.db.Exec(receivedIndex)
	return err
}

func (s *Storage) statusType() (string, error) {
	schema, err := quoteIdent(s.opts.Schema)
	if err != nil {
		return "", fmt.Errorf("schema: %w", err)
	}
	return schema + `."status_enum"`, nil
}

func (s *Storage) quotedTable(base string) (string, error) {
	schema, err := quoteIdent(s.opts.Schema)
	if err != nil {
		return "", fmt.Errorf("schema: %w", err)
	}
	table, err := quoteIdent(base)
	if err != nil {
		return "", err
	}
	return schema + "." + table, nil
}
