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
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/internal/utility"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/xsql"
)

type Storage struct {
	gapOpts *ingap.Options
	opts    *Options
	db      *xsql.DB
	tx      *xsql.Tx
}

func (s *Storage) execer() Execer {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

func (s *Storage) beginClaim(ctx context.Context) (tx *xsql.Tx, own bool, err error) {
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
		tx:      xsql.WrapTx(tx, logSQL(s.opts)),
	}
	return &newS, nil
}

// InTx reports whether the storage is bound to a transaction.
func (s *Storage) InTx() bool {
	return s.tx != nil
}

// ClaimPublishedBatch implements [storage.Storage].
func (s *Storage) ClaimPublishedBatch(ctx context.Context, batchSize int) (es []*entity.Envelope, err error) {
	return s.claimBatch(ctx, batchSize, "published", "")
}

// ClaimReceivedBatch implements [storage.Storage].
func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int, group string) (es []*entity.Envelope, err error) {
	return s.claimBatch(ctx, batchSize, "received", group)
}

func (s *Storage) claimBatch(ctx context.Context, batchSize int, base, group string) (es []*entity.Envelope, err error) {
	o := s.gapOpts
	now := time.Now()
	table, err := s.quotedTable(base)
	if err != nil {
		return nil, err
	}
	statusType, err := s.statusType()
	if err != nil {
		return nil, err
	}

	received := base == "received"

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
	var rows *sql.Rows
	if received {
		if err = s.failExhaustedReceived(ctx, tx, table, statusType, now, group); err != nil {
			return nil, err
		}
		rows, err = tx.QueryContext(ctx, claimReceivedSQL(table, statusType),
			o.Version,
			enum.StatusPending,
			now,
			now,
			o.MaxRetries,
			group,
			batchSize,
		)
	} else {
		if err = s.failExhausted(ctx, tx, table, statusType, now); err != nil {
			return nil, err
		}
		rows, err = tx.QueryContext(ctx, claimPublishedSQL(table, statusType),
			o.Version,
			enum.StatusPending,
			now,
			now,
			o.MaxRetries,
			batchSize,
		)
	}
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

	leaseToken := utility.GenFastToken(16)
	leaseUntil := now.Add(o.LeaseTTL())
	placeholders := make([]string, len(ids))
	params := make([]any, 0, len(ids)+2)
	params = append(params, leaseToken, leaseUntil)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		params = append(params, id)
	}
	for _, pub := range pubs {
		pub.LeaseToken = leaseToken
	}
	for _, rec := range recs {
		rec.LeaseToken = leaseToken
	}
	script2 := "UPDATE " + table + ` SET "retries" = "retries" + 1, "lease_token" = $1, "lease_until" = $2 WHERE "id" IN (` + strings.Join(placeholders, ", ") + ")"
	if _, err = tx.ExecContext(ctx, script2, params...); err != nil {
		return nil, err
	}
	if received {
		return recsToEntities(recs)
	}
	return pubsToEntities(pubs)
}

func (s *Storage) failExhausted(ctx context.Context, tx *xsql.Tx, table, statusType string, now time.Time) error {
	retentionSeconds := int64(s.gapOpts.Retention(enum.StatusFailed) / time.Second)
	_, err := tx.ExecContext(ctx, failExhaustedSQL(table, statusType), enum.StatusFailed, retentionSeconds, s.gapOpts.Version, enum.StatusPending, now, now, s.gapOpts.MaxRetries)
	return err
}

func (s *Storage) failExhaustedReceived(ctx context.Context, tx *xsql.Tx, table, statusType string, now time.Time, group string) error {
	retentionSeconds := int64(s.gapOpts.Retention(enum.StatusFailed) / time.Second)
	_, err := tx.ExecContext(ctx, failExhaustedReceivedSQL(table, statusType), enum.StatusFailed, retentionSeconds, s.gapOpts.Version, enum.StatusPending, now, now, s.gapOpts.MaxRetries, group)
	return err
}

func claimPublishedSQL(table, statusType string) string {
	return fmt.Sprintf(`
SELECT "id","created_at","version","topic","status","headers","payload","retries","expired_at"
FROM %s
WHERE "version" = $1
AND "status" = $2::%s
AND "next_attempt_at" <= $3
AND ("lease_until" IS NULL OR "lease_until" <= $4)
AND "retries" < $5
LIMIT $6 FOR UPDATE SKIP LOCKED`, table, statusType)
}

func claimReceivedSQL(table, statusType string) string {
	return fmt.Sprintf(`
SELECT "id","created_at","version","topic","status","headers","payload","retries","expired_at","group"
FROM %s
WHERE "version" = $1
AND "status" = $2::%s
AND "next_attempt_at" <= $3
AND ("lease_until" IS NULL OR "lease_until" <= $4)
AND "retries" < $5
AND "group" = $6
LIMIT $7 FOR UPDATE SKIP LOCKED`, table, statusType)
}

func failExhaustedSQL(table, statusType string) string {
	return fmt.Sprintf(`
UPDATE %s
SET "status" = $1::%s, "expired_at" = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second'), "lease_token" = NULL, "lease_until" = NULL
WHERE "version" = $3
AND "status" = $4::%s
AND "next_attempt_at" <= $5
AND ("lease_until" IS NULL OR "lease_until" <= $6)
AND "retries" >= $7`, table, statusType, statusType)
}

func failExhaustedReceivedSQL(table, statusType string) string {
	return fmt.Sprintf(`
UPDATE %s
SET "status" = $1::%s, "expired_at" = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second'), "lease_token" = NULL, "lease_until" = NULL
WHERE "version" = $3
AND "status" = $4::%s
AND "next_attempt_at" <= $5
AND ("lease_until" IS NULL OR "lease_until" <= $6)
AND "retries" >= $7
AND "group" = $8`, table, statusType, statusType)
}

// CreatePublished implements [storage.Storage].
func (s *Storage) CreatePublished(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}
	envelope.WithLease(s.gapOpts.LeaseTTL())
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
		`"id","created_at","version","topic","status","headers","payload","retries","lease_token","lease_until","next_attempt_at","expired_at"` +
		") VALUES($1, $2, $3, $4, $5::" + statusType + ", $6, $7, $8, $9, $10, $11, $12)"
	_, err = s.execer().ExecContext(ctx, script,
		pub.ID,
		pub.CreatedAt,
		pub.Version,
		pub.Topic,
		pub.Status,
		pub.Headers,
		pub.Payload,
		pub.Retries,
		pub.LeaseToken,
		pub.LeaseUntil,
		pub.NextAttemptAt,
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
	envelope.WithLease(s.gapOpts.LeaseTTL())
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
		`"id","created_at","version","topic","status","headers","payload","retries","lease_token","lease_until","next_attempt_at","expired_at","group"` +
		") VALUES($1, $2, $3, $4, $5::" + statusType + ", $6, $7, $8, $9, $10, $11, $12, $13)"
	_, err = s.execer().ExecContext(ctx, script,
		rec.ID,
		rec.CreatedAt,
		rec.Version,
		rec.Topic,
		rec.Status,
		rec.Headers,
		rec.Payload,
		rec.Retries,
		rec.LeaseToken,
		rec.LeaseUntil,
		rec.NextAttemptAt,
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
func (s *Storage) UpdateStatusPublished(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, "published", id, leaseToken)
	}
	return s.updateStatus(ctx, "published", id, leaseToken, status)
}

// UpdateStatusReceived implements [storage.Storage].
func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, "received", id, leaseToken)
	}
	return s.updateStatus(ctx, "received", id, leaseToken, status)
}

func (s *Storage) updateStatus(ctx context.Context, base string, id uint, leaseToken string, status enum.Status) error {
	table, err := s.quotedTable(base)
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	retentionSeconds := int64(s.gapOpts.Retention(status) / time.Second)
	result, err := s.execer().ExecContext(ctx, updateStatusSQL(table, statusType), status, retentionSeconds, id, enum.StatusPending, leaseToken)
	if err != nil {
		return err
	}
	if n, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if n == 0 {
		return storage.ErrLeaseLost
	}
	if id != 0 {
		slog.Debug("updated status", slog.String("table", base), slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func updateStatusSQL(table, statusType string) string {
	return "UPDATE " + table + ` SET "status" = $1::` + statusType + `, "expired_at" = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 second'), "lease_token" = NULL, "lease_until" = NULL WHERE "id" = $3 AND "status" = $4::` + statusType + ` AND "lease_token" = $5`
}

// DeleteExpiredPublishedBatch deletes one batch of expired terminal published messages.
func (s *Storage) DeleteExpiredPublishedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, "published", batchSize)
}

// DeleteExpiredReceivedBatch deletes one batch of expired terminal received messages.
func (s *Storage) DeleteExpiredReceivedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, "received", batchSize)
}

func (s *Storage) deleteExpiredBatch(ctx context.Context, base string, batchSize int) (int64, error) {
	table, err := s.quotedTable(base)
	if err != nil {
		return 0, err
	}
	statusType, err := s.statusType()
	if err != nil {
		return 0, err
	}
	result, err := s.execer().ExecContext(ctx, deleteExpiredSQL(table, statusType), enum.StatusSucceeded, enum.StatusFailed, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func deleteExpiredSQL(table, statusType string) string {
	return `WITH expired AS (` +
		`SELECT "id" FROM ` + table + ` WHERE "expired_at" <= CURRENT_TIMESTAMP ` +
		`AND "status" IN ($1::` + statusType + `, $2::` + statusType + `) ` +
		`ORDER BY "expired_at", "id" LIMIT $3 FOR UPDATE SKIP LOCKED` +
		`) DELETE FROM ` + table + ` AS target USING expired WHERE target."id" = expired."id"`
}

func (s *Storage) scheduleRetry(ctx context.Context, base string, id uint, leaseToken string) error {
	table, err := s.quotedTable(base)
	if err != nil {
		return err
	}
	statusType, err := s.statusType()
	if err != nil {
		return err
	}
	nextAttemptAt := time.Now().Add(s.gapOpts.RetryInterval())
	result, err := s.execer().ExecContext(ctx, retryAttemptSQL(table, statusType), nextAttemptAt, id, enum.StatusPending, leaseToken)
	if err != nil {
		return err
	}
	if n, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if n == 0 {
		return storage.ErrLeaseLost
	}
	if id != 0 {
		slog.Debug("scheduled retry", slog.String("table", base), slog.Any("id", id), slog.Time("next_attempt_at", nextAttemptAt))
	}
	return nil
}

func retryAttemptSQL(table, statusType string) string {
	return "UPDATE " + table + ` SET "lease_token" = NULL, "lease_until" = NULL, "next_attempt_at" = $1 WHERE "id" = $2 AND "status" = $3::` + statusType + ` AND "lease_token" = $4`
}

func logSQL(opts *Options) bool {
	return opts != nil && opts.Log
}

// NewStorage creates a storage adapter.
func NewStorage(gapOpts *ingap.Options, opts *Options, db *sql.DB) *Storage {
	s := &Storage{
		gapOpts: gapOpts,
		opts:    opts,
		db:      xsql.WrapDB(db, logSQL(opts)),
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
	if err = s.migrateStatusType(context.Background(), []string{published, received}, statusType); err != nil {
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
	if _, err = s.db.Exec(receivedIndex); err != nil {
		return err
	}
	publishedCleanupIndex, err := cleanupIndexSQL(published, "idx_published_cleanup")
	if err != nil {
		return err
	}
	receivedCleanupIndex, err := cleanupIndexSQL(received, "idx_received_cleanup")
	if err != nil {
		return err
	}
	if _, err = s.db.Exec(publishedCleanupIndex); err != nil {
		return err
	}
	_, err = s.db.Exec(receivedCleanupIndex)
	return err
}

func (s *Storage) migrateStatusType(ctx context.Context, tables []string, statusType string) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.opts.Schema+".status_enum"); err != nil {
		return err
	}
	var unexpected int
	err = tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM pg_enum e
JOIN pg_type t ON t.oid = e.enumtypid
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
  AND t.typname = 'status_enum'
  AND e.enumlabel NOT IN ('Pending', 'Succeeded', 'Failed')`, s.opts.Schema).Scan(&unexpected)
	if err != nil {
		return err
	}
	if unexpected == 0 {
		return tx.Commit()
	}

	for _, table := range tables {
		if _, err = tx.ExecContext(ctx,
			"UPDATE "+table+` SET "status" = $1::`+statusType+` WHERE "status"::text NOT IN ($2, $3, $4)`,
			enum.StatusPending, enum.StatusPending, enum.StatusSucceeded, enum.StatusFailed,
		); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx,
			"ALTER TABLE "+table+` ALTER COLUMN "status" TYPE text USING "status"::text`,
		); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "DROP TYPE "+statusType); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TYPE "+statusType+" AS ENUM ('Pending', 'Succeeded', 'Failed')"); err != nil {
		return err
	}
	for _, table := range tables {
		if _, err = tx.ExecContext(ctx,
			"ALTER TABLE "+table+` ALTER COLUMN "status" TYPE `+statusType+` USING "status"::`+statusType,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
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
