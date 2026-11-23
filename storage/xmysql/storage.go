package xmysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/internal/utility"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/internal/xsql"
	dbmodel "github.com/loopopen/gap/storage/xmysql/internal/model"
)

type storageAdapter struct {
	gapOpts *ingap.Options
	opts    *Options
	db      *xsql.DB
	tx      *xsql.Tx

	pool      *xsql.Pool
	closeOnce sync.Once
}

func (s *storageAdapter) execer() xsql.Execer {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

func (s *storageAdapter) beginClaim(ctx context.Context) (tx *xsql.Tx, own bool, err error) {
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
func (s *storageAdapter) Bind(txer txer.Txer) (storage.Storage, error) {
	tx, ok := txer.Tx().(*sql.Tx)
	if !ok {
		return nil, errx.ErrInvalidSQLTx
	}
	newS := storageAdapter{
		gapOpts: s.gapOpts,
		opts:    s.opts,
		db:      s.db,
		tx:      xsql.WrapTx(tx, logSQL(s.opts)),
	}
	return &newS, nil
}

// InTx reports whether the storage is bound to a transaction.
func (s *storageAdapter) InTx() bool {
	return s.tx != nil
}

// ClaimPublishedBatch implements [storage.Storage].
func (s *storageAdapter) ClaimPublishedBatch(ctx context.Context, batchSize int, brokerName string) (es []*entity.Envelope, err error) {
	o := s.gapOpts
	now := time.Now()

	var pubs []*dbmodel.Published
	table, err := s.quotedTable("published")
	if err != nil {
		return nil, err
	}
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
	if err = s.failExhaustedPublished(ctx, tx, table, brokerName); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, claimPublishedSQL(table),
		claimFilterArgs(o.Version, enum.StatusPending, brokerName, o.MaxRetries, batchSize)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pub dbmodel.Published
		err = rows.Scan(
			&pub.ID,
			&pub.CreatedAt,
			&pub.Version,
			&pub.Topic,
			&pub.BrokerName,
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	c := len(pubs)
	if c == 0 {
		return nil, nil
	}
	leaseToken := utility.GenFastToken(16)
	leaseUntil := now.Add(o.LeaseTTL())
	placeholders := make([]string, c)
	params := make([]any, c+2)
	params[0] = leaseToken
	params[1] = int64(o.LeaseTTL() / time.Second)
	for i, p := range pubs {
		p.LeaseToken = leaseToken
		p.LeaseUntil = sql.Null[time.Time]{V: leaseUntil, Valid: true}
		placeholders[i] = "?"
		params[i+2] = p.ID
		p.Retries++
	}
	script2 := "UPDATE " + table + " SET `retries` = `retries` + 1, `lease_token` = ?, `lease_until` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)) WHERE `id` IN (" + strings.Join(placeholders, ", ") + ")"
	_, err = tx.ExecContext(ctx, script2, params...)
	if err != nil {
		return nil, err
	}
	es, err = dbmodel.PublishedToEntities(pubs)
	if err != nil {
		return nil, err
	}
	return es, nil
}

// ClaimReceivedBatch implements [storage.Storage].
func (s *storageAdapter) ClaimReceivedBatch(ctx context.Context, batchSize int, group, brokerName string) (es []*entity.Envelope, err error) {
	o := s.gapOpts
	now := time.Now()

	var recs []*dbmodel.Received
	table, err := s.quotedTable("received")
	if err != nil {
		return nil, err
	}
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
	if err = s.failExhaustedReceived(ctx, tx, table, group, brokerName); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, claimReceivedSQL(table),
		claimFilterArgs(o.Version, enum.StatusPending, brokerName, o.MaxRetries, batchSize, group)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var rec dbmodel.Received
		err = rows.Scan(
			&rec.ID,
			&rec.CreatedAt,
			&rec.Version,
			&rec.Topic,
			&rec.BrokerName,
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
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	c := len(recs)
	if c == 0 {
		return nil, nil
	}
	leaseToken := utility.GenFastToken(16)
	leaseUntil := now.Add(o.LeaseTTL())
	placeholders := make([]string, c)
	params := make([]any, c+2)
	params[0] = leaseToken
	params[1] = int64(o.LeaseTTL() / time.Second)
	for i, r := range recs {
		r.LeaseToken = leaseToken
		r.LeaseUntil = sql.Null[time.Time]{V: leaseUntil, Valid: true}
		placeholders[i] = "?"
		params[i+2] = r.ID
		r.Retries++
	}
	script2 := "UPDATE " + table + " SET `retries` = `retries` + 1, `lease_token` = ?, `lease_until` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)) WHERE `id` IN (" + strings.Join(placeholders, ", ") + ")"
	_, err = tx.ExecContext(ctx, script2, params...)
	if err != nil {
		return nil, err
	}
	es, err = dbmodel.ReceivedToEntities(recs)
	if err != nil {
		return nil, err
	}
	return es, nil
}

func (s *storageAdapter) failExhaustedPublished(ctx context.Context, tx *xsql.Tx, table, brokerName string) error {
	retentionSeconds := int64(s.gapOpts.Retention(enum.StatusFailed) / time.Second)
	_, err := tx.ExecContext(ctx, failExhaustedPublishedSQL(table),
		exhaustedArgs(enum.StatusFailed, retentionSeconds, s.gapOpts.Version, brokerName, s.gapOpts.MaxRetries)...)
	return err
}

func (s *storageAdapter) failExhaustedReceived(ctx context.Context, tx *xsql.Tx, table, group, brokerName string) error {
	retentionSeconds := int64(s.gapOpts.Retention(enum.StatusFailed) / time.Second)
	_, err := tx.ExecContext(ctx, failExhaustedReceivedSQL(table),
		exhaustedArgs(enum.StatusFailed, retentionSeconds, s.gapOpts.Version, brokerName, s.gapOpts.MaxRetries, group)...)
	return err
}

func claimPublishedSQL(table string) string {
	return fmt.Sprintf(
		"SELECT `id`,`created_at`,`version`,`topic`,`broker_name`,`status`,`headers`,`payload`,`retries`,`expired_at` FROM %s "+
			"WHERE `version` = ? "+
			"AND `status` = ? "+
			"AND `broker_name` = ? "+
			"AND `next_attempt_at` <= CURRENT_TIMESTAMP(3) "+
			"AND (`lease_until` IS NULL OR `lease_until` <= CURRENT_TIMESTAMP(3)) "+
			"AND `retries` < ? "+
			"LIMIT ? FOR UPDATE SKIP LOCKED",
		table,
	)
}

func claimReceivedSQL(table string) string {
	return fmt.Sprintf(
		"SELECT `id`,`created_at`,`version`,`topic`,`broker_name`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group` FROM %s "+
			"WHERE `version` = ? "+
			"AND `status` = ? "+
			"AND `broker_name` = ? "+
			"AND `next_attempt_at` <= CURRENT_TIMESTAMP(3) "+
			"AND (`lease_until` IS NULL OR `lease_until` <= CURRENT_TIMESTAMP(3)) "+
			"AND `retries` < ? "+
			"AND `group` = ? "+
			"LIMIT ? FOR UPDATE SKIP LOCKED",
		table,
	)
}

func failExhaustedPublishedSQL(table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET `status` = ?, `expired_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)), `lease_token` = NULL, `lease_until` = NULL "+
			"WHERE `version` = ? "+
			"AND `status` = ? "+
			"AND `broker_name` = ? "+
			"AND `next_attempt_at` <= CURRENT_TIMESTAMP(3) "+
			"AND (`lease_until` IS NULL OR `lease_until` <= CURRENT_TIMESTAMP(3)) "+
			"AND `retries` >= ?",
		table,
	)
}

func failExhaustedReceivedSQL(table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET `status` = ?, `expired_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)), `lease_token` = NULL, `lease_until` = NULL "+
			"WHERE `version` = ? "+
			"AND `status` = ? "+
			"AND `broker_name` = ? "+
			"AND `next_attempt_at` <= CURRENT_TIMESTAMP(3) "+
			"AND (`lease_until` IS NULL OR `lease_until` <= CURRENT_TIMESTAMP(3)) "+
			"AND `retries` >= ? "+
			"AND `group` = ?",
		table,
	)
}

func claimFilterArgs(version string, status enum.Status, brokerName string, maxRetries, batchSize int, extra ...any) []any {
	args := make([]any, 0, 4+len(extra))
	args = append(args, version, status, brokerName, maxRetries)
	args = append(args, extra...)
	args = append(args, batchSize)
	return args
}

func exhaustedArgs(failed enum.Status, retention int64, version, brokerName string, maxRetries int, extra ...any) []any {
	args := make([]any, 0, 5+len(extra))
	args = append(args, failed, retention, version, enum.StatusPending, brokerName, maxRetries)
	return append(args, extra...)
}

// CreatePublished implements [storage.Storage].
func (s *storageAdapter) CreatePublished(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}

	envelope.WithLease(s.gapOpts.LeaseTTL())
	pub := new(dbmodel.Published).FromEntity(envelope)
	if pub.Err() != nil {
		return pub.Err()
	}
	table, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`broker_name`,`status`,`headers`,`payload`,`retries`,`lease_token`,`lease_until`,`next_attempt_at`,`expired_at`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)), CURRENT_TIMESTAMP(3), ?)"
	xer := s.execer()
	_, err = xer.ExecContext(ctx, fmt.Sprintf(script, table),
		pub.ID,
		pub.CreatedAt,
		pub.Version,
		pub.Topic,
		pub.BrokerName,
		pub.Status,
		string(pub.Headers),
		string(pub.Payload),
		pub.Retries,
		pub.LeaseToken,
		int64(s.gapOpts.LeaseTTL()/time.Second),
		pub.ExpiredAt,
	)
	if err != nil {
		return err
	}

	envelope.Log().Debug("created published envelope")
	return nil
}

// CreateReceived implements [storage.Storage].
func (s *storageAdapter) CreateReceived(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}

	envelope.WithLease(s.gapOpts.LeaseTTL())
	rec := new(dbmodel.Received).FromEntity(envelope)
	if rec.Err() != nil {
		return rec.Err()
	}
	table, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`broker_name`,`status`,`headers`,`payload`,`retries`,`lease_token`,`lease_until`,`next_attempt_at`,`expired_at`,`group`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)), CURRENT_TIMESTAMP(3), ?, ?)"
	_, err = s.execer().ExecContext(ctx, fmt.Sprintf(script, table),
		rec.ID,
		rec.CreatedAt,
		rec.Version,
		rec.Topic,
		rec.BrokerName,
		rec.Status,
		string(rec.Headers),
		string(rec.Payload),
		rec.Retries,
		rec.LeaseToken,
		int64(s.gapOpts.LeaseTTL()/time.Second),
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
func (s *storageAdapter) UpdateStatusPublished(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, "published", id, leaseToken)
	}
	table, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	result, err := s.updateStatus(ctx, table, id, leaseToken, status)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return storage.ErrLeaseLost
	}

	if id != 0 {
		slog.Debug("updated status published", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

// UpdateStatusReceived implements [storage.Storage].
func (s *storageAdapter) UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, "received", id, leaseToken)
	}
	table, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	result, err := s.updateStatus(ctx, table, id, leaseToken, status)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return storage.ErrLeaseLost
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func (s *storageAdapter) updateStatus(ctx context.Context, table string, id uint, leaseToken string, status enum.Status) (sql.Result, error) {
	retentionSeconds := int64(s.gapOpts.Retention(status) / time.Second)
	return s.execer().ExecContext(ctx, updateStatusSQL(table), status, retentionSeconds, id, enum.StatusPending, leaseToken)
}

func (s *storageAdapter) scheduleRetry(ctx context.Context, base string, id uint, leaseToken string) error {
	table, err := s.quotedTable(base)
	if err != nil {
		return err
	}
	retryAfter := s.gapOpts.RetryInterval()
	result, err := s.execer().ExecContext(ctx, retryAttemptSQL(table), int64(retryAfter/time.Second), id, enum.StatusPending, leaseToken)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return storage.ErrLeaseLost
	}
	if id != 0 {
		slog.Debug("scheduled retry", slog.String("table", base), slog.Any("id", id), slog.Duration("retry_after", retryAfter))
	}
	return nil
}

func retryAttemptSQL(table string) string {
	return "UPDATE " + table + " SET `lease_token` = NULL, `lease_until` = NULL, `next_attempt_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)) WHERE `id` = ? AND `status` = ? AND `lease_token` = ?"
}

func updateStatusSQL(table string) string {
	return "UPDATE " + table + " SET `status` = ?, `expired_at` = TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3)), `lease_token` = NULL, `lease_until` = NULL WHERE `id` = ? AND `status` = ? AND `lease_token` = ?"
}

// DeleteExpiredPublishedBatch deletes one batch of expired terminal published messages.
func (s *storageAdapter) DeleteExpiredPublishedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, "published", batchSize)
}

// DeleteExpiredReceivedBatch deletes one batch of expired terminal received messages.
func (s *storageAdapter) DeleteExpiredReceivedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, "received", batchSize)
}

func (s *storageAdapter) deleteExpiredBatch(ctx context.Context, base string, batchSize int) (int64, error) {
	table, err := s.quotedTable(base)
	if err != nil {
		return 0, err
	}
	result, err := s.execer().ExecContext(ctx, deleteExpiredSQL(table), enum.StatusSucceeded, enum.StatusFailed, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func deleteExpiredSQL(table string) string {
	return "DELETE target FROM " + table + " AS target JOIN (" +
		"SELECT `id` FROM " + table + " WHERE `expired_at` <= CURRENT_TIMESTAMP(3) " +
		"AND `status` IN (?, ?) ORDER BY `expired_at`, `id` LIMIT ? FOR UPDATE SKIP LOCKED" +
		") AS expired ON target.`id` = expired.`id`"
}

func logSQL(opts *Options) bool {
	return opts != nil && opts.Log
}

// newStorage creates a storage adapter.
func newStorage(gapOpts *ingap.Options, opts *Options, db *sql.DB) *storageAdapter {
	s := &storageAdapter{
		gapOpts: gapOpts,
		opts:    opts,
		db:      xsql.WrapDB(db, logSQL(opts)),
	}
	var _ storage.Storage = s
	return s
}

func (s *storageAdapter) init() error {
	published, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	received, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	if _, err = s.db.Exec(publishedCreateSQL(published)); err != nil {
		return err
	}
	if _, err = s.db.Exec(receivedCreateSQL(received)); err != nil {
		return err
	}

	publishedName, err := s.tableName("published")
	if err != nil {
		return err
	}
	receivedName, err := s.tableName("received")
	if err != nil {
		return err
	}
	if err := s.ensureClaimIndex(publishedName, "idx_published_claim"); err != nil {
		return err
	}
	if err := s.ensureClaimIndex(receivedName, "idx_received_claim"); err != nil {
		return err
	}
	if err := s.ensureCleanupIndex(publishedName, "idx_published_cleanup"); err != nil {
		return err
	}
	return s.ensureCleanupIndex(receivedName, "idx_received_cleanup")
}

func (s *storageAdapter) ensureClaimIndex(table, index string) error {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		table, index,
	).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	sql, err := claimIndexSQL(table, index)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(sql)
	if isDuplicateIndex(err) {
		return nil
	}
	return err
}

func claimIndexSQL(table, index string) (string, error) {
	quotedTable, err := xsql.Quote(xsql.MySQL, table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := xsql.Quote(xsql.MySQL, index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"CREATE INDEX %s ON %s (`version`, `status`, `broker_name`, `next_attempt_at`, `lease_until`, `retries`)",
		quotedIndex, quotedTable,
	), nil
}

func (s *storageAdapter) ensureCleanupIndex(table, index string) error {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		table, index,
	).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	sql, err := cleanupIndexSQL(table, index)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(sql)
	if isDuplicateIndex(err) {
		return nil
	}
	return err
}

func cleanupIndexSQL(table, index string) (string, error) {
	quotedTable, err := xsql.Quote(xsql.MySQL, table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := xsql.Quote(xsql.MySQL, index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (`expired_at`, `id`)", quotedIndex, quotedTable), nil
}

func (s *storageAdapter) tableName(base string) (string, error) {
	name := s.opts.Schema + "_" + base
	if _, err := xsql.Quote(xsql.MySQL, name); err != nil {
		return "", fmt.Errorf("schema: %w", err)
	}
	return name, nil
}

func (s *storageAdapter) quotedTable(base string) (string, error) {
	name, err := s.tableName(base)
	if err != nil {
		return "", err
	}
	return xsql.Quote(xsql.MySQL, name)
}

func isDuplicateIndex(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1061
}
