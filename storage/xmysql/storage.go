package xmysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/internal/utility"
	"github.com/loopopen/gap/storage"
)

var mysqlIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Storage struct {
	gapOpts *ingap.Options
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
	o := s.gapOpts
	now := time.Now()

	var pubs []*Published
	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` FROM %s " +
		"WHERE `version` = ? " +
		"AND `status` = ? " +
		"AND `next_attempt_at` <= ? " +
		"AND (`lease_until` IS NULL OR `lease_until` <= ?) " +
		"AND `retries` < ? " +
		"LIMIT ? FOR UPDATE SKIP LOCKED"

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
	if err = s.failExhausted(ctx, tx, table, now); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(script1, table),
		o.Version,
		enum.StatusPending,
		now,
		now,
		o.MaxRetries,
		batchSize,
	)
	if err != nil {
		return nil, err
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
	params[1] = leaseUntil
	for i, p := range pubs {
		p.LeaseToken = leaseToken
		placeholders[i] = "?"
		params[i+2] = p.ID
		p.Retries++
	}
	script2 := "UPDATE " + table + " SET `retries` = `retries` + 1, `lease_token` = ?, `lease_until` = ? WHERE `id` IN (" + strings.Join(placeholders, ", ") + ")"
	_, err = tx.ExecContext(ctx, script2, params...)
	if err != nil {
		return nil, err
	}
	es, err = pubsToEntities(pubs)
	if err != nil {
		return nil, err
	}
	return es, nil
}

// ClaimReceivedBatch implements [storage.Storage].
func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int) (es []*entity.Envelope, err error) {
	o := s.gapOpts
	now := time.Now()

	var recs []*Received
	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group` FROM %s " +
		"WHERE `version` = ? " +
		"AND `status` = ? " +
		"AND `next_attempt_at` <= ? " +
		"AND (`lease_until` IS NULL OR `lease_until` <= ?) " +
		"AND `retries` < ? " +
		"LIMIT ? FOR UPDATE SKIP LOCKED"

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
	if err = s.failExhausted(ctx, tx, table, now); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(script1, table),
		o.Version,
		enum.StatusPending,
		now,
		now,
		o.MaxRetries,
		batchSize,
	)
	if err != nil {
		return nil, err
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
	params[1] = leaseUntil
	for i, r := range recs {
		r.LeaseToken = leaseToken
		placeholders[i] = "?"
		params[i+2] = r.ID
		r.Retries++
	}
	script2 := "UPDATE " + table + " SET `retries` = `retries` + 1, `lease_token` = ?, `lease_until` = ? WHERE `id` IN (" + strings.Join(placeholders, ", ") + ")"
	_, err = tx.ExecContext(ctx, script2, params...)
	if err != nil {
		return nil, err
	}
	es, err = recsToEntities(recs)
	if err != nil {
		return nil, err
	}
	return es, nil
}

func (s *Storage) failExhausted(ctx context.Context, tx *sql.Tx, table string, now time.Time) error {
	_, err := tx.ExecContext(ctx, failExhaustedSQL(table), enum.StatusFailed, s.gapOpts.Version, enum.StatusPending, now, s.gapOpts.MaxRetries, now)
	return err
}

func failExhaustedSQL(table string) string {
	return "UPDATE " + table + " SET `status` = ?, `lease_token` = NULL, `lease_until` = NULL " +
		"WHERE `version` = ? AND `status` = ? AND `next_attempt_at` <= ? AND `retries` >= ? AND (`lease_until` IS NULL OR `lease_until` <= ?)"
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
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`lease_token`,`lease_until`,`next_attempt_at`,`expired_at`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	xer := s.execer()
	_, err = xer.ExecContext(ctx, fmt.Sprintf(script, table),
		pub.ID,
		pub.CreatedAt,
		pub.Version,
		pub.Topic,
		pub.Status,
		string(pub.Headers),
		string(pub.Payload),
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
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`lease_token`,`lease_until`,`next_attempt_at`,`expired_at`,`group`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	_, err = s.execer().ExecContext(ctx, fmt.Sprintf(script, table),
		rec.ID,
		rec.CreatedAt,
		rec.Version,
		rec.Topic,
		rec.Status,
		string(rec.Headers),
		string(rec.Payload),
		rec.Retries,
		rec.LeaseToken, rec.LeaseUntil, rec.NextAttemptAt,
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
func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
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

func (s *Storage) updateStatus(ctx context.Context, table string, id uint, leaseToken string, status enum.Status) (sql.Result, error) {
	return s.execer().ExecContext(ctx, updateStatusSQL(table), status, id, enum.StatusPending, leaseToken)
}

func updateStatusSQL(table string) string {
	return "UPDATE " + table + " SET `status` = ?, `lease_token` = NULL, `lease_until` = NULL WHERE `id` = ? AND `status` = ? AND `lease_token` = ?"
}

func NewStorage(gapOpts *ingap.Options, opts *Options, db *sql.DB) *Storage {
	s := &Storage{
		gapOpts: gapOpts,
		opts:    opts,
		db:      db,
	}
	var _ storage.Storage = s
	return s
}

func (s *Storage) init() error {
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
	if err := s.ensureStatusColumn(context.Background(), publishedName, published); err != nil {
		return err
	}
	if err := s.ensureStatusColumn(context.Background(), receivedName, received); err != nil {
		return err
	}
	if err := s.ensureClaimIndex(publishedName, "idx_published_claim"); err != nil {
		return err
	}
	return s.ensureClaimIndex(receivedName, "idx_received_claim")
}

func (s *Storage) ensureStatusColumn(ctx context.Context, tableName, quotedTable string) error {
	var columnType string
	err := s.db.QueryRowContext(ctx,
		`SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'status'`,
		tableName,
	).Scan(&columnType)
	if err != nil {
		return err
	}
	if strings.EqualFold(columnType, statusColumnType) {
		return nil
	}
	if _, err := s.db.ExecContext(ctx,
		"UPDATE "+quotedTable+" SET `status` = ? WHERE `status` NOT IN (?, ?, ?)",
		enum.StatusPending, enum.StatusPending, enum.StatusSucceeded, enum.StatusFailed,
	); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "ALTER TABLE "+quotedTable+" MODIFY COLUMN `status` "+statusColumnType+" NOT NULL")
	return err
}

func (s *Storage) ensureClaimIndex(table, index string) error {
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
	quotedTable, err := quoteMySQLIdent(table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := quoteMySQLIdent(index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"CREATE INDEX %s ON %s (`version`, `status`, `next_attempt_at`, `lease_until`, `retries`)",
		quotedIndex, quotedTable,
	), nil
}

func (s *Storage) tableName(base string) (string, error) {
	name := s.opts.Schema + "_" + base
	if _, err := quoteMySQLIdent(name); err != nil {
		return "", fmt.Errorf("schema: %w", err)
	}
	return name, nil
}

func (s *Storage) quotedTable(base string) (string, error) {
	name, err := s.tableName(base)
	if err != nil {
		return "", err
	}
	return "`" + name + "`", nil
}

func quoteMySQLIdent(name string) (string, error) {
	if len(name) == 0 || len(name) > 64 || !mysqlIdentRE.MatchString(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return "`" + name + "`", nil
}

func isDuplicateIndex(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1061
}
