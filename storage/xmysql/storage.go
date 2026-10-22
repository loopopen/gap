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
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/storage"
)

var mysqlIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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
	o := s.gapOpts
	minutesAgo := time.Now().Add(-o.Lookback())

	var pubs []*Published
	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at` FROM %s " +
		"WHERE `version` = ? " +
		"AND `status` IN (?, ?) " +
		"AND `retries` < ? " +
		"AND `created_at` < ? " +
		"LIMIT ? FOR UPDATE SKIP LOCKED"
	const script2 = "UPDATE %s SET `status` = ?, `retries` = `retries` + 1 WHERE `id` IN (%s)"

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
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(script1, table),
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
	placeholders := make([]string, c)
	params := make([]any, c+1)
	params[0] = enum.StatusProcessing
	for i, p := range pubs {
		placeholders[i] = "?"
		params[i+1] = p.ID
		p.Retries++
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(script2, table, strings.Join(placeholders, ", ")), params...)
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
	minutesAgo := time.Now().Add(-o.Lookback())

	var recs []*Received
	const script1 = "SELECT `id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group` FROM %s " +
		"WHERE `version` = ? " +
		"AND `status` IN (?, ?) " +
		"AND `retries` < ? " +
		"AND `created_at` < ? " +
		"LIMIT ? FOR UPDATE SKIP LOCKED"
	const script2 = "UPDATE %s SET `status` = ?, `retries` = `retries` + 1 WHERE `id` IN (%s)"

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
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(script1, table),
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
	placeholders := make([]string, c)
	params := make([]any, c+1)
	params[0] = enum.StatusProcessing
	for i, r := range recs {
		placeholders[i] = "?"
		params[i+1] = r.ID
		r.Retries++
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(script2, table, strings.Join(placeholders, ", ")), params...)
	if err != nil {
		return nil, err
	}
	es, err = recsToEntities(recs)
	if err != nil {
		return nil, err
	}
	return es, nil
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
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
	const script = "INSERT INTO %s (" +
		"`id`,`created_at`,`version`,`topic`,`status`,`headers`,`payload`,`retries`,`expired_at`,`group`" +
		") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	_, err = s.execer().ExecContext(ctx, fmt.Sprintf(script, table),
		rec.ID,
		rec.CreatedAt,
		rec.Version,
		rec.Topic,
		rec.Status,
		string(rec.Headers),
		string(rec.Payload),
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
	table, err := s.quotedTable("published")
	if err != nil {
		return err
	}
	script := "UPDATE " + table + " SET `status` = ? WHERE "
	args := []any{status}
	if id != 0 {
		script += "`id` = ?"
		args = append(args, id)
	} else {
		script += "`version` = ? AND `status` = ?"
		args = append(args, s.gapOpts.Version, src)
	}
	_, err = s.execer().ExecContext(ctx, script, args...)
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status published", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

// UpdateStatusReceived implements [storage.Storage].
func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	table, err := s.quotedTable("received")
	if err != nil {
		return err
	}
	script := "UPDATE " + table + " SET `status` = ? WHERE "
	args := []any{status}
	if id != 0 {
		script += "`id` = ?"
		args = append(args, id)
	} else {
		script += "`version` = ? AND `status` = ?"
		args = append(args, s.gapOpts.Version, src)
	}
	_, err = s.execer().ExecContext(ctx, script, args...)
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func NewStorage(gapOpts *gap.Options, opts *Options, db *sql.DB) *Storage {
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
	if err := s.ensureClaimIndex(publishedName, "idx_published_claim"); err != nil {
		return err
	}
	return s.ensureClaimIndex(receivedName, "idx_received_claim")
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
		"CREATE INDEX %s ON %s (`version`, `status`, `retries`, `created_at`)",
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
