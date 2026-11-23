package xgorm

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/txer"
	"github.com/loopopen/gap/internal/utility"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/xsql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type TableCommentter interface {
	TableComment() string
}

type Storage struct {
	gapOpts *ingap.Options
	opts    *Options
	db      *gorm.DB

	pool      *xsql.Pool
	closeOnce sync.Once
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
	err := s.insertMessage(ctx, &Published{}, pub.Model, "")
	if err != nil {
		return err
	}

	envelope.Log().Debug("created published envelope")
	return nil
}

// CreateReceived persists a newly received message.
func (s *Storage) CreateReceived(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}

	envelope.WithLease(s.gapOpts.LeaseTTL())
	rec := new(Received).FromEntity(envelope)
	if rec.err != nil {
		return rec.err
	}
	err := s.insertMessage(ctx, &Received{}, rec.Model, rec.Group)
	if err != nil {
		return err
	}

	envelope.Log().Debug("created received envelope")
	return nil
}

// NewStorage creates a storage adapter.
func NewStorage(gapOpts *ingap.Options, opts *Options, db *gorm.DB) *Storage {
	stor := &Storage{
		gapOpts: gapOpts,
		opts:    opts,
	}
	stor.setDB(db)

	var _ storage.Storage = stor
	return stor
}

func (s *Storage) setDB(db *gorm.DB) {
	conf := *db.Config
	namer := schema.NamingStrategy{
		SingularTable: true,
	}

	switch db.Dialector.Name() {
	case xsql.MySQL:
		namer.TablePrefix = s.opts.Schema + "_"
	case xsql.Postgres:
		namer.TablePrefix = s.opts.Schema + "."
	}

	conf.NamingStrategy = namer
	if s.opts.LogLevel != 0 {
		conf.Logger = logger.Default.LogMode(logger.LogLevel(s.opts.LogLevel))
	}
	dbClone := db.Session(&gorm.Session{NewDB: true})
	dbClone.Config = &conf
	s.db = dbClone
}

func (s *Storage) init() error {
	if _, err := xsql.Quote(s.db.Dialector.Name(), s.opts.Schema); err != nil {
		return fmt.Errorf("schema: %w", err)
	}

	switch s.db.Dialector.Name() {
	case xsql.Postgres:
		createSchema, createType, err := postgresSchemaSQL(s.opts.Schema)
		if err != nil {
			return err
		}
		if err := s.db.Exec(createSchema).Error; err != nil {
			return err
		}
		if err := s.db.Exec(createType).Error; err != nil {
			return err
		}
	}

	return s.autoMigrate(&Published{}, &Received{})
}

func (s *Storage) autoMigrate(models ...any) error {
	dname := s.db.Dialector.Name()
	for _, model := range models {
		stmt := &gorm.Statement{DB: s.db}
		if err := stmt.Parse(model); err != nil {
			return err
		}
		tableName := stmt.Schema.Table

		if err := s.db.AutoMigrate(model); err != nil {
			return err
		}

		if dname == xsql.Postgres {
			migrator := s.db.Table(tableName).Migrator()
			if migrator.HasColumn(&Model{}, "status") {
				sql, err := postgresStatusSQL(tableName, s.opts.Schema)
				if err != nil {
					return err
				}
				if err := s.db.Exec(sql).Error; err != nil {
					return err
				}
			}
		}

		var tableComment string
		if tc, ok := model.(TableCommentter); ok {
			tableComment = tc.TableComment()
		}

		if tableComment != "" {
			sql, err := tableCommentSQL(dname, tableName, tableComment)
			if err != nil {
				return err
			}
			if err := s.db.Exec(sql).Error; err != nil {
				return err
			}
		}

		if err := s.ensureClaimIndex(dname, tableName, claimIndexName(model)); err != nil {
			return err
		}
		if err := s.ensureCleanupIndex(dname, tableName, cleanupIndexName(model)); err != nil {
			return err
		}
	}
	return nil
}

// Bind binds the publisher or storage to the supplied transaction.
func (s *Storage) Bind(txer txer.Txer) (storage.Storage, error) {
	db, ok := txer.Tx().(*gorm.DB)
	if !ok {
		return nil, errx.ErrInvalidGormTx
	}
	newStor := Storage{
		gapOpts: s.gapOpts,
		opts:    s.opts,
	}
	newStor.setDB(db)
	return &newStor, nil
}

// InTx reports whether the storage is bound to a transaction.
func (s *Storage) InTx() bool {
	if s.db == nil {
		return false
	}
	_, ok := s.db.Statement.ConnPool.(gorm.TxCommitter)
	return ok
}

func (s *Storage) retryUpdates() map[string]any {
	return map[string]any{
		"lease_token":     nil,
		"lease_until":     nil,
		"next_attempt_at": s.databaseAfter(int64(s.gapOpts.RetryInterval() / time.Second)),
	}
}

// UpdateStatusPublished updates the state of a leased published message.
func (s *Storage) UpdateStatusPublished(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, &Published{}, "published", id, leaseToken)
	}
	if err := s.updateStatus(ctx, &Published{}, id, leaseToken, status); err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status published", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func (s *Storage) scheduleRetry(ctx context.Context, model any, kind string, id uint, leaseToken string) error {
	result := s.db.WithContext(ctx).
		Model(model).
		Where("id = ? AND status = ? AND lease_token = ?", id, enum.StatusPending, leaseToken).
		Updates(s.retryUpdates())
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return storage.ErrLeaseLost
	}
	if id != 0 {
		slog.Debug("scheduled retry", slog.String("table", kind), slog.Any("id", id), slog.Duration("retry_after", s.gapOpts.RetryInterval()))
	}
	return nil
}

// ClaimPublishedBatch claims a batch of due published messages.
func (s *Storage) ClaimPublishedBatch(ctx context.Context, batchSize int, brokerName string) ([]*entity.Envelope, error) {
	o := s.gapOpts
	now := time.Now()

	var pubs []*Published
	var es []*entity.Envelope
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if result := s.failExhausted(tx, &Published{}, "", brokerName); result.Error != nil {
			return result.Error
		}

		err := s.claimQuery(tx, &Published{}, batchSize, "", brokerName).Scan(&pubs).Error
		if err != nil {
			return err
		}
		if len(pubs) == 0 {
			return nil
		}
		leaseToken := utility.GenFastToken(16)
		leaseUntil := now.Add(o.LeaseTTL())
		var ids []any
		for _, p := range pubs {
			ids = append(ids, p.ID)
			p.Retries++
			p.LeaseToken = leaseToken
			p.LeaseUntil = sql.Null[time.Time]{V: leaseUntil, Valid: true}
		}
		if err := tx.
			Model(&Published{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(map[string]any{
				"retries":     gorm.Expr("retries + 1"),
				"lease_token": leaseToken,
				"lease_until": s.databaseAfter(int64(o.LeaseTTL() / time.Second)),
			}).Error; err != nil {
			return err
		}
		var convErr error
		es, convErr = pubsToEntities(pubs)
		return convErr
	})
	if err != nil {
		return nil, err
	}
	return es, nil
}

// ClaimReceivedBatch claims a batch of due received messages for a consumer group.
func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int, group, brokerName string) ([]*entity.Envelope, error) {
	o := s.gapOpts
	now := time.Now()

	var recs []*Received
	var es []*entity.Envelope
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if result := s.failExhausted(tx, &Received{}, group, brokerName); result.Error != nil {
			return result.Error
		}
		err := s.claimQuery(tx, &Received{}, batchSize, group, brokerName).Scan(&recs).Error
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		leaseToken := utility.GenFastToken(16)
		leaseUntil := now.Add(o.LeaseTTL())
		var ids []any
		for _, p := range recs {
			ids = append(ids, p.ID)
			p.Retries++
			p.LeaseToken = leaseToken
			p.LeaseUntil = sql.Null[time.Time]{V: leaseUntil, Valid: true}
		}
		if err := tx.
			Model(&Received{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(map[string]any{
				"retries":     gorm.Expr("retries + 1"),
				"lease_token": leaseToken,
				"lease_until": s.databaseAfter(int64(o.LeaseTTL() / time.Second)),
			}).Error; err != nil {
			return err
		}
		var convErr error
		es, convErr = recsToEntities(recs)
		return convErr
	})
	if err != nil {
		return nil, err
	}
	return es, nil
}

func (s *Storage) failExhausted(tx *gorm.DB, model any, group, brokerName string) *gorm.DB {
	return s.exhaustedQuery(tx, model, group, brokerName).
		Updates(map[string]any{
			"status":      enum.StatusFailed,
			"expired_at":  s.terminalExpiration(enum.StatusFailed),
			"lease_token": nil,
			"lease_until": nil,
		})
}

// pendingDueQuery selects this version's pending messages that are due and have no live lease.
func (s *Storage) pendingDueQuery(tx *gorm.DB, model any) *gorm.DB {
	return tx.
		Model(model).
		Where(&Model{Version: s.gapOpts.Version}).
		Where("status = ? AND next_attempt_at <= "+s.databaseNow()+" AND (lease_until IS NULL OR lease_until <= "+s.databaseNow()+")", enum.StatusPending)
}

func (s *Storage) claimQuery(tx *gorm.DB, model any, batchSize int, group, brokerName string) *gorm.DB {
	return s.withBrokerName(s.withReceivedGroup(s.pendingDueQuery(tx, model), model, group), brokerName).
		Where("retries < ?", s.gapOpts.MaxRetries).
		Limit(batchSize).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
}

func (s *Storage) exhaustedQuery(tx *gorm.DB, model any, group, brokerName string) *gorm.DB {
	return s.withBrokerName(s.withReceivedGroup(s.pendingDueQuery(tx, model), model, group), brokerName).
		Where("retries >= ?", s.gapOpts.MaxRetries)
}

func (s *Storage) withBrokerName(tx *gorm.DB, brokerName string) *gorm.DB {
	return tx.Where("broker_name = ?", brokerName)
}

func (s *Storage) withReceivedGroup(tx *gorm.DB, model any, group string) *gorm.DB {
	if _, ok := model.(*Received); !ok {
		return tx
	}
	return tx.Where(clause.Eq{Column: clause.Column{Name: "group"}, Value: group})
}

// UpdateStatusReceived updates the state of a leased received message.
func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, leaseToken string, status enum.Status) error {
	if status == enum.StatusPending {
		return s.scheduleRetry(ctx, &Received{}, "received", id, leaseToken)
	}
	if err := s.updateStatus(ctx, &Received{}, id, leaseToken, status); err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func (s *Storage) updateStatus(ctx context.Context, model any, id uint, leaseToken string, status enum.Status) error {
	result := s.db.WithContext(ctx).
		Model(model).
		Where("id = ? AND status = ? AND lease_token = ?", id, enum.StatusPending, leaseToken).
		Updates(statusUpdates(status, s.terminalExpiration(status)))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return storage.ErrLeaseLost
	}
	return nil
}

func statusUpdates(status enum.Status, expiredAt any) map[string]any {
	return map[string]any{
		"status":      status,
		"expired_at":  expiredAt,
		"lease_token": nil,
		"lease_until": nil,
	}
}

func (s *Storage) terminalExpiration(status enum.Status) any {
	return s.databaseAfter(int64(s.gapOpts.Retention(status) / time.Second))
}

func (s *Storage) databaseNow() string {
	if s.db != nil && s.db.Dialector != nil && s.db.Dialector.Name() == xsql.MySQL {
		return "CURRENT_TIMESTAMP(3)"
	}
	return "CURRENT_TIMESTAMP"
}

func (s *Storage) databaseAfter(seconds int64) any {
	switch {
	case s.db != nil && s.db.Dialector != nil && s.db.Dialector.Name() == xsql.MySQL:
		return clause.Expr{SQL: "TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))", Vars: []any{seconds}}
	case s.db != nil && s.db.Dialector != nil && s.db.Dialector.Name() == xsql.Postgres:
		return clause.Expr{SQL: "CURRENT_TIMESTAMP + (? * INTERVAL '1 second')", Vars: []any{seconds}}
	default:
		return time.Now().Add(time.Duration(seconds) * time.Second)
	}
}

func (s *Storage) insertMessage(ctx context.Context, model any, row Model, group string) error {
	values := map[string]any{
		"id":              row.ID,
		"created_at":      row.CreatedAt,
		"version":         row.Version,
		"topic":           row.Topic,
		"broker_name":     row.BrokerName,
		"status":          row.Status,
		"headers":         row.Headers,
		"payload":         row.Payload,
		"retries":         row.Retries,
		"lease_token":     row.LeaseToken,
		"lease_until":     s.databaseAfter(int64(s.gapOpts.LeaseTTL() / time.Second)),
		"next_attempt_at": clause.Expr{SQL: s.databaseNow()},
		"expired_at":      nil,
	}
	if group != "" || modelIsReceived(model) {
		values["group"] = group
	}
	return s.db.WithContext(ctx).Model(model).Create(values).Error
}

func modelIsReceived(model any) bool {
	_, ok := model.(*Received)
	return ok
}

// DeleteExpiredPublishedBatch deletes one batch of expired terminal published messages.
func (s *Storage) DeleteExpiredPublishedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, &Published{}, batchSize)
}

// DeleteExpiredReceivedBatch deletes one batch of expired terminal received messages.
func (s *Storage) DeleteExpiredReceivedBatch(ctx context.Context, batchSize int) (int64, error) {
	return s.deleteExpiredBatch(ctx, &Received{}, batchSize)
}

func (s *Storage) deleteExpiredBatch(ctx context.Context, model any, batchSize int) (int64, error) {
	stmt := &gorm.Statement{DB: s.db}
	if err := stmt.Parse(model); err != nil {
		return 0, err
	}
	table := stmt.Schema.Table
	statuses := []any{enum.StatusSucceeded, enum.StatusFailed}

	switch s.db.Dialector.Name() {
	case xsql.MySQL:
		quoted, err := xsql.Quote(xsql.MySQL, table)
		if err != nil {
			return 0, err
		}
		result := s.db.WithContext(ctx).Exec(mysqlDeleteExpiredSQL(quoted), statuses[0], statuses[1], batchSize)
		return result.RowsAffected, result.Error
	case xsql.Postgres:
		quoted, err := xsql.QuoteQualified(xsql.Postgres, table)
		if err != nil {
			return 0, err
		}
		statusType, err := xsql.Quote(xsql.Postgres, s.opts.Schema)
		if err != nil {
			return 0, err
		}
		result := s.db.WithContext(ctx).Exec(postgresDeleteExpiredSQL(quoted, statusType+`."status_enum"`), statuses[0], statuses[1], batchSize)
		return result.RowsAffected, result.Error
	default:
		var ids []uint
		var deleted int64
		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(model).
				Select("id").
				Where("expired_at <= ? AND status IN ?", time.Now(), statuses).
				Order("expired_at, id").
				Limit(batchSize).
				Find(&ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			result := tx.Where("id IN ? AND expired_at <= ? AND status IN ?", ids, time.Now(), statuses).Delete(model)
			if result.Error != nil {
				return result.Error
			}
			deleted = result.RowsAffected
			return nil
		})
		return deleted, err
	}
}

func mysqlDeleteExpiredSQL(table string) string {
	return "DELETE target FROM " + table + " AS target JOIN (" +
		"SELECT `id` FROM " + table + " WHERE `expired_at` <= CURRENT_TIMESTAMP(3) " +
		"AND `status` IN (?, ?) ORDER BY `expired_at`, `id` LIMIT ? FOR UPDATE SKIP LOCKED" +
		") AS expired ON target.`id` = expired.`id`"
}

func postgresDeleteExpiredSQL(table, statusType string) string {
	return `WITH expired AS (` +
		`SELECT "id" FROM ` + table + ` WHERE "expired_at" <= CURRENT_TIMESTAMP ` +
		`AND "status" IN (?::` + statusType + `, ?::` + statusType + `) ` +
		`ORDER BY "expired_at", "id" LIMIT ? FOR UPDATE SKIP LOCKED` +
		`) DELETE FROM ` + table + ` AS target USING expired WHERE target."id" = expired."id"`
}

func claimIndexName(model any) string {
	switch model.(type) {
	case *Published:
		return "idx_published_claim"
	case *Received:
		return "idx_received_claim"
	default:
		return ""
	}
}

func cleanupIndexName(model any) string {
	switch model.(type) {
	case *Published:
		return "idx_published_cleanup"
	case *Received:
		return "idx_received_cleanup"
	default:
		return ""
	}
}

func (s *Storage) ensureCleanupIndex(dialect xsql.Dialector, table, index string) error {
	if index == "" {
		return fmt.Errorf("cleanup index name is empty")
	}
	switch dialect {
	case xsql.MySQL:
		var n int
		err := s.db.Raw(
			`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
			table, index,
		).Scan(&n).Error
		if err != nil || n > 0 {
			return err
		}
		sql, err := mysqlCleanupIndexSQL(table, index)
		if err != nil {
			return err
		}
		err = s.db.Exec(sql).Error
		if err != nil && strings.Contains(err.Error(), "Duplicate key name") {
			return nil
		}
		return err
	case xsql.Postgres:
		sql, err := postgresCleanupIndexSQL(table, index)
		if err != nil {
			return err
		}
		return s.db.Exec(sql).Error
	default:
		return fmt.Errorf("cleanup index is not implemented for %s", dialect)
	}
}

func mysqlCleanupIndexSQL(table, index string) (string, error) {
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

func postgresCleanupIndexSQL(table, index string) (string, error) {
	quotedTable, err := xsql.QuoteQualified(xsql.Postgres, table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := xsql.Quote(xsql.Postgres, index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s ON %s ("expired_at", "id") WHERE "expired_at" IS NOT NULL`,
		quotedIndex, quotedTable,
	), nil
}

func (s *Storage) ensureClaimIndex(dialect xsql.Dialector, table, index string) error {
	if index == "" {
		return fmt.Errorf("claim index name is empty")
	}
	switch dialect {
	case xsql.MySQL:
		return s.ensureMySQLClaimIndex(table, index)
	case xsql.Postgres:
		return s.ensurePostgresClaimIndex(table, index)
	default:
		return fmt.Errorf("claim index is not implemented for %s", dialect)
	}
}

func (s *Storage) ensureMySQLClaimIndex(table, index string) error {
	var n int
	err := s.db.Raw(
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		table, index,
	).Scan(&n).Error
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	sql, err := mysqlClaimIndexSQL(table, index)
	if err != nil {
		return err
	}
	err = s.db.Exec(sql).Error
	if err != nil && strings.Contains(err.Error(), "Duplicate key name") {
		return nil
	}
	return err
}

func (s *Storage) ensurePostgresClaimIndex(table, index string) error {
	sql, err := postgresClaimIndexSQL(table, index)
	if err != nil {
		return err
	}
	return s.db.Exec(sql).Error
}

func mysqlClaimIndexSQL(table, index string) (string, error) {
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

func postgresClaimIndexSQL(table, index string) (string, error) {
	quotedTable, err := xsql.QuoteQualified(xsql.Postgres, table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := xsql.Quote(xsql.Postgres, index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s ON %s ("version", "status", "broker_name", "next_attempt_at", "lease_until", "retries")`,
		quotedIndex, quotedTable,
	), nil
}

func postgresSchemaSQL(schema string) (string, string, error) {
	quoted, err := xsql.Quote(xsql.Postgres, schema)
	if err != nil {
		return "", "", err
	}
	createSchema := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoted)
	createType := fmt.Sprintf(`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type t 
        JOIN pg_namespace n ON n.oid = t.typnamespace 
        WHERE t.typname = 'status_enum' 
        AND n.nspname = '%s'
    ) THEN
        CREATE TYPE %s."status_enum" AS ENUM ('Pending', 'Succeeded', 'Failed');
    END IF;
END
$$;`, schema, quoted)
	return createSchema, createType, nil
}

func postgresStatusSQL(tableName, schema string) (string, error) {
	table, err := xsql.QuoteQualified(xsql.Postgres, tableName)
	if err != nil {
		return "", err
	}
	quotedSchema, err := xsql.Quote(xsql.Postgres, schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
ALTER TABLE %s 
ALTER COLUMN "status" TYPE %s."status_enum"
USING "status"::%s."status_enum";`, table, quotedSchema, quotedSchema), nil
}

func tableCommentSQL(dialect xsql.Dialector, tableName, comment string) (string, error) {
	literal := strings.ReplaceAll(comment, `'`, `''`)
	switch dialect {
	case xsql.MySQL:
		quoted, err := xsql.Quote(xsql.MySQL, tableName)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("ALTER TABLE %s COMMENT = '%s'", quoted, literal), nil
	case xsql.Postgres:
		table, err := xsql.QuoteQualified(xsql.Postgres, tableName)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", table, literal), nil
	default:
		return "", fmt.Errorf("table comments are not implemented for %s", dialect)
	}
}
