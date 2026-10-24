package xgorm

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/loopopen/gap/v2/spi/entity"
	"github.com/loopopen/gap/v2/spi/enum"
	"github.com/loopopen/gap/v2/spi/errx"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/loopopen/gap/v2/spi/txer"
	"github.com/loopopen/gap/v2/storage"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

const maxIdentLen = 63

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type TableCommentter interface {
	TableComment() string
}

type Storage struct {
	gapOpts *internalgap.Options
	opts    *Options
	db      *gorm.DB
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
	err := s.db.WithContext(ctx).Create(pub).Error
	if err != nil {
		return err
	}

	envelope.Log().Debug("created published envelope")
	return nil
}

func (s *Storage) CreateReceived(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}

	rec := new(Received).FromEntity(envelope)
	if rec.err != nil {
		return rec.err
	}
	err := s.db.WithContext(ctx).Create(rec).Error
	if err != nil {
		return err
	}

	envelope.Log().Debug("created received envelope")
	return nil
}

func NewStorage(gapOpts *internalgap.Options, opts *Options, db *gorm.DB) *Storage {
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
	case "mysql":
		namer.TablePrefix = s.opts.Schema + "_"
	case "postgres":
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
	if _, err := quoteIdent(s.opts.Schema); err != nil {
		return fmt.Errorf("schema: %w", err)
	}

	switch s.db.Dialector.Name() {
	case "postgres":
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
		if err := s.db.AutoMigrate(model); err != nil {
			return err
		}

		stmt := &gorm.Statement{DB: s.db}
		if err := stmt.Parse(model); err != nil {
			return err
		}
		tableName := stmt.Schema.Table

		if dname == "postgres" {
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
	}
	return nil
}

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

func (s *Storage) InTx() bool {
	if s.db == nil {
		return false
	}
	_, ok := s.db.Statement.ConnPool.(gorm.TxCommitter)
	return ok
}

func (s *Storage) UpdateStatusPublished(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	db := s.db.WithContext(ctx).Model(&Published{})
	if id != 0 {
		db.Where(&Model{ID: id})
	} else {
		db.Where(&Model{
			Version: s.gapOpts.Version,
			Status:  Status(src),
		})
	}

	err := db.Updates(&Model{Status: Status(status)}).Error
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status published", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func (s *Storage) ClaimPublishedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error) {
	o := s.gapOpts
	minutsAge := time.Now().Add(-o.Lookback())

	var pubs []*Published
	var es []*entity.Envelope
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.
			Model(&Published{}).
			Where(&Model{Version: o.Version}).
			Where(clause.IN{
				Column: "status",
				Values: []any{enum.StatusPending, enum.StatusFailed},
			}).
			Where(clause.Lt{
				Column: "retries",
				Value:  o.MaxRetries,
			}).
			Where(clause.Lt{
				Column: "created_at",
				Value:  minutsAge,
			}).
			Limit(batchSize).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Scan(&pubs).Error
		if err != nil {
			return err
		}
		if len(pubs) == 0 {
			return nil
		}
		var ids []any
		for _, p := range pubs {
			ids = append(ids, p.ID)
			p.Retries++
		}
		if err := tx.
			Model(&Published{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(map[string]any{
				"status":  enum.StatusProcessing,
				"retries": gorm.Expr("retries + 1"),
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

func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error) {
	o := s.gapOpts
	minutsAge := time.Now().Add(-o.Lookback())

	var recs []*Received
	var es []*entity.Envelope
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.
			Model(&Received{}).
			Where(&Model{Version: o.Version}).
			Where(clause.IN{
				Column: "status",
				Values: []any{enum.StatusPending, enum.StatusFailed},
			}).
			Where(clause.Lt{
				Column: "retries",
				Value:  o.MaxRetries,
			}).
			Where(clause.Lt{
				Column: "created_at",
				Value:  minutsAge,
			}).
			Limit(batchSize).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Scan(&recs).Error
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		var ids []any
		for _, p := range recs {
			ids = append(ids, p.ID)
			p.Retries++
		}
		if err := tx.
			Model(&Received{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(map[string]any{
				"status":  enum.StatusProcessing,
				"retries": gorm.Expr("retries + 1"),
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

func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	db := s.db.WithContext(ctx).Model(&Received{})
	if id != 0 {
		db.Where(&Model{ID: id})
	} else {
		db.Where(&Model{Version: s.gapOpts.Version, Status: Status(src)})
	}

	err := db.Updates(&Model{Status: Status(status)}).Error
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
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

func (s *Storage) ensureClaimIndex(dialect, table, index string) error {
	if index == "" {
		return fmt.Errorf("claim index name is empty")
	}
	switch dialect {
	case "mysql":
		return s.ensureMySQLClaimIndex(table, index)
	case "postgres":
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

func postgresClaimIndexSQL(table, index string) (string, error) {
	quotedTable, err := quoteQualified(table)
	if err != nil {
		return "", err
	}
	quotedIndex, err := quoteIdent(index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s ON %s ("version", "status", "retries", "created_at")`,
		quotedIndex, quotedTable,
	), nil
}

func quoteMySQLIdent(name string) (string, error) {
	if len(name) == 0 || len(name) > 64 || !identRE.MatchString(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return "`" + name + "`", nil
}

func quoteIdent(name string) (string, error) {
	if len(name) > maxIdentLen || !identRE.MatchString(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return `"` + name + `"`, nil
}

func quoteQualified(name string) (string, error) {
	parts := strings.Split(name, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		q, err := quoteIdent(part)
		if err != nil {
			return "", err
		}
		quoted = append(quoted, q)
	}
	return strings.Join(quoted, "."), nil
}

func postgresSchemaSQL(schema string) (string, string, error) {
	quoted, err := quoteIdent(schema)
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
        CREATE TYPE %s."status_enum" AS ENUM ('Pending', 'Processing', 'Succeeded', 'Failed');
    END IF;
END
$$;`, schema, quoted)
	return createSchema, createType, nil
}

func postgresStatusSQL(tableName, schema string) (string, error) {
	table, err := quoteQualified(tableName)
	if err != nil {
		return "", err
	}
	quotedSchema, err := quoteIdent(schema)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`
ALTER TABLE %s 
ALTER COLUMN "status" TYPE %s."status_enum"
USING "status"::%s."status_enum";`, table, quotedSchema, quotedSchema), nil
}

func tableCommentSQL(dialect, tableName, comment string) (string, error) {
	literal := strings.ReplaceAll(comment, `'`, `''`)
	switch dialect {
	case "mysql":
		if _, err := quoteIdent(tableName); err != nil {
			return "", err
		}
		return fmt.Sprintf("ALTER TABLE `%s` COMMENT = '%s'", tableName, literal), nil
	case "postgres":
		table, err := quoteQualified(tableName)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", table, literal), nil
	default:
		return "", fmt.Errorf("table comments are not implemented for %s", dialect)
	}
}
