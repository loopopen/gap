package xgorm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/txer"
	"github.com/lopolopen/gap/storage"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type TableCommentter interface {
	TableComment() string
}

type Storage struct {
	gapOpts *gap.Options
	opts    *Options
	db      *gorm.DB
}

// CreatePublished implements [storage.Storage].
func (s *Storage) CreatePublished(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}

	pub := new(Published).FromEntity(envelope)
	err := s.db.Create(pub).Error
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
	err := s.db.Create(rec).Error
	if err != nil {
		return err
	}

	envelope.Log().Debug("created received envelope")
	return nil
}

func NewStorage(gapOpts *gap.Options, db *gorm.DB) *Storage {
	opts := gapOpts.StorageOptions
	stor := &Storage{
		gapOpts: gapOpts,
		opts:    opts.(*Options),
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
	db.Config = &conf
	s.db = db
}

func (s *Storage) init() error {
	schema := s.opts.Schema

	switch s.db.Dialector.Name() {
	case "postgres":
		if err := s.db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, schema)).Error; err != nil {
			return err
		}
		if err := s.db.Exec(fmt.Sprintf(`
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type t 
        JOIN pg_namespace n ON n.oid = t.typnamespace 
        WHERE t.typname = 'status_enum' 
        AND n.nspname = '%s'
    ) THEN
        CREATE TYPE %s.status_enum AS ENUM ('Pending', 'Processing', 'Succeeded', 'Failed');
    END IF;
END
$$;`, schema, schema)).Error; err != nil {
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
				sql := fmt.Sprintf(`
ALTER TABLE %s 
ALTER COLUMN "status" TYPE %s.status_enum
USING "status"::%s.status_enum;`, tableName, s.opts.Schema, s.opts.Schema)
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
			if dname == "mysql" {
				sql := fmt.Sprintf("ALTER TABLE `%s` COMMENT = '%s'", tableName, tableComment)
				if err := s.db.Exec(sql).Error; err != nil {
					return err
				}
			} else {
				panic("unimplemented")
			}
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
			Status:  src,
		})
	}

	err := db.Updates(&Model{Status: status}).Error
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
	s.db.Transaction(func(tx *gorm.DB) error {
		err := tx.WithContext(ctx).
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
		}
		err = tx.WithContext(ctx).
			Model(&Published{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(&Model{Status: enum.StatusProcessing}).Error
		if err != nil {
			return err
		}
		return nil
	})
	var es []*entity.Envelope
	for _, p := range pubs {
		es = append(es, p.ToEntity())
	}
	return es, nil
}

func (s *Storage) ClaimReceivedBatch(ctx context.Context, batchSize int) ([]*entity.Envelope, error) {
	o := s.gapOpts
	minutsAge := time.Now().Add(-o.Lookback())

	var recs []*Received
	s.db.Transaction(func(tx *gorm.DB) error {
		err := tx.WithContext(ctx).
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
		}
		err = tx.WithContext(ctx).
			Model(&Received{}).
			Where(clause.IN{
				Column: "id",
				Values: ids,
			}).
			Updates(&Model{Status: enum.StatusProcessing}).Error
		if err != nil {
			return err
		}
		return nil
	})
	var es []*entity.Envelope
	for _, p := range recs {
		es = append(es, p.ToEntity())
	}
	return es, nil
}

func (s *Storage) UpdateStatusReceived(ctx context.Context, id uint, src enum.Status, status enum.Status) error {
	db := s.db.WithContext(ctx).Model(&Received{})
	if id != 0 {
		db.Where(&Model{ID: id})
	} else {
		db.Where(&Model{Version: s.gapOpts.Version, Status: src})
	}

	err := db.Updates(&Model{Status: status}).Error
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}
