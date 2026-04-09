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
	if s.opts.Schema != "" {
		switch s.db.Dialector.Name() {
		case "postgres":
			if err := s.db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, s.opts.Schema)).Error; err != nil {
				return err
			}
		}
	}

	return migrateWithComment(s.db,
		&Published{},
		&Received{},
	)
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
		db.Where("`id` = ?", id)
	} else {
		db.Where("`version` = ? AND `status` = ?", s.gapOpts.Version, src)
	}

	err := db.Update("`status`", status).Error
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status published", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}

func migrateWithComment(db *gorm.DB, models ...any) error {
	for _, model := range models {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(model); err != nil {
			return err
		}
		tableName := stmt.Schema.Table

		if err := db.AutoMigrate(model); err != nil {
			return err
		}

		var tableComment string
		if tc, ok := model.(TableCommentter); ok {
			tableComment = tc.TableComment()
		}

		if tableComment != "" {
			if db.Dialector.Name() == "mysql" {
				sql := fmt.Sprintf("ALTER TABLE `%s` COMMENT = '%s'", tableName, tableComment)
				if err := db.Exec(sql).Error; err != nil {
					return err
				}
			} else {
				panic("unimplemented")
			}
		}
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
			Where("`version` = ?", o.Version).
			Where("`status` IN ?", []enum.Status{enum.StatusPending, enum.StatusFailed}).
			Where("`retries` < ?", o.MaxRetries).
			Where("`created_at` < ?", minutsAge).
			Limit(batchSize).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Scan(&pubs).Error
		if err != nil {
			return err
		}
		if len(pubs) == 0 {
			return nil
		}
		var ids []uint
		for _, p := range pubs {
			ids = append(ids, p.ID)
		}
		err = tx.WithContext(ctx).
			Model(&Published{}).
			Where("`id` IN ?", ids).
			Update("`status`", enum.StatusProcessing).Error
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
			Where("`version` = ?", o.Version).
			Where("`status` IN ?", []enum.Status{enum.StatusPending, enum.StatusFailed}).
			Where("`retries` < ?", o.MaxRetries).
			Where("`created_at` < ?", minutsAge).
			Limit(batchSize).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Scan(&recs).Error
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil
		}
		var ids []uint
		for _, p := range recs {
			ids = append(ids, p.ID)
		}
		err = tx.WithContext(ctx).
			Model(&Received{}).
			Where("`id` IN ?", ids).
			Update("`status`", enum.StatusProcessing).Error
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
		db.Where("`id` = ?", id)
	} else {
		db.Where("`version` = ? AND `status` = ?", s.gapOpts.Version, src)
	}

	err := db.Update("`status`", status).Error
	if err != nil {
		return err
	}

	if id != 0 {
		slog.Debug("updated status of received", slog.Any("id", id), slog.String("status", status.String()))
	}
	return nil
}
