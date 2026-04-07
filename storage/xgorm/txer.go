package xgorm

import (
	"context"
	"database/sql"

	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/storage"
	"gorm.io/gorm"
)

type Txer struct {
	*storage.TxerBase
	db *gorm.DB
}

func newTxer(db *gorm.DB) *Txer {
	txer := &Txer{
		TxerBase: &storage.TxerBase{},
		db:       db,
	}
	return txer
}

func (tx *Txer) Tx() any {
	return tx.db
}

type wappter struct {
	db  *gorm.DB
	opt *sql.TxOptions
}

func With(db *gorm.DB, opts ...*sql.TxOptions) *wappter {
	var opt *sql.TxOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	return &wappter{
		db:  db,
		opt: opt,
	}
}

func (w wappter) DoInTx(ctx context.Context, action func(context.Context, gap.Txer) error) error {
	return DoInTx(ctx, action, w.db, w.opt)
}

func DoInTx(ctx context.Context, action func(context.Context, gap.Txer) error, db *gorm.DB, opts ...*sql.TxOptions) error {
	var txer gap.Txer
	err := db.WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			txer = newTxer(tx)
			return action(ctx, txer)
		})
	if err != nil {
		return err
	}

	if txer != nil {
		txer.Flush()
	}
	return nil

}
