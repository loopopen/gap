package xgorm

import (
	"context"
	"database/sql"

	"github.com/loopopen/gap"
	"github.com/loopopen/gap/storage"
	"gorm.io/gorm"
)

type gormTx struct {
	*storage.TxerBase
	db *gorm.DB
}

func newTxer(db *gorm.DB) *gormTx {
	txer := &gormTx{
		TxerBase: &storage.TxerBase{},
		db:       db,
	}
	return txer
}

// Tx returns the underlying database transaction.
func (tx *gormTx) Tx() any {
	return tx.db
}

// DoInTx runs a callback inside a database transaction.
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
