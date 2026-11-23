package storage

import (
	"context"
	"database/sql"

	"github.com/loopopen/gap/internal/txer"
)

// DoInTx runs a callback inside a database transaction.
func DoInTx(ctx context.Context, action func(context.Context, txer.Txer) error, db *sql.DB, opts ...*sql.TxOptions) (err error) {
	var sqlOpts *sql.TxOptions
	if len(opts) > 0 {
		sqlOpts = opts[0]
	}

	var tx *sql.Tx
	tx, err = db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return
	}

	var txer txer.Txer = newTxer(tx)

	panicked := true
	defer func() {
		//rollback when panic or error
		if panicked || err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = action(ctx, txer); err != nil {
		panicked = false
		return
	}

	if err = tx.Commit(); err != nil {
		panicked = false
		return
	}

	panicked = false
	txer.Flush()
	return
}

type sqlTx struct {
	*TxerBase
	tx *sql.Tx
}

func newTxer(tx *sql.Tx) *sqlTx {
	txer := &sqlTx{
		TxerBase: &TxerBase{},
		tx:       tx,
	}
	return txer
}

// Tx returns the underlying database transaction.
func (tx *sqlTx) Tx() any {
	return tx.tx
}
