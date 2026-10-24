package xsql

import (
	"context"
	"database/sql"

	"github.com/loopopen/gap/v2"
	"github.com/loopopen/gap/v2/storage"
)

type SqlTx struct {
	*storage.TxerBase
	tx *sql.Tx
}

func newTxer(tx *sql.Tx) *SqlTx {
	txer := &SqlTx{
		TxerBase: &storage.TxerBase{},
		tx:       tx,
	}
	return txer
}

func (tx *SqlTx) Tx() any {
	return tx.tx
}

func DoInTx(ctx context.Context, action func(context.Context, gap.Txer) error, db *sql.DB, opts ...*sql.TxOptions) (err error) {
	var sqlOpts *sql.TxOptions
	if len(opts) > 0 {
		sqlOpts = opts[0]
	}

	var tx *sql.Tx
	tx, err = db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return
	}

	var txer gap.Txer = newTxer(tx)

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
