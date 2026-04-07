package sqltx

import (
	"database/sql"

	"github.com/lopolopen/gap"
	"github.com/lopolopen/gap/storage"
)

type SqlTx struct {
	*storage.TxerBase
	tx *sql.Tx
}

func New(tx *sql.Tx) *SqlTx {
	x := &SqlTx{
		TxerBase: &storage.TxerBase{},
		tx:       tx,
	}
	var _ gap.Txer = x
	return x
}

func (tx *SqlTx) Tx() any {
	return tx.tx
}
