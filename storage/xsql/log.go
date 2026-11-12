package xsql

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"
)

// DB is a connection pool that can record statements it issues.
type DB struct {
	db    *sql.DB
	logOn bool
}

// Tx is a transaction that records statements with the same policy as its pool.
type Tx struct {
	tx    *sql.Tx
	logOn bool
}

// WrapDB records statements when logSQL is set. A nil pool stays nil.
func WrapDB(db *sql.DB, logSQL bool) *DB {
	if db == nil {
		return nil
	}
	return &DB{db: db, logOn: logSQL}
}

// WrapTx records statements on an externally owned transaction.
func WrapTx(tx *sql.Tx, logSQL bool) *Tx {
	if tx == nil {
		return nil
	}
	return &Tx{tx: tx, logOn: logSQL}
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, logOn: d.logOn}, nil
}

func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return execLogged(d.logOn, query, args, func() (sql.Result, error) {
		return d.db.ExecContext(ctx, query, args...)
	})
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return queryLogged(d.logOn, query, args, func() (*sql.Rows, error) {
		return d.db.QueryContext(ctx, query, args...)
	})
}

func (d *DB) QueryRow(query string, args ...any) *Row {
	return d.QueryRowContext(context.Background(), query, args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	started := time.Now()
	return &Row{
		row:     d.db.QueryRowContext(ctx, query, args...),
		query:   query,
		nargs:   len(args),
		logOn:   d.logOn,
		started: started,
	}
}

func (t *Tx) Commit() error { return t.tx.Commit() }

func (t *Tx) Rollback() error { return t.tx.Rollback() }

func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return execLogged(t.logOn, query, args, func() (sql.Result, error) {
		return t.tx.ExecContext(ctx, query, args...)
	})
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return queryLogged(t.logOn, query, args, func() (*sql.Rows, error) {
		return t.tx.QueryContext(ctx, query, args...)
	})
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	return &Row{
		row:     t.tx.QueryRowContext(ctx, query, args...),
		query:   query,
		nargs:   len(args),
		logOn:   t.logOn,
		started: time.Now(),
	}
}

// Row delays its log until Scan, which is when database/sql runs the query.
type Row struct {
	row     *sql.Row
	query   string
	nargs   int
	logOn   bool
	started time.Time
}

func (r *Row) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if r.logOn {
		logSQL(r.query, r.nargs, time.Since(r.started), err)
	}
	return err
}

func execLogged(logOn bool, query string, args []any, fn func() (sql.Result, error)) (sql.Result, error) {
	started := time.Now()
	result, err := fn()
	if logOn {
		logSQL(query, len(args), time.Since(started), err)
	}
	return result, err
}

func queryLogged(logOn bool, query string, args []any, fn func() (*sql.Rows, error)) (*sql.Rows, error) {
	started := time.Now()
	rows, err := fn()
	if logOn {
		logSQL(query, len(args), time.Since(started), err)
	}
	return rows, err
}

func logSQL(query string, nargs int, elapsed time.Duration, err error) {
	attrs := []any{
		slog.String("query", compactSQL(query)),
		slog.Int("args", nargs),
		slog.Duration("duration", elapsed),
	}
	if err != nil {
		attrs = append(attrs, slog.Any("err", err))
	}
	slog.Debug("sql", attrs...)
}

func compactSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}
