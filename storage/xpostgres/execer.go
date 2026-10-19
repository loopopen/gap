package xpostgres

import (
	"context"
	"database/sql"
)

type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
