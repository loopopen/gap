package xpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/loopopen/gap/storage/xsql"
)

var openPoolDB = func(ctx context.Context, dsn string, role xsql.Role) (*sql.DB, error) {
	if role == xsql.RoleDashboard {
		return openDashboard(ctx, dsn)
	}
	return openPostgres(ctx, dsn)
}

var registry = xsql.NewRegistry(func(ctx context.Context, dsn string, role xsql.Role) (*sql.DB, error) {
	return openPoolDB(ctx, dsn, role)
})

func openPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	return xsql.Open(ctx, "pgx", dsn)
}

func openDashboard(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("postgres dsn is empty")
	}
	return xsql.OpenWithSetup(ctx, "pgx", dsn, dashboardLimitSQL())
}

func dashboardLimitSQL() string {
	ms := xsql.DashboardMaxExecution / time.Millisecond
	return fmt.Sprintf("SET statement_timeout = %d", ms)
}
