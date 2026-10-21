package xmysql

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
	return openMySQL(ctx, dsn)
}

var registry = xsql.NewRegistry(func(ctx context.Context, dsn string, role xsql.Role) (*sql.DB, error) {
	return openPoolDB(ctx, dsn, role)
})

func openMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("mysql dsn is empty")
	}
	return xsql.Open(ctx, "mysql", dsn)
}

func openDashboard(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("mysql dsn is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probe, err := xsql.Open(ctx, "mysql", dsn)
	if err != nil {
		return nil, err
	}
	var version string
	err = probe.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	_ = probe.Close()
	if err != nil {
		return nil, err
	}
	return xsql.OpenWithSetup(ctx, "mysql", dsn, dashboardLimitSQL(version))
}

func dashboardLimitSQL(version string) string {
	if strings.Contains(strings.ToLower(version), "mariadb") {
		seconds := xsql.DashboardMaxExecution / time.Second
		return fmt.Sprintf("SET SESSION max_statement_time = %d", seconds)
	}
	ms := xsql.DashboardMaxExecution / time.Millisecond
	return fmt.Sprintf("SET SESSION max_execution_time = %d", ms)
}
