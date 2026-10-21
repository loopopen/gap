package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

// Open pings a new pool. The caller applies limits.
func Open(ctx context.Context, driverName, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%s dsn is empty", driverName)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// OpenWithSetup pings a new pool and runs setupSQL on every physical connection.
func OpenWithSetup(ctx context.Context, driverName, dsn, setupSQL string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%s dsn is empty", driverName)
	}
	if strings.TrimSpace(setupSQL) == "" {
		return nil, errors.New("session setup sql is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probe, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	opener, ok := probe.Driver().(driver.DriverContext)
	if !ok {
		_ = probe.Close()
		return nil, fmt.Errorf("%s driver cannot run session setup", driverName)
	}
	base, err := opener.OpenConnector(dsn)
	_ = probe.Close()
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(SetupConnector{Connector: base, SQL: setupSQL})
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// SetupConnector runs SQL once on each new connection before it joins the pool.
type SetupConnector struct {
	driver.Connector
	SQL string
}

func (c SetupConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("connection cannot run session setup")
	}
	if _, err = execer.ExecContext(ctx, c.SQL, nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
