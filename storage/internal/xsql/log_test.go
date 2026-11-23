package xsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestSQLLogRecordsStatementWithoutValues(t *testing.T) {
	buf := captureDebug(t)
	db := openFake(t, "ok")
	logged := WrapDB(db, true)
	const secret = "secret-payload"

	if _, err := logged.ExecContext(context.Background(), "UPDATE t SET v = ?", secret); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := logged.QueryRowContext(context.Background(), "SELECT n\nFROM t", secret).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("scan = %d", n)
	}
	rows, err := logged.QueryContext(context.Background(), "SELECT n FROM t", secret)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	tx, err := logged.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(context.Background(), "DELETE FROM t WHERE id = ?", secret); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	for _, query := range []string{
		`query="UPDATE t SET v = ?"`,
		`query="SELECT n FROM t"`,
		`query="DELETE FROM t WHERE id = ?"`,
	} {
		if !strings.Contains(got, query) {
			t.Fatalf("log missing %s:\n%s", query, got)
		}
	}
	if strings.Contains(got, secret) {
		t.Fatalf("log included argument values:\n%s", got)
	}
	if strings.Count(got, "\n") != 4 {
		t.Fatalf("expected 4 statements, got:\n%s", got)
	}
}

func TestSQLLogRecordsScanError(t *testing.T) {
	buf := captureDebug(t)
	logged := WrapDB(openFake(t, "fail"), true)
	var n int64
	err := logged.QueryRow("SELECT missing").Scan(&n)
	if err == nil {
		t.Fatal("expected scan error")
	}
	got := buf.String()
	if !strings.Contains(got, `query="SELECT missing"`) || !strings.Contains(got, "boom") {
		t.Fatalf("log = %s", got)
	}
}

func TestSQLLogDisabled(t *testing.T) {
	buf := captureDebug(t)
	logged := WrapDB(openFake(t, "ok"), false)
	if _, err := logged.Exec("UPDATE t SET v = ?", "secret-payload"); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("disabled logger wrote %s", buf.String())
	}
	if WrapDB(nil, true) != nil || WrapTx(nil, true) != nil {
		t.Fatal("nil connection was wrapped")
	}
}

func captureDebug(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func openFake(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("xsql-log-test", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func init() {
	sql.Register("xsql-log-test", logDriver{})
}

type logDriver struct{}

func (logDriver) Open(name string) (driver.Conn, error) {
	return &logConn{fail: name == "fail"}, nil
}

type logConn struct{ fail bool }

func (c *logConn) Prepare(string) (driver.Stmt, error) { return &logStmt{fail: c.fail}, nil }
func (c *logConn) Close() error                        { return nil }
func (c *logConn) Begin() (driver.Tx, error)           { return logTx{}, nil }

type logStmt struct{ fail bool }

func (s *logStmt) Close() error  { return nil }
func (s *logStmt) NumInput() int { return -1 }
func (s *logStmt) Exec([]driver.Value) (driver.Result, error) {
	if s.fail {
		return nil, errors.New("boom")
	}
	return logResult{}, nil
}
func (s *logStmt) Query([]driver.Value) (driver.Rows, error) {
	if s.fail {
		return nil, errors.New("boom")
	}
	return &logRows{vals: []driver.Value{int64(1)}}, nil
}

type logTx struct{}

func (logTx) Commit() error   { return nil }
func (logTx) Rollback() error { return nil }

type logResult struct{}

func (logResult) LastInsertId() (int64, error) { return 0, nil }
func (logResult) RowsAffected() (int64, error) { return 1, nil }

type logRows struct {
	vals []driver.Value
	i    int
}

func (r *logRows) Columns() []string { return []string{"n"} }
func (r *logRows) Close() error      { return nil }
func (r *logRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	dest[0] = r.vals[r.i]
	r.i++
	return nil
}
