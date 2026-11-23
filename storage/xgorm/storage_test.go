package xgorm

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage/internal/xsql"
	dbmodel "github.com/loopopen/gap/storage/xgorm/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestApplyMessageFilters(t *testing.T) {
	db, _ := openStorage(t)
	from := time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	id := uint(1844674407370955)
	filtered := applyMessageFilters(db.Model(&dbmodel.Received{}), &entity.MessageFilter{
		ID:          &id,
		Status:      enum.StatusPending,
		Topic:       "orders.created",
		Group:       "notifications",
		CreatedFrom: &from,
		CreatedTo:   &to,
		HeaderKey:   "tenant-id",
		HeaderValue: "customer-42",
		Payload:     "customer-42",
	}, true)

	where := whereText(filtered)
	for _, condition := range []string{
		"`id` = ?",
		"`status` = ?",
		"`topic` = ?",
		"`group` = ?",
		"`created_at` >= ?",
		"`created_at` <= ?",
		"headers LIKE ? ESCAPE '!'",
		"`payload` LIKE ?",
	} {
		if !strings.Contains(where, condition) {
			t.Fatalf("where clause %q does not contain %q", where, condition)
		}
	}
}

func TestQuotedGroupColumn(t *testing.T) {
	mysql, err := quotedGroupColumn(xsql.MySQL)
	if err != nil {
		t.Fatal(err)
	}
	if mysql != "`group`" {
		t.Fatalf("mysql group column = %q", mysql)
	}
	postgres, err := quotedGroupColumn(xsql.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	if postgres != `"group"` {
		t.Fatalf("postgres group column = %q", postgres)
	}
}

func TestMessageOrderByID(t *testing.T) {
	ascending := messageOrderByID(nil)
	if ascending.Column.Name != "id" || ascending.Desc {
		t.Fatalf("ascending order = %#v, want id ASC", ascending)
	}

	descending := messageOrderByID(&entity.MessageFilter{Descending: true})
	if descending.Column.Name != "id" || !descending.Desc {
		t.Fatalf("descending order = %#v, want id DESC", descending)
	}
}

func TestFailExhaustedWhere(t *testing.T) {
	db, stor := openStorage(t)
	stor.gapOpts.MaxRetries = 3
	where := whereText(stor.exhaustedQuery(db, &dbmodel.Published{}, "", "orders"))
	assertPendingDueWhere(t, where)
	if !strings.Contains(where, "retries >= ?") || !strings.Contains(where, "broker_name = ?") {
		t.Fatalf("fail exhausted WHERE %q does not filter broker name and retries", where)
	}
}

func TestClaimWhere(t *testing.T) {
	db, stor := openStorage(t)
	stor.gapOpts.MaxRetries = 3
	where := whereText(stor.claimQuery(db, &dbmodel.Published{}, 10, "", "orders"))
	assertPendingDueWhere(t, where)
	if !strings.Contains(where, "retries < ?") || !strings.Contains(where, "broker_name = ?") {
		t.Fatalf("claim WHERE %q does not filter broker name and retries", where)
	}
	if strings.Contains(where, "retries >= ?") || strings.Contains(where, "`group`") {
		t.Fatalf("published claim WHERE %q includes the wrong clause", where)
	}

	receivedWhere := whereText(stor.claimQuery(db, &dbmodel.Received{}, 10, "billing", "orders"))
	assertPendingDueWhere(t, receivedWhere)
	if !strings.Contains(receivedWhere, "`group` = ?") {
		t.Fatalf("received claim WHERE %q does not filter group", receivedWhere)
	}
	exhausted := whereText(stor.exhaustedQuery(db, &dbmodel.Received{}, "billing", "orders"))
	if !strings.Contains(exhausted, "`group` = ?") || !strings.Contains(exhausted, "retries >= ?") {
		t.Fatalf("received exhausted WHERE %q does not filter group", exhausted)
	}
}

func assertPendingDueWhere(t *testing.T, where string) {
	t.Helper()
	for _, condition := range []string{
		"`version` = ?",
		"status = ?",
		"next_attempt_at <= CURRENT_TIMESTAMP(3)",
		"lease_until IS NULL OR lease_until <= CURRENT_TIMESTAMP(3)",
	} {
		if !strings.Contains(where, condition) {
			t.Fatalf("WHERE %q does not contain %q", where, condition)
		}
	}
}

func TestRetryUpdatesKeepStatus(t *testing.T) {
	_, stor := openStorage(t)
	stor.gapOpts.RetryIntervalSeconds = 10
	updates := stor.retryUpdates()
	if _, ok := updates["status"]; ok {
		t.Fatal("retry update changes status")
	}
	expr, ok := updates["next_attempt_at"].(clause.Expr)
	if !ok {
		t.Fatalf("next attempt = %T, want clause.Expr", updates["next_attempt_at"])
	}
	if expr.SQL != "TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))" {
		t.Fatalf("next attempt SQL = %q", expr.SQL)
	}
	if len(expr.Vars) != 1 || expr.Vars[0] != int64(10) {
		t.Fatalf("next attempt vars = %#v", expr.Vars)
	}
	if updates["lease_token"] != nil || updates["lease_until"] != nil {
		t.Fatalf("retry update = %#v, want cleared lease", updates)
	}
}

func TestStatusUpdatesWriteStatusDirectly(t *testing.T) {
	expiredAt := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	updates := statusUpdates(enum.StatusFailed, expiredAt)
	if updates["status"] != enum.StatusFailed {
		t.Fatalf("status update = %v, want %v", updates["status"], enum.StatusFailed)
	}
	if updates["expired_at"] != expiredAt {
		t.Fatalf("expiration = %v, want %v", updates["expired_at"], expiredAt)
	}
	for _, field := range []string{"retries", "next_attempt_at"} {
		if _, ok := updates[field]; ok {
			t.Fatalf("status update unexpectedly changes %s", field)
		}
	}
}

func TestTerminalExpirationUsesDatabaseClock(t *testing.T) {
	_, stor := openStorage(t)
	stor.gapOpts.SucceededRetentionSeconds = 3600
	expr, ok := stor.terminalExpiration(enum.StatusSucceeded).(clause.Expr)
	if !ok {
		t.Fatalf("expiration = %T, want clause.Expr", stor.terminalExpiration(enum.StatusSucceeded))
	}
	if expr.SQL != "TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(3))" {
		t.Fatalf("expiration SQL = %q", expr.SQL)
	}
	if len(expr.Vars) != 1 || expr.Vars[0] != int64(3600) {
		t.Fatalf("expiration vars = %#v", expr.Vars)
	}
}

func TestCleanupSQLUsesTerminalStatusExpiryAndSkipLocked(t *testing.T) {
	for _, sql := range []string{
		mysqlDeleteExpiredSQL("`gap_published`"),
		postgresDeleteExpiredSQL(`"gap"."published"`, `"gap"."status_enum"`),
	} {
		for _, condition := range []string{"expired_at", "status", "SKIP LOCKED"} {
			if !strings.Contains(sql, condition) {
				t.Fatalf("cleanup SQL %q does not contain %q", sql, condition)
			}
		}
	}
}

func TestSetDBSharedStatementLeaksCallerClause(t *testing.T) {
	db, stor := openStorage(t)
	if stor.db.Statement != db.Statement {
		t.Fatal("setDB kept a distinct Statement")
	}
	db.Where("user_id = ?", 7)
	t.Logf("after Where, storage where=%q caller where=%q", whereText(stor.db.WithContext(context.Background())), whereText(db))
	setClone(db, 0)
	db.Where("user_id = ?", 7)
	t.Logf("after clone=0 Where, storage where=%q", whereText(stor.db.WithContext(context.Background())))
	// gorm.Open returns clone=1, so Where builds a new Statement and does not
	// touch the shared one. Writing the shared Statement is what Claim's
	// WithContext copies. That is the same object Bind keeps for the caller's tx.
	db.Statement.AddClause(clause.Where{
		Exprs: []clause.Expression{
			clause.Expr{SQL: "user_id = ?", Vars: []any{7}},
		},
	})
	sql := storageSQL(t, stor)
	t.Logf("after AddClause: %s", sql)
	t.Logf("shared clauses: %#v", db.Statement.Clauses)
	if strings.Contains(sql, "user_id") {
		t.Fatalf("storage query copied the caller's clause from the shared Statement: %s", sql)
	}
}

func storageSQL(t *testing.T, stor *storageAdapter) string {
	t.Helper()
	var query *gorm.DB
	sql := stor.db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		t.Logf("toSQL tx callbacks nil=%v clone=%v", callbacksNil(tx), cloneOf(tx))
		query = tx.WithContext(context.Background()).Model(&dbmodel.Published{}).Find(&[]dbmodel.Published{})
		t.Logf("inner sql=%q err=%v clauses=%v callbacks nil=%v dry=%v", query.Statement.SQL.String(), query.Error, clauseNames(query.Statement), callbacksNil(query), query.DryRun)
		return query
	})
	return sql
}

func setClone(db *gorm.DB, n int) {
	field := reflect.ValueOf(db).Elem().FieldByName("clone")
	*(*int)(unsafe.Pointer(field.UnsafeAddr())) = n
}

func whereText(db *gorm.DB) string {
	c, ok := db.Statement.Clauses["WHERE"]
	if !ok || c.Expression == nil {
		return ""
	}
	return strings.TrimSpace(clauseString(c.Expression))
}

func clauseString(expr clause.Expression) string {
	stmt := &gorm.Statement{
		DB:      &gorm.DB{Config: &gorm.Config{Dialector: nopDialector{}}},
		Clauses: map[string]clause.Clause{},
	}
	expr.Build(stmt)
	return stmt.SQL.String()
}

func callbacksNil(db *gorm.DB) bool {
	return reflect.ValueOf(db).Elem().FieldByName("callbacks").IsNil()
}

func cloneOf(db *gorm.DB) int64 {
	return reflect.ValueOf(db).Elem().FieldByName("clone").Int()
}

func clauseNames(stmt *gorm.Statement) []string {
	if stmt == nil {
		return nil
	}
	names := make([]string, 0, len(stmt.Clauses))
	for name := range stmt.Clauses {
		names = append(names, name)
	}
	return names
}

func openStorage(t *testing.T) (*gorm.DB, *storageAdapter) {
	t.Helper()
	db, err := gorm.Open(nopDialector{}, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Discard,
	})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}
	stor := newStorage(&ingap.Options{
		Version: "v1",
	}, &Options{Schema: "gap"}, db)
	return db, stor
}

type nopDialector struct{}

func (nopDialector) Name() string                    { return xsql.MySQL }
func (nopDialector) Initialize(*gorm.DB) error       { return nil }
func (nopDialector) Migrator(*gorm.DB) gorm.Migrator { return nil }
func (nopDialector) DataTypeOf(*schema.Field) string { return "" }

func (nopDialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (nopDialector) BindVarTo(writer clause.Writer, _ *gorm.Statement, _ any) {
	writer.WriteByte('?')
}

func (nopDialector) QuoteTo(writer clause.Writer, str string) {
	var builder strings.Builder
	builder.WriteByte('`')
	builder.WriteString(str)
	builder.WriteByte('`')
	writer.WriteString(builder.String())
}

func (nopDialector) Explain(sql string, vars ...any) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func TestPostgresSchemaSQL(t *testing.T) {
	createSchema, createType, err := postgresSchemaSQL("gap")
	if err != nil {
		t.Fatal(err)
	}
	if createSchema != `CREATE SCHEMA IF NOT EXISTS "gap"` {
		t.Fatal(createSchema)
	}
	if !strings.Contains(createType, `n.nspname = 'gap'`) {
		t.Fatal(createType)
	}
	if !strings.Contains(createType, `CREATE TYPE "gap"."status_enum"`) {
		t.Fatal(createType)
	}

	if _, _, err := postgresSchemaSQL(`gap"; DROP SCHEMA public; --`); err == nil {
		t.Fatal("accepted injected schema")
	}
}

func TestPostgresStatusSQL(t *testing.T) {
	sql, err := postgresStatusSQL("gap.published", "gap")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `ALTER TABLE "gap"."published"`) {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, `TYPE "gap"."status_enum"`) {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, `USING "status"::"gap"."status_enum"`) {
		t.Fatal(sql)
	}
	if _, err := postgresStatusSQL("gap.published", "gap;drop"); err == nil {
		t.Fatal("accepted injected schema")
	}
}

func TestPostgresClaimIndexSQL(t *testing.T) {
	sql, err := postgresClaimIndexSQL("gap.published", "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	if sql != `CREATE INDEX IF NOT EXISTS "idx_published_claim" ON "gap"."published" ("version", "status", "broker_name", "next_attempt_at", "lease_until", "retries")` {
		t.Fatal(sql)
	}
	if _, err := postgresClaimIndexSQL(`gap.published"; drop`, "idx_published_claim"); err == nil {
		t.Fatal("accepted injected table")
	}
}

func TestTableCommentSQL(t *testing.T) {
	sql, err := tableCommentSQL(xsql.Postgres, "gap.published", "it's ready")
	if err != nil {
		t.Fatal(err)
	}
	if sql != `COMMENT ON TABLE "gap"."published" IS 'it''s ready'` {
		t.Fatal(sql)
	}

	sql, err = tableCommentSQL(xsql.MySQL, "gap_published", "it's ready")
	if err != nil {
		t.Fatal(err)
	}
	if sql != "ALTER TABLE `gap_published` COMMENT = 'it''s ready'" {
		t.Fatal(sql)
	}

	if _, err := tableCommentSQL("sqlite", "published", "c"); err == nil {
		t.Fatal("expected unimplemented dialect to return an error")
	}
}
