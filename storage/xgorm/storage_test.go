package xgorm

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/loopopen/gap/v2/spi/entity"
	"github.com/loopopen/gap/v2/spi/enum"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
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
	filtered := applyMessageFilters(db.Model(&Received{}), &entity.MessageFilter{
		ID:          &id,
		Status:      enum.StatusPending,
		Topic:       "orders.created",
		Group:       "notifications",
		CreatedFrom: &from,
		CreatedTo:   &to,
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
		"`payload` LIKE ?",
	} {
		if !strings.Contains(where, condition) {
			t.Fatalf("where clause %q does not contain %q", where, condition)
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

func storageSQL(t *testing.T, stor *Storage) string {
	t.Helper()
	var query *gorm.DB
	sql := stor.db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		t.Logf("toSQL tx callbacks nil=%v clone=%v", callbacksNil(tx), cloneOf(tx))
		query = tx.WithContext(context.Background()).Model(&Published{}).Find(&[]Published{})
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

func openStorage(t *testing.T) (*gorm.DB, *Storage) {
	t.Helper()
	db, err := gorm.Open(nopDialector{}, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Discard,
	})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}
	stor := NewStorage(&internalgap.Options{
		Version: "v1",
	}, &Options{Schema: "gap"}, db)
	return db, stor
}

type nopDialector struct{}

func (nopDialector) Name() string                    { return "mysql" }
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
