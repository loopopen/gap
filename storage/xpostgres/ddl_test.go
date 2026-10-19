package xpostgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
)

func TestQuotedTable(t *testing.T) {
	s := &Storage{opts: &Options{Schema: "gap"}}
	table, err := s.quotedTable("published")
	if err != nil {
		t.Fatal(err)
	}
	if table != `"gap"."published"` {
		t.Fatal(table)
	}
	s.opts.Schema = `gap"; drop schema public; --`
	if _, err := s.quotedTable("published"); err == nil {
		t.Fatal("accepted injected schema")
	}
}

func TestCreateSQL(t *testing.T) {
	published := publishedCreateSQL(`"gap"."published"`, `"gap"."status_enum"`)
	if strings.Contains(published, `"group"`) {
		t.Fatal(published)
	}
	if !strings.Contains(published, `"retries" integer NOT NULL DEFAULT 0`) {
		t.Fatal(published)
	}
	if !strings.Contains(published, `"status" "gap"."status_enum" NOT NULL`) {
		t.Fatal(published)
	}

	received := receivedCreateSQL(`"gap"."received"`, `"gap"."status_enum"`)
	if !strings.Contains(received, `"group" varchar(128) NOT NULL`) {
		t.Fatal(received)
	}
}

func TestClaimIndexSQL(t *testing.T) {
	sql, err := claimIndexSQL(`"gap"."published"`, "idx_published_claim")
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE INDEX IF NOT EXISTS "idx_published_claim" ON "gap"."published" ("version", "status", "retries", "created_at")`
	if sql != want {
		t.Fatal(sql)
	}
	if _, err := claimIndexSQL(`"gap"."published"`, `idx"; drop`); err == nil {
		t.Fatal("accepted injected index")
	}
}

func TestStatusTypeSQL(t *testing.T) {
	sql := statusTypeSQL("gap", `"gap"`)
	if !strings.Contains(sql, `n.nspname = 'gap'`) {
		t.Fatal(sql)
	}
	if !strings.Contains(sql, `CREATE TYPE "gap"."status_enum"`) {
		t.Fatal(sql)
	}
}

func TestWherePlaceholders(t *testing.T) {
	h := NewWhereHelper()
	Add(h, `"status" = ?`, enum.StatusFailed)
	Add(h, `"topic" = ?`, "orders")
	if h.String() != `WHERE "status" = $1 AND "topic" = $2` {
		t.Fatal(h.String())
	}
}

func TestOpenPostgresEmptyDSN(t *testing.T) {
	for _, dsn := range []string{"", " ", "\t"} {
		db, err := openPostgres(context.Background(), dsn)
		if err == nil || db != nil {
			t.Fatalf("dsn %q: db=%v err=%v", dsn, db, err)
		}
		if !strings.Contains(err.Error(), "empty") {
			t.Fatal(err)
		}
	}
}

func TestOpenPostgresPingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	db, err := openPostgres(ctx, "postgres://root@127.0.0.1:1/gap?sslmode=disable&connect_timeout=1")
	if err == nil || db != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
}

func TestCreateStorageEmptyDSN(t *testing.T) {
	_, err := Factory{}.CreateStorage(&gap.Options{
		Context: context.Background(),
		StorageOptions: &Options{
			Schema: "gap",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatal(err)
	}
}

func TestSharedPoolReusesDB(t *testing.T) {
	resetDSNPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	var opens atomic.Int32
	openPoolDB = func(context.Context, string) (*sql.DB, error) {
		opens.Add(1)
		return sql.Open("pgx", "postgres://postgres@127.0.0.1:1/gap?sslmode=disable")
	}

	ctx := context.Background()
	first, err := sharedPool(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := sharedPool(ctx, "dsn-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.db != second.db {
		t.Fatal("same dsn opened two pools")
	}
	other, err := sharedPool(ctx, "dsn-b")
	if err != nil {
		t.Fatal(err)
	}
	if other.db == first.db {
		t.Fatal("different dsn shared a pool")
	}
	if got := opens.Load(); got != 2 {
		t.Fatalf("opens = %d", got)
	}
}

func TestSharedPoolDoesNotCacheOpenError(t *testing.T) {
	resetDSNPools(t)
	orig := openPoolDB
	t.Cleanup(func() { openPoolDB = orig })

	var opens atomic.Int32
	openPoolDB = func(context.Context, string) (*sql.DB, error) {
		opens.Add(1)
		return nil, errors.New("down")
	}
	for range 2 {
		db, err := sharedPool(context.Background(), "dsn-down")
		if err == nil || db != nil {
			t.Fatalf("db=%v err=%v", db, err)
		}
	}
	if got := opens.Load(); got != 2 {
		t.Fatalf("opens = %d", got)
	}
}

func TestEnsureSchemaRetriesAfterFailure(t *testing.T) {
	pool := &dsnPool{schemas: map[string]struct{}{}}
	var calls int
	if err := pool.ensureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.ensureSchema("gap", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.ensureSchema("other", func() error {
		calls++
		return errors.New("boom")
	}); err == nil {
		t.Fatal("expected init error")
	}
	if err := pool.ensureSchema("other", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("init calls = %d", calls)
	}
}

func resetDSNPools(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		dsnPools.Range(func(key, value any) bool {
			dsnPools.Delete(key)
			if pool, ok := value.(*dsnPool); ok && pool.db != nil {
				_ = pool.db.Close()
			}
			return true
		})
	})
	dsnPools.Range(func(key, value any) bool {
		dsnPools.Delete(key)
		if pool, ok := value.(*dsnPool); ok && pool.db != nil {
			_ = pool.db.Close()
		}
		return true
	})
}

func TestPostgresPoolLimits(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://root@127.0.0.1:1/gap?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	configurePool(db)
	if got := db.Stats().MaxOpenConnections; got != maxOpenConns {
		t.Fatalf("max open connections = %d", got)
	}
}

func TestCreateStoragePluginMismatch(t *testing.T) {
	_, err := Factory{}.CreateStorage(&gap.Options{
		StorageOptions: mismatchPlugin{},
	})
	if err == nil {
		t.Fatal("expected plugin mismatch")
	}
}

type mismatchPlugin struct{}

func (mismatchPlugin) PluginType() enum.Plugin { return enum.PluginMySQL }

func TestToEntityInvalidHeaders(t *testing.T) {
	p := &Published{ID: 7, Headers: "{"}
	en := p.ToEntity()
	if p.err == nil {
		t.Fatal("expected decode error")
	}
	if en.Headers != nil {
		t.Fatalf("headers = %#v", en.Headers)
	}
}

func TestFromEntityPayloadAndHeaders(t *testing.T) {
	en := &entity.Envelope{
		ID:      4,
		Topic:   "t",
		Message: map[string]string{"a": "b"},
		Headers: map[string]string{"h": "v"},
	}
	p := new(Published).FromEntity(en)
	if p.err != nil {
		t.Fatal(p.err)
	}
	if p.Payload != `{"a":"b"}` {
		t.Fatalf("payload = %q", p.Payload)
	}
	if p.Headers != `{"h":"v"}` {
		t.Fatalf("headers = %q", p.Headers)
	}
	if p.Status != enum.StatusPending {
		t.Fatalf("status = %s", p.Status)
	}
}
