package xgorm

import (
	"strings"
	"testing"
)

func TestQuoteIdent(t *testing.T) {
	quoted, err := quoteIdent("gap")
	if err != nil {
		t.Fatal(err)
	}
	if quoted != `"gap"` {
		t.Fatalf("quoted = %s", quoted)
	}

	for _, name := range []string{"", "gap-schema", `gap";drop`, "1gap", "gap.published", strings.Repeat("a", 64)} {
		if _, err := quoteIdent(name); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
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
	if sql != `CREATE INDEX IF NOT EXISTS "idx_published_claim" ON "gap"."published" ("version", "status", "retries", "created_at")` {
		t.Fatal(sql)
	}
	if _, err := postgresClaimIndexSQL(`gap.published"; drop`, "idx_published_claim"); err == nil {
		t.Fatal("accepted injected table")
	}
}

func TestTableCommentSQL(t *testing.T) {
	sql, err := tableCommentSQL("postgres", "gap.published", "it's ready")
	if err != nil {
		t.Fatal(err)
	}
	if sql != `COMMENT ON TABLE "gap"."published" IS 'it''s ready'` {
		t.Fatal(sql)
	}

	sql, err = tableCommentSQL("mysql", "gap_published", "it's ready")
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
