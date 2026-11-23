package xsql

import (
	"strings"
	"testing"
)

func TestQuote(t *testing.T) {
	mysql, err := Quote(MySQL, "gap_published")
	if err != nil {
		t.Fatal(err)
	}
	if mysql != "`gap_published`" {
		t.Fatalf("mysql quote = %s", mysql)
	}
	postgres, err := Quote(Postgres, "gap")
	if err != nil {
		t.Fatal(err)
	}
	if postgres != `"gap"` {
		t.Fatalf("postgres quote = %s", postgres)
	}

	long63 := strings.Repeat("a", postgresIdentMax)
	if _, err := Quote(Postgres, long63); err != nil {
		t.Fatal(err)
	}
	if _, err := Quote(MySQL, long63+"a"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "gap-schema", `gap";drop`, "1gap", "gap.published", strings.Repeat("a", mysqlIdentMax+1)} {
		if _, err := Quote(MySQL, name); err == nil {
			t.Fatalf("mysql accepted %q", name)
		}
		if _, err := Quote(Postgres, name); err == nil {
			t.Fatalf("postgres accepted %q", name)
		}
	}
	if _, err := Quote(Postgres, strings.Repeat("a", postgresIdentMax+1)); err == nil {
		t.Fatal("postgres accepted a 64-character identifier")
	}
	if _, err := Quote("sqlite", "gap"); err == nil {
		t.Fatal("accepted unknown dialect")
	}
}

func TestQuoteQualified(t *testing.T) {
	quoted, err := QuoteQualified(Postgres, "gap.published")
	if err != nil {
		t.Fatal(err)
	}
	if quoted != `"gap"."published"` {
		t.Fatalf("qualified = %s", quoted)
	}
	if _, err := QuoteQualified(Postgres, `gap.published";drop`); err == nil {
		t.Fatal("accepted injected qualifier")
	}
	if _, err := QuoteQualified(MySQL, "gap.published"); err == nil {
		t.Fatal("mysql accepted a qualified identifier")
	}
}
