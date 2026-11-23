package xsql

import (
	"fmt"
	"regexp"
	"strings"
)

type Dialect = string

const (
	MySQL    Dialect = "mysql"
	Postgres Dialect = "postgres"
)

const (
	mysqlIdentMax    = 64
	postgresIdentMax = 63
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Quote returns a dialect-quoted identifier, or an error when name is not a single safe identifier.
func Quote(d Dialect, name string) (string, error) {
	var mark string
	var max int
	switch d {
	case MySQL:
		mark, max = "`", mysqlIdentMax
	case Postgres:
		mark, max = `"`, postgresIdentMax
	default:
		return "", fmt.Errorf("quote is not implemented for dialect %q", d)
	}
	if len(name) == 0 || len(name) > max || !identRE.MatchString(name) {
		return "", fmt.Errorf("invalid identifier %q", name)
	}
	return mark + name + mark, nil
}

// QuoteQualified quotes a dotted PostgreSQL name such as schema.table.
func QuoteQualified(d Dialect, name string) (string, error) {
	if d != Postgres {
		return "", fmt.Errorf("qualified identifier is not implemented for dialect %q", d)
	}
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		q, err := Quote(d, part)
		if err != nil {
			return "", err
		}
		quoted[i] = q
	}
	return strings.Join(quoted, "."), nil
}
