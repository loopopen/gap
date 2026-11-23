package contracttest

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"

	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/internal/storagetest"
	"github.com/loopopen/gap/storage/xgorm"
	"github.com/loopopen/gap/storage/xmysql"
	"github.com/loopopen/gap/storage/xpostgres"
	mysqldriver "gorm.io/driver/mysql"
	postgresdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type catalogColumn struct {
	dataType     string
	udtName      string
	columnType   string
	nullable     bool
	defaultValue sql.NullString
	maxLength    sql.NullInt64
	generated    bool
}

type catalogTable struct {
	columns    map[string]catalogColumn
	indexes    map[string][]string
	primaryKey []string
	statusEnum []string
}

func TestMySQLFinalSchemaContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_MYSQL_DSN")
	t.Parallel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		open func(*ingap.Options, string) (storage.Storage, error)
	}{
		{
			name: "native",
			open: func(opts *ingap.Options, schema string) (storage.Storage, error) {
				return (&xmysql.Options{Schema: schema, DSN: dsn}).OpenStorage(opts, storage.RoleData)
			},
		},
		{
			name: "gorm",
			open: func(opts *ingap.Options, schema string) (storage.Storage, error) {
				gormOpts := new(xgorm.Options).With(
					xgorm.Schema(schema),
					xgorm.DSN(dsn, func(dsn string) gorm.Dialector { return mysqldriver.Open(dsn) }),
				)
				return gormOpts.OpenStorage(opts, storage.RoleData)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := storagetest.Schema("schema_mysql_" + test.name)
			stor, err := test.open(schemaContractOptions(), schema)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				closeStorage(stor)
				dropSQLStorage(t, db, "mysql", schema)
			})

			for _, table := range []string{"published", "received"} {
				t.Run(table, func(t *testing.T) {
					name := schema + "_" + table
					got := readMySQLCatalog(t, db, name)
					assertCriticalTableSchema(t, "mysql", table, got)
				})
			}
		})
	}
}

func TestPostgresFinalSchemaContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_POSTGRES_DSN")
	t.Parallel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name string
		open func(*ingap.Options, string) (storage.Storage, error)
	}{
		{
			name: "native",
			open: func(opts *ingap.Options, schema string) (storage.Storage, error) {
				return (&xpostgres.Options{Schema: schema, DSN: dsn}).OpenStorage(opts, storage.RoleData)
			},
		},
		{
			name: "gorm",
			open: func(opts *ingap.Options, schema string) (storage.Storage, error) {
				gormOpts := new(xgorm.Options).With(
					xgorm.Schema(schema),
					xgorm.DSN(dsn, func(dsn string) gorm.Dialector { return postgresdriver.Open(dsn) }),
				)
				return gormOpts.OpenStorage(opts, storage.RoleData)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := storagetest.Schema("schema_postgres_" + test.name)
			stor, err := test.open(schemaContractOptions(), schema)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				closeStorage(stor)
				dropSQLStorage(t, db, "postgres", schema)
			})

			for _, table := range []string{"published", "received"} {
				t.Run(table, func(t *testing.T) {
					got := readPostgresCatalog(t, db, schema, table)
					assertCriticalTableSchema(t, "postgres", table, got)
				})
			}
		})
	}
}

func schemaContractOptions() *ingap.Options {
	opts := new(ingap.Options).With(ingap.Version("schema-contract"))
	opts.InitRuntime()
	return opts
}

func readMySQLCatalog(t *testing.T, db *sql.DB, table string) catalogTable {
	t.Helper()
	got := catalogTable{
		columns: map[string]catalogColumn{},
		indexes: map[string][]string{},
	}

	rows, err := db.Query(`
SELECT column_name, data_type, column_type, is_nullable, column_default,
       character_maximum_length, extra
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ?
ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, nullable, extra string
		var column catalogColumn
		if err := rows.Scan(
			&name,
			&column.dataType,
			&column.columnType,
			&nullable,
			&column.defaultValue,
			&column.maxLength,
			&extra,
		); err != nil {
			t.Fatal(err)
		}
		column.dataType = strings.ToLower(column.dataType)
		column.columnType = strings.ToLower(column.columnType)
		column.nullable = nullable == "YES"
		column.generated = strings.Contains(strings.ToLower(extra), "auto_increment")
		got.columns[name] = column
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`
SELECT index_name, column_name
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ?
ORDER BY index_name, seq_in_index`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var index, column string
		if err := rows.Scan(&index, &column); err != nil {
			t.Fatal(err)
		}
		got.indexes[index] = append(got.indexes[index], column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	got.primaryKey = got.indexes["PRIMARY"]
	delete(got.indexes, "PRIMARY")
	return got
}

func readPostgresCatalog(t *testing.T, db *sql.DB, schema, table string) catalogTable {
	t.Helper()
	got := catalogTable{
		columns: map[string]catalogColumn{},
		indexes: map[string][]string{},
	}

	rows, err := db.Query(`
SELECT column_name, data_type, udt_name, is_nullable, column_default,
       character_maximum_length, is_identity
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`, schema, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, nullable, identity string
		var column catalogColumn
		if err := rows.Scan(
			&name,
			&column.dataType,
			&column.udtName,
			&nullable,
			&column.defaultValue,
			&column.maxLength,
			&identity,
		); err != nil {
			t.Fatal(err)
		}
		column.dataType = strings.ToLower(column.dataType)
		column.udtName = strings.ToLower(column.udtName)
		column.nullable = nullable == "YES"
		column.generated = identity == "YES" ||
			column.defaultValue.Valid && strings.Contains(strings.ToLower(column.defaultValue.String), "nextval(")
		got.columns[name] = column
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`
SELECT index_class.relname, attribute.attname
FROM pg_catalog.pg_class AS table_class
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = table_class.relnamespace
JOIN pg_catalog.pg_index AS index_meta ON index_meta.indrelid = table_class.oid
JOIN pg_catalog.pg_class AS index_class ON index_class.oid = index_meta.indexrelid
CROSS JOIN LATERAL unnest(index_meta.indkey) WITH ORDINALITY AS key(attnum, position)
JOIN pg_catalog.pg_attribute AS attribute
  ON attribute.attrelid = table_class.oid AND attribute.attnum = key.attnum
WHERE namespace.nspname = $1 AND table_class.relname = $2
  AND key.position <= index_meta.indnkeyatts
ORDER BY index_class.relname, key.position`, schema, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var index, column string
		if err := rows.Scan(&index, &column); err != nil {
			t.Fatal(err)
		}
		got.indexes[index] = append(got.indexes[index], column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`
SELECT key_column.column_name
FROM information_schema.table_constraints AS constraint_info
JOIN information_schema.key_column_usage AS key_column
  ON key_column.constraint_catalog = constraint_info.constraint_catalog
 AND key_column.constraint_schema = constraint_info.constraint_schema
 AND key_column.constraint_name = constraint_info.constraint_name
WHERE constraint_info.table_schema = $1 AND constraint_info.table_name = $2
  AND constraint_info.constraint_type = 'PRIMARY KEY'
ORDER BY key_column.ordinal_position`, schema, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
		got.primaryKey = append(got.primaryKey, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	rows, err = db.Query(`
SELECT enum_value.enumlabel
FROM pg_catalog.pg_type AS enum_type
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = enum_type.typnamespace
JOIN pg_catalog.pg_enum AS enum_value ON enum_value.enumtypid = enum_type.oid
WHERE namespace.nspname = $1 AND enum_type.typname = 'status_enum'
ORDER BY enum_value.enumsortorder`, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		got.statusEnum = append(got.statusEnum, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func assertCriticalTableSchema(t *testing.T, dialect, table string, got catalogTable) {
	t.Helper()

	nullability := map[string]bool{
		"id":              false,
		"created_at":      false,
		"version":         false,
		"topic":           false,
		"broker_name":     false,
		"status":          false,
		"headers":         true,
		"payload":         true,
		"retries":         false,
		"lease_token":     true,
		"lease_until":     true,
		"next_attempt_at": false,
		"expired_at":      true,
	}
	if table == "received" {
		nullability["group"] = false
	}
	for name, nullable := range nullability {
		column, ok := got.columns[name]
		if !ok {
			t.Errorf("column %q is missing", name)
			continue
		}
		if column.nullable != nullable {
			t.Errorf("column %q nullable = %t, want %t", name, column.nullable, nullable)
		}
	}

	lengths := map[string]int64{
		"version":     16,
		"topic":       256,
		"broker_name": 128,
		"lease_token": 32,
	}
	if table == "received" {
		lengths["group"] = 128
	}
	for name, length := range lengths {
		column := requireCatalogColumn(t, got, name)
		if !column.maxLength.Valid || column.maxLength.Int64 != length {
			t.Errorf("column %q maximum length = %v, want %d", name, column.maxLength, length)
		}
	}

	id := requireCatalogColumn(t, got, "id")
	if !id.generated {
		t.Error("id is not generated by the database")
	}
	if !slices.Equal(got.primaryKey, []string{"id"}) {
		t.Errorf("primary key columns = %v, want [id]", got.primaryKey)
	}

	retries := requireCatalogColumn(t, got, "retries")
	if !isIntegerType(retries.dataType) {
		t.Errorf("retries type = %q, want an integer type", retries.dataType)
	}
	if !isZeroDefault(retries.defaultValue) {
		t.Errorf("retries default = %v, want numeric zero", retries.defaultValue)
	}

	payload := requireCatalogColumn(t, got, "payload")
	status := requireCatalogColumn(t, got, "status")
	switch dialect {
	case "mysql":
		if payload.dataType != "longtext" {
			t.Errorf("payload type = %q, want longtext", payload.dataType)
		}
		if status.dataType != "enum" {
			t.Errorf("status type = %q, want enum", status.dataType)
		}
		for _, value := range []string{"pending", "succeeded", "failed"} {
			if !strings.Contains(status.columnType, "'"+value+"'") {
				t.Errorf("status column type %q does not contain %q", status.columnType, value)
			}
		}
	case "postgres":
		if payload.dataType != "text" {
			t.Errorf("payload type = %q, want text", payload.dataType)
		}
		if status.dataType != "user-defined" || status.udtName != "status_enum" {
			t.Errorf("status type = %q/%q, want user-defined/status_enum", status.dataType, status.udtName)
		}
		if !slices.Equal(got.statusEnum, []string{"Pending", "Succeeded", "Failed"}) {
			t.Errorf("status enum values = %v, want [Pending Succeeded Failed]", got.statusEnum)
		}
	default:
		t.Fatalf("unsupported schema contract dialect %q", dialect)
	}

	claim := "idx_" + table + "_claim"
	wantClaim := []string{"version", "status", "broker_name", "next_attempt_at", "lease_until", "retries"}
	if !slices.Equal(got.indexes[claim], wantClaim) {
		t.Errorf("index %q columns = %v, want %v", claim, got.indexes[claim], wantClaim)
	}
	cleanup := "idx_" + table + "_cleanup"
	wantCleanup := []string{"expired_at", "id"}
	if !slices.Equal(got.indexes[cleanup], wantCleanup) {
		t.Errorf("index %q columns = %v, want %v", cleanup, got.indexes[cleanup], wantCleanup)
	}
}

func requireCatalogColumn(t *testing.T, table catalogTable, name string) catalogColumn {
	t.Helper()
	column, ok := table.columns[name]
	if !ok {
		t.Fatalf("column %q is missing", name)
	}
	return column
}

func isIntegerType(dataType string) bool {
	switch dataType {
	case "smallint", "integer", "int", "bigint", "tinyint", "mediumint":
		return true
	default:
		return false
	}
}

func isZeroDefault(value sql.NullString) bool {
	if !value.Valid {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(value.String))
	return normalized == "0" || normalized == "'0'" ||
		strings.HasPrefix(normalized, "0::") || strings.HasPrefix(normalized, "'0'::")
}
