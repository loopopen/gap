package contracttest

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/loopopen/gap"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/internal/storagetest"
	"github.com/loopopen/gap/storage/xgorm"
	"github.com/loopopen/gap/storage/xmysql"
	"github.com/loopopen/gap/storage/xpostgres"
	"github.com/loopopen/gap/storage/xsql"
	mysqldriver "gorm.io/driver/mysql"
	postgresdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestMySQLContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_MYSQL_DSN")
	t.Parallel()
	storagetest.Run(t, sqlFactory("mysql", dsn, func(gapOpts *ingap.Options, schema string) (storage.Storage, error) {
		return (&xmysql.Options{Schema: schema, DSN: dsn}).OpenStorage(gapOpts, storage.RoleData)
	}))
}

func TestPostgresContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_POSTGRES_DSN")
	t.Parallel()
	storagetest.Run(t, sqlFactory("pgx", dsn, func(gapOpts *ingap.Options, schema string) (storage.Storage, error) {
		return (&xpostgres.Options{Schema: schema, DSN: dsn}).OpenStorage(gapOpts, storage.RoleData)
	}))
}

func TestGormMySQLContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_MYSQL_DSN")
	t.Parallel()
	runGormContract(t, "xgorm_mysql", dsn, func() gorm.Dialector { return mysqldriver.Open(dsn) })
}

func TestGormPostgresContract(t *testing.T) {
	dsn := storagetest.RequireDSN(t, "GAP_TEST_POSTGRES_DSN")
	t.Parallel()
	runGormContract(t, "xgorm_postgres", dsn, func() gorm.Dialector { return postgresdriver.Open(dsn) })
}

type openSQLStorage func(*ingap.Options, string) (storage.Storage, error)

func sqlFactory(driver, dsn string, openStorage openSQLStorage) storagetest.Factory {
	return func(t *testing.T, gapOpts *ingap.Options) storagetest.Harness {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.PingContext(context.Background()); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}

		schema := storagetest.Schema(driver)
		stor, err := openStorage(gapOpts, schema)
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			dropSQLStorage(t, db, driver, schema)
			closeStorage(stor)
			_ = db.Close()
		})

		return storagetest.Harness{
			Storage: stor,
			Transact: func(ctx context.Context, action func(storage.Storage) error) error {
				return xsql.DoInTx(ctx, func(_ context.Context, tx gap.Txer) error {
					bound, err := stor.Bind(tx)
					if err != nil {
						return err
					}
					return action(bound)
				}, db)
			},
		}
	}
}

func runGormContract(t *testing.T, prefix, dsn string, dialector func() gorm.Dialector) {
	storagetest.Run(t, func(t *testing.T, gapOpts *ingap.Options) storagetest.Harness {
		db, err := gorm.Open(dialector(), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		if err := sqlDB.PingContext(context.Background()); err != nil {
			_ = sqlDB.Close()
			t.Fatal(err)
		}

		schema := storagetest.Schema(prefix)
		opts := new(xgorm.Options).With(
			xgorm.Schema(schema),
			xgorm.DSN(dsn, func(string) gorm.Dialector { return dialector() }),
		)
		stor, err := opts.OpenStorage(gapOpts, storage.RoleData)
		if err != nil {
			_ = sqlDB.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			dropSQLStorage(t, sqlDB, db.Dialector.Name(), schema)
			closeStorage(stor)
			_ = sqlDB.Close()
		})

		return storagetest.Harness{
			Storage: stor,
			Transact: func(ctx context.Context, action func(storage.Storage) error) error {
				return xgorm.DoInTx(ctx, func(_ context.Context, tx gap.Txer) error {
					bound, err := stor.Bind(tx)
					if err != nil {
						return err
					}
					return action(bound)
				}, db)
			},
		}
	})
}

func closeStorage(stor storage.Storage) {
	if c, ok := stor.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

func dropSQLStorage(t *testing.T, db *sql.DB, driver, schema string) {
	t.Helper()
	var statement string
	switch driver {
	case "mysql":
		statement = fmt.Sprintf("DROP TABLE IF EXISTS `%s_received`, `%s_published`", schema, schema)
	case "pgx", "postgres":
		statement = fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schema)
	default:
		t.Errorf("unsupported contract cleanup driver %q", driver)
		return
	}
	if _, err := db.Exec(statement); err != nil {
		t.Errorf("clean contract schema %q: %v", schema, err)
	}
}
