# Changelog

## v1.0.0

Stable release of the core module and the bundled broker and storage modules. Install every module you use at `v1.0.0`. Upgrading from v1.0.0-beta.7 is described in [UPGRADING.md](UPGRADING.md) and [UPGRADING.zh-cn.md](UPGRADING.zh-cn.md).

### Changed

- The Snowflake node is set with `gap.SetWorkerID`. `gap.WorkerID` is no longer an option. The latest successful call is used. If it is never called, the first `gap.New` derives a node from a MAC address, or from a random value when no address is available.
- GORM storage opens its own pool from a DSN and a dialector. `xgorm.DB` is removed. Business transactions still use the application's `*gorm.DB` through `DoInTx`.
- MySQL, PostgreSQL, and GORM share one connection pool per DSN and role. The pool closes when the last runtime releases it. The dashboard uses a separate pool.
- A handler that omits its consumer group, including `gap.HandleTopic`, uses `gap.DefaultGroup` (`default`). A group passed explicitly, including a group filled in by `gapc`, stays as written.
- `gap.OptionsGetter` is removed. `gap.BrokerOptionsGetter` remains the type used by `gap.From`.

### Unchanged

- Tables created by v1.0.0-beta.7 are the v1 schema. Opening storage runs `CREATE TABLE IF NOT EXISTS` and does not alter an existing table.
- A row is claimed only for a broker name, and for received rows a group, that this process registered. A missing sender or handler for a claimed row returns it to pending. A registered group with no handler for that topic, a handler panic, invalid JSON, and `gap.Permanent` become failed.
