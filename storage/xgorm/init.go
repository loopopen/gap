package xgorm

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"

	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/gap/storage/xsql"
	"gorm.io/gorm"
)

type gormPools struct {
	dataOnce sync.Once
	dataDB   *gorm.DB
	dataErr  error

	dashboardOnce sync.Once
	dashboardDB   *gorm.DB
}

var gormPoolCache sync.Map

func (o *Options) OpenStorage(gapOpts *gap.Options, role storage.Role) (storage.Storage, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	pools := poolsFor(o)
	dataDB, err := pools.loadData(gapOpts, o)
	if err != nil {
		return nil, err
	}
	if role != storage.RoleDashboard {
		return createStorage(gapOpts, o, dataDB)
	}

	pools.dashboardOnce.Do(func() {
		var openErr error
		pools.dashboardDB, _, openErr = openDashboardDB(o, dataDB)
		if openErr != nil {
			slog.Warn("dashboard is sharing the data-plane gorm pool", slog.Any("err", openErr))
			pools.dashboardDB = dataDB
		}
	})
	return createStorage(gapOpts, o, pools.dashboardDB)
}

func poolsFor(opts *Options) *gormPools {
	value, _ := gormPoolCache.LoadOrStore(opts, new(gormPools))
	return value.(*gormPools)
}

func (p *gormPools) loadData(gapOpts *gap.Options, opts *Options) (*gorm.DB, error) {
	p.dataOnce.Do(func() {
		if opts.DB != nil {
			p.dataDB = opts.DB
			return
		}
		dialector := opts.dialector(opts.dsn)
		if dialector == nil {
			p.dataErr = errors.New("gorm dialector is nil")
			return
		}
		p.dataDB, p.dataErr = gorm.Open(dialector)
		if p.dataErr != nil {
			p.dataErr = fmt.Errorf("open data-plane gorm db: %w", p.dataErr)
			return
		}
		sqlDB, err := p.dataDB.DB()
		if err != nil {
			p.dataErr = fmt.Errorf("get data-plane sql db: %w", err)
			return
		}
		xsql.ConfigureDataPool(sqlDB, xsql.DataPlaneMaxOpen(gapOpts))
	})
	return p.dataDB, p.dataErr
}

func createStorage(gapOpts *gap.Options, opts *Options, db *gorm.DB) (storage.Storage, error) {
	stor := NewStorage(gapOpts, opts, db)
	if err := stor.init(); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func (o *Options) validate() error {
	hasDB := o.DB != nil
	hasDSN := strings.TrimSpace(o.dsn) != ""
	hasDialector := o.dialector != nil
	if hasDSN != hasDialector {
		return errors.New("gorm dsn and dialector must be configured together")
	}
	if hasDB == hasDSN {
		return errors.New("configure either gorm db or dsn with dialector")
	}
	return nil
}

func openDashboardDB(opts *Options, dataDB *gorm.DB) (*gorm.DB, bool, error) {
	dataSQLDB, err := dataDB.DB()
	if err != nil {
		return nil, false, fmt.Errorf("get data-plane sql db: %w", err)
	}

	dialector := dataDB.Dialector
	if opts.dialector != nil {
		dialector = opts.dialector(opts.dsn)
	}
	if dialector == nil {
		return nil, false, errors.New("dashboard gorm dialector is nil")
	}

	dashboardDB, err := gorm.Open(dialector, cloneGormConfig(dataDB.Config))
	if err != nil {
		return nil, false, fmt.Errorf("open dashboard gorm db: %w", err)
	}
	dashboardSQLDB, err := dashboardDB.DB()
	if err != nil {
		return nil, false, fmt.Errorf("get dashboard sql db: %w", err)
	}
	if dashboardSQLDB == dataSQLDB {
		return dataDB, false, errors.New("gorm dialector reused the data-plane sql db")
	}

	xsql.ConfigureDashboardPool(dashboardSQLDB)
	return dashboardDB, true, nil
}

func cloneGormConfig(src *gorm.Config) *gorm.Config {
	return &gorm.Config{
		SkipDefaultTransaction:                   src.SkipDefaultTransaction,
		DefaultTransactionTimeout:                src.DefaultTransactionTimeout,
		DefaultContextTimeout:                    src.DefaultContextTimeout,
		NamingStrategy:                           src.NamingStrategy,
		FullSaveAssociations:                     src.FullSaveAssociations,
		Logger:                                   src.Logger,
		NowFunc:                                  src.NowFunc,
		DryRun:                                   src.DryRun,
		PrepareStmt:                              src.PrepareStmt,
		PrepareStmtMaxSize:                       src.PrepareStmtMaxSize,
		PrepareStmtTTL:                           src.PrepareStmtTTL,
		DisableAutomaticPing:                     src.DisableAutomaticPing,
		DisableForeignKeyConstraintWhenMigrating: src.DisableForeignKeyConstraintWhenMigrating,
		IgnoreRelationshipsWhenMigrating:         src.IgnoreRelationshipsWhenMigrating,
		DisableNestedTransaction:                 src.DisableNestedTransaction,
		AllowGlobalUpdate:                        src.AllowGlobalUpdate,
		QueryFields:                              src.QueryFields,
		CreateBatchSize:                          src.CreateBatchSize,
		TranslateError:                           src.TranslateError,
		PropagateUnscoped:                        src.PropagateUnscoped,
		ClauseBuilders:                           maps.Clone(src.ClauseBuilders),
		Plugins:                                  maps.Clone(src.Plugins),
	}
}

func init() {
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginGORM, version)
}
