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
	"github.com/loopopen/gap/internal/plugin"
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

type Factory struct {
	pools sync.Map
}

func (f *Factory) CreateStorage(gapOpts *gap.Options) (storage.Storage, error) {
	opts, err := gormOptions(gapOpts)
	if err != nil {
		return nil, err
	}
	pools := f.getPools(opts)
	dataDB, err := pools.loadData(gapOpts, opts)
	if err != nil {
		return nil, err
	}
	return createStorage(gapOpts, dataDB)
}

func (f *Factory) CreateDashboardStorage(gapOpts *gap.Options) (storage.Storage, error) {
	opts, err := gormOptions(gapOpts)
	if err != nil {
		return nil, err
	}

	pools := f.getPools(opts)
	dataDB, err := pools.loadData(gapOpts, opts)
	if err != nil {
		return nil, err
	}
	pools.dashboardOnce.Do(func() {
		var openErr error
		pools.dashboardDB, _, openErr = openDashboardDB(opts, dataDB)
		if openErr != nil {
			slog.Warn("dashboard is sharing the data-plane gorm pool", slog.Any("err", openErr))
			pools.dashboardDB = dataDB
		}
	})

	return createStorage(gapOpts, pools.dashboardDB)
}

func (f *Factory) getPools(opts *Options) *gormPools {
	value, _ := f.pools.LoadOrStore(opts, new(gormPools))
	return value.(*gormPools)
}

func (p *gormPools) loadData(gapOpts *gap.Options, opts *Options) (*gorm.DB, error) {
	p.dataOnce.Do(func() {
		if opts.DB != nil {
			p.dataDB = opts.DB
			return
		}
		dialector := opts._dialector(opts._dsn)
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

func createStorage(gapOpts *gap.Options, db *gorm.DB) (storage.Storage, error) {
	stor := NewStorage(gapOpts, db)
	if err := stor.init(); err != nil {
		slog.Error("failed to init storage", slog.Any("err", err))
		return nil, err
	}
	return stor, nil
}

func gormOptions(gapOpts *gap.Options) (*Options, error) {
	o := gapOpts.StorageOptions
	if o == nil {
		return nil, errors.New("no storage plugin configured")
	}
	if o.PluginType() != enum.PluginGORM {
		return nil, errors.New("storage plugin does not match")
	}
	opts := o.(*Options)
	hasDB := opts.DB != nil
	hasDSN := strings.TrimSpace(opts._dsn) != ""
	hasDialector := opts._dialector != nil
	if hasDSN != hasDialector {
		return nil, errors.New("gorm dsn and dialector must be configured together")
	}
	if hasDB == hasDSN {
		return nil, errors.New("configure either gorm db or dsn with dialector")
	}
	return opts, nil
}

func openDashboardDB(opts *Options, dataDB *gorm.DB) (*gorm.DB, bool, error) {
	dataSQLDB, err := dataDB.DB()
	if err != nil {
		return nil, false, fmt.Errorf("get data-plane sql db: %w", err)
	}

	dialector := dataDB.Dialector
	if opts._dialector != nil {
		dialector = opts._dialector(opts._dsn)
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
	plugin.Register[storage.FactoryIface](enum.PluginGORM, &Factory{})
	dashboard.AddMeta(enum.PluginKindStorage, enum.PluginGORM, version)
}

var (
	_ storage.FactoryIface     = (*Factory)(nil)
	_ storage.DashboardFactory = (*Factory)(nil)
)
