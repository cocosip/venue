package venue

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/internal/directorypath"
	"github.com/cocosip/venue/pkg/cleanup"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/health"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/pool"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/recovery"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/statistics"
	"github.com/cocosip/venue/pkg/tenant"
	"github.com/cocosip/venue/pkg/volume"
	"github.com/cocosip/venue/pkg/watcher"
)

// Venue is the main container that manages all components and their lifecycle.
// It provides dependency injection similar to .NET Core's service container.
type Venue struct {
	config  *config.Config
	logger  *logging.Runtime
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
	running bool
	closed  bool

	// Core components
	tenantManager          core.TenantManager
	metadataRepo           core.MetadataRepository
	dirQuotaRepo           core.DirectoryQuotaRepository
	tenantQuotaManager     core.TenantQuotaManager
	dirQuotaManager        core.DirectoryQuotaManager
	volumes                map[string]core.StorageVolume
	fileScheduler          core.FileScheduler
	storagePool            core.StoragePool
	cleanupServiceCore     core.CleanupService
	fileWatcherCore        core.FileWatcher
	fileWatcherAutoManager core.FileWatcherAutoManager
	databaseHealthChecker  core.DatabaseHealthChecker
	orphanRecoveryCore     core.OrphanRecoveryService

	// Background services
	cleanupService     *cleanup.BackgroundCleanupService
	fileWatcherService *watcher.BackgroundFileWatcherService
	healthCheckService *health.DatabaseHealthCheckService
	orphanRecovery     *recovery.OrphanRecoveryService
	metadataBackupCore *MetadataBackupService

	// statisticsRecorder is the runtime's recorder: statistics.Noop when
	// statistics are disabled, so every instrumented call site can forward
	// unconditionally.
	statisticsRecorder core.StatisticsRecorder
	statisticsOutput   *statistics.OutputService

	// recoveryMu guards incompleteRecoveries, which records metadata databases
	// that were quarantined during startup and could not be restored from a
	// backup. The callbacks that append run on the repository-opening goroutine.
	recoveryMu           sync.Mutex
	incompleteRecoveries []string
}

// NewVenue creates a new Venue instance with the given configuration.
// This is the main entry point for initializing the entire system.
func NewVenue(cfg *config.Config) (*Venue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil: %w", core.ErrInvalidArgument)
	}

	runtimeConfig := cfg.Clone()
	runtimeConfig.ApplyDefaults()
	if err := runtimeConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger := logging.Disabled()
	if runtimeConfig.Logging != nil {
		var err error
		logger, err = logging.New(*runtimeConfig.Logging)
		if err != nil {
			return nil, fmt.Errorf("invalid logging configuration: %w", err)
		}
	}

	v := &Venue{
		config: runtimeConfig,
		logger: logger,
	}

	// Initialize all components
	if err := v.initialize(); err != nil {
		// NewVenue owns every resource it opened before the failure; release
		// them so a failed construction cannot strand a database lock.
		v.closeRepositories()
		return nil, fmt.Errorf("failed to initialize venue: %w", err)
	}

	v.emit(context.Background(), slog.LevelInfo, "initialized", "Venue initialized successfully")

	return v, nil
}

// initializeStatistics builds the runtime statistics recorder and the optional
// periodic output service from configuration.
//
// Statistics are disabled by default: a disabled configuration installs the noop
// recorder, so every instrumented call site forwards unconditionally and pays
// nothing.
func (v *Venue) initializeStatistics() error {
	options := statisticsOptions(v.config.Statistics)
	if !options.Enabled {
		v.statisticsRecorder = statistics.Noop
		return nil
	}

	recorder, err := statistics.NewRecorder(options)
	if err != nil {
		return fmt.Errorf("invalid statistics configuration: %w", err)
	}
	v.statisticsRecorder = recorder

	if options.Output.Enabled {
		v.statisticsOutput = statistics.NewOutputService(recorder, options, v.logger)
	}

	return nil
}

// statisticsOptions maps the public statistics configuration onto the
// statistics package options.
func statisticsOptions(cfg config.StatisticsConfig) statistics.Options {
	return statistics.Options{
		Enabled:    cfg.Enabled,
		WindowSize: cfg.WindowSize,
		Retention:  cfg.Retention,
		MaxSeries:  cfg.MaxSeries,
		Dimensions: statistics.DimensionOptions{
			TenantID:  cfg.Dimensions.TenantID,
			VolumeID:  cfg.Dimensions.VolumeID,
			WatcherID: cfg.Dimensions.WatcherID,
			Operation: cfg.Dimensions.Operation,
		},
		Output: statistics.OutputOptions{
			Enabled:               cfg.Output.Enabled,
			Sink:                  cfg.Output.Sink,
			Interval:              cfg.Output.Interval,
			QueryWindow:           cfg.Output.QueryWindow,
			IncludeEmptySnapshots: cfg.Output.IncludeEmptySnapshots,
		},
	}
}

// maxVolumeHealthCheckAttempts bounds the startup health retries for one volume,
// matching Locus (locus/src/Locus.Storage/StoragePool.cs:164-209).
const maxVolumeHealthCheckAttempts = 10

// prepareVolumes waits for every configured volume to become healthy and performs
// the optional warm-up write.
//
// A volume that answers the first probe is never delayed: the configured
// InitialDelay applies before the first *retry*, so an always-healthy local
// volume costs one probe. A volume that does not answer is retried up to
// maxVolumeHealthCheckAttempts times, separated by HealthCheckDelay, and must
// answer two consecutive probes to count as healthy.
func (v *Venue) prepareVolumes(ctx context.Context) error {
	for _, volConfig := range v.config.Volumes {
		volume, ok := v.volumes[volConfig.VolumeID]
		if !ok {
			continue
		}

		if !volumeProbe(ctx, volume) {
			if err := v.waitForVolume(ctx, volume, volConfig); err != nil {
				return err
			}
		}

		v.warmVolume(ctx, volume, volConfig)
	}

	return nil
}

// volumeProbe probes a volume immediately, preferring the forced-probe
// capability over a possibly cached IsHealthy answer.
func volumeProbe(ctx context.Context, volume core.StorageVolume) bool {
	if probe, ok := volume.(core.StorageVolumeHealthProbe); ok {
		return probe.ProbeHealth(ctx)
	}
	return volume.IsHealthy(ctx)
}

// waitForVolume retries the health probe of one volume until it is healthy or the
// attempt budget is exhausted.
//
// Errors:
//   - ErrStorageVolumeUnavailable when the volume never answered two consecutive
//     probes
//   - the context error when startup was cancelled while waiting
func (v *Venue) waitForVolume(ctx context.Context, volume core.StorageVolume, volConfig config.VolumeConfig) error {
	v.emit(ctx, slog.LevelWarn, "volume_not_ready",
		"Volume did not answer its first health probe; waiting before retrying",
		slog.String("volume_id", volConfig.VolumeID))

	if err := sleepContext(ctx, volConfig.InitialDelay); err != nil {
		return err
	}

	healthy := 0
	for attempt := 1; attempt <= maxVolumeHealthCheckAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if volumeProbe(ctx, volume) {
			healthy++
			if healthy >= 2 {
				v.emit(ctx, slog.LevelInfo, "volume_ready", "Volume became healthy",
					slog.String("volume_id", volConfig.VolumeID),
					slog.Int("attempts", attempt))
				return nil
			}
		} else {
			healthy = 0
		}

		if attempt < maxVolumeHealthCheckAttempts {
			if err := sleepContext(ctx, volConfig.HealthCheckDelay); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf(
		"volume %s is not healthy after %d attempts: %w",
		volConfig.VolumeID,
		maxVolumeHealthCheckAttempts,
		core.ErrStorageVolumeUnavailable,
	)
}

// warmVolume performs the optional warm-up write for one volume. Warm-up is
// advisory: a failure is reported and never unmounts or disables the volume.
func (v *Venue) warmVolume(ctx context.Context, volume core.StorageVolume, volConfig config.VolumeConfig) {
	if !volConfig.WarmupOnStartup {
		return
	}

	warmup, ok := volume.(core.StorageVolumeWritePathWarmup)
	if !ok {
		return
	}

	if err := warmup.WarmWritePathCache(ctx); err != nil {
		v.emit(ctx, slog.LevelWarn, "volume_warmup_failed",
			"Volume write-path warm-up failed",
			slog.String("volume_id", volConfig.VolumeID),
			errorTypeAttr(err))
		return
	}

	v.emit(ctx, slog.LevelInfo, "volume_warmed_up", "Volume write path warmed up",
		slog.String("volume_id", volConfig.VolumeID))
}

// sleepContext waits for d or until ctx is done, whichever comes first. A
// non-positive duration waits for nothing but still observes cancellation.
func sleepContext(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// initialize initializes all components in the correct order using configuration.
func (v *Venue) initialize() error {
	ctx := context.Background()

	// 0. Initialize statistics. The recorder exists before every instrumented
	// component so no operation is recorded into a missing recorder.
	v.emit(ctx, slog.LevelInfo, "statistics_initializing", "Initializing runtime statistics")
	if err := v.initializeStatistics(); err != nil {
		return err
	}

	// 1. Initialize tenant manager
	v.emit(ctx, slog.LevelInfo, "tenant_manager_initializing", "Initializing tenant manager")
	tenantMgr, err := tenant.NewTenantManager(&tenant.TenantManagerOptions{
		RootPath:         v.config.MetadataDirectory,
		MetadataPath:     v.config.TenantManager.MetadataPath,
		CacheTTL:         v.config.TenantManager.CacheTTL,
		EnableAutoCreate: v.config.AutoCreateTenants,
		Logging:          v.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create tenant manager: %w", err)
	}
	v.tenantManager = tenantMgr

	// Create tenants from configuration
	if len(v.config.Tenants) > 0 {
		v.emit(ctx, slog.LevelInfo, "tenants_configuring", "Creating tenants from configuration", slog.Int("count", len(v.config.Tenants)))
		for _, tenantCfg := range v.config.Tenants {
			if err := v.tenantManager.CreateTenant(ctx, tenantCfg.TenantID); err != nil && err != core.ErrTenantAlreadyExists {
				return fmt.Errorf("failed to create tenant %s: %w", tenantCfg.TenantID, err)
			}

			// Enable or disable tenant based on configuration
			if tenantCfg.Enabled {
				if err := v.tenantManager.EnableTenant(ctx, tenantCfg.TenantID); err != nil {
					return fmt.Errorf("failed to enable tenant %s: %w", tenantCfg.TenantID, err)
				}
				v.emit(ctx, slog.LevelInfo, "tenant_enabled", "Tenant enabled", slog.String("tenant_id", tenantCfg.TenantID))
			} else {
				if err := v.tenantManager.DisableTenant(ctx, tenantCfg.TenantID); err != nil {
					return fmt.Errorf("failed to disable tenant %s: %w", tenantCfg.TenantID, err)
				}
				v.emit(ctx, slog.LevelInfo, "tenant_disabled", "Tenant disabled", slog.String("tenant_id", tenantCfg.TenantID))
			}
		}
	}

	// 2. Initialize metadata repository
	v.emit(ctx, slog.LevelInfo, "metadata_repository_initializing", "Initializing metadata repository")
	metadataOptions := sqliteRepositoryOptions(v.config)
	metadataOptions.OnCorruptedDatabase = v.quarantineReporter()
	metadataOptions.OnRecoveryIncomplete = v.recoveryIncompleteReporter()
	metadataOptions.Logging = v.logger
	metadataOptions.StatisticsRecorder = v.statisticsRecorder
	metaRepo, err := metadata.NewSQLiteMetadataRepository(metadataOptions)
	if err != nil {
		return fmt.Errorf("failed to create metadata repository: %w", err)
	}
	v.metadataRepo = metaRepo

	// 2b. Initialize the periodic metadata backup runner (disabled unless a
	// backup directory and a positive interval are configured).
	if v.config.Sqlite.BackupDirectory != "" && v.config.Sqlite.BackupInterval > 0 {
		backupService, err := newMetadataBackupService(
			metaRepo,
			v.config.Sqlite.BackupDirectory,
			v.config.Sqlite.BackupInterval,
			v.config.Sqlite.BackupRetention,
			v.logger,
		)
		if err != nil {
			return fmt.Errorf("failed to create metadata backup service: %w", err)
		}
		v.metadataBackupCore = backupService
		v.emit(ctx, slog.LevelInfo, "metadata_backup_configured", "Periodic metadata backups configured",
			slog.Duration("interval", v.config.Sqlite.BackupInterval))
	}

	// 3. Initialize quota managers
	v.emit(ctx, slog.LevelInfo, "quota_managers_initializing", "Initializing quota managers")
	v.tenantQuotaManager = quota.NewTenantQuotaManager(int(v.config.DefaultTenantQuota))
	// Set tenant quotas from configuration
	for _, tenantCfg := range v.config.Tenants {
		if tenantCfg.Quota != nil {
			if err := v.tenantQuotaManager.SetQuota(ctx, tenantCfg.TenantID, int(*tenantCfg.Quota)); err != nil {
				return fmt.Errorf("failed to set quota for tenant %s: %w", tenantCfg.TenantID, err)
			}
			if *tenantCfg.Quota > 0 {
				v.emit(ctx, slog.LevelInfo, "tenant_quota_set", "Tenant quota set", slog.String("tenant_id", tenantCfg.TenantID), slog.Int64("quota", *tenantCfg.Quota))
			} else {
				v.emit(ctx, slog.LevelInfo, "tenant_quota_unlimited", "Tenant quota unlimited", slog.String("tenant_id", tenantCfg.TenantID))
			}
		} else if v.config.DefaultTenantQuota > 0 {
			// No per-tenant override: the manager's global limit already applies
			// to this tenant, so materializing an override here would freeze the
			// tenant against a later SetGlobalLimit.
			v.emit(ctx, slog.LevelInfo, "tenant_quota_default_applies", "Tenant uses the global quota limit", slog.String("tenant_id", tenantCfg.TenantID), slog.Int64("quota", v.config.DefaultTenantQuota))
		}
	}
	if err := v.reconcileTenantQuotaCounts(ctx); err != nil {
		return fmt.Errorf("failed to reconcile tenant quotas: %w", err)
	}

	dirQuotaRepo, err := quota.NewSQLiteDirectoryQuotaRepository(&quota.SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                   v.config.QuotaDirectory,
		Sqlite:                     sqliteOptions(v.config.Sqlite),
		MaxOpenDatabases:           v.config.Sqlite.MaxOpenDatabases,
		OpenDatabaseIdleTimeout:    v.config.Sqlite.OpenDatabaseIdleTimeout,
		RecoverCorruptedDatabase:   v.config.Sqlite.RecoverCorruptedDatabase,
		CorruptedDatabaseRetention: v.config.Sqlite.CorruptedDatabaseRetention,
		Logging:                    v.logger,
		OnCorruptedDatabase:        v.quarantineReporter(),
	})
	if err != nil {
		return fmt.Errorf("failed to create directory quota repository: %w", err)
	}
	v.dirQuotaRepo = dirQuotaRepo

	dirQuotaMgr, err := quota.NewDirectoryQuotaManager(dirQuotaRepo)
	if err != nil {
		return fmt.Errorf("failed to create directory quota manager: %w", err)
	}
	v.dirQuotaManager = dirQuotaMgr
	if err := v.reconcileDirectoryQuotaCounts(ctx); err != nil {
		return fmt.Errorf("failed to reconcile directory quotas: %w", err)
	}

	// The metadata database is the only store whose loss cannot be recomputed:
	// directory quota counts are derived from metadata and are reconciled just
	// above, so a quarantined quota database is a healthy degraded state rather
	// than a startup failure.
	if err := v.validateTenantDatabases(ctx); err != nil {
		return err
	}
	if err := v.checkStartupRecovery(); err != nil {
		return err
	}

	// 4. Initialize storage volumes
	v.emit(ctx, slog.LevelInfo, "volumes_initializing", "Initializing storage volumes", slog.Int("count", len(v.config.Volumes)))
	v.volumes = make(map[string]core.StorageVolume)
	for _, volConfig := range v.config.Volumes {
		var vol core.StorageVolume
		var err error

		// Create volume based on type
		switch volConfig.VolumeType {
		case "LocalFileSystem", "":
			// Default to LocalFileSystem if not specified
			vol, err = volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
				VolumeID:            volConfig.VolumeID,
				VolumeType:          volConfig.VolumeType,
				MountPath:           volConfig.MountPath,
				ShardDepth:          volConfig.ShardingDepth,
				EnableFsync:         volConfig.EnableFsync,
				HealthCheckCacheTTL: volConfig.HealthCheckCacheTTL,
			})
		default:
			return fmt.Errorf("unsupported volume type %s for volume %s", volConfig.VolumeType, volConfig.VolumeID)
		}

		if err != nil {
			return fmt.Errorf("failed to create volume %s: %w", volConfig.VolumeID, err)
		}
		v.volumes[volConfig.VolumeID] = vol
		v.emit(ctx, slog.LevelInfo, "volume_initialized", "Volume initialized", slog.String("volume_id", volConfig.VolumeID), slog.String("volume_type", volConfig.VolumeType))
	}

	if len(v.volumes) == 0 {
		return fmt.Errorf("no volumes configured")
	}

	// Wait for the volumes to become usable before any component can select one,
	// and perform the optional warm-up write.
	if err := v.prepareVolumes(ctx); err != nil {
		return err
	}

	// 5. Initialize file scheduler
	v.emit(ctx, slog.LevelInfo, "file_scheduler_initializing", "Initializing file scheduler")
	fileScheduler, err := scheduler.NewFileScheduler(metaRepo, v.volumes, &scheduler.FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         v.config.RetryPolicy.MaxRetryCount,
			InitialRetryDelay:     v.config.RetryPolicy.InitialRetryDelay,
			UseExponentialBackoff: v.config.RetryPolicy.UseExponentialBackoff,
			MaxRetryDelay:         v.config.RetryPolicy.MaxRetryDelay,
		},
		ProcessingTimeout:           v.config.Cleanup.ProcessingTimeout,
		RecoverTimedOutOnEmptyQueue: v.config.Cleanup.RecoverTimedOutOnEmptyQueue,
		TimedOutReclaimCooldown:     v.config.Cleanup.TimedOutReclaimCooldown,
		// Reclaim batch sizes come from configuration so an operator can bound
		// both the synchronous empty-queue pass and the opportunistic background
		// pass, including disabling either one with a negative value.
		EmptyQueueReclaimBatchSize:         v.config.Cleanup.EmptyQueueReclaimBatchSize,
		BackgroundTimedOutReclaimEnabled:   v.config.Cleanup.EnableBackgroundTimedOutReclaim,
		BackgroundTimedOutReclaimBatchSize: v.config.Cleanup.BackgroundTimedOutReclaimBatchSize,
		// The background reclaim pass reports its contained failures through the
		// instance runtime; without it those failures would be silent.
		Logging: v.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create file scheduler: %w", err)
	}
	v.fileScheduler = fileScheduler

	// 6. Initialize storage pool
	v.emit(ctx, slog.LevelInfo, "storage_pool_initializing", "Initializing storage pool")
	storagePool, err := pool.NewStoragePool(&pool.StoragePoolOptions{
		TenantManager:         v.tenantManager,
		MetadataRepository:    metaRepo,
		FileScheduler:         fileScheduler,
		Volumes:               v.volumes,
		TenantQuotaManager:    v.tenantQuotaManager,
		DirectoryQuotaManager: v.dirQuotaManager,
		StatisticsRecorder:    v.statisticsRecorder,
	})
	if err != nil {
		return fmt.Errorf("failed to create storage pool: %w", err)
	}
	v.storagePool = storagePool

	// 7. Initialize cleanup service (core)
	v.emit(ctx, slog.LevelInfo, "cleanup_service_initializing", "Initializing cleanup service")
	disposition, err := core.ParsePermanentlyFailedDisposition(v.config.Cleanup.PermanentlyFailedDisposition)
	if err != nil {
		return fmt.Errorf("invalid permanently failed disposition: %w", err)
	}
	retiredVolumes := make(map[string]core.RetiredVolumeDisposition, len(v.config.Cleanup.RetiredVolumes))
	for _, retired := range v.config.Cleanup.RetiredVolumes {
		retiredDisposition, err := core.ParseRetiredVolumeDisposition(retired.Disposition)
		if err != nil {
			return fmt.Errorf("invalid retired volume disposition for %s: %w", retired.VolumeID, err)
		}
		retiredVolumes[retired.VolumeID] = retiredDisposition
	}
	cleanupServiceCore, err := cleanup.NewCleanupService(&cleanup.CleanupServiceOptions{
		TenantManager:                v.tenantManager,
		MetadataRepository:           metaRepo,
		FileScheduler:                fileScheduler,
		Volumes:                      v.volumes,
		TenantQuotaManager:           v.tenantQuotaManager,
		DirectoryQuotaManager:        v.dirQuotaManager,
		DirectoryQuotaRepository:     dirQuotaRepo,
		DefaultProcessingTimeout:     v.config.Cleanup.ProcessingTimeout,
		Logging:                      v.logger,
		PermanentlyFailedDisposition: disposition,
		DeadLetter: cleanup.DeadLetterOptions{
			RootPath:             v.config.Cleanup.DeadLetter.RootPath,
			IncludeTenantInPath:  v.config.Cleanup.DeadLetter.IncludeTenantInPath,
			IncludeDatePartition: v.config.Cleanup.DeadLetter.IncludeDatePartition,
			ShardingDepth:        v.config.Cleanup.DeadLetter.ShardingDepth,
		},
		RetiredVolumes:             retiredVolumes,
		MetadataDirectory:          v.config.MetadataDirectory,
		QuotaDirectory:             v.config.QuotaDirectory,
		CorruptedDatabaseRetention: v.config.Sqlite.CorruptedDatabaseRetention,
	})
	if err != nil {
		return fmt.Errorf("failed to create cleanup service: %w", err)
	}
	v.cleanupServiceCore = cleanupServiceCore

	// 8. Initialize background cleanup service (if enabled)
	if v.config.EnableBackgroundCleanup {
		v.emit(ctx, slog.LevelInfo, "background_cleanup_initializing", "Initializing background cleanup service")
		bgCleanupService, err := cleanup.NewBackgroundCleanupService(&cleanup.BackgroundCleanupServiceOptions{
			CleanupService:                 cleanupServiceCore,
			Logging:                        v.logger,
			CleanupInterval:                v.config.Cleanup.CleanupInterval,
			InitialDelay:                   v.config.Cleanup.InitialDelay,
			CleanupEmptyDirectories:        v.config.Cleanup.CleanupEmptyDirectories,
			CleanupTimedOutFiles:           v.config.Cleanup.CleanupTimedOutFiles,
			ProcessingTimeout:              v.config.Cleanup.ProcessingTimeout,
			CleanupPermanentlyFailedFiles:  v.config.Cleanup.CleanupPermanentlyFailedFiles,
			FailedFileRetentionPeriod:      v.config.Cleanup.FailedFileRetentionPeriod,
			CleanupCompletedRecords:        v.config.Cleanup.CleanupCompletedRecords,
			CompletedRecordRetentionPeriod: v.config.Cleanup.CompletedRecordRetentionPeriod,
			CleanupOrphanedMetadata:        v.config.Cleanup.CleanupOrphanedMetadata,
			CleanupJunkFiles:               v.config.Cleanup.CleanupJunkFiles,
			JunkFileCleanupInterval:        v.config.Cleanup.JunkFileCleanupInterval,
			CleanupInvalidDatabaseBackups:  v.config.Cleanup.CleanupInvalidDatabaseBackups,
			OptimizeDatabases:              v.config.Cleanup.OptimizeDatabases,
			DatabaseOptimizationInterval:   v.config.Cleanup.DatabaseOptimizationInterval,
		})
		if err != nil {
			return fmt.Errorf("failed to create background cleanup service: %w", err)
		}
		v.cleanupService = bgCleanupService
	}

	// 9. Initialize file watcher (if there are watchers or roots configured)
	if len(v.config.FileWatchers) > 0 || len(v.config.FileWatcherRoots) > 0 {
		v.emit(ctx, slog.LevelInfo, "file_watcher_initializing", "Initializing file watcher service",
			slog.Int("watchers", len(v.config.FileWatchers)), slog.Int("roots", len(v.config.FileWatcherRoots)))
		fileWatcherCore, err := watcher.NewFileWatcher(&watcher.FileWatcherOptions{
			TenantManager:        v.tenantManager,
			StoragePool:          v.storagePool,
			ConfigurationRootDir: v.config.FileWatcherConfigurationDirectory,
			Logging:              v.logger,
			StatisticsRecorder:   v.statisticsRecorder,
		})
		if err != nil {
			return fmt.Errorf("failed to create file watcher: %w", err)
		}
		v.fileWatcherCore = fileWatcherCore

		// Register all file watchers from configuration
		for _, watcherCfg := range v.config.FileWatchers {
			if !watcherCfg.Enabled {
				v.emit(ctx, slog.LevelInfo, "file_watcher_disabled", "Skipping disabled file watcher", slog.String("watcher_id", watcherCfg.WatcherID))
				continue
			}

			watcherConfig := &core.FileWatcherConfiguration{
				WatcherID:                         watcherCfg.WatcherID,
				TenantID:                          watcherCfg.TenantID,
				WatchPath:                         watcherCfg.WatchPath,
				MultiTenantMode:                   watcherCfg.MultiTenantMode,
				AutoCreateTenantDirectories:       watcherCfg.AutoCreateTenantDirectories,
				IncludeSubdirectories:             watcherCfg.IncludeSubdirectories,
				FilePatterns:                      watcherCfg.FilePatterns,
				PostImportAction:                  core.ParsePostImportAction(watcherCfg.PostImportAction),
				MoveToDirectory:                   watcherCfg.MoveToDirectory,
				PollingInterval:                   watcherCfg.PollingInterval,
				MaxFileSizeBytes:                  watcherCfg.MaxFileSizeBytes,
				MinFileAge:                        watcherCfg.MinFileAge,
				MaxConcurrentImports:              watcherCfg.MaxConcurrentImports,
				MaxPostImportActionRetryCount:     watcherCfg.MaxPostImportActionRetryCount,
				PostImportActionRetryInitialDelay: watcherCfg.PostImportActionRetryInitialDelay,
				PostImportActionRetryMaxDelay:     watcherCfg.PostImportActionRetryMaxDelay,
				Enabled:                           watcherCfg.Enabled,

				// The configuration expresses the two housekeeping switches in
				// negative form so their zero value means "enabled"; the runtime
				// model keeps the positive form.
				AutoCreateTenantDirectoriesCacheTTL:     watcherCfg.AutoCreateTenantDirectoriesCacheTTL,
				FileStabilityCheckDelay:                 watcherCfg.FileStabilityCheckDelay,
				SkipStabilityCheckAfterAge:              watcherCfg.SkipStabilityCheckAfterAge,
				EnableImportedFilesPruneThrottle:        !watcherCfg.DisableImportedFilesPruneThrottle,
				ImportedFilesPruneInterval:              watcherCfg.ImportedFilesPruneInterval,
				EnableImportedFilesHistoryFlushDebounce: !watcherCfg.DisableImportedFilesHistoryFlushDebounce,
				ImportedFilesHistoryFlushInterval:       watcherCfg.ImportedFilesHistoryFlushInterval,
			}

			if err := fileWatcherCore.RegisterWatcher(ctx, watcherConfig); err != nil {
				return fmt.Errorf("failed to register file watcher %s: %w", watcherCfg.WatcherID, err)
			}
			v.emit(ctx, slog.LevelInfo, "file_watcher_registered", "File watcher registered", slog.String("watcher_id", watcherCfg.WatcherID))
		}

		// Create background service. InitialEnabled carries the configured
		// enabled state explicitly: the watcher service treats an unset flag as
		// "scan by default", so a pointed value is how configuration disables it
		// at construction.
		watcherServiceEnabled := v.config.FileWatcherService.Enabled
		bgFileWatcherService, err := watcher.NewBackgroundFileWatcherService(&watcher.BackgroundFileWatcherServiceOptions{
			FileWatcher:          fileWatcherCore,
			Logging:              v.logger,
			Enabled:              v.config.FileWatcherService.Enabled,
			ConfigurationRootDir: v.config.FileWatcherConfigurationDirectory,
			InitialEnabled:       &watcherServiceEnabled,
			ServiceOptions: core.FileWatcherServiceOptions{
				Enabled:                 v.config.FileWatcherService.Enabled,
				DefaultPollingInterval:  v.config.FileWatcherService.DefaultPollingInterval,
				MinimumPollingInterval:  v.config.FileWatcherService.MinimumPollingInterval,
				MaximumPollingInterval:  v.config.FileWatcherService.MaximumPollingInterval,
				DisabledCheckInterval:   v.config.FileWatcherService.DisabledCheckInterval,
				MaxParallelWatcherScans: v.config.FileWatcherService.MaxParallelWatcherScans,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create background file watcher service: %w", err)
		}
		v.fileWatcherService = bgFileWatcherService

		// Derive per-tenant watchers from the configured roots.
		if len(v.config.FileWatcherRoots) > 0 {
			roots := make([]core.FileWatcherRootConfiguration, 0, len(v.config.FileWatcherRoots))
			for _, rootCfg := range v.config.FileWatcherRoots {
				roots = append(roots, core.FileWatcherRootConfiguration{
					RootPath:                          rootCfg.RootPath,
					MultiTenantMode:                   rootCfg.MultiTenantMode,
					Enabled:                           rootCfg.Enabled,
					IncludeSubdirectories:             rootCfg.IncludeSubdirectories,
					FilePatterns:                      append([]string(nil), rootCfg.FilePatterns...),
					PostImportAction:                  core.ParsePostImportAction(rootCfg.PostImportAction),
					MoveToDirectory:                   rootCfg.MoveToDirectory,
					PollingInterval:                   rootCfg.PollingInterval,
					MaxFileSizeBytes:                  rootCfg.MaxFileSizeBytes,
					MinFileAge:                        rootCfg.MinFileAge,
					MaxConcurrentImports:              rootCfg.MaxConcurrentImports,
					MaxPostImportActionRetryCount:     rootCfg.MaxPostImportActionRetryCount,
					PostImportActionRetryInitialDelay: rootCfg.PostImportActionRetryInitialDelay,
					PostImportActionRetryMaxDelay:     rootCfg.PostImportActionRetryMaxDelay,

					AutoCreateTenantDirectoriesCacheTTL:     rootCfg.AutoCreateTenantDirectoriesCacheTTL,
					FileStabilityCheckDelay:                 rootCfg.FileStabilityCheckDelay,
					SkipStabilityCheckAfterAge:              rootCfg.SkipStabilityCheckAfterAge,
					EnableImportedFilesPruneThrottle:        !rootCfg.DisableImportedFilesPruneThrottle,
					ImportedFilesPruneInterval:              rootCfg.ImportedFilesPruneInterval,
					EnableImportedFilesHistoryFlushDebounce: !rootCfg.DisableImportedFilesHistoryFlushDebounce,
					ImportedFilesHistoryFlushInterval:       rootCfg.ImportedFilesHistoryFlushInterval,
				})
			}

			autoManager, err := watcher.NewFileWatcherAutoManager(&watcher.FileWatcherAutoManagerOptions{
				FileWatcher:          fileWatcherCore,
				Logging:              v.logger,
				ConfigurationRootDir: v.config.FileWatcherConfigurationDirectory,
				Roots:                roots,
			})
			if err != nil {
				return fmt.Errorf("failed to create file watcher auto manager: %w", err)
			}
			v.fileWatcherAutoManager = autoManager

			created, err := autoManager.DiscoverAndCreateWatchers(ctx)
			if err != nil {
				return fmt.Errorf("failed to derive file watchers from configured roots: %w", err)
			}
			v.emit(ctx, slog.LevelInfo, "file_watcher_roots_applied", "Derived file watchers from configured roots",
				slog.Int("watcher_roots", len(roots)), slog.Int("watchers", created))
		}
	}

	// 10. Initialize orphan recovery (if enabled)
	if v.config.OrphanRecovery.Enabled {
		v.emit(ctx, slog.LevelInfo, "orphan_recovery_initializing", "Initializing orphan recovery service")
		orphanRecoveryService, err := recovery.NewOrphanRecoveryService(&recovery.OrphanRecoveryServiceOptions{
			MetadataRepository:    metaRepo,
			Volumes:               v.volumes,
			TenantManager:         v.tenantManager,
			TenantQuotaManager:    v.tenantQuotaManager,
			DirectoryQuotaManager: v.dirQuotaManager,
			Logging:               v.logger,
			RecoveryInterval:      v.config.OrphanRecovery.RecoveryInterval,
			InitialDelay:          v.config.OrphanRecovery.InitialDelay,
			RunOnStartup:          v.config.OrphanRecovery.RunOnStartup,
		})
		if err != nil {
			return fmt.Errorf("failed to create orphan recovery service: %w", err)
		}
		v.orphanRecoveryCore = orphanRecoveryService
		v.orphanRecovery = orphanRecoveryService
	}

	// 11. Initialize health check service (if enabled)
	if v.config.EnableDatabaseHealthCheck {
		v.emit(ctx, slog.LevelInfo, "database_health_initializing", "Initializing database health check service")
		volumePaths := make([]string, 0, len(v.volumes))
		for _, vol := range v.volumes {
			volumePaths = append(volumePaths, vol.MountPath())
		}

		healthChecker, err := health.NewDatabaseHealthChecker(&health.DatabaseHealthCheckerOptions{
			// The SQLite engine keeps one database per tenant file below these
			// roots, so the checker inspects the roots and ignores the legacy
			// single-database path fields.
			MetadataDataPath:       v.config.MetadataDirectory,
			DirectoryQuotaDataPath: v.config.QuotaDirectory,
			VolumePaths:            volumePaths,
			Logging:                v.logger,
		})
		if err != nil {
			return fmt.Errorf("failed to create database health checker: %w", err)
		}
		v.databaseHealthChecker = healthChecker

		healthCheckService, err := health.NewDatabaseHealthCheckService(&health.DatabaseHealthCheckServiceOptions{
			DatabaseHealthChecker: healthChecker,
			Logging:               v.logger,
			InitialDelay:          v.config.DatabaseHealthCheck.InitialDelay,
			MaxRetries:            v.config.DatabaseHealthCheck.MaxRetries,
			RetryDelay:            v.config.DatabaseHealthCheck.RetryDelay,
			CheckOnStartupOnly:    v.config.DatabaseHealthCheck.CheckOnStartupOnly,
			PeriodicCheckInterval: v.config.DatabaseHealthCheck.PeriodicCheckInterval,
		})
		if err != nil {
			return fmt.Errorf("failed to create database health check service: %w", err)
		}
		v.healthCheckService = healthCheckService
	}

	return nil
}

// quotaReconcilePageSize bounds how many metadata records a quota recount keeps
// in memory at once.
const quotaReconcilePageSize = 500

// ReconcileQuotaCounts rebuilds the tenant and directory file counts from
// persisted metadata.
//
// NewVenue runs the same reconciliation at startup. Expose this method for
// operator repair: if a count drifted because a process died mid-transition, or
// because files were removed out-of-band, calling it restores both counters
// without a restart. It is safe to call while the runtime is running.
func (v *Venue) ReconcileQuotaCounts(ctx context.Context) error {
	if err := v.reconcileTenantQuotaCounts(ctx); err != nil {
		return fmt.Errorf("failed to reconcile tenant quotas: %w", err)
	}
	if err := v.reconcileDirectoryQuotaCounts(ctx); err != nil {
		return fmt.Errorf("failed to reconcile directory quotas: %w", err)
	}
	return nil
}

// forEachCountedRecord visits every metadata record that consumes quota for one
// tenant, in bounded pages when the repository supports paging.
func (v *Venue) forEachCountedRecord(ctx context.Context, tenantID string, visit func(*core.FileMetadata) error) error {
	for _, status := range quotaCountedStatuses {
		if err := ctx.Err(); err != nil {
			return err
		}
		if reader, ok := v.metadataRepo.(core.StatusPageReader); ok {
			cursor := ""
			for {
				page, err := reader.GetByStatusPage(ctx, tenantID, status, cursor, quotaReconcilePageSize)
				if err != nil {
					return fmt.Errorf("failed to page status %s for tenant %s: %w", status, tenantID, err)
				}
				for _, record := range page.Records {
					if err := visit(record); err != nil {
						return err
					}
				}
				if page.NextCursor == "" {
					break
				}
				cursor = page.NextCursor
			}
			continue
		}

		files, err := v.metadataRepo.GetByStatus(ctx, tenantID, status, 0)
		if err != nil {
			return fmt.Errorf("failed to list status %s for tenant %s: %w", status, tenantID, err)
		}
		for _, record := range files {
			if err := visit(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Venue) reconcileTenantQuotaCounts(ctx context.Context) error {
	tenants, err := v.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tenants: %w", err)
	}
	for _, tenantContext := range tenants {
		count := 0
		if err := v.forEachCountedRecord(ctx, tenantContext.ID, func(*core.FileMetadata) error {
			count++
			return nil
		}); err != nil {
			return fmt.Errorf("failed to count files for tenant %s: %w", tenantContext.ID, err)
		}
		if err := v.tenantQuotaManager.SetFileCount(ctx, tenantContext.ID, count); err != nil {
			return fmt.Errorf("failed to set count for tenant %s: %w", tenantContext.ID, err)
		}
	}
	return nil
}

func (v *Venue) reconcileDirectoryQuotaCounts(ctx context.Context) error {
	tenants, err := v.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tenants: %w", err)
	}
	for _, tenantContext := range tenants {
		counts := make(map[string]int)
		existing, err := v.dirQuotaRepo.GetAll(ctx, tenantContext.ID)
		if err != nil {
			return fmt.Errorf("failed to list quotas for tenant %s: %w", tenantContext.ID, err)
		}
		// Seed from the configured directory rows so an explicit limit survives
		// even when its directory currently holds no file.
		for _, quota := range existing {
			counts[directorypath.Normalize(quota.DirectoryPath)] = 0
		}
		if err := v.forEachCountedRecord(ctx, tenantContext.ID, func(record *core.FileMetadata) error {
			counts[directorypath.Normalize(record.DirectoryPath)]++
			return nil
		}); err != nil {
			return fmt.Errorf("failed to count directory usage for tenant %s: %w", tenantContext.ID, err)
		}
		for directoryPath, count := range counts {
			if err := v.dirQuotaManager.SetFileCount(ctx, tenantContext.ID, directoryPath, count); err != nil {
				return fmt.Errorf("failed to set directory count for tenant %s: %w", tenantContext.ID, err)
			}
		}
	}
	return nil
}

var quotaCountedStatuses = [...]core.FileProcessingStatus{
	core.FileStatusPending,
	core.FileStatusProcessing,
	core.FileStatusCompleted,
	core.FileStatusFailed,
	core.FileStatusPermanentlyFailed,
	core.FileStatusDeleteRequested,
}

// backgroundService is the lifecycle surface shared by Venue's background
// services, used for start rollback and reverse-order shutdown.
type backgroundService interface {
	Start() error
	Stop() error
}

// voidBackgroundService adapts a background service whose lifecycle cannot fail
// to the backgroundService surface, so it joins the same start rollback and
// reverse-order shutdown as the services that can.
type voidBackgroundService struct {
	start func()
	stop  func()
}

func (s voidBackgroundService) Start() error {
	if s.start != nil {
		s.start()
	}
	return nil
}

func (s voidBackgroundService) Stop() error {
	if s.stop != nil {
		s.stop()
	}
	return nil
}

// Start starts all enabled background services.
//
// Start must be called before the background services do work. It is not
// required before using StoragePool, TenantManager, or the other synchronous
// components.
func (v *Venue) Start() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return fmt.Errorf("venue is stopped; create a new instance with NewVenue")
	}
	if v.running {
		return fmt.Errorf("venue is already running")
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())
	v.running = true

	v.emit(v.ctx, slog.LevelInfo, "services_starting", "Starting Venue services")

	// Start in dependency order; roll back everything already started when a
	// later service cannot start.
	services := []struct {
		name    string
		started string
		service backgroundService
	}{
		{"health check", "health_check_started", v.healthCheckService},
		{"cleanup", "cleanup_started", v.cleanupService},
		{"orphan recovery", "orphan_recovery_started", v.orphanRecovery},
		{"file watcher", "file_watcher_started", v.fileWatcherService},
		{"metadata backup", "metadata_backup_started", v.metadataBackupService()},
		{"statistics output", "statistics_output_started", v.statisticsOutputService()},
	}

	started := make([]backgroundService, 0, len(services))
	for _, entry := range services {
		if isNilService(entry.service) {
			continue
		}
		if err := entry.service.Start(); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				_ = started[i].Stop()
			}
			v.running = false
			return fmt.Errorf("failed to start %s service: %w", entry.name, err)
		}
		started = append(started, entry.service)
		v.emit(v.ctx, slog.LevelInfo, entry.started, "Background service started")
	}

	v.emit(v.ctx, slog.LevelInfo, "services_started", "Venue services started successfully")
	return nil
}

// Stop stops the background services, closes the repositories opened by
// NewVenue, and releases their database locks.
//
// Stop is idempotent and safe in a deferred shutdown path: calling it without
// Start still releases the repositories, and calling it again returns nil.
// After Stop the synchronous accessors must not be used.
func (v *Venue) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return nil
	}

	if v.running {
		v.emit(v.ctx, slog.LevelInfo, "services_stopping", "Stopping Venue services")

		// Stop services in reverse start order.
		toStop := []struct {
			name    string
			failed  string
			service backgroundService
		}{
			{"statistics output", "statistics_output_stop_failed", v.statisticsOutputService()},
			{"metadata backup", "metadata_backup_stop_failed", v.metadataBackupService()},
			{"file watcher", "file_watcher_stop_failed", v.fileWatcherService},
			{"orphan recovery", "orphan_recovery_stop_failed", v.orphanRecovery},
			{"cleanup", "cleanup_stop_failed", v.cleanupService},
			{"health check", "health_check_stop_failed", v.healthCheckService},
		}
		for _, entry := range toStop {
			if isNilService(entry.service) {
				continue
			}
			if err := entry.service.Stop(); err != nil {
				v.emit(v.ctx, slog.LevelError, entry.failed, "Failed to stop background service", errorTypeAttr(err))
			}
		}

		v.running = false
	}

	v.closeRepositories()

	if v.cancel != nil {
		v.cancel()
		v.cancel = nil
	}
	v.closed = true

	v.emit(v.ctx, slog.LevelInfo, "services_stopped", "Venue services stopped and resources released")
	return nil
}

// metadataBackupService adapts the optional periodic backup runner to the
// background service surface. It returns nil when backups are not configured, so
// Start and Stop skip it exactly like every other disabled service.
func (v *Venue) metadataBackupService() backgroundService {
	if v.metadataBackupCore == nil {
		return nil
	}
	return voidBackgroundService{start: v.metadataBackupCore.Start, stop: v.metadataBackupCore.Stop}
}

// statisticsOutputService adapts the optional statistics output loop to the
// background service surface. It returns nil when periodic output is not
// configured.
func (v *Venue) statisticsOutputService() backgroundService {
	if v.statisticsOutput == nil {
		return nil
	}
	return voidBackgroundService{start: v.statisticsOutput.Start, stop: v.statisticsOutput.Stop}
}

// closeRepositories releases every repository opened by NewVenue. It is
// idempotent so it can run on both the construction-failure and the Stop path.
func (v *Venue) closeRepositories() {
	// The scheduler owns background timed-out reclaim passes that write to the
	// metadata repository, so it must stop before that repository is closed.
	if v.fileScheduler != nil {
		if err := scheduler.CloseFileScheduler(v.fileScheduler); err != nil {
			v.emit(v.ctx, slog.LevelError, "file_scheduler_close_failed", "Failed to close file scheduler", errorTypeAttr(err))
		}
		v.fileScheduler = nil
	}

	if v.fileWatcherCore != nil {
		// The watcher owns its persisted import history; closing it flushes any
		// pending write. The capability is optional so core.FileWatcher stays a
		// minimal queue-import contract.
		if closer, ok := v.fileWatcherCore.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				v.emit(v.ctx, slog.LevelError, "file_watcher_close_failed", "Failed to close file watcher", errorTypeAttr(err))
			}
		}
		v.fileWatcherCore = nil
	}

	if v.metadataRepo != nil {
		if err := v.metadataRepo.Close(); err != nil {
			v.emit(v.ctx, slog.LevelError, "metadata_close_failed", "Failed to close metadata repository", errorTypeAttr(err))
		}
		v.metadataRepo = nil
	}

	if v.dirQuotaRepo != nil {
		if err := v.dirQuotaRepo.Close(); err != nil {
			v.emit(v.ctx, slog.LevelError, "quota_close_failed", "Failed to close directory quota repository", errorTypeAttr(err))
		}
		v.dirQuotaRepo = nil
	}
}

// quarantineReporter returns the callback that reports a database directory the
// repositories had to quarantine. Only the directory name is logged: a full
// physical path must never reach the log.
func (v *Venue) quarantineReporter() func(string) {
	return func(quarantinedPath string) {
		v.emit(context.Background(), slog.LevelError, "database_quarantined",
			"Unopenable database was quarantined and recreated",
			slog.String("quarantine_directory", filepath.Base(quarantinedPath)))
	}
}

// recoveryIncompleteReporter returns the callback that records a quarantined
// metadata database that no backup could replace, so the runtime is about to
// serve an empty queue store.
//
// The event is always reported as a degraded state; FailFastOnStartupRecoveryFailure
// only decides whether NewVenue then fails instead of continuing.
func (v *Venue) recoveryIncompleteReporter() func(string) {
	return func(quarantinedPath string) {
		v.recoveryMu.Lock()
		v.incompleteRecoveries = append(v.incompleteRecoveries, filepath.Base(quarantinedPath))
		count := len(v.incompleteRecoveries)
		v.recoveryMu.Unlock()

		v.emit(context.Background(), slog.LevelWarn, "database_recovery_incomplete",
			"Metadata database was recreated empty because no usable backup was available",
			slog.String("quarantine_directory", filepath.Base(quarantinedPath)),
			slog.Int("incomplete_recoveries", count))
	}
}

// validateTenantDatabases opens every known tenant database once, but only when
// corruption recovery is enabled.
//
// SQLite databases are opened lazily, so without this pass a corrupt database
// would only be discovered on the first request for its tenant. Validating at
// startup keeps the recovery flag's semantics ("recover during startup") and lets
// FailFastOnStartupRecoveryFailure decide the outcome before the runtime serves
// traffic. The pass is skipped when recovery is disabled, because then a corrupt
// database must fail the request that touches it rather than be quarantined.
func (v *Venue) validateTenantDatabases(ctx context.Context) error {
	if !v.config.Sqlite.RecoverCorruptedDatabase {
		return nil
	}

	lister, ok := v.metadataRepo.(interface {
		KnownTenantIDs(context.Context) ([]string, error)
	})
	if !ok {
		return nil
	}

	tenantIDs, err := lister.KnownTenantIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to enumerate tenants for startup validation: %w", err)
	}

	for _, tenantID := range tenantIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Any read opens the tenant's database, which is where corruption is
		// detected and quarantined. An empty result is a healthy database.
		if _, err := v.metadataRepo.GetByStatus(ctx, tenantID, core.FileStatusPending, 1); err != nil {
			return fmt.Errorf("failed to validate the metadata database of tenant %s: %w", tenantID, err)
		}
	}

	return nil
}

// checkStartupRecovery reports the degraded state recorded during repository
// construction and fails startup when the operator asked for fail-fast
// behaviour.
func (v *Venue) checkStartupRecovery() error {
	v.recoveryMu.Lock()
	incomplete := len(v.incompleteRecoveries)
	v.recoveryMu.Unlock()

	if incomplete == 0 {
		return nil
	}

	if !v.config.FailFastOnStartupRecoveryFailure {
		v.emit(context.Background(), slog.LevelWarn, "startup_degraded",
			"Starting with an empty metadata database; queued file records were not recovered",
			slog.Int("incomplete_recoveries", incomplete))
		return nil
	}

	return fmt.Errorf(
		"%d metadata database(s) could not be recovered from a backup and startup fail-fast is enabled: %w",
		incomplete,
		core.ErrDatabaseError,
	)
}

// isNilService reports whether a background service is absent, including a
// typed nil pointer stored in an interface.
func isNilService(service backgroundService) bool {
	if service == nil {
		return true
	}
	value := reflect.ValueOf(service)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

// IsRunning returns whether the venue is currently running.
func (v *Venue) IsRunning() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.running
}

// Component accessors (dependency injection getters)

// TenantManager returns the tenant manager instance.
func (v *Venue) TenantManager() core.TenantManager {
	return v.tenantManager
}

// StoragePool returns the storage pool instance.
func (v *Venue) StoragePool() core.StoragePool {
	return v.storagePool
}

// FileScheduler returns the file scheduler instance.
func (v *Venue) FileScheduler() core.FileScheduler {
	return v.fileScheduler
}

// MetadataRepository returns the metadata repository instance.
func (v *Venue) MetadataRepository() core.MetadataRepository {
	return v.metadataRepo
}

// TenantQuotaManager returns the tenant quota manager instance.
func (v *Venue) TenantQuotaManager() core.TenantQuotaManager {
	return v.tenantQuotaManager
}

// TenantQuotaAdministrator returns the tenant quota manager's limit
// administration capability.
//
// The capability is optional so a minimal TenantQuotaManager implementation
// stays valid; the runtime's own manager always provides it, so the second result
// is false only for a caller-supplied manager.
func (v *Venue) TenantQuotaAdministrator() (core.TenantQuotaAdministrator, bool) {
	administrator, ok := v.tenantQuotaManager.(core.TenantQuotaAdministrator)
	return administrator, ok
}

// Statistics returns the runtime statistics reader.
//
// It is never nil: when Statistics.Enabled is false the returned reader answers
// every query with an empty snapshot. The returned value is owned by the Venue
// instance and stays usable until Stop.
func (v *Venue) Statistics() core.StatisticsReader {
	if reader, ok := v.statisticsRecorder.(core.StatisticsReader); ok {
		return reader
	}
	return statistics.Noop
}

// StatisticsRecorder returns the runtime statistics recorder, so an application
// can add its own measurements to the same bounded window set.
//
// It is never nil: when statistics are disabled the returned recorder drops
// every record.
func (v *Venue) StatisticsRecorder() core.StatisticsRecorder {
	if v.statisticsRecorder == nil {
		return statistics.Noop
	}
	return v.statisticsRecorder
}

// DirectoryQuotaManager returns the directory quota manager instance.
func (v *Venue) DirectoryQuotaManager() core.DirectoryQuotaManager {
	return v.dirQuotaManager
}

// CleanupService returns the cleanup service instance.
func (v *Venue) CleanupService() core.CleanupService {
	return v.cleanupServiceCore
}

// OrphanRecovery returns the orphan recovery service instance (nil when
// OrphanRecovery.Enabled is false).
func (v *Venue) OrphanRecovery() core.OrphanRecoveryService {
	return v.orphanRecoveryCore
}

// FileWatcher returns the file watcher instance (may be nil if not enabled).
func (v *Venue) FileWatcher() core.FileWatcher {
	return v.fileWatcherCore
}

// FileWatcherAutoManager returns the root-configuration watcher manager (nil
// when no watcher roots are configured).
func (v *Venue) FileWatcherAutoManager() core.FileWatcherAutoManager {
	return v.fileWatcherAutoManager
}

// FileWatcherService returns the background watcher service (nil when no
// watchers or roots are configured).
func (v *Venue) FileWatcherService() *watcher.BackgroundFileWatcherService {
	return v.fileWatcherService
}

// DatabaseHealthChecker returns the database health checker instance (may be nil if not enabled).
func (v *Venue) DatabaseHealthChecker() core.DatabaseHealthChecker {
	return v.databaseHealthChecker
}

// Volumes returns the map of all storage volumes.
func (v *Venue) Volumes() map[string]core.StorageVolume {
	return v.volumes
}

// Config returns an independent copy of this Venue instance's runtime configuration.
func (v *Venue) Config() *config.Config {
	return v.config.Clone()
}

// Logging returns this Venue instance's private logging runtime.
func (v *Venue) Logging() *logging.Runtime {
	return v.logger
}

func (v *Venue) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	v.logger.Emit(ctx, logging.Record{
		Level: level, Component: "venue", Event: event, Message: message, Attrs: attrs,
	})
}

func errorTypeAttr(err error) slog.Attr {
	return slog.String("error_type", fmt.Sprintf("%T", err))
}
