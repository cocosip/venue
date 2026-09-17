package venue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/internal/directorypath"
	"github.com/cocosip/venue/pkg/cleanup"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/health"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/pool"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/scheduler"
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

	// Core components
	tenantManager         core.TenantManager
	metadataRepo          core.MetadataRepository
	dirQuotaRepo          core.DirectoryQuotaRepository
	tenantQuotaManager    core.TenantQuotaManager
	dirQuotaManager       core.DirectoryQuotaManager
	volumes               map[string]core.StorageVolume
	fileScheduler         core.FileScheduler
	storagePool           core.StoragePool
	cleanupServiceCore    core.CleanupService
	fileWatcherCore       core.FileWatcher
	databaseHealthChecker core.DatabaseHealthChecker

	// Background services
	cleanupService     *cleanup.BackgroundCleanupService
	fileWatcherService *watcher.BackgroundFileWatcherService
	healthCheckService *health.DatabaseHealthCheckService
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
		return nil, fmt.Errorf("failed to initialize venue: %w", err)
	}

	v.emit(context.Background(), slog.LevelInfo, "initialized", "Venue initialized successfully")

	return v, nil
}

// initialize initializes all components in the correct order using configuration.
func (v *Venue) initialize() error {
	ctx := context.Background()

	// 1. Initialize tenant manager
	v.emit(ctx, slog.LevelInfo, "tenant_manager_initializing", "Initializing tenant manager")
	tenantMgr, err := tenant.NewTenantManager(&tenant.TenantManagerOptions{
		RootPath:         v.config.MetadataDirectory,
		MetadataPath:     v.config.TenantManager.MetadataPath,
		CacheTTL:         v.config.TenantManager.CacheTTL,
		EnableAutoCreate: v.config.AutoCreateTenants,
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
	metaRepo, err := metadata.NewBadgerMetadataRepository(&metadata.BadgerRepositoryOptions{
		TenantID:         "shared", // Multi-tenant repository
		DataPath:         v.config.MetadataDirectory,
		CacheTTL:         v.config.Metadata.CacheTTL,
		MaxCacheEntries:  v.config.Metadata.MaxCacheEntries,
		GCInterval:       v.config.BadgerDB.GCInterval,
		GCDiscardRatio:   v.config.BadgerDB.GCDiscardRatio,
		MemTableSize:     int64(v.config.BadgerDB.MemTableSize) << 20,
		ValueLogFileSize: int64(v.config.BadgerDB.ValueLogFileSize) << 20,
		BlockCacheSize:   int64(v.config.BadgerDB.BlockCacheSize) << 20,
		SyncWrites:       v.config.BadgerDB.SyncWrites,
	})
	if err != nil {
		return fmt.Errorf("failed to create metadata repository: %w", err)
	}
	v.metadataRepo = metaRepo

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
			if err := v.tenantQuotaManager.SetQuota(ctx, tenantCfg.TenantID, int(v.config.DefaultTenantQuota)); err != nil {
				return fmt.Errorf("failed to set default quota for tenant %s: %w", tenantCfg.TenantID, err)
			}
			v.emit(ctx, slog.LevelInfo, "tenant_quota_defaulted", "Tenant quota set to default", slog.String("tenant_id", tenantCfg.TenantID), slog.Int64("quota", v.config.DefaultTenantQuota))
		}
	}
	if err := v.reconcileTenantQuotaCounts(ctx); err != nil {
		return fmt.Errorf("failed to reconcile tenant quotas: %w", err)
	}

	dirQuotaRepo, err := quota.NewBadgerDirectoryQuotaRepository(&quota.BadgerDirectoryQuotaRepositoryOptions{
		DataPath:         v.config.QuotaDirectory,
		GCInterval:       v.config.BadgerDB.GCInterval,
		GCDiscardRatio:   v.config.BadgerDB.GCDiscardRatio,
		MemTableSize:     int64(v.config.BadgerDB.MemTableSize/2) << 20, // Half size for quota
		ValueLogFileSize: int64(v.config.BadgerDB.ValueLogFileSize/2) << 20,
		BlockCacheSize:   int64(v.config.BadgerDB.BlockCacheSize/2) << 20,
		SyncWrites:       v.config.BadgerDB.SyncWrites,
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
				VolumeID:    volConfig.VolumeID,
				VolumeType:  volConfig.VolumeType,
				MountPath:   volConfig.MountPath,
				ShardDepth:  volConfig.ShardingDepth,
				EnableFsync: volConfig.EnableFsync,
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

	// 5. Initialize file scheduler
	v.emit(ctx, slog.LevelInfo, "file_scheduler_initializing", "Initializing file scheduler")
	fileScheduler, err := scheduler.NewFileScheduler(metaRepo, v.volumes, &scheduler.FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         v.config.RetryPolicy.MaxRetryCount,
			InitialRetryDelay:     v.config.RetryPolicy.InitialRetryDelay,
			UseExponentialBackoff: v.config.RetryPolicy.UseExponentialBackoff,
			MaxRetryDelay:         v.config.RetryPolicy.MaxRetryDelay,
		},
		ProcessingTimeout: v.config.Cleanup.ProcessingTimeout,
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
	})
	if err != nil {
		return fmt.Errorf("failed to create storage pool: %w", err)
	}
	v.storagePool = storagePool

	// 7. Initialize cleanup service (core)
	v.emit(ctx, slog.LevelInfo, "cleanup_service_initializing", "Initializing cleanup service")
	cleanupServiceCore, err := cleanup.NewCleanupService(&cleanup.CleanupServiceOptions{
		TenantManager:            v.tenantManager,
		MetadataRepository:       metaRepo,
		FileScheduler:            fileScheduler,
		Volumes:                  v.volumes,
		TenantQuotaManager:       v.tenantQuotaManager,
		DirectoryQuotaManager:    v.dirQuotaManager,
		DirectoryQuotaRepository: dirQuotaRepo,
		DefaultProcessingTimeout: v.config.Cleanup.ProcessingTimeout,
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
			CleanupCompletedRecords:        v.config.Cleanup.CleanupCompletedRecords,
			CompletedRecordRetentionPeriod: v.config.Cleanup.CompletedRecordRetentionPeriod,
			OptimizeDatabases:              v.config.Cleanup.OptimizeDatabases,
			DatabaseOptimizationInterval:   v.config.Cleanup.DatabaseOptimizationInterval,
		})
		if err != nil {
			return fmt.Errorf("failed to create background cleanup service: %w", err)
		}
		v.cleanupService = bgCleanupService
	}

	// 9. Initialize file watcher (if there are watchers configured)
	if len(v.config.FileWatchers) > 0 {
		v.emit(ctx, slog.LevelInfo, "file_watcher_initializing", "Initializing file watcher service", slog.Int("watchers", len(v.config.FileWatchers)))
		fileWatcherCore, err := watcher.NewFileWatcher(&watcher.FileWatcherOptions{
			TenantManager:        v.tenantManager,
			StoragePool:          v.storagePool,
			ConfigurationRootDir: v.config.FileWatcherConfigurationDirectory,
			Logging:              v.logger,
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
				WatcherID:                   watcherCfg.WatcherID,
				TenantID:                    watcherCfg.TenantID,
				WatchPath:                   watcherCfg.WatchPath,
				MultiTenantMode:             watcherCfg.MultiTenantMode,
				AutoCreateTenantDirectories: watcherCfg.AutoCreateTenantDirectories,
				IncludeSubdirectories:       watcherCfg.IncludeSubdirectories,
				FilePatterns:                watcherCfg.FilePatterns,
				PostImportAction:            core.ParsePostImportAction(watcherCfg.PostImportAction),
				MoveToDirectory:             watcherCfg.MoveToDirectory,
				PollingInterval:             watcherCfg.PollingInterval,
				MaxFileSizeBytes:            watcherCfg.MaxFileSizeBytes,
				MinFileAge:                  watcherCfg.MinFileAge,
				MaxConcurrentImports:        watcherCfg.MaxConcurrentImports,
				Enabled:                     watcherCfg.Enabled,
			}

			if err := fileWatcherCore.RegisterWatcher(ctx, watcherConfig); err != nil {
				return fmt.Errorf("failed to register file watcher %s: %w", watcherCfg.WatcherID, err)
			}
			v.emit(ctx, slog.LevelInfo, "file_watcher_registered", "File watcher registered", slog.String("watcher_id", watcherCfg.WatcherID))
		}

		// Create background service
		bgFileWatcherService, err := watcher.NewBackgroundFileWatcherService(&watcher.BackgroundFileWatcherServiceOptions{
			FileWatcher:  fileWatcherCore,
			Logging:      v.logger,
			InitialDelay: v.config.Cleanup.InitialDelay,
		})
		if err != nil {
			return fmt.Errorf("failed to create background file watcher service: %w", err)
		}
		v.fileWatcherService = bgFileWatcherService
	}

	// 10. Initialize health check service (if enabled)
	if v.config.EnableDatabaseHealthCheck {
		v.emit(ctx, slog.LevelInfo, "database_health_initializing", "Initializing database health check service")
		volumePaths := make([]string, 0, len(v.volumes))
		for _, vol := range v.volumes {
			volumePaths = append(volumePaths, vol.MountPath())
		}

		healthChecker, err := health.NewDatabaseHealthChecker(&health.DatabaseHealthCheckerOptions{
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

func (v *Venue) reconcileTenantQuotaCounts(ctx context.Context) error {
	tenants, err := v.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tenants: %w", err)
	}
	for _, tenantContext := range tenants {
		count := 0
		for _, status := range quotaCountedStatuses {
			files, err := v.metadataRepo.GetByStatus(ctx, tenantContext.ID, status, 0)
			if err != nil {
				return fmt.Errorf("failed to count status %s for tenant %s: %w", status, tenantContext.ID, err)
			}
			count += len(files)
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
		for _, quota := range existing {
			counts[directorypath.Normalize(quota.DirectoryPath)] = 0
		}
		for _, status := range quotaCountedStatuses {
			files, err := v.metadataRepo.GetByStatus(ctx, tenantContext.ID, status, 0)
			if err != nil {
				return fmt.Errorf("failed to list status %s for tenant %s: %w", status, tenantContext.ID, err)
			}
			for _, file := range files {
				counts[directorypath.Normalize(file.DirectoryPath)]++
			}
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

// Start starts all background services.
func (v *Venue) Start() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.running {
		return fmt.Errorf("venue is already running")
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())
	v.running = true

	v.emit(v.ctx, slog.LevelInfo, "services_starting", "Starting Venue services")

	// Start health check service first (if enabled)
	if v.healthCheckService != nil {
		if err := v.healthCheckService.Start(); err != nil {
			v.running = false
			return fmt.Errorf("failed to start health check service: %w", err)
		}
	}

	// Start cleanup service (if enabled)
	if v.cleanupService != nil {
		if err := v.cleanupService.Start(); err != nil {
			if v.healthCheckService != nil {
				_ = v.healthCheckService.Stop()
			}
			v.running = false
			return fmt.Errorf("failed to start cleanup service: %w", err)
		}
	}

	// Start file watcher service (if enabled)
	if v.fileWatcherService != nil {
		if err := v.fileWatcherService.Start(); err != nil {
			if v.cleanupService != nil {
				_ = v.cleanupService.Stop()
			}
			if v.healthCheckService != nil {
				_ = v.healthCheckService.Stop()
			}
			v.running = false
			return fmt.Errorf("failed to start file watcher service: %w", err)
		}
	}

	v.emit(v.ctx, slog.LevelInfo, "services_started", "Venue services started successfully")
	return nil
}

// Stop stops all background services and releases resources.
func (v *Venue) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.running {
		return fmt.Errorf("venue is not running")
	}

	v.emit(v.ctx, slog.LevelInfo, "services_stopping", "Stopping Venue services")

	// Stop services in reverse order
	if v.fileWatcherService != nil {
		if err := v.fileWatcherService.Stop(); err != nil {
			v.emit(v.ctx, slog.LevelError, "file_watcher_stop_failed", "Failed to stop file watcher service", errorTypeAttr(err))
		}
	}

	if v.cleanupService != nil {
		if err := v.cleanupService.Stop(); err != nil {
			v.emit(v.ctx, slog.LevelError, "cleanup_stop_failed", "Failed to stop cleanup service", errorTypeAttr(err))
		}
	}

	if v.healthCheckService != nil {
		if err := v.healthCheckService.Stop(); err != nil {
			v.emit(v.ctx, slog.LevelError, "health_check_stop_failed", "Failed to stop health check service", errorTypeAttr(err))
		}
	}

	// Close repositories
	if v.metadataRepo != nil {
		if err := v.metadataRepo.Close(); err != nil {
			v.emit(v.ctx, slog.LevelError, "metadata_close_failed", "Failed to close metadata repository", errorTypeAttr(err))
		}
	}

	if v.dirQuotaRepo != nil {
		if err := v.dirQuotaRepo.Close(); err != nil {
			v.emit(v.ctx, slog.LevelError, "quota_close_failed", "Failed to close directory quota repository", errorTypeAttr(err))
		}
	}

	v.cancel()
	v.running = false

	v.emit(v.ctx, slog.LevelInfo, "services_stopped", "Venue services stopped successfully")
	return nil
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

// DirectoryQuotaManager returns the directory quota manager instance.
func (v *Venue) DirectoryQuotaManager() core.DirectoryQuotaManager {
	return v.dirQuotaManager
}

// CleanupService returns the cleanup service instance.
func (v *Venue) CleanupService() core.CleanupService {
	return v.cleanupServiceCore
}

// FileWatcher returns the file watcher instance (may be nil if not enabled).
func (v *Venue) FileWatcher() core.FileWatcher {
	return v.fileWatcherCore
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
