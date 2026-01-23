package venue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/cleanup"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/health"
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
	logger  *slog.Logger
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

// VenueOptions contains configuration for initializing Venue.
type VenueOptions struct {
	// Config is the configuration loaded from file or created programmatically.
	Config *config.Config

	// Logger is the logger instance to use. If nil, uses slog.Default().
	Logger *slog.Logger
}

// NewVenue creates a new Venue instance with the given options.
// This is the main entry point for initializing the entire system.
func NewVenue(opts *VenueOptions) (*Venue, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.Config == nil {
		return nil, fmt.Errorf("config cannot be nil: %w", core.ErrInvalidArgument)
	}

	// Validate configuration
	if err := opts.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set logger
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	v := &Venue{
		config: opts.Config,
		logger: logger,
	}

	// Initialize all components
	if err := v.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize venue: %w", err)
	}

	logger.Info("Venue initialized successfully")

	return v, nil
}

// initialize initializes all components in the correct order using configuration.
func (v *Venue) initialize() error {
	ctx := context.Background()

	// 1. Initialize tenant manager
	v.logger.Info("Initializing tenant manager")
	tenantMgr, err := tenant.NewTenantManager(&tenant.TenantManagerOptions{
		RootPath:         v.config.MetadataDirectory,
		MetadataPath:     v.config.TenantManagerOptions.MetadataPath,
		CacheTTL:         v.config.TenantManagerOptions.CacheTTL,
		EnableAutoCreate: v.config.AutoCreateTenants,
	})
	if err != nil {
		return fmt.Errorf("failed to create tenant manager: %w", err)
	}
	v.tenantManager = tenantMgr

	// Create tenants from configuration
	if len(v.config.Tenants) > 0 {
		v.logger.Info("Creating tenants from configuration", "count", len(v.config.Tenants))
		for _, tenantCfg := range v.config.Tenants {
			if err := v.tenantManager.CreateTenant(ctx, tenantCfg.TenantId); err != nil && err != core.ErrTenantAlreadyExists {
				return fmt.Errorf("failed to create tenant %s: %w", tenantCfg.TenantId, err)
			}

			// Enable or disable tenant based on configuration
			if tenantCfg.Enabled {
				if err := v.tenantManager.EnableTenant(ctx, tenantCfg.TenantId); err != nil {
					return fmt.Errorf("failed to enable tenant %s: %w", tenantCfg.TenantId, err)
				}
				v.logger.Info("Tenant enabled", "tenantId", tenantCfg.TenantId, "quota", tenantCfg.Quota)
			} else {
				if err := v.tenantManager.DisableTenant(ctx, tenantCfg.TenantId); err != nil {
					return fmt.Errorf("failed to disable tenant %s: %w", tenantCfg.TenantId, err)
				}
				v.logger.Info("Tenant disabled", "tenantId", tenantCfg.TenantId)
			}
		}
	}

	// 2. Initialize metadata repository
	v.logger.Info("Initializing metadata repository")
	metaRepo, err := metadata.NewBadgerMetadataRepository(&metadata.BadgerRepositoryOptions{
		TenantID:       "shared", // Multi-tenant repository
		DataPath:       v.config.MetadataDirectory,
		CacheTTL:       v.config.MetadataOptions.CacheTTL,
		GCInterval:     v.config.BadgerDBOptions.GCInterval,
		GCDiscardRatio: v.config.BadgerDBOptions.GCDiscardRatio,
	})
	if err != nil {
		return fmt.Errorf("failed to create metadata repository: %w", err)
	}
	v.metadataRepo = metaRepo

	// 3. Initialize quota managers
	v.logger.Info("Initializing quota managers")
	v.tenantQuotaManager = quota.NewTenantQuotaManager()

	// Set tenant quotas from configuration
	for _, tenantCfg := range v.config.Tenants {
		if tenantCfg.Quota != nil {
			// Use tenant-specific quota
			if *tenantCfg.Quota > 0 {
				_ = v.tenantQuotaManager.SetQuota(ctx, tenantCfg.TenantId, int(*tenantCfg.Quota))
				v.logger.Info("Tenant quota set", "tenantId", tenantCfg.TenantId, "quota", *tenantCfg.Quota)
			} else if *tenantCfg.Quota == 0 {
				// 0 means unlimited
				v.logger.Info("Tenant quota unlimited", "tenantId", tenantCfg.TenantId)
			}
		} else if v.config.DefaultTenantQuota > 0 {
			// Use default quota if no specific quota is set
			_ = v.tenantQuotaManager.SetQuota(ctx, tenantCfg.TenantId, int(v.config.DefaultTenantQuota))
			v.logger.Info("Tenant quota set to default", "tenantId", tenantCfg.TenantId, "quota", v.config.DefaultTenantQuota)
		}
	}

	dirQuotaRepo, err := quota.NewBadgerDirectoryQuotaRepository(&quota.BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       v.config.QuotaDirectory,
		GCInterval:     v.config.BadgerDBOptions.GCInterval,
		GCDiscardRatio: v.config.BadgerDBOptions.GCDiscardRatio,
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

	// 4. Initialize storage volumes
	v.logger.Info("Initializing storage volumes", "count", len(v.config.Volumes))
	v.volumes = make(map[string]core.StorageVolume)
	for _, volConfig := range v.config.Volumes {
		var vol core.StorageVolume
		var err error

		// Create volume based on type
		switch volConfig.VolumeType {
		case "LocalFileSystem", "":
			// Default to LocalFileSystem if not specified
			vol, err = volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
				VolumeID:   volConfig.VolumeId,
				VolumeType: volConfig.VolumeType,
				MountPath:  volConfig.MountPath,
				ShardDepth: volConfig.ShardingDepth,
			})
		default:
			return fmt.Errorf("unsupported volume type %s for volume %s", volConfig.VolumeType, volConfig.VolumeId)
		}

		if err != nil {
			return fmt.Errorf("failed to create volume %s: %w", volConfig.VolumeId, err)
		}
		v.volumes[volConfig.VolumeId] = vol
		v.logger.Info("Volume initialized", "volumeID", volConfig.VolumeId, "volumeType", volConfig.VolumeType, "mountPath", volConfig.MountPath)
	}

	if len(v.volumes) == 0 {
		return fmt.Errorf("no volumes configured")
	}

	// 5. Initialize file scheduler
	v.logger.Info("Initializing file scheduler")
	fileScheduler, err := scheduler.NewFileScheduler(metaRepo, v.volumes, &scheduler.FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         v.config.RetryPolicy.MaxRetryCount,
			InitialRetryDelay:     v.config.RetryPolicy.InitialRetryDelay,
			UseExponentialBackoff: v.config.RetryPolicy.UseExponentialBackoff,
			MaxRetryDelay:         v.config.RetryPolicy.MaxRetryDelay,
		},
		ProcessingTimeout: v.config.CleanupOptions.ProcessingTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create file scheduler: %w", err)
	}
	v.fileScheduler = fileScheduler

	// 6. Initialize storage pool
	v.logger.Info("Initializing storage pool")
	storagePool, err := pool.NewStoragePool(&pool.StoragePoolOptions{
		TenantManager:      v.tenantManager,
		MetadataRepository: metaRepo,
		FileScheduler:      fileScheduler,
		Volumes:            v.volumes,
	})
	if err != nil {
		return fmt.Errorf("failed to create storage pool: %w", err)
	}
	v.storagePool = storagePool

	// 7. Initialize cleanup service (core)
	v.logger.Info("Initializing cleanup service")
	cleanupServiceCore, err := cleanup.NewCleanupService(&cleanup.CleanupServiceOptions{
		MetadataRepository:       metaRepo,
		FileScheduler:            fileScheduler,
		Volumes:                  v.volumes,
		TenantQuotaManager:       v.tenantQuotaManager,
		DirectoryQuotaManager:    v.dirQuotaManager,
		DefaultProcessingTimeout: v.config.CleanupOptions.ProcessingTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create cleanup service: %w", err)
	}
	v.cleanupServiceCore = cleanupServiceCore

	// 8. Initialize background cleanup service (if enabled)
	if v.config.EnableBackgroundCleanup {
		v.logger.Info("Initializing background cleanup service")
		bgCleanupService, err := cleanup.NewBackgroundCleanupService(&cleanup.BackgroundCleanupServiceOptions{
			CleanupService:                cleanupServiceCore,
			Logger:                        v.logger,
			CleanupInterval:               v.config.CleanupOptions.CleanupInterval,
			InitialDelay:                  v.config.CleanupOptions.InitialDelay,
			CleanupEmptyDirectories:       v.config.CleanupOptions.CleanupEmptyDirectories,
			CleanupTimedOutFiles:          v.config.CleanupOptions.CleanupTimedOutFiles,
			ProcessingTimeout:             v.config.CleanupOptions.ProcessingTimeout,
			CleanupPermanentlyFailedFiles: v.config.CleanupOptions.CleanupPermanentlyFailedFiles,
			OptimizeDatabases:             v.config.CleanupOptions.OptimizeDatabases,
			DatabaseOptimizationInterval:  v.config.CleanupOptions.DatabaseOptimizationInterval,
		})
		if err != nil {
			return fmt.Errorf("failed to create background cleanup service: %w", err)
		}
		v.cleanupService = bgCleanupService
	}

	// 9. Initialize file watcher (if there are watchers configured)
	if len(v.config.FileWatchers) > 0 {
		v.logger.Info("Initializing file watcher service", "watchers", len(v.config.FileWatchers))
		fileWatcherCore, err := watcher.NewFileWatcher(&watcher.FileWatcherOptions{
			TenantManager:        v.tenantManager,
			StoragePool:          v.storagePool,
			ConfigurationRootDir: v.config.FileWatcherConfigurationDirectory,
		})
		if err != nil {
			return fmt.Errorf("failed to create file watcher: %w", err)
		}
		v.fileWatcherCore = fileWatcherCore

		// Register all file watchers from configuration
		for _, watcherCfg := range v.config.FileWatchers {
			if !watcherCfg.Enabled {
				v.logger.Info("Skipping disabled file watcher", "watcherId", watcherCfg.WatcherId)
				continue
			}

			watcherConfig := &core.FileWatcherConfiguration{
				WatcherID:                   watcherCfg.WatcherId,
				TenantID:                    watcherCfg.TenantId,
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
				return fmt.Errorf("failed to register file watcher %s: %w", watcherCfg.WatcherId, err)
			}
			v.logger.Info("File watcher registered", "watcherId", watcherCfg.WatcherId, "path", watcherCfg.WatchPath)
		}

		// Create background service
		bgFileWatcherService, err := watcher.NewBackgroundFileWatcherService(&watcher.BackgroundFileWatcherServiceOptions{
			FileWatcher:  fileWatcherCore,
			Logger:       v.logger,
			InitialDelay: v.config.CleanupOptions.InitialDelay,
		})
		if err != nil {
			return fmt.Errorf("failed to create background file watcher service: %w", err)
		}
		v.fileWatcherService = bgFileWatcherService
	}

	// 10. Initialize health check service (if enabled)
	if v.config.EnableDatabaseHealthCheck {
		v.logger.Info("Initializing database health check service")
		volumePaths := make([]string, 0, len(v.volumes))
		for _, vol := range v.volumes {
			volumePaths = append(volumePaths, vol.MountPath())
		}

		healthChecker, err := health.NewDatabaseHealthChecker(&health.DatabaseHealthCheckerOptions{
			MetadataDataPath:       v.config.MetadataDirectory,
			DirectoryQuotaDataPath: v.config.QuotaDirectory,
			VolumePaths:            volumePaths,
		})
		if err != nil {
			return fmt.Errorf("failed to create database health checker: %w", err)
		}
		v.databaseHealthChecker = healthChecker

		healthCheckService, err := health.NewDatabaseHealthCheckService(&health.DatabaseHealthCheckServiceOptions{
			DatabaseHealthChecker: healthChecker,
			Logger:                v.logger,
			InitialDelay:          v.config.DatabaseHealthCheckOptions.InitialDelay,
			MaxRetries:            v.config.DatabaseHealthCheckOptions.MaxRetries,
			RetryDelay:            v.config.DatabaseHealthCheckOptions.RetryDelay,
			CheckOnStartupOnly:    v.config.DatabaseHealthCheckOptions.CheckOnStartupOnly,
			PeriodicCheckInterval: v.config.DatabaseHealthCheckOptions.PeriodicCheckInterval,
		})
		if err != nil {
			return fmt.Errorf("failed to create database health check service: %w", err)
		}
		v.healthCheckService = healthCheckService
	}

	return nil
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

	v.logger.Info("Starting Venue services")

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

	v.logger.Info("Venue services started successfully")
	return nil
}

// Stop stops all background services and releases resources.
func (v *Venue) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.running {
		return fmt.Errorf("venue is not running")
	}

	v.logger.Info("Stopping Venue services")

	// Stop services in reverse order
	if v.fileWatcherService != nil {
		if err := v.fileWatcherService.Stop(); err != nil {
			v.logger.Error("Failed to stop file watcher service", "error", err)
		}
	}

	if v.cleanupService != nil {
		if err := v.cleanupService.Stop(); err != nil {
			v.logger.Error("Failed to stop cleanup service", "error", err)
		}
	}

	if v.healthCheckService != nil {
		if err := v.healthCheckService.Stop(); err != nil {
			v.logger.Error("Failed to stop health check service", "error", err)
		}
	}

	// Close repositories
	if v.metadataRepo != nil {
		if err := v.metadataRepo.Close(); err != nil {
			v.logger.Error("Failed to close metadata repository", "error", err)
		}
	}

	if v.dirQuotaRepo != nil {
		if err := v.dirQuotaRepo.Close(); err != nil {
			v.logger.Error("Failed to close directory quota repository", "error", err)
		}
	}

	v.cancel()
	v.running = false

	v.logger.Info("Venue services stopped successfully")
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

// Config returns the configuration.
func (v *Venue) Config() *config.Config {
	return v.config
}

// Logger returns the logger instance.
func (v *Venue) Logger() *slog.Logger {
	return v.logger
}
