package config

import (
	"time"

	"github.com/cocosip/venue/pkg/logging"
)

// WithMetadataDirectory sets the metadata directory.
func (c *Config) WithMetadataDirectory(path string) *Config { c.MetadataDirectory = path; return c }

// WithQuotaDirectory sets the quota directory.
func (c *Config) WithQuotaDirectory(path string) *Config { c.QuotaDirectory = path; return c }

// WithFileWatcherConfigurationDirectory sets the watcher configuration directory.
func (c *Config) WithFileWatcherConfigurationDirectory(path string) *Config {
	c.FileWatcherConfigurationDirectory = path
	return c
}

// WithAutoCreateTenants enables or disables automatic tenant creation.
func (c *Config) WithAutoCreateTenants(enabled bool) *Config {
	c.AutoCreateTenants = enabled
	return c
}

// WithDefaultTenantQuota sets the default tenant quota.
func (c *Config) WithDefaultTenantQuota(quota int64) *Config {
	c.DefaultTenantQuota = quota
	return c
}

// WithDatabaseHealthCheckEnabled enables or disables database health checks.
func (c *Config) WithDatabaseHealthCheckEnabled(enabled bool) *Config {
	c.EnableDatabaseHealthCheck = enabled
	return c
}

// WithRetryPolicy sets retry policy configuration.
func (c *Config) WithRetryPolicy(value *RetryPolicyConfig) *Config {
	if value != nil {
		c.RetryPolicy = *value
	}
	return c
}

// WithTenantManager sets tenant manager configuration.
func (c *Config) WithTenantManager(value *TenantManagerConfig) *Config {
	if value != nil {
		c.TenantManager = *value
	}
	return c
}

// WithMetadata sets metadata repository configuration.
func (c *Config) WithMetadata(value *MetadataConfig) *Config {
	if value != nil {
		c.Metadata = *value
	}
	return c
}

// WithSqlite sets the SQLite engine configuration.
func (c *Config) WithSqlite(value *SqliteConfig) *Config {
	if value != nil {
		c.Sqlite = *value
	}
	return c
}

// WithVolumes replaces configured volumes with copies of values.
func (c *Config) WithVolumes(values ...*VolumeConfig) *Config {
	c.Volumes = c.Volumes[:0]
	for _, value := range values {
		if value != nil {
			c.Volumes = append(c.Volumes, *value)
		}
	}
	return c
}

// AddVolume appends one volume configuration.
func (c *Config) AddVolume(value *VolumeConfig) *Config {
	if value != nil {
		c.Volumes = append(c.Volumes, *value)
	}
	return c
}

// WithTenants replaces configured tenants with copies of values.
func (c *Config) WithTenants(values ...*TenantConfig) *Config {
	c.Tenants = c.Tenants[:0]
	for _, value := range values {
		if value != nil {
			c.Tenants = append(c.Tenants, cloneTenantConfig(*value))
		}
	}
	return c
}

// AddTenant appends one tenant configuration.
func (c *Config) AddTenant(value *TenantConfig) *Config {
	if value != nil {
		c.Tenants = append(c.Tenants, cloneTenantConfig(*value))
	}
	return c
}

// WithFileWatchers replaces configured file watchers with copies of values.
func (c *Config) WithFileWatchers(values ...*FileWatcherConfig) *Config {
	c.FileWatchers = c.FileWatchers[:0]
	for _, value := range values {
		if value != nil {
			c.FileWatchers = append(c.FileWatchers, cloneFileWatcherConfig(*value))
		}
	}
	return c
}

// WithFileWatcherRoots replaces the root watcher templates with copies.
func (c *Config) WithFileWatcherRoots(values ...*FileWatcherRootConfig) *Config {
	c.FileWatcherRoots = c.FileWatcherRoots[:0]
	for _, value := range values {
		if value != nil {
			c.FileWatcherRoots = append(c.FileWatcherRoots, cloneFileWatcherRootConfig(*value))
		}
	}
	return c
}

// AddFileWatcherRoot appends one root watcher template.
func (c *Config) AddFileWatcherRoot(value *FileWatcherRootConfig) *Config {
	if value != nil {
		c.FileWatcherRoots = append(c.FileWatcherRoots, cloneFileWatcherRootConfig(*value))
	}
	return c
}

// WithFileWatcherService sets the global background watcher service options.
func (c *Config) WithFileWatcherService(value *FileWatcherServiceConfig) *Config {
	if value != nil {
		c.FileWatcherService = *value
	}
	return c
}

// WithSourceCleanup sets durable post-import source cleanup options.
func (c *Config) WithSourceCleanup(value *SourceCleanupConfig) *Config {
	if value != nil {
		c.SourceCleanup = *value
	}
	return c
}

// NewSourceCleanupConfig returns source cleanup options with Venue defaults.
func NewSourceCleanupConfig() *SourceCleanupConfig {
	value := DefaultConfig().SourceCleanup
	return &value
}

// WithEnabled enables or disables durable source cleanup.
func (c *SourceCleanupConfig) WithEnabled(enabled bool) *SourceCleanupConfig {
	c.Enabled = enabled
	return c
}

// WithDatabasePath sets the source cleanup SQLite database path.
func (c *SourceCleanupConfig) WithDatabasePath(value string) *SourceCleanupConfig {
	c.DatabasePath = value
	return c
}

// WithPollingInterval sets the cleanup worker polling interval.
func (c *SourceCleanupConfig) WithPollingInterval(value time.Duration) *SourceCleanupConfig {
	c.PollingInterval = value
	return c
}

// WithMaxConcurrentActions bounds concurrent source cleanup actions.
func (c *SourceCleanupConfig) WithMaxConcurrentActions(value int) *SourceCleanupConfig {
	c.MaxConcurrentActions = value
	return c
}

// WithMaxActiveJobs bounds durable active cleanup reservations.
func (c *SourceCleanupConfig) WithMaxActiveJobs(value int) *SourceCleanupConfig {
	c.MaxActiveJobs = value
	return c
}

// WithTerminalJobRetention sets how long terminal cleanup records are kept.
func (c *SourceCleanupConfig) WithTerminalJobRetention(value time.Duration) *SourceCleanupConfig {
	c.TerminalJobRetentionPeriod = value
	return c
}

// WithImportReservationTimeout sets the stale import reservation timeout.
func (c *SourceCleanupConfig) WithImportReservationTimeout(value time.Duration) *SourceCleanupConfig {
	c.ImportReservationTimeout = value
	return c
}

// WithDatabaseOptimization enables or disables cleanup database optimization.
func (c *SourceCleanupConfig) WithDatabaseOptimization(enabled bool) *SourceCleanupConfig {
	c.EnableDatabaseOptimization = enabled
	return c
}

// WithDatabaseOptimizationInterval sets the optimization interval.
func (c *SourceCleanupConfig) WithDatabaseOptimizationInterval(value time.Duration) *SourceCleanupConfig {
	c.DatabaseOptimizationInterval = value
	return c
}

// WithTerminalPruneBatchSize sets the terminal record prune batch size.
func (c *SourceCleanupConfig) WithTerminalPruneBatchSize(value int) *SourceCleanupConfig {
	c.TerminalPruneBatchSize = value
	return c
}

// NewFileWatcherServiceConfig returns the default global watcher options.
func NewFileWatcherServiceConfig() *FileWatcherServiceConfig {
	value := DefaultConfig().FileWatcherService
	return &value
}

// WithEnabled enables or disables the background watcher service globally.
func (c *FileWatcherServiceConfig) WithEnabled(enabled bool) *FileWatcherServiceConfig {
	c.Enabled = enabled
	return c
}

// WithDefaultPollingInterval sets the interval used when a watcher omits one.
func (c *FileWatcherServiceConfig) WithDefaultPollingInterval(value time.Duration) *FileWatcherServiceConfig {
	c.DefaultPollingInterval = value
	return c
}

// WithPollingIntervalBounds sets the clamp applied to per-watcher intervals.
func (c *FileWatcherServiceConfig) WithPollingIntervalBounds(minimum, maximum time.Duration) *FileWatcherServiceConfig {
	c.MinimumPollingInterval = minimum
	c.MaximumPollingInterval = maximum
	return c
}

// WithDisabledCheckInterval sets how often a disabled service rechecks itself.
func (c *FileWatcherServiceConfig) WithDisabledCheckInterval(value time.Duration) *FileWatcherServiceConfig {
	c.DisabledCheckInterval = value
	return c
}

// WithMaxParallelScans bounds how many watchers one cycle scans in parallel.
func (c *FileWatcherServiceConfig) WithMaxParallelScans(value int) *FileWatcherServiceConfig {
	c.MaxParallelWatcherScans = value
	return c
}

// NewFileWatcherRootConfig returns a multi-tenant root watcher template.
func NewFileWatcherRootConfig(rootPath string) *FileWatcherRootConfig {
	return &FileWatcherRootConfig{
		RootPath:                          rootPath,
		MultiTenantMode:                   true,
		Enabled:                           true,
		IncludeSubdirectories:             true,
		FilePatterns:                      []string{"*.*"},
		PostImportAction:                  "Delete",
		PollingInterval:                   30 * time.Second,
		MinFileAge:                        5 * time.Second,
		MaxConcurrentImports:              4,
		MaxPostImportActionRetryCount:     defaultMaxPostImportActionRetryCount,
		PostImportActionRetryInitialDelay: defaultPostImportActionRetryInitialDelay,
		PostImportActionRetryMaxDelay:     defaultPostImportActionRetryMaxDelay,
		SourceCleanupFailureDirectory:     defaultSourceCleanupFailureDirectory,

		AutoCreateTenantDirectoriesCacheTTL: defaultAutoCreateTenantDirectoriesCacheTTL,
		FileStabilityCheckDelay:             defaultFileStabilityCheckDelay,
		SkipStabilityCheckAfterAge:          defaultSkipStabilityCheckAfterAge,
		ImportedFilesPruneInterval:          defaultImportedFilesPruneInterval,
		ImportedFilesHistoryFlushInterval:   defaultImportedFilesHistoryFlushInterval,
	}
}

// WithRootPath sets the directory whose subdirectories are tenants.
func (c *FileWatcherRootConfig) WithRootPath(value string) *FileWatcherRootConfig {
	c.RootPath = value
	return c
}

// WithMultiTenantMode enables or disables per-tenant derivation.
func (c *FileWatcherRootConfig) WithMultiTenantMode(enabled bool) *FileWatcherRootConfig {
	c.MultiTenantMode = enabled
	return c
}

// WithEnabled enables or disables every derived watcher.
func (c *FileWatcherRootConfig) WithEnabled(enabled bool) *FileWatcherRootConfig {
	c.Enabled = enabled
	return c
}

// WithSubdirectories enables or disables recursive scanning for derived watchers.
func (c *FileWatcherRootConfig) WithSubdirectories(enabled bool) *FileWatcherRootConfig {
	c.IncludeSubdirectories = enabled
	return c
}

// WithFilePatterns replaces the derived watcher file patterns.
func (c *FileWatcherRootConfig) WithFilePatterns(values ...string) *FileWatcherRootConfig {
	c.FilePatterns = append([]string(nil), values...)
	return c
}

// WithPostImportAction sets the derived watcher post-import action.
func (c *FileWatcherRootConfig) WithPostImportAction(value string) *FileWatcherRootConfig {
	c.PostImportAction = value
	return c
}

// WithMoveToDirectory sets the derived watcher move destination.
func (c *FileWatcherRootConfig) WithMoveToDirectory(value string) *FileWatcherRootConfig {
	c.MoveToDirectory = value
	return c
}

// WithSourceCleanupFailureDirectory sets the derived watcher quarantine directory.
func (c *FileWatcherRootConfig) WithSourceCleanupFailureDirectory(value string) *FileWatcherRootConfig {
	c.SourceCleanupFailureDirectory = value
	return c
}

// WithPollingInterval sets the derived watcher polling interval.
func (c *FileWatcherRootConfig) WithPollingInterval(value time.Duration) *FileWatcherRootConfig {
	c.PollingInterval = value
	return c
}

// WithMaxFileSizeBytes sets the derived watcher file-size limit.
func (c *FileWatcherRootConfig) WithMaxFileSizeBytes(value int64) *FileWatcherRootConfig {
	c.MaxFileSizeBytes = value
	return c
}

// WithMinFileAge sets the derived watcher minimum file age.
func (c *FileWatcherRootConfig) WithMinFileAge(value time.Duration) *FileWatcherRootConfig {
	c.MinFileAge = value
	return c
}

// WithMaxConcurrentImports sets the derived watcher concurrency limit.
func (c *FileWatcherRootConfig) WithMaxConcurrentImports(value int) *FileWatcherRootConfig {
	c.MaxConcurrentImports = value
	return c
}

// WithPostImportActionRetry configures delete or move retry attempts and backoff.
func (c *FileWatcherRootConfig) WithPostImportActionRetry(maxAttempts int, initialDelay, maxDelay time.Duration) *FileWatcherRootConfig {
	c.MaxPostImportActionRetryCount = maxAttempts
	c.PostImportActionRetryInitialDelay = initialDelay
	c.PostImportActionRetryMaxDelay = maxDelay
	return c
}

// WithAutoCreateTenantDirectoryCacheTTL sets how long the tenant list used by
// automatic tenant-directory creation is cached for every derived watcher.
func (c *FileWatcherRootConfig) WithAutoCreateTenantDirectoryCacheTTL(value time.Duration) *FileWatcherRootConfig {
	c.AutoCreateTenantDirectoriesCacheTTL = value
	return c
}

// WithStabilityChecks configures the delayed second stability probe of every
// derived watcher.
func (c *FileWatcherRootConfig) WithStabilityChecks(delay, skipAfterAge time.Duration) *FileWatcherRootConfig {
	c.FileStabilityCheckDelay = delay
	c.SkipStabilityCheckAfterAge = skipAfterAge
	return c
}

// WithImportedFilesHistoryPersistence configures import-history housekeeping for
// every derived watcher.
func (c *FileWatcherRootConfig) WithImportedFilesHistoryPersistence(
	pruneThrottle bool,
	pruneInterval time.Duration,
	flushDebounce bool,
	flushInterval time.Duration,
) *FileWatcherRootConfig {
	c.DisableImportedFilesPruneThrottle = !pruneThrottle
	c.ImportedFilesPruneInterval = pruneInterval
	c.DisableImportedFilesHistoryFlushDebounce = !flushDebounce
	c.ImportedFilesHistoryFlushInterval = flushInterval
	return c
}

// AddFileWatcher appends one file watcher configuration.
func (c *Config) AddFileWatcher(value *FileWatcherConfig) *Config {
	if value != nil {
		c.FileWatchers = append(c.FileWatchers, cloneFileWatcherConfig(*value))
	}
	return c
}

// WithBackgroundCleanupEnabled enables or disables background cleanup.
func (c *Config) WithBackgroundCleanupEnabled(enabled bool) *Config {
	c.EnableBackgroundCleanup = enabled
	return c
}

// WithCleanup sets cleanup configuration.
func (c *Config) WithCleanup(value *CleanupConfig) *Config {
	if value != nil {
		c.Cleanup = *value
	}
	return c
}

// WithDatabaseHealthCheck sets database health-check configuration.
func (c *Config) WithDatabaseHealthCheck(value *DatabaseHealthCheckConfig) *Config {
	if value != nil {
		c.DatabaseHealthCheck = *value
	}
	return c
}

// WithOrphanRecovery sets orphan-file recovery configuration.
func (c *Config) WithOrphanRecovery(value *OrphanRecoveryConfig) *Config {
	if value != nil {
		c.OrphanRecovery = *value
	}
	return c
}

// WithLogging sets instance logging configuration.
func (c *Config) WithLogging(value *logging.Config) *Config { c.Logging = value; return c }

// WithFailFastOnStartupRecoveryFailure makes startup fail when a database had to
// be quarantined and could not be restored from a backup.
func (c *Config) WithFailFastOnStartupRecoveryFailure(enabled bool) *Config {
	c.FailFastOnStartupRecoveryFailure = enabled
	return c
}

// WithStatistics sets in-process statistics configuration.
func (c *Config) WithStatistics(value *StatisticsConfig) *Config {
	if value != nil {
		c.Statistics = *value
	}
	return c
}

// NewStatisticsConfig returns statistics configuration with Venue defaults.
func NewStatisticsConfig() *StatisticsConfig {
	return &StatisticsConfig{
		WindowSize: 5 * time.Minute,
		Retention:  time.Hour,
		MaxSeries:  defaultStatisticsMaxSeries,
		Dimensions: StatisticsDimensionConfig{VolumeID: true, WatcherID: true, Operation: true},
		Output: StatisticsOutputConfig{
			Sink:        "Logging",
			Interval:    time.Minute,
			QueryWindow: 15 * time.Minute,
		},
	}
}

// WithEnabled turns statistics collection on or off.
func (c *StatisticsConfig) WithEnabled(enabled bool) *StatisticsConfig {
	c.Enabled = enabled
	return c
}

// WithWindowSize sets the aggregation bucket size.
func (c *StatisticsConfig) WithWindowSize(value time.Duration) *StatisticsConfig {
	c.WindowSize = value
	return c
}

// WithRetention sets how long in-memory buckets are kept.
func (c *StatisticsConfig) WithRetention(value time.Duration) *StatisticsConfig {
	c.Retention = value
	return c
}

// WithMaxSeries bounds the number of retained statistics series.
func (c *StatisticsConfig) WithMaxSeries(value int) *StatisticsConfig {
	c.MaxSeries = value
	return c
}

// WithDimensions sets which statistics dimensions are retained.
func (c *StatisticsConfig) WithDimensions(value *StatisticsDimensionConfig) *StatisticsConfig {
	if value != nil {
		c.Dimensions = *value
	}
	return c
}

// WithOutput sets periodic statistics output configuration.
func (c *StatisticsConfig) WithOutput(value *StatisticsOutputConfig) *StatisticsConfig {
	if value != nil {
		c.Output = *value
	}
	return c
}

// NewStatisticsDimensionConfig returns the default dimension selection.
func NewStatisticsDimensionConfig() *StatisticsDimensionConfig {
	return &StatisticsDimensionConfig{VolumeID: true, WatcherID: true, Operation: true}
}

// WithTenantIDDimension retains or drops the tenant_id dimension.
func (c *StatisticsDimensionConfig) WithTenantIDDimension(enabled bool) *StatisticsDimensionConfig {
	c.TenantID = enabled
	return c
}

// WithVolumeIDDimension retains or drops the volume_id dimension.
func (c *StatisticsDimensionConfig) WithVolumeIDDimension(enabled bool) *StatisticsDimensionConfig {
	c.VolumeID = enabled
	return c
}

// WithWatcherIDDimension retains or drops the watcher_id dimension.
func (c *StatisticsDimensionConfig) WithWatcherIDDimension(enabled bool) *StatisticsDimensionConfig {
	c.WatcherID = enabled
	return c
}

// WithOperationDimension retains or drops the operation dimension.
func (c *StatisticsDimensionConfig) WithOperationDimension(enabled bool) *StatisticsDimensionConfig {
	c.Operation = enabled
	return c
}

// NewStatisticsOutputConfig returns the default statistics output configuration.
func NewStatisticsOutputConfig() *StatisticsOutputConfig {
	return &StatisticsOutputConfig{
		Sink:        "Logging",
		Interval:    time.Minute,
		QueryWindow: 15 * time.Minute,
	}
}

// WithOutputEnabled turns periodic statistics output on or off.
func (c *StatisticsOutputConfig) WithOutputEnabled(enabled bool) *StatisticsOutputConfig {
	c.Enabled = enabled
	return c
}

// WithOutputInterval sets the delay between summaries.
func (c *StatisticsOutputConfig) WithOutputInterval(value time.Duration) *StatisticsOutputConfig {
	c.Interval = value
	return c
}

// WithOutputQueryWindow sets the time range included in each summary.
func (c *StatisticsOutputConfig) WithOutputQueryWindow(value time.Duration) *StatisticsOutputConfig {
	c.QueryWindow = value
	return c
}

// WithIncludeEmptySnapshots logs summaries even when every counter is zero.
func (c *StatisticsOutputConfig) WithIncludeEmptySnapshots(enabled bool) *StatisticsOutputConfig {
	c.IncludeEmptySnapshots = enabled
	return c
}

// NewRetryPolicyConfig returns the default retry policy configuration.
func NewRetryPolicyConfig() *RetryPolicyConfig {
	value := DefaultConfig().RetryPolicy
	return &value
}

// WithMaxRetryCount sets the maximum retry count.
func (c *RetryPolicyConfig) WithMaxRetryCount(value int) *RetryPolicyConfig {
	c.MaxRetryCount = value
	return c
}

// WithInitialRetryDelay sets the initial retry delay.
func (c *RetryPolicyConfig) WithInitialRetryDelay(value time.Duration) *RetryPolicyConfig {
	c.InitialRetryDelay = value
	return c
}

// WithExponentialBackoff enables or disables exponential backoff.
func (c *RetryPolicyConfig) WithExponentialBackoff(enabled bool) *RetryPolicyConfig {
	c.UseExponentialBackoff = enabled
	return c
}

// WithMaxRetryDelay sets the maximum retry delay.
func (c *RetryPolicyConfig) WithMaxRetryDelay(value time.Duration) *RetryPolicyConfig {
	c.MaxRetryDelay = value
	return c
}

// NewTenantManagerConfig returns default tenant manager configuration.
func NewTenantManagerConfig() *TenantManagerConfig {
	value := DefaultConfig().TenantManager
	return &value
}

// WithMetadataPath sets the tenant metadata path.
func (c *TenantManagerConfig) WithMetadataPath(value string) *TenantManagerConfig {
	c.MetadataPath = value
	return c
}

// WithCacheTTL sets the tenant cache lifetime.
func (c *TenantManagerConfig) WithCacheTTL(value time.Duration) *TenantManagerConfig {
	c.CacheTTL = value
	return c
}

// NewMetadataConfig returns default metadata configuration.
func NewMetadataConfig() *MetadataConfig {
	value := DefaultConfig().Metadata
	return &value
}

// WithCacheTTL sets the metadata cache lifetime.
func (c *MetadataConfig) WithCacheTTL(value time.Duration) *MetadataConfig {
	c.CacheTTL = value
	return c
}

// WithMaxCacheEntries sets the metadata cache entry limit.
func (c *MetadataConfig) WithMaxCacheEntries(value int) *MetadataConfig {
	c.MaxCacheEntries = value
	return c
}

// NewSqliteConfig returns the default SQLite engine configuration.
func NewSqliteConfig() *SqliteConfig {
	value := DefaultConfig().Sqlite
	return &value
}

// WithJournalMode sets the SQLite journal mode.
func (c *SqliteConfig) WithJournalMode(value string) *SqliteConfig {
	c.JournalMode = value
	return c
}

// WithSynchronousMode sets the SQLite synchronous mode.
func (c *SqliteConfig) WithSynchronousMode(value string) *SqliteConfig {
	c.SynchronousMode = value
	return c
}

// WithCacheSizeKb sets the per-connection SQLite page cache.
func (c *SqliteConfig) WithCacheSizeKb(value int) *SqliteConfig {
	c.CacheSizeKb = value
	return c
}

// WithBusyTimeout sets how long a statement waits for a SQLite lock.
func (c *SqliteConfig) WithBusyTimeout(value time.Duration) *SqliteConfig {
	c.BusyTimeoutMs = int(value / time.Millisecond)
	return c
}

// WithCheckpointAfterBatch enables or disables a passive WAL checkpoint after a
// committed write batch.
func (c *SqliteConfig) WithCheckpointAfterBatch(enabled bool) *SqliteConfig {
	c.CheckpointAfterBatch = enabled
	return c
}

// WithMaxOpenConns sets the connection limit of each tenant's pool.
func (c *SqliteConfig) WithMaxOpenConns(value int) *SqliteConfig {
	c.MaxOpenConns = value
	return c
}

// WithMaxOpenDatabases bounds how many tenant database handles stay open.
func (c *SqliteConfig) WithMaxOpenDatabases(value int) *SqliteConfig {
	c.MaxOpenDatabases = value
	return c
}

// WithOpenDatabaseIdleTimeout closes a tenant handle that has been idle this long.
func (c *SqliteConfig) WithOpenDatabaseIdleTimeout(value time.Duration) *SqliteConfig {
	c.OpenDatabaseIdleTimeout = value
	return c
}

// WithCorruptedDatabaseRecovery enables or disables quarantining a corrupt
// database file and recreating it.
func (c *SqliteConfig) WithCorruptedDatabaseRecovery(enabled bool) *SqliteConfig {
	c.RecoverCorruptedDatabase = enabled
	return c
}

// WithCorruptedDatabaseRetention sets how long a quarantined database is kept.
func (c *SqliteConfig) WithCorruptedDatabaseRetention(value time.Duration) *SqliteConfig {
	c.CorruptedDatabaseRetention = value
	return c
}

// WithBackup configures the per-tenant consistent backup tree. An empty
// directory disables backups.
func (c *SqliteConfig) WithBackup(directory string, interval time.Duration, retention time.Duration) *SqliteConfig {
	c.BackupDirectory = directory
	c.BackupInterval = interval
	c.BackupRetention = retention
	return c
}

// WithAutoRestoreFromBackup enables or disables restoring the newest backup of a
// quarantined tenant database.
func (c *SqliteConfig) WithAutoRestoreFromBackup(enabled bool) *SqliteConfig {
	c.AutoRestoreFromBackup = enabled
	return c
}

// WithBackupVerificationSkipped turns the backup integrity check on or off.
// Verification is on by default, so false keeps it enabled.
func (c *SqliteConfig) WithBackupVerificationSkipped(skip bool) *SqliteConfig {
	c.SkipBackupVerification = skip
	return c
}

// WithOptimizeIdleTenantDatabases enables or disables VACUUM of tenants that
// currently have no open handle.
func (c *SqliteConfig) WithOptimizeIdleTenantDatabases(enabled bool) *SqliteConfig {
	c.OptimizeIdleTenantDatabases = enabled
	return c
}

// WithStartupHealthChecks sets how long startup waits before the first
// health-check retry for this volume and the delay between attempts.
func (c *VolumeConfig) WithStartupHealthChecks(initialDelay, healthCheckDelay time.Duration) *VolumeConfig {
	c.InitialDelay = initialDelay
	c.HealthCheckDelay = healthCheckDelay
	return c
}

// WithWarmupOnStartup enables or disables the throwaway warmup write performed
// after the volume passed its startup health checks.
func (c *VolumeConfig) WithWarmupOnStartup(enabled bool) *VolumeConfig {
	c.WarmupOnStartup = enabled
	return c
}

// WithTimedOutReclaimBatchSizes sets the immediate and background timed-out
// reclaim batch sizes.
func (c *CleanupConfig) WithTimedOutReclaimBatchSizes(emptyQueueBatchSize, backgroundBatchSize int) *CleanupConfig {
	c.EmptyQueueReclaimBatchSize = emptyQueueBatchSize
	c.BackgroundTimedOutReclaimBatchSize = backgroundBatchSize
	return c
}

// WithBackgroundTimedOutReclaim enables or disables the opportunistic
// background reclaim of timed-out files.
func (c *CleanupConfig) WithBackgroundTimedOutReclaim(enabled bool) *CleanupConfig {
	c.EnableBackgroundTimedOutReclaim = enabled
	return c
}

// NewVolumeConfig returns an empty local filesystem volume configuration.
func NewVolumeConfig() *VolumeConfig {
	return &VolumeConfig{VolumeType: "LocalFileSystem", ShardingDepth: 2, EnableFsync: true}
}

// WithVolumeID sets the volume identifier.
func (c *VolumeConfig) WithVolumeID(value string) *VolumeConfig { c.VolumeID = value; return c }

// WithMountPath sets the volume mount path.
func (c *VolumeConfig) WithMountPath(value string) *VolumeConfig { c.MountPath = value; return c }

// WithVolumeType sets the volume implementation type.
func (c *VolumeConfig) WithVolumeType(value string) *VolumeConfig { c.VolumeType = value; return c }

// WithShardingDepth sets the physical directory sharding depth.
func (c *VolumeConfig) WithShardingDepth(value int) *VolumeConfig { c.ShardingDepth = value; return c }

// WithFsync enables or disables fsync after volume writes.
func (c *VolumeConfig) WithFsync(enabled bool) *VolumeConfig { c.EnableFsync = enabled; return c }

// WithHealthCheckCacheTTL sets how long the volume health probe is cached.
// Zero selects the runtime default; a negative value disables caching.
func (c *VolumeConfig) WithHealthCheckCacheTTL(value time.Duration) *VolumeConfig {
	c.HealthCheckCacheTTL = value
	return c
}

// NewTenantConfig returns a tenant configuration with the supplied identifier.
func NewTenantConfig(tenantID string) *TenantConfig {
	return &TenantConfig{TenantID: tenantID, Enabled: true}
}

// WithTenantID sets the tenant identifier.
func (c *TenantConfig) WithTenantID(value string) *TenantConfig { c.TenantID = value; return c }

// WithEnabled enables or disables the tenant.
func (c *TenantConfig) WithEnabled(enabled bool) *TenantConfig { c.Enabled = enabled; return c }

// WithQuota sets an explicit tenant quota.
func (c *TenantConfig) WithQuota(value int64) *TenantConfig { c.Quota = &value; return c }

// WithoutQuota makes the tenant use the configured default quota.
func (c *TenantConfig) WithoutQuota() *TenantConfig { c.Quota = nil; return c }

// NewFileWatcherConfig returns default file watcher configuration.
//
// The Go defaults mirror the Locus defaults: a watcher is enabled, scans
// subdirectories, requires a 5 second minimum file age, and uses 4 concurrent
// imports. File-bound configuration must set these explicitly, because a boolean
// key that is absent from the source cannot be distinguished from an explicit
// false value.
func NewFileWatcherConfig() *FileWatcherConfig {
	return &FileWatcherConfig{
		Enabled:                           true,
		IncludeSubdirectories:             true,
		FilePatterns:                      []string{"*.*"},
		PostImportAction:                  "Delete",
		PollingInterval:                   30 * time.Second,
		MinFileAge:                        5 * time.Second,
		MaxConcurrentImports:              4,
		MaxPostImportActionRetryCount:     defaultMaxPostImportActionRetryCount,
		PostImportActionRetryInitialDelay: defaultPostImportActionRetryInitialDelay,
		PostImportActionRetryMaxDelay:     defaultPostImportActionRetryMaxDelay,
		SourceCleanupFailureDirectory:     defaultSourceCleanupFailureDirectory,

		AutoCreateTenantDirectoriesCacheTTL: defaultAutoCreateTenantDirectoriesCacheTTL,
		FileStabilityCheckDelay:             defaultFileStabilityCheckDelay,
		SkipStabilityCheckAfterAge:          defaultSkipStabilityCheckAfterAge,
		ImportedFilesPruneInterval:          defaultImportedFilesPruneInterval,
		ImportedFilesHistoryFlushInterval:   defaultImportedFilesHistoryFlushInterval,
	}
}

// WithAutoCreateTenantDirectoryCacheTTL sets how long the tenant list used by
// automatic tenant-directory creation is cached.
func (c *FileWatcherConfig) WithAutoCreateTenantDirectoryCacheTTL(value time.Duration) *FileWatcherConfig {
	c.AutoCreateTenantDirectoriesCacheTTL = value
	return c
}

// WithStabilityChecks configures the delayed second stability probe. A negative
// delay disables the probe; a negative skip age always probes.
func (c *FileWatcherConfig) WithStabilityChecks(delay, skipAfterAge time.Duration) *FileWatcherConfig {
	c.FileStabilityCheckDelay = delay
	c.SkipStabilityCheckAfterAge = skipAfterAge
	return c
}

// WithImportedFilesHistoryPersistence configures import-history housekeeping:
// whether stale pruning is throttled, the minimum prune interval, whether
// history writes are debounced, and the minimum debounce flush interval.
func (c *FileWatcherConfig) WithImportedFilesHistoryPersistence(
	pruneThrottle bool,
	pruneInterval time.Duration,
	flushDebounce bool,
	flushInterval time.Duration,
) *FileWatcherConfig {
	c.DisableImportedFilesPruneThrottle = !pruneThrottle
	c.ImportedFilesPruneInterval = pruneInterval
	c.DisableImportedFilesHistoryFlushDebounce = !flushDebounce
	c.ImportedFilesHistoryFlushInterval = flushInterval
	return c
}

// WithWatcherID sets the watcher identifier.
func (c *FileWatcherConfig) WithWatcherID(value string) *FileWatcherConfig {
	c.WatcherID = value
	return c
}

// WithTenantID sets the watcher tenant identifier.
func (c *FileWatcherConfig) WithTenantID(value string) *FileWatcherConfig {
	c.TenantID = value
	return c
}

// WithMultiTenantMode enables or disables multi-tenant directory mode.
func (c *FileWatcherConfig) WithMultiTenantMode(enabled bool) *FileWatcherConfig {
	c.MultiTenantMode = enabled
	return c
}

// WithAutoCreateTenantDirectories controls automatic tenant directory creation.
func (c *FileWatcherConfig) WithAutoCreateTenantDirectories(enabled bool) *FileWatcherConfig {
	c.AutoCreateTenantDirectories = enabled
	return c
}

// WithWatchPath sets the watched directory.
func (c *FileWatcherConfig) WithWatchPath(value string) *FileWatcherConfig {
	c.WatchPath = value
	return c
}

// WithEnabled enables or disables the watcher.
func (c *FileWatcherConfig) WithEnabled(enabled bool) *FileWatcherConfig {
	c.Enabled = enabled
	return c
}

// WithSubdirectories enables or disables recursive watching.
func (c *FileWatcherConfig) WithSubdirectories(enabled bool) *FileWatcherConfig {
	c.IncludeSubdirectories = enabled
	return c
}

// WithFilePatterns replaces watcher file patterns.
func (c *FileWatcherConfig) WithFilePatterns(values ...string) *FileWatcherConfig {
	c.FilePatterns = append([]string(nil), values...)
	return c
}

// WithPostImportAction sets the post-import action.
func (c *FileWatcherConfig) WithPostImportAction(value string) *FileWatcherConfig {
	c.PostImportAction = value
	return c
}

// WithMoveToDirectory sets the post-import move destination.
func (c *FileWatcherConfig) WithMoveToDirectory(value string) *FileWatcherConfig {
	c.MoveToDirectory = value
	return c
}

// WithSourceCleanupFailureDirectory sets the watcher quarantine directory.
func (c *FileWatcherConfig) WithSourceCleanupFailureDirectory(value string) *FileWatcherConfig {
	c.SourceCleanupFailureDirectory = value
	return c
}

// WithPollingInterval sets the watcher polling interval.
func (c *FileWatcherConfig) WithPollingInterval(value time.Duration) *FileWatcherConfig {
	c.PollingInterval = value
	return c
}

// WithMaxFileSizeBytes sets the watcher file-size limit.
func (c *FileWatcherConfig) WithMaxFileSizeBytes(value int64) *FileWatcherConfig {
	c.MaxFileSizeBytes = value
	return c
}

// WithMinFileAge sets the minimum file age before import.
func (c *FileWatcherConfig) WithMinFileAge(value time.Duration) *FileWatcherConfig {
	c.MinFileAge = value
	return c
}

// WithMaxConcurrentImports sets the watcher concurrency limit.
func (c *FileWatcherConfig) WithMaxConcurrentImports(value int) *FileWatcherConfig {
	c.MaxConcurrentImports = value
	return c
}

// WithPostImportActionRetry configures delete or move retry attempts and backoff.
func (c *FileWatcherConfig) WithPostImportActionRetry(maxAttempts int, initialDelay, maxDelay time.Duration) *FileWatcherConfig {
	c.MaxPostImportActionRetryCount = maxAttempts
	c.PostImportActionRetryInitialDelay = initialDelay
	c.PostImportActionRetryMaxDelay = maxDelay
	return c
}

// NewCleanupConfig returns default cleanup configuration.
func NewCleanupConfig() *CleanupConfig {
	value := DefaultConfig().Cleanup
	return &value
}

// WithCleanupInterval sets the cleanup interval.
func (c *CleanupConfig) WithCleanupInterval(value time.Duration) *CleanupConfig {
	c.CleanupInterval = value
	return c
}

// WithInitialDelay sets the initial cleanup delay.
func (c *CleanupConfig) WithInitialDelay(value time.Duration) *CleanupConfig {
	c.InitialDelay = value
	return c
}

// WithEmptyDirectoryCleanup controls empty-directory cleanup.
func (c *CleanupConfig) WithEmptyDirectoryCleanup(enabled bool) *CleanupConfig {
	c.CleanupEmptyDirectories = enabled
	return c
}

// WithTimedOutFileCleanup controls timed-out processing cleanup.
func (c *CleanupConfig) WithTimedOutFileCleanup(enabled bool) *CleanupConfig {
	c.CleanupTimedOutFiles = enabled
	return c
}

// WithProcessingTimeout sets the processing timeout.
func (c *CleanupConfig) WithProcessingTimeout(value time.Duration) *CleanupConfig {
	c.ProcessingTimeout = value
	return c
}

// WithPermanentlyFailedFileCleanup controls permanently-failed cleanup.
func (c *CleanupConfig) WithPermanentlyFailedFileCleanup(enabled bool) *CleanupConfig {
	c.CleanupPermanentlyFailedFiles = enabled
	return c
}

// WithFailedFileRetention sets failed-file retention.
func (c *CleanupConfig) WithFailedFileRetention(value time.Duration) *CleanupConfig {
	c.FailedFileRetentionPeriod = value
	return c
}

// WithCompletedRecordCleanup controls completed-record cleanup.
func (c *CleanupConfig) WithCompletedRecordCleanup(enabled bool) *CleanupConfig {
	c.CleanupCompletedRecords = enabled
	return c
}

// WithCompletedRecordRetention sets completed-record retention.
func (c *CleanupConfig) WithCompletedRecordRetention(value time.Duration) *CleanupConfig {
	c.CompletedRecordRetentionPeriod = value
	return c
}

// WithOrphanedMetadataCleanup enables or disables orphaned-metadata cleanup.
func (c *CleanupConfig) WithOrphanedMetadataCleanup(enabled bool) *CleanupConfig {
	c.CleanupOrphanedMetadata = enabled
	return c
}

// WithTimedOutReclaimOnEmptyQueue controls immediate timed-out reclaim when a
// worker finds no claimable file.
func (c *CleanupConfig) WithTimedOutReclaimOnEmptyQueue(enabled bool) *CleanupConfig {
	c.RecoverTimedOutOnEmptyQueue = enabled
	return c
}

// WithTimedOutReclaimCooldown sets the per-tenant cooldown between immediate
// timed-out reclaim attempts.
func (c *CleanupConfig) WithTimedOutReclaimCooldown(value time.Duration) *CleanupConfig {
	c.TimedOutReclaimCooldown = value
	return c
}

// WithPermanentlyFailedDisposition sets how permanently failed payloads are
// handled after their retention elapses: "Keep", "MoveToDeadLetter", or "Delete".
func (c *CleanupConfig) WithPermanentlyFailedDisposition(value string) *CleanupConfig {
	c.PermanentlyFailedDisposition = value
	return c
}

// WithDeadLetter sets the dead-letter layout.
func (c *CleanupConfig) WithDeadLetter(value *DeadLetterConfig) *CleanupConfig {
	if value != nil {
		c.DeadLetter = *value
	}
	return c
}

// WithJunkFileCleanup enables or disables OS junk-file cleanup.
func (c *CleanupConfig) WithJunkFileCleanup(enabled bool) *CleanupConfig {
	c.CleanupJunkFiles = enabled
	return c
}

// WithJunkFileCleanupInterval sets the minimum interval between junk-file sweeps.
func (c *CleanupConfig) WithJunkFileCleanupInterval(value time.Duration) *CleanupConfig {
	c.JunkFileCleanupInterval = value
	return c
}

// WithInvalidDatabaseBackupCleanup enables or disables quarantined database
// backup removal.
func (c *CleanupConfig) WithInvalidDatabaseBackupCleanup(enabled bool) *CleanupConfig {
	c.CleanupInvalidDatabaseBackups = enabled
	return c
}

// WithRetiredVolumes replaces the retired volume declarations with copies.
func (c *CleanupConfig) WithRetiredVolumes(values ...*RetiredVolumeConfig) *CleanupConfig {
	c.RetiredVolumes = c.RetiredVolumes[:0]
	for _, value := range values {
		if value != nil {
			c.RetiredVolumes = append(c.RetiredVolumes, *value)
		}
	}
	return c
}

// AddRetiredVolume appends one retired volume declaration.
func (c *CleanupConfig) AddRetiredVolume(value *RetiredVolumeConfig) *CleanupConfig {
	if value != nil {
		c.RetiredVolumes = append(c.RetiredVolumes, *value)
	}
	return c
}

// NewDeadLetterConfig returns the default dead-letter layout.
func NewDeadLetterConfig() *DeadLetterConfig {
	value := DefaultConfig().Cleanup.DeadLetter
	return &value
}

// WithRootPath sets the dead-letter root, relative to the volume mount path.
func (c *DeadLetterConfig) WithRootPath(value string) *DeadLetterConfig {
	c.RootPath = value
	return c
}

// WithTenantInPath controls whether the tenant ID appears in the path.
func (c *DeadLetterConfig) WithTenantInPath(enabled bool) *DeadLetterConfig {
	c.IncludeTenantInPath = enabled
	return c
}

// WithDatePartition controls whether a yyyyMMdd partition appears in the path.
func (c *DeadLetterConfig) WithDatePartition(enabled bool) *DeadLetterConfig {
	c.IncludeDatePartition = enabled
	return c
}

// WithShardingDepth sets the shard depth under the dead-letter root.
func (c *DeadLetterConfig) WithShardingDepth(value int) *DeadLetterConfig {
	c.ShardingDepth = value
	return c
}

// NewRetiredVolumeConfig returns a retired volume declaration that keeps metadata.
func NewRetiredVolumeConfig(volumeID string) *RetiredVolumeConfig {
	return &RetiredVolumeConfig{VolumeID: volumeID, Disposition: "Keep"}
}

// WithDisposition sets the retired volume disposition ("Keep" or
// "PurgeMetadataOnly").
func (c *RetiredVolumeConfig) WithDisposition(value string) *RetiredVolumeConfig {
	c.Disposition = value
	return c
}

// WithDatabaseOptimization controls periodic database optimization.
func (c *CleanupConfig) WithDatabaseOptimization(enabled bool) *CleanupConfig {
	c.OptimizeDatabases = enabled
	return c
}

// WithDatabaseOptimizationInterval sets the database optimization interval.
func (c *CleanupConfig) WithDatabaseOptimizationInterval(value time.Duration) *CleanupConfig {
	c.DatabaseOptimizationInterval = value
	return c
}

// NewDatabaseHealthCheckConfig returns default database health-check configuration.
func NewDatabaseHealthCheckConfig() *DatabaseHealthCheckConfig {
	value := DefaultConfig().DatabaseHealthCheck
	return &value
}

// NewOrphanRecoveryConfig returns default orphan-file recovery configuration.
func NewOrphanRecoveryConfig() *OrphanRecoveryConfig {
	value := DefaultConfig().OrphanRecovery
	return &value
}

// WithEnabled enables or disables orphan-file recovery.
func (c *OrphanRecoveryConfig) WithEnabled(enabled bool) *OrphanRecoveryConfig {
	c.Enabled = enabled
	return c
}

// WithRunOnStartup controls whether one recovery scan runs during startup.
func (c *OrphanRecoveryConfig) WithRunOnStartup(enabled bool) *OrphanRecoveryConfig {
	c.RunOnStartup = enabled
	return c
}

// WithRecoveryInterval sets the delay between periodic recovery scans.
func (c *OrphanRecoveryConfig) WithRecoveryInterval(value time.Duration) *OrphanRecoveryConfig {
	c.RecoveryInterval = value
	return c
}

// WithInitialDelay sets the delay before the first recovery scan.
func (c *OrphanRecoveryConfig) WithInitialDelay(value time.Duration) *OrphanRecoveryConfig {
	c.InitialDelay = value
	return c
}

// WithInitialDelay sets the initial health-check delay.
func (c *DatabaseHealthCheckConfig) WithInitialDelay(value time.Duration) *DatabaseHealthCheckConfig {
	c.InitialDelay = value
	return c
}

// WithMaxRetries sets the health-check retry limit.
func (c *DatabaseHealthCheckConfig) WithMaxRetries(value int) *DatabaseHealthCheckConfig {
	c.MaxRetries = value
	return c
}

// WithRetryDelay sets the delay between health-check retries.
func (c *DatabaseHealthCheckConfig) WithRetryDelay(value time.Duration) *DatabaseHealthCheckConfig {
	c.RetryDelay = value
	return c
}

// WithStartupOnly controls whether checks only run during startup.
func (c *DatabaseHealthCheckConfig) WithStartupOnly(enabled bool) *DatabaseHealthCheckConfig {
	c.CheckOnStartupOnly = enabled
	return c
}

// WithPeriodicCheckInterval sets the periodic health-check interval.
func (c *DatabaseHealthCheckConfig) WithPeriodicCheckInterval(value time.Duration) *DatabaseHealthCheckConfig {
	c.PeriodicCheckInterval = value
	return c
}

func cloneTenantConfig(value TenantConfig) TenantConfig {
	if value.Quota != nil {
		quota := *value.Quota
		value.Quota = &quota
	}
	return value
}

func cloneFileWatcherConfig(value FileWatcherConfig) FileWatcherConfig {
	value.FilePatterns = append([]string(nil), value.FilePatterns...)
	return value
}

func cloneFileWatcherRootConfig(value FileWatcherRootConfig) FileWatcherRootConfig {
	value.FilePatterns = append([]string(nil), value.FilePatterns...)
	return value
}
