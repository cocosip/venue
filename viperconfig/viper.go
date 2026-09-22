// Package viperconfig adapts Viper sources and configuration files to Config.
// Applications that do not import this package do not compile Viper.
package viperconfig

import (
	"fmt"

	"github.com/cocosip/venue/config"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// Load decodes a Viper source into an independent Venue configuration.
func Load(source *viper.Viper) (*config.Config, error) {
	if source == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}

	result := config.DefaultConfig()
	if err := source.Unmarshal(result, func(decoder *mapstructure.DecoderConfig) {
		decoder.ZeroFields = false
	}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	// Logging contains a runtime slog.Handler and is never file-bound.
	result.Logging = nil
	result.ApplyDefaults()
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return result, nil
}

// LoadFromFile reads a Viper-supported file, preferring the "venue" section.
func LoadFromFile(filePath string) (*config.Config, error) {
	return LoadFromFileSection(filePath, "venue")
}

// LoadFromFileSection reads a Viper-supported file from sectionName. An empty
// section reads the root, and a missing section falls back to the root.
func LoadFromFileSection(filePath, sectionName string) (*config.Config, error) {
	source := viper.New()
	source.SetConfigFile(filePath)
	if err := source.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	if sectionName != "" {
		if section := source.Sub(sectionName); section != nil {
			return Load(section)
		}
	}
	return Load(source)
}

// LoadSection decodes a named section from an application-owned Viper source.
func LoadSection(source *viper.Viper, key string) (*config.Config, error) {
	if source == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}
	section := source.Sub(key)
	if section == nil {
		return nil, fmt.Errorf("section %q not found in configuration", key)
	}
	return Load(section)
}

// NewWithDefaults creates a Viper source initialized from Venue defaults.
// Environment and file-source policy remain application-owned.
func NewWithDefaults() *viper.Viper {
	source := viper.New()
	defaults := config.DefaultConfig()
	source.SetDefault("metadataDirectory", defaults.MetadataDirectory)
	source.SetDefault("quotaDirectory", defaults.QuotaDirectory)
	source.SetDefault("fileWatcherConfigurationDirectory", defaults.FileWatcherConfigurationDirectory)
	source.SetDefault("autoCreateTenants", defaults.AutoCreateTenants)
	source.SetDefault("defaultTenantQuota", defaults.DefaultTenantQuota)
	source.SetDefault("enableDatabaseHealthCheck", defaults.EnableDatabaseHealthCheck)
	source.SetDefault("failFastOnStartupRecoveryFailure", defaults.FailFastOnStartupRecoveryFailure)
	source.SetDefault("enableBackgroundCleanup", defaults.EnableBackgroundCleanup)
	source.SetDefault("retryPolicy.maxRetryCount", defaults.RetryPolicy.MaxRetryCount)
	source.SetDefault("retryPolicy.initialRetryDelay", defaults.RetryPolicy.InitialRetryDelay)
	source.SetDefault("retryPolicy.useExponentialBackoff", defaults.RetryPolicy.UseExponentialBackoff)
	source.SetDefault("retryPolicy.maxRetryDelay", defaults.RetryPolicy.MaxRetryDelay)
	source.SetDefault("cleanupOptions.cleanupInterval", defaults.Cleanup.CleanupInterval)
	source.SetDefault("cleanupOptions.initialDelay", defaults.Cleanup.InitialDelay)
	source.SetDefault("cleanupOptions.cleanupEmptyDirectories", defaults.Cleanup.CleanupEmptyDirectories)
	source.SetDefault("cleanupOptions.cleanupTimedOutFiles", defaults.Cleanup.CleanupTimedOutFiles)
	source.SetDefault("cleanupOptions.processingTimeout", defaults.Cleanup.ProcessingTimeout)
	source.SetDefault("cleanupOptions.recoverTimedOutOnEmptyQueue", defaults.Cleanup.RecoverTimedOutOnEmptyQueue)
	source.SetDefault("cleanupOptions.timedOutReclaimCooldown", defaults.Cleanup.TimedOutReclaimCooldown)
	source.SetDefault("cleanupOptions.cleanupPermanentlyFailedFiles", defaults.Cleanup.CleanupPermanentlyFailedFiles)
	source.SetDefault("cleanupOptions.failedFileRetentionPeriod", defaults.Cleanup.FailedFileRetentionPeriod)
	source.SetDefault("cleanupOptions.cleanupCompletedRecords", defaults.Cleanup.CleanupCompletedRecords)
	source.SetDefault("cleanupOptions.completedRecordRetentionPeriod", defaults.Cleanup.CompletedRecordRetentionPeriod)
	source.SetDefault("cleanupOptions.optimizeDatabases", defaults.Cleanup.OptimizeDatabases)
	source.SetDefault("cleanupOptions.databaseOptimizationInterval", defaults.Cleanup.DatabaseOptimizationInterval)
	source.SetDefault("cleanupOptions.emptyQueueReclaimBatchSize", defaults.Cleanup.EmptyQueueReclaimBatchSize)
	source.SetDefault("cleanupOptions.enableBackgroundTimedOutReclaim", defaults.Cleanup.EnableBackgroundTimedOutReclaim)
	source.SetDefault("cleanupOptions.backgroundTimedOutReclaimBatchSize", defaults.Cleanup.BackgroundTimedOutReclaimBatchSize)
	source.SetDefault("cleanupOptions.cleanupJunkFiles", defaults.Cleanup.CleanupJunkFiles)
	source.SetDefault("cleanupOptions.junkFileCleanupInterval", defaults.Cleanup.JunkFileCleanupInterval)
	source.SetDefault("cleanupOptions.cleanupInvalidDatabaseBackups", defaults.Cleanup.CleanupInvalidDatabaseBackups)
	source.SetDefault("cleanupOptions.permanentlyFailedDisposition", defaults.Cleanup.PermanentlyFailedDisposition)
	source.SetDefault("cleanupOptions.deadLetter.rootPath", defaults.Cleanup.DeadLetter.RootPath)
	source.SetDefault("cleanupOptions.deadLetter.includeTenantInPath", defaults.Cleanup.DeadLetter.IncludeTenantInPath)
	source.SetDefault("cleanupOptions.deadLetter.includeDatePartition", defaults.Cleanup.DeadLetter.IncludeDatePartition)
	source.SetDefault("cleanupOptions.deadLetter.shardingDepth", defaults.Cleanup.DeadLetter.ShardingDepth)
	source.SetDefault("watcherServiceOptions.enabled", defaults.FileWatcherService.Enabled)
	source.SetDefault("watcherServiceOptions.defaultPollingInterval", defaults.FileWatcherService.DefaultPollingInterval)
	source.SetDefault("watcherServiceOptions.minimumPollingInterval", defaults.FileWatcherService.MinimumPollingInterval)
	source.SetDefault("watcherServiceOptions.maximumPollingInterval", defaults.FileWatcherService.MaximumPollingInterval)
	source.SetDefault("watcherServiceOptions.disabledCheckInterval", defaults.FileWatcherService.DisabledCheckInterval)
	source.SetDefault("watcherServiceOptions.maxParallelWatcherScans", defaults.FileWatcherService.MaxParallelWatcherScans)
	source.SetDefault("statisticsOptions.enabled", defaults.Statistics.Enabled)
	source.SetDefault("statisticsOptions.windowSize", defaults.Statistics.WindowSize)
	source.SetDefault("statisticsOptions.retention", defaults.Statistics.Retention)
	source.SetDefault("statisticsOptions.maxSeries", defaults.Statistics.MaxSeries)
	source.SetDefault("statisticsOptions.dimensions.tenantId", defaults.Statistics.Dimensions.TenantID)
	source.SetDefault("statisticsOptions.dimensions.volumeId", defaults.Statistics.Dimensions.VolumeID)
	source.SetDefault("statisticsOptions.dimensions.watcherId", defaults.Statistics.Dimensions.WatcherID)
	source.SetDefault("statisticsOptions.dimensions.operation", defaults.Statistics.Dimensions.Operation)
	source.SetDefault("statisticsOptions.output.enabled", defaults.Statistics.Output.Enabled)
	source.SetDefault("statisticsOptions.output.sink", defaults.Statistics.Output.Sink)
	source.SetDefault("statisticsOptions.output.interval", defaults.Statistics.Output.Interval)
	source.SetDefault("statisticsOptions.output.queryWindow", defaults.Statistics.Output.QueryWindow)
	source.SetDefault("statisticsOptions.output.includeEmptySnapshots", defaults.Statistics.Output.IncludeEmptySnapshots)
	source.SetDefault("badgerDBOptions.syncWrites", defaults.BadgerDB.SyncWrites)
	source.SetDefault("badgerDBOptions.recoverCorruptedDatabase", defaults.BadgerDB.RecoverCorruptedDatabase)
	source.SetDefault("badgerDBOptions.corruptedDatabaseRetention", defaults.BadgerDB.CorruptedDatabaseRetention)
	source.SetDefault("badgerDBOptions.backupDirectory", defaults.BadgerDB.BackupDirectory)
	source.SetDefault("badgerDBOptions.backupInterval", defaults.BadgerDB.BackupInterval)
	source.SetDefault("badgerDBOptions.backupRetention", defaults.BadgerDB.BackupRetention)
	source.SetDefault("badgerDBOptions.autoRestoreFromBackup", defaults.BadgerDB.AutoRestoreFromBackup)
	source.SetDefault("sqliteOptions.journalMode", defaults.Sqlite.JournalMode)
	source.SetDefault("sqliteOptions.synchronousMode", defaults.Sqlite.SynchronousMode)
	source.SetDefault("sqliteOptions.cacheSizeKb", defaults.Sqlite.CacheSizeKb)
	source.SetDefault("sqliteOptions.busyTimeoutMs", defaults.Sqlite.BusyTimeoutMs)
	source.SetDefault("sqliteOptions.checkpointAfterBatch", defaults.Sqlite.CheckpointAfterBatch)
	source.SetDefault("sqliteOptions.maxOpenConns", defaults.Sqlite.MaxOpenConns)
	source.SetDefault("sqliteOptions.maxOpenDatabases", defaults.Sqlite.MaxOpenDatabases)
	source.SetDefault("sqliteOptions.openDatabaseIdleTimeout", defaults.Sqlite.OpenDatabaseIdleTimeout)
	source.SetDefault("sqliteOptions.recoverCorruptedDatabase", defaults.Sqlite.RecoverCorruptedDatabase)
	source.SetDefault("sqliteOptions.corruptedDatabaseRetention", defaults.Sqlite.CorruptedDatabaseRetention)
	source.SetDefault("sqliteOptions.backupDirectory", defaults.Sqlite.BackupDirectory)
	source.SetDefault("sqliteOptions.backupInterval", defaults.Sqlite.BackupInterval)
	source.SetDefault("sqliteOptions.backupRetention", defaults.Sqlite.BackupRetention)
	source.SetDefault("sqliteOptions.autoRestoreFromBackup", defaults.Sqlite.AutoRestoreFromBackup)
	source.SetDefault("sqliteOptions.skipBackupVerification", defaults.Sqlite.SkipBackupVerification)
	source.SetDefault("sqliteOptions.optimizeIdleTenantDatabases", defaults.Sqlite.OptimizeIdleTenantDatabases)
	source.SetDefault("orphanRecoveryOptions.enabled", defaults.OrphanRecovery.Enabled)
	source.SetDefault("orphanRecoveryOptions.runOnStartup", defaults.OrphanRecovery.RunOnStartup)
	source.SetDefault("orphanRecoveryOptions.recoveryInterval", defaults.OrphanRecovery.RecoveryInterval)
	source.SetDefault("orphanRecoveryOptions.initialDelay", defaults.OrphanRecovery.InitialDelay)
	source.SetDefault("databaseHealthCheckOptions.initialDelay", defaults.DatabaseHealthCheck.InitialDelay)
	source.SetDefault("databaseHealthCheckOptions.maxRetries", defaults.DatabaseHealthCheck.MaxRetries)
	source.SetDefault("databaseHealthCheckOptions.retryDelay", defaults.DatabaseHealthCheck.RetryDelay)
	source.SetDefault("databaseHealthCheckOptions.checkOnStartupOnly", defaults.DatabaseHealthCheck.CheckOnStartupOnly)
	source.SetDefault("databaseHealthCheckOptions.periodicCheckInterval", defaults.DatabaseHealthCheck.PeriodicCheckInterval)
	return source
}
