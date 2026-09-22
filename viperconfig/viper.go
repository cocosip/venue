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
	source.SetDefault("badgerDBOptions.syncWrites", defaults.BadgerDB.SyncWrites)
	source.SetDefault("badgerDBOptions.recoverCorruptedDatabase", defaults.BadgerDB.RecoverCorruptedDatabase)
	source.SetDefault("badgerDBOptions.corruptedDatabaseRetention", defaults.BadgerDB.CorruptedDatabaseRetention)
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
