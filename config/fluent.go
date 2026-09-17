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

// WithBadgerDB sets BadgerDB configuration.
func (c *Config) WithBadgerDB(value *BadgerDBConfig) *Config {
	if value != nil {
		c.BadgerDB = *value
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

// WithLogging sets instance logging configuration.
func (c *Config) WithLogging(value *logging.Config) *Config { c.Logging = value; return c }

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

// NewBadgerDBConfig returns default BadgerDB configuration.
func NewBadgerDBConfig() *BadgerDBConfig {
	value := DefaultConfig().BadgerDB
	return &value
}

// WithGCInterval sets the BadgerDB garbage-collection interval.
func (c *BadgerDBConfig) WithGCInterval(value time.Duration) *BadgerDBConfig {
	c.GCInterval = value
	return c
}

// WithGCDiscardRatio sets the BadgerDB garbage-collection discard ratio.
func (c *BadgerDBConfig) WithGCDiscardRatio(value float64) *BadgerDBConfig {
	c.GCDiscardRatio = value
	return c
}

// WithMemTableSize sets the BadgerDB memtable size in MB.
func (c *BadgerDBConfig) WithMemTableSize(value int) *BadgerDBConfig {
	c.MemTableSize = value
	return c
}

// WithValueLogFileSize sets the BadgerDB value-log file size in MB.
func (c *BadgerDBConfig) WithValueLogFileSize(value int) *BadgerDBConfig {
	c.ValueLogFileSize = value
	return c
}

// WithBlockCacheSize sets the BadgerDB block cache size in MB.
func (c *BadgerDBConfig) WithBlockCacheSize(value int) *BadgerDBConfig {
	c.BlockCacheSize = value
	return c
}

// WithSyncWrites enables or disables synchronous BadgerDB writes.
func (c *BadgerDBConfig) WithSyncWrites(enabled bool) *BadgerDBConfig {
	c.SyncWrites = enabled
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
func NewFileWatcherConfig() *FileWatcherConfig {
	return &FileWatcherConfig{
		FilePatterns:         []string{"*.*"},
		PostImportAction:     "Delete",
		PollingInterval:      30 * time.Second,
		MinFileAge:           3 * time.Second,
		MaxConcurrentImports: 4,
	}
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
