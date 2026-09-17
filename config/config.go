// Package config defines Venue configuration independently from configuration
// files, environment variables, and binding libraries.
package config

import (
	"fmt"
	"time"

	"github.com/cocosip/venue/pkg/logging"
)

// Config configures one Venue runtime instance.
type Config struct {
	MetadataDirectory                 string                    `json:"metadataDirectory" yaml:"metadataDirectory" mapstructure:"metadataDirectory"`
	QuotaDirectory                    string                    `json:"quotaDirectory" yaml:"quotaDirectory" mapstructure:"quotaDirectory"`
	FileWatcherConfigurationDirectory string                    `json:"fileWatcherConfigurationDirectory" yaml:"fileWatcherConfigurationDirectory" mapstructure:"fileWatcherConfigurationDirectory"`
	AutoCreateTenants                 bool                      `json:"autoCreateTenants" yaml:"autoCreateTenants" mapstructure:"autoCreateTenants"`
	DefaultTenantQuota                int64                     `json:"defaultTenantQuota" yaml:"defaultTenantQuota" mapstructure:"defaultTenantQuota"`
	EnableDatabaseHealthCheck         bool                      `json:"enableDatabaseHealthCheck" yaml:"enableDatabaseHealthCheck" mapstructure:"enableDatabaseHealthCheck"`
	RetryPolicy                       RetryPolicyConfig         `json:"retryPolicy" yaml:"retryPolicy" mapstructure:"retryPolicy"`
	TenantManager                     TenantManagerConfig       `json:"tenantManagerOptions" yaml:"tenantManagerOptions" mapstructure:"tenantManagerOptions"`
	Metadata                          MetadataConfig            `json:"metadataOptions" yaml:"metadataOptions" mapstructure:"metadataOptions"`
	BadgerDB                          BadgerDBConfig            `json:"badgerDBOptions" yaml:"badgerDBOptions" mapstructure:"badgerDBOptions"`
	Volumes                           []VolumeConfig            `json:"volumes" yaml:"volumes" mapstructure:"volumes"`
	Tenants                           []TenantConfig            `json:"tenants" yaml:"tenants" mapstructure:"tenants"`
	FileWatchers                      []FileWatcherConfig       `json:"fileWatchers" yaml:"fileWatchers" mapstructure:"fileWatchers"`
	EnableBackgroundCleanup           bool                      `json:"enableBackgroundCleanup" yaml:"enableBackgroundCleanup" mapstructure:"enableBackgroundCleanup"`
	Cleanup                           CleanupConfig             `json:"cleanupOptions" yaml:"cleanupOptions" mapstructure:"cleanupOptions"`
	DatabaseHealthCheck               DatabaseHealthCheckConfig `json:"databaseHealthCheckOptions" yaml:"databaseHealthCheckOptions" mapstructure:"databaseHealthCheckOptions"`
	Logging                           *logging.Config           `json:"-" yaml:"-" mapstructure:"-"`
}

// TenantManagerConfig configures tenant metadata and caching.
type TenantManagerConfig struct {
	MetadataPath string        `json:"metadataPath" yaml:"metadataPath" mapstructure:"metadataPath"`
	CacheTTL     time.Duration `json:"cacheTTL" yaml:"cacheTTL" mapstructure:"cacheTTL"`
}

// MetadataConfig configures metadata caching.
type MetadataConfig struct {
	CacheTTL        time.Duration `json:"cacheTTL" yaml:"cacheTTL" mapstructure:"cacheTTL"`
	MaxCacheEntries int           `json:"maxCacheEntries" yaml:"maxCacheEntries" mapstructure:"maxCacheEntries"`
}

// BadgerDBConfig configures BadgerDB repositories.
type BadgerDBConfig struct {
	GCInterval       time.Duration `json:"gcInterval" yaml:"gcInterval" mapstructure:"gcInterval"`
	GCDiscardRatio   float64       `json:"gcDiscardRatio" yaml:"gcDiscardRatio" mapstructure:"gcDiscardRatio"`
	MemTableSize     int           `json:"memTableSize" yaml:"memTableSize" mapstructure:"memTableSize"`
	ValueLogFileSize int           `json:"valueLogFileSize" yaml:"valueLogFileSize" mapstructure:"valueLogFileSize"`
	BlockCacheSize   int           `json:"blockCacheSize" yaml:"blockCacheSize" mapstructure:"blockCacheSize"`
	SyncWrites       bool          `json:"syncWrites" yaml:"syncWrites" mapstructure:"syncWrites"`
}

// RetryPolicyConfig configures failed-file retry behavior.
type RetryPolicyConfig struct {
	MaxRetryCount         int           `json:"maxRetryCount" yaml:"maxRetryCount" mapstructure:"maxRetryCount"`
	InitialRetryDelay     time.Duration `json:"initialRetryDelay" yaml:"initialRetryDelay" mapstructure:"initialRetryDelay"`
	UseExponentialBackoff bool          `json:"useExponentialBackoff" yaml:"useExponentialBackoff" mapstructure:"useExponentialBackoff"`
	MaxRetryDelay         time.Duration `json:"maxRetryDelay" yaml:"maxRetryDelay" mapstructure:"maxRetryDelay"`
}

// VolumeConfig configures one storage volume.
type VolumeConfig struct {
	VolumeID      string `json:"volumeId" yaml:"volumeId" mapstructure:"volumeId"`
	MountPath     string `json:"mountPath" yaml:"mountPath" mapstructure:"mountPath"`
	VolumeType    string `json:"volumeType" yaml:"volumeType" mapstructure:"volumeType"`
	ShardingDepth int    `json:"shardingDepth" yaml:"shardingDepth" mapstructure:"shardingDepth"`
	EnableFsync   bool   `json:"enableFsync" yaml:"enableFsync" mapstructure:"enableFsync"`
}

// TenantConfig configures one tenant.
type TenantConfig struct {
	TenantID string `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`
	Enabled  bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Quota    *int64 `json:"quota" yaml:"quota" mapstructure:"quota"`
}

// FileWatcherConfig configures one import watcher.
type FileWatcherConfig struct {
	WatcherID                   string        `json:"watcherId" yaml:"watcherId" mapstructure:"watcherId"`
	TenantID                    string        `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`
	MultiTenantMode             bool          `json:"multiTenantMode" yaml:"multiTenantMode" mapstructure:"multiTenantMode"`
	AutoCreateTenantDirectories bool          `json:"autoCreateTenantDirectories" yaml:"autoCreateTenantDirectories" mapstructure:"autoCreateTenantDirectories"`
	WatchPath                   string        `json:"watchPath" yaml:"watchPath" mapstructure:"watchPath"`
	Enabled                     bool          `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	IncludeSubdirectories       bool          `json:"includeSubdirectories" yaml:"includeSubdirectories" mapstructure:"includeSubdirectories"`
	FilePatterns                []string      `json:"filePatterns" yaml:"filePatterns" mapstructure:"filePatterns"`
	PostImportAction            string        `json:"postImportAction" yaml:"postImportAction" mapstructure:"postImportAction"`
	MoveToDirectory             string        `json:"moveToDirectory" yaml:"moveToDirectory" mapstructure:"moveToDirectory"`
	PollingInterval             time.Duration `json:"pollingInterval" yaml:"pollingInterval" mapstructure:"pollingInterval"`
	MaxFileSizeBytes            int64         `json:"maxFileSizeBytes" yaml:"maxFileSizeBytes" mapstructure:"maxFileSizeBytes"`
	MinFileAge                  time.Duration `json:"minFileAge" yaml:"minFileAge" mapstructure:"minFileAge"`
	MaxConcurrentImports        int           `json:"maxConcurrentImports" yaml:"maxConcurrentImports" mapstructure:"maxConcurrentImports"`
}

// CleanupConfig configures background cleanup.
type CleanupConfig struct {
	CleanupInterval                time.Duration `json:"cleanupInterval" yaml:"cleanupInterval" mapstructure:"cleanupInterval"`
	InitialDelay                   time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`
	CleanupEmptyDirectories        bool          `json:"cleanupEmptyDirectories" yaml:"cleanupEmptyDirectories" mapstructure:"cleanupEmptyDirectories"`
	CleanupTimedOutFiles           bool          `json:"cleanupTimedOutFiles" yaml:"cleanupTimedOutFiles" mapstructure:"cleanupTimedOutFiles"`
	ProcessingTimeout              time.Duration `json:"processingTimeout" yaml:"processingTimeout" mapstructure:"processingTimeout"`
	CleanupPermanentlyFailedFiles  bool          `json:"cleanupPermanentlyFailedFiles" yaml:"cleanupPermanentlyFailedFiles" mapstructure:"cleanupPermanentlyFailedFiles"`
	FailedFileRetentionPeriod      time.Duration `json:"failedFileRetentionPeriod" yaml:"failedFileRetentionPeriod" mapstructure:"failedFileRetentionPeriod"`
	CleanupCompletedRecords        bool          `json:"cleanupCompletedRecords" yaml:"cleanupCompletedRecords" mapstructure:"cleanupCompletedRecords"`
	CompletedRecordRetentionPeriod time.Duration `json:"completedRecordRetentionPeriod" yaml:"completedRecordRetentionPeriod" mapstructure:"completedRecordRetentionPeriod"`
	OptimizeDatabases              bool          `json:"optimizeDatabases" yaml:"optimizeDatabases" mapstructure:"optimizeDatabases"`
	DatabaseOptimizationInterval   time.Duration `json:"databaseOptimizationInterval" yaml:"databaseOptimizationInterval" mapstructure:"databaseOptimizationInterval"`
}

// DatabaseHealthCheckConfig configures startup and periodic database checks.
type DatabaseHealthCheckConfig struct {
	InitialDelay          time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`
	MaxRetries            int           `json:"maxRetries" yaml:"maxRetries" mapstructure:"maxRetries"`
	RetryDelay            time.Duration `json:"retryDelay" yaml:"retryDelay" mapstructure:"retryDelay"`
	CheckOnStartupOnly    bool          `json:"checkOnStartupOnly" yaml:"checkOnStartupOnly" mapstructure:"checkOnStartupOnly"`
	PeriodicCheckInterval time.Duration `json:"periodicCheckInterval" yaml:"periodicCheckInterval" mapstructure:"periodicCheckInterval"`
}

// New returns a Config initialized with Venue defaults.
func New() *Config {
	return DefaultConfig()
}

// DefaultConfig returns a fully initialized Venue configuration.
func DefaultConfig() *Config {
	return &Config{
		MetadataDirectory:                 "./venue-metadata",
		QuotaDirectory:                    "./venue-quota",
		FileWatcherConfigurationDirectory: "./venue-watchers",
		AutoCreateTenants:                 true,
		EnableDatabaseHealthCheck:         true,
		RetryPolicy: RetryPolicyConfig{
			MaxRetryCount:         3,
			InitialRetryDelay:     5 * time.Second,
			UseExponentialBackoff: true,
			MaxRetryDelay:         5 * time.Minute,
		},
		TenantManager: TenantManagerConfig{CacheTTL: 5 * time.Minute},
		Metadata: MetadataConfig{
			CacheTTL:        5 * time.Minute,
			MaxCacheEntries: 10000,
		},
		BadgerDB: BadgerDBConfig{
			GCInterval:       10 * time.Minute,
			GCDiscardRatio:   0.5,
			MemTableSize:     32,
			ValueLogFileSize: 64,
			BlockCacheSize:   64,
		},
		Volumes: []VolumeConfig{{
			VolumeID: "default-volume", MountPath: "./venue-storage/default",
			VolumeType: "LocalFileSystem", ShardingDepth: 2, EnableFsync: true,
		}},
		Tenants:                 []TenantConfig{},
		FileWatchers:            []FileWatcherConfig{},
		EnableBackgroundCleanup: true,
		Cleanup: CleanupConfig{
			CleanupInterval:                time.Hour,
			InitialDelay:                   time.Minute,
			CleanupEmptyDirectories:        true,
			CleanupTimedOutFiles:           true,
			ProcessingTimeout:              30 * time.Minute,
			CleanupPermanentlyFailedFiles:  true,
			FailedFileRetentionPeriod:      3 * 24 * time.Hour,
			CleanupCompletedRecords:        true,
			CompletedRecordRetentionPeriod: 0,
			OptimizeDatabases:              true,
			DatabaseOptimizationInterval:   24 * time.Hour,
		},
		DatabaseHealthCheck: DatabaseHealthCheckConfig{
			InitialDelay:          2 * time.Second,
			MaxRetries:            3,
			RetryDelay:            time.Second,
			CheckOnStartupOnly:    true,
			PeriodicCheckInterval: time.Hour,
		},
	}
}

// ApplyDefaults fills zero-valued duration, capacity, and path settings. Start
// with DefaultConfig or New when boolean defaults must also be retained.
func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	d := DefaultConfig()
	if c.MetadataDirectory == "" {
		c.MetadataDirectory = d.MetadataDirectory
	}
	if c.QuotaDirectory == "" {
		c.QuotaDirectory = d.QuotaDirectory
	}
	if c.FileWatcherConfigurationDirectory == "" {
		c.FileWatcherConfigurationDirectory = d.FileWatcherConfigurationDirectory
	}
	if c.RetryPolicy.MaxRetryCount == 0 {
		c.RetryPolicy.MaxRetryCount = d.RetryPolicy.MaxRetryCount
	}
	if c.RetryPolicy.InitialRetryDelay == 0 {
		c.RetryPolicy.InitialRetryDelay = d.RetryPolicy.InitialRetryDelay
	}
	if c.RetryPolicy.MaxRetryDelay == 0 {
		c.RetryPolicy.MaxRetryDelay = d.RetryPolicy.MaxRetryDelay
	}
	if c.TenantManager.CacheTTL == 0 {
		c.TenantManager.CacheTTL = d.TenantManager.CacheTTL
	}
	if c.Metadata.CacheTTL == 0 {
		c.Metadata.CacheTTL = d.Metadata.CacheTTL
	}
	if c.Metadata.MaxCacheEntries == 0 {
		c.Metadata.MaxCacheEntries = d.Metadata.MaxCacheEntries
	}
	if c.BadgerDB.GCInterval == 0 {
		c.BadgerDB.GCInterval = d.BadgerDB.GCInterval
	}
	if c.BadgerDB.GCDiscardRatio == 0 {
		c.BadgerDB.GCDiscardRatio = d.BadgerDB.GCDiscardRatio
	}
	if c.BadgerDB.MemTableSize == 0 {
		c.BadgerDB.MemTableSize = d.BadgerDB.MemTableSize
	}
	if c.BadgerDB.ValueLogFileSize == 0 {
		c.BadgerDB.ValueLogFileSize = d.BadgerDB.ValueLogFileSize
	}
	if c.BadgerDB.BlockCacheSize == 0 {
		c.BadgerDB.BlockCacheSize = d.BadgerDB.BlockCacheSize
	}
	if c.Cleanup.CleanupInterval == 0 {
		c.Cleanup.CleanupInterval = d.Cleanup.CleanupInterval
	}
	if c.Cleanup.InitialDelay == 0 {
		c.Cleanup.InitialDelay = d.Cleanup.InitialDelay
	}
	if c.Cleanup.ProcessingTimeout == 0 {
		c.Cleanup.ProcessingTimeout = d.Cleanup.ProcessingTimeout
	}
	if c.Cleanup.FailedFileRetentionPeriod == 0 {
		c.Cleanup.FailedFileRetentionPeriod = d.Cleanup.FailedFileRetentionPeriod
	}
	if c.Cleanup.DatabaseOptimizationInterval == 0 {
		c.Cleanup.DatabaseOptimizationInterval = d.Cleanup.DatabaseOptimizationInterval
	}
	if c.DatabaseHealthCheck.InitialDelay == 0 {
		c.DatabaseHealthCheck.InitialDelay = d.DatabaseHealthCheck.InitialDelay
	}
	if c.DatabaseHealthCheck.MaxRetries == 0 {
		c.DatabaseHealthCheck.MaxRetries = d.DatabaseHealthCheck.MaxRetries
	}
	if c.DatabaseHealthCheck.RetryDelay == 0 {
		c.DatabaseHealthCheck.RetryDelay = d.DatabaseHealthCheck.RetryDelay
	}
	if c.DatabaseHealthCheck.PeriodicCheckInterval == 0 {
		c.DatabaseHealthCheck.PeriodicCheckInterval = d.DatabaseHealthCheck.PeriodicCheckInterval
	}
	for i := range c.Volumes {
		if c.Volumes[i].VolumeType == "" {
			c.Volumes[i].VolumeType = "LocalFileSystem"
		}
	}
	for i := range c.FileWatchers {
		if c.FileWatchers[i].PollingInterval == 0 {
			c.FileWatchers[i].PollingInterval = 30 * time.Second
		}
		if c.FileWatchers[i].MinFileAge == 0 {
			c.FileWatchers[i].MinFileAge = 3 * time.Second
		}
		if c.FileWatchers[i].MaxConcurrentImports == 0 {
			c.FileWatchers[i].MaxConcurrentImports = 4
		}
		if len(c.FileWatchers[i].FilePatterns) == 0 {
			c.FileWatchers[i].FilePatterns = []string{"*.*"}
		}
		if c.FileWatchers[i].PostImportAction == "" {
			c.FileWatchers[i].PostImportAction = "Delete"
		}
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config cannot be nil")
	}
	if c.MetadataDirectory == "" {
		return fmt.Errorf("MetadataDirectory is required")
	}
	if c.QuotaDirectory == "" {
		return fmt.Errorf("QuotaDirectory is required")
	}
	if len(c.Volumes) == 0 {
		return fmt.Errorf("at least one volume is required")
	}
	if c.DefaultTenantQuota < 0 {
		return fmt.Errorf("DefaultTenantQuota cannot be negative")
	}
	volumeIDs := make(map[string]bool)
	for i, volume := range c.Volumes {
		if volume.VolumeID == "" {
			return fmt.Errorf("volume[%d]: VolumeID is required", i)
		}
		if volumeIDs[volume.VolumeID] {
			return fmt.Errorf("volume[%d]: duplicate VolumeID: %s", i, volume.VolumeID)
		}
		volumeIDs[volume.VolumeID] = true
		if volume.MountPath == "" {
			return fmt.Errorf("volume[%d]: MountPath is required", i)
		}
		if volume.ShardingDepth < 0 || volume.ShardingDepth > 3 {
			return fmt.Errorf("volume[%d]: ShardingDepth must be between 0 and 3", i)
		}
	}
	tenantIDs := make(map[string]bool)
	for i, tenant := range c.Tenants {
		if tenant.TenantID == "" {
			return fmt.Errorf("tenant[%d]: TenantID is required", i)
		}
		if tenantIDs[tenant.TenantID] {
			return fmt.Errorf("tenant[%d]: duplicate TenantID: %s", i, tenant.TenantID)
		}
		if tenant.Quota != nil && *tenant.Quota < 0 {
			return fmt.Errorf("tenant[%d]: Quota cannot be negative", i)
		}
		tenantIDs[tenant.TenantID] = true
	}
	watcherIDs := make(map[string]bool)
	for i, watcher := range c.FileWatchers {
		if watcher.WatcherID == "" {
			return fmt.Errorf("fileWatcher[%d]: WatcherID is required", i)
		}
		if watcherIDs[watcher.WatcherID] {
			return fmt.Errorf("fileWatcher[%d]: duplicate WatcherID: %s", i, watcher.WatcherID)
		}
		watcherIDs[watcher.WatcherID] = true
		if watcher.WatchPath == "" {
			return fmt.Errorf("fileWatcher[%d]: WatchPath is required", i)
		}
		if !watcher.MultiTenantMode && watcher.TenantID == "" {
			return fmt.Errorf("fileWatcher[%d]: TenantID is required when MultiTenantMode is false", i)
		}
		if watcher.PostImportAction != "Delete" && watcher.PostImportAction != "Move" && watcher.PostImportAction != "Keep" {
			return fmt.Errorf("fileWatcher[%d]: PostImportAction must be 'Delete', 'Move', or 'Keep'", i)
		}
		if watcher.PostImportAction == "Move" && watcher.MoveToDirectory == "" {
			return fmt.Errorf("fileWatcher[%d]: MoveToDirectory is required when PostImportAction is 'Move'", i)
		}
	}
	return nil
}

// Clone returns a deep copy suitable for ownership by one Venue instance.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Volumes = append([]VolumeConfig(nil), c.Volumes...)
	clone.Tenants = make([]TenantConfig, len(c.Tenants))
	for i, tenant := range c.Tenants {
		clone.Tenants[i] = tenant
		if tenant.Quota != nil {
			quota := *tenant.Quota
			clone.Tenants[i].Quota = &quota
		}
	}
	clone.FileWatchers = make([]FileWatcherConfig, len(c.FileWatchers))
	for i, watcher := range c.FileWatchers {
		clone.FileWatchers[i] = watcher
		clone.FileWatchers[i].FilePatterns = append([]string(nil), watcher.FilePatterns...)
	}
	if c.Logging != nil {
		loggingConfig := *c.Logging
		clone.Logging = &loggingConfig
	}
	return &clone
}
