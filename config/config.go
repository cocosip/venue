package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete Venue configuration, matching Locus structure.
type Config struct {
	// MetadataDirectory is the directory for metadata databases
	MetadataDirectory string `json:"metadataDirectory" yaml:"metadataDirectory" mapstructure:"metadataDirectory"`

	// QuotaDirectory is the directory for quota databases
	QuotaDirectory string `json:"quotaDirectory" yaml:"quotaDirectory" mapstructure:"quotaDirectory"`

	// FileWatcherConfigurationDirectory is the directory for file watcher configurations
	FileWatcherConfigurationDirectory string `json:"fileWatcherConfigurationDirectory" yaml:"fileWatcherConfigurationDirectory" mapstructure:"fileWatcherConfigurationDirectory"`

	// AutoCreateTenants enables automatic tenant creation when referenced
	AutoCreateTenants bool `json:"autoCreateTenants" yaml:"autoCreateTenants" mapstructure:"autoCreateTenants"`

	// DefaultTenantQuota is the default quota for new tenants (0 = unlimited)
	DefaultTenantQuota int64 `json:"defaultTenantQuota" yaml:"defaultTenantQuota" mapstructure:"defaultTenantQuota"`

	// EnableDatabaseHealthCheck enables database health checking on startup
	EnableDatabaseHealthCheck bool `json:"enableDatabaseHealthCheck" yaml:"enableDatabaseHealthCheck" mapstructure:"enableDatabaseHealthCheck"`

	// RetryPolicy defines the retry policy for failed file operations
	RetryPolicy RetryPolicyConfig `json:"retryPolicy" yaml:"retryPolicy" mapstructure:"retryPolicy"`

	// TenantManagerOptions defines tenant manager configuration
	TenantManagerOptions TenantManagerOptionsConfig `json:"tenantManagerOptions" yaml:"tenantManagerOptions" mapstructure:"tenantManagerOptions"`

	// MetadataOptions defines metadata repository configuration
	MetadataOptions MetadataOptionsConfig `json:"metadataOptions" yaml:"metadataOptions" mapstructure:"metadataOptions"`

	// BadgerDBOptions defines BadgerDB database configuration (used by metadata and quota repositories)
	BadgerDBOptions BadgerDBOptionsConfig `json:"badgerDBOptions" yaml:"badgerDBOptions" mapstructure:"badgerDBOptions"`

	// Volumes defines the storage volumes
	Volumes []VolumeConfig `json:"volumes" yaml:"volumes" mapstructure:"volumes"`

	// Tenants defines the tenant configurations
	Tenants []TenantConfig `json:"tenants" yaml:"tenants" mapstructure:"tenants"`

	// FileWatchers defines the file watcher configurations
	FileWatchers []FileWatcherConfig `json:"fileWatchers" yaml:"fileWatchers" mapstructure:"fileWatchers"`

	// EnableBackgroundCleanup enables the background cleanup service
	EnableBackgroundCleanup bool `json:"enableBackgroundCleanup" yaml:"enableBackgroundCleanup" mapstructure:"enableBackgroundCleanup"`

	// CleanupOptions defines the cleanup service options
	CleanupOptions CleanupOptionsConfig `json:"cleanupOptions" yaml:"cleanupOptions" mapstructure:"cleanupOptions"`

	// DatabaseHealthCheckOptions defines the database health check options
	DatabaseHealthCheckOptions DatabaseHealthCheckOptionsConfig `json:"databaseHealthCheckOptions" yaml:"databaseHealthCheckOptions" mapstructure:"databaseHealthCheckOptions"`
}

// TenantManagerOptionsConfig defines tenant manager configuration.
type TenantManagerOptionsConfig struct {
	// MetadataPath is the directory for tenant metadata files (if empty, uses RootPath/.locus/tenants)
	MetadataPath string `json:"metadataPath" yaml:"metadataPath" mapstructure:"metadataPath"`

	// CacheTTL is the cache time-to-live for tenant information
	CacheTTL time.Duration `json:"cacheTTL" yaml:"cacheTTL" mapstructure:"cacheTTL"`
}

// MetadataOptionsConfig defines metadata repository configuration.
type MetadataOptionsConfig struct {
	// CacheTTL is the cache time-to-live for metadata
	CacheTTL time.Duration `json:"cacheTTL" yaml:"cacheTTL" mapstructure:"cacheTTL"`

	// MaxCacheEntries is the maximum number of entries in the metadata cache (0 = use default 10000)
	MaxCacheEntries int `json:"maxCacheEntries" yaml:"maxCacheEntries" mapstructure:"maxCacheEntries"`
}

// BadgerDBOptionsConfig defines BadgerDB database configuration.
type BadgerDBOptionsConfig struct {
	// GCInterval is the garbage collection interval for BadgerDB
	GCInterval time.Duration `json:"gcInterval" yaml:"gcInterval" mapstructure:"gcInterval"`

	// GCDiscardRatio is the discard ratio for garbage collection (0.0 - 1.0)
	GCDiscardRatio float64 `json:"gcDiscardRatio" yaml:"gcDiscardRatio" mapstructure:"gcDiscardRatio"`

	// MemTableSize is the size of each memtable in MB (0 = use default 32MB for metadata, 16MB for quota)
	MemTableSize int `json:"memTableSize" yaml:"memTableSize" mapstructure:"memTableSize"`

	// ValueLogFileSize is the size of each value log file in MB (0 = use default 64MB)
	ValueLogFileSize int `json:"valueLogFileSize" yaml:"valueLogFileSize" mapstructure:"valueLogFileSize"`

	// BlockCacheSize is the size of the block cache in MB (0 = use default 64MB for metadata, 32MB for quota)
	BlockCacheSize int `json:"blockCacheSize" yaml:"blockCacheSize" mapstructure:"blockCacheSize"`

	// SyncWrites enables synchronous writes. Disable for better performance (at the cost of durability).
	SyncWrites bool `json:"syncWrites" yaml:"syncWrites" mapstructure:"syncWrites"`
}

// RetryPolicyConfig defines the retry policy for failed operations.
type RetryPolicyConfig struct {
	// MaxRetryCount is the maximum number of retries
	MaxRetryCount int `json:"maxRetryCount" yaml:"maxRetryCount" mapstructure:"maxRetryCount"`

	// InitialRetryDelay is the initial delay before the first retry
	InitialRetryDelay time.Duration `json:"initialRetryDelay" yaml:"initialRetryDelay" mapstructure:"initialRetryDelay"`

	// UseExponentialBackoff enables exponential backoff for retries
	UseExponentialBackoff bool `json:"useExponentialBackoff" yaml:"useExponentialBackoff" mapstructure:"useExponentialBackoff"`

	// MaxRetryDelay is the maximum delay between retries
	MaxRetryDelay time.Duration `json:"maxRetryDelay" yaml:"maxRetryDelay" mapstructure:"maxRetryDelay"`
}

// VolumeConfig represents a storage volume configuration.
type VolumeConfig struct {
	// VolumeId is the unique identifier for this volume
	VolumeId string `json:"volumeId" yaml:"volumeId" mapstructure:"volumeId"`

	// MountPath is the physical mount path
	MountPath string `json:"mountPath" yaml:"mountPath" mapstructure:"mountPath"`

	// VolumeType is the type of volume (e.g., "LocalFileSystem")
	VolumeType string `json:"volumeType" yaml:"volumeType" mapstructure:"volumeType"`

	// ShardingDepth is the depth of directory sharding (0-3)
	ShardingDepth int `json:"shardingDepth" yaml:"shardingDepth" mapstructure:"shardingDepth"`

	// EnableFsync enables fsync after file writes for durability. Disable for better performance.
	EnableFsync bool `json:"enableFsync" yaml:"enableFsync" mapstructure:"enableFsync"`
}

// TenantConfig represents a tenant configuration.
type TenantConfig struct {
	// TenantId is the unique identifier for this tenant
	TenantId string `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`

	// Enabled indicates if this tenant is active
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// Quota is the storage quota for this tenant in bytes (nil = use default, 0 = unlimited)
	Quota *int64 `json:"quota" yaml:"quota" mapstructure:"quota"`
}

// FileWatcherConfig represents a file watcher configuration.
type FileWatcherConfig struct {
	// WatcherId is the unique identifier for this watcher
	WatcherId string `json:"watcherId" yaml:"watcherId" mapstructure:"watcherId"`

	// TenantId is the tenant to import files for (empty for multi-tenant mode)
	TenantId string `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`

	// MultiTenantMode enables multi-tenant mode (subdirectory = tenant ID)
	MultiTenantMode bool `json:"multiTenantMode" yaml:"multiTenantMode" mapstructure:"multiTenantMode"`

	// AutoCreateTenantDirectories automatically creates tenant subdirectories
	AutoCreateTenantDirectories bool `json:"autoCreateTenantDirectories" yaml:"autoCreateTenantDirectories" mapstructure:"autoCreateTenantDirectories"`

	// WatchPath is the directory to watch for files
	WatchPath string `json:"watchPath" yaml:"watchPath" mapstructure:"watchPath"`

	// Enabled indicates if this watcher is active
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// IncludeSubdirectories enables recursive directory watching
	IncludeSubdirectories bool `json:"includeSubdirectories" yaml:"includeSubdirectories" mapstructure:"includeSubdirectories"`

	// FilePatterns defines the file patterns to watch (e.g., ["*.pdf", "*.docx"])
	FilePatterns []string `json:"filePatterns" yaml:"filePatterns" mapstructure:"filePatterns"`

	// PostImportAction defines what to do after importing ("Delete", "Move", "Keep")
	PostImportAction string `json:"postImportAction" yaml:"postImportAction" mapstructure:"postImportAction"`

	// MoveToDirectory is the directory to move files to (when PostImportAction = "Move")
	MoveToDirectory string `json:"moveToDirectory" yaml:"moveToDirectory" mapstructure:"moveToDirectory"`

	// PollingInterval is the interval between directory scans
	PollingInterval time.Duration `json:"pollingInterval" yaml:"pollingInterval" mapstructure:"pollingInterval"`

	// MaxFileSizeBytes is the maximum file size to import (0 = unlimited)
	MaxFileSizeBytes int64 `json:"maxFileSizeBytes" yaml:"maxFileSizeBytes" mapstructure:"maxFileSizeBytes"`

	// MinFileAge is the minimum file age before importing
	MinFileAge time.Duration `json:"minFileAge" yaml:"minFileAge" mapstructure:"minFileAge"`

	// MaxConcurrentImports is the maximum number of concurrent imports
	MaxConcurrentImports int `json:"maxConcurrentImports" yaml:"maxConcurrentImports" mapstructure:"maxConcurrentImports"`
}

// CleanupOptionsConfig defines the cleanup service options.
type CleanupOptionsConfig struct {
	// CleanupInterval is the interval between cleanup runs
	CleanupInterval time.Duration `json:"cleanupInterval" yaml:"cleanupInterval" mapstructure:"cleanupInterval"`

	// InitialDelay is the delay before the first cleanup run
	InitialDelay time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`

	// CleanupEmptyDirectories enables empty directory cleanup
	CleanupEmptyDirectories bool `json:"cleanupEmptyDirectories" yaml:"cleanupEmptyDirectories" mapstructure:"cleanupEmptyDirectories"`

	// CleanupTimedOutFiles enables timed-out file cleanup
	CleanupTimedOutFiles bool `json:"cleanupTimedOutFiles" yaml:"cleanupTimedOutFiles" mapstructure:"cleanupTimedOutFiles"`

	// ProcessingTimeout is the timeout for file processing
	ProcessingTimeout time.Duration `json:"processingTimeout" yaml:"processingTimeout" mapstructure:"processingTimeout"`

	// CleanupPermanentlyFailedFiles enables permanently failed file cleanup
	CleanupPermanentlyFailedFiles bool `json:"cleanupPermanentlyFailedFiles" yaml:"cleanupPermanentlyFailedFiles" mapstructure:"cleanupPermanentlyFailedFiles"`

	// FailedFileRetentionPeriod is the retention period for failed files
	FailedFileRetentionPeriod time.Duration `json:"failedFileRetentionPeriod" yaml:"failedFileRetentionPeriod" mapstructure:"failedFileRetentionPeriod"`

	// CleanupCompletedRecords enables cleanup of completed file records
	CleanupCompletedRecords bool `json:"cleanupCompletedRecords" yaml:"cleanupCompletedRecords" mapstructure:"cleanupCompletedRecords"`

	// CompletedRecordRetentionPeriod is the retention period for completed records
	CompletedRecordRetentionPeriod time.Duration `json:"completedRecordRetentionPeriod" yaml:"completedRecordRetentionPeriod" mapstructure:"completedRecordRetentionPeriod"`

	// OptimizeDatabases enables database optimization
	OptimizeDatabases bool `json:"optimizeDatabases" yaml:"optimizeDatabases" mapstructure:"optimizeDatabases"`

	// DatabaseOptimizationInterval is the interval between database optimization runs
	DatabaseOptimizationInterval time.Duration `json:"databaseOptimizationInterval" yaml:"databaseOptimizationInterval" mapstructure:"databaseOptimizationInterval"`
}

// DatabaseHealthCheckOptionsConfig defines the database health check options.
type DatabaseHealthCheckOptionsConfig struct {
	// InitialDelay is the delay before the first health check
	InitialDelay time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`

	// MaxRetries is the maximum number of health check retries
	MaxRetries int `json:"maxRetries" yaml:"maxRetries" mapstructure:"maxRetries"`

	// RetryDelay is the delay between health check retries
	RetryDelay time.Duration `json:"retryDelay" yaml:"retryDelay" mapstructure:"retryDelay"`

	// CheckOnStartupOnly if true, only checks on startup
	CheckOnStartupOnly bool `json:"checkOnStartupOnly" yaml:"checkOnStartupOnly" mapstructure:"checkOnStartupOnly"`

	// PeriodicCheckInterval is the interval for periodic checks (if not startup-only)
	PeriodicCheckInterval time.Duration `json:"periodicCheckInterval" yaml:"periodicCheckInterval" mapstructure:"periodicCheckInterval"`
}

// DefaultConfig returns a default configuration matching Locus defaults.
func DefaultConfig() *Config {
	return &Config{
		MetadataDirectory:                 "./venue-metadata",
		QuotaDirectory:                    "./venue-quota",
		FileWatcherConfigurationDirectory: "./venue-watchers",
		AutoCreateTenants:                 false,
		DefaultTenantQuota:                0,
		EnableDatabaseHealthCheck:         true,
		RetryPolicy: RetryPolicyConfig{
			MaxRetryCount:         3,
			InitialRetryDelay:     5 * time.Second,
			UseExponentialBackoff: true,
			MaxRetryDelay:         5 * time.Minute,
		},
		TenantManagerOptions: TenantManagerOptionsConfig{
			MetadataPath: "",              // Empty = use MetadataDirectory/.locus/tenants
			CacheTTL:     5 * time.Minute, // Default cache TTL
		},
		MetadataOptions: MetadataOptionsConfig{
			CacheTTL:        5 * time.Minute, // Default cache TTL
			MaxCacheEntries: 10000,           // Default max cache entries
		},
		BadgerDBOptions: BadgerDBOptionsConfig{
			GCInterval:       10 * time.Minute, // Default GC interval
			GCDiscardRatio:   0.5,              // Default discard ratio
			MemTableSize:     32,               // 32MB default for metadata
			ValueLogFileSize: 64,               // 64MB default
			BlockCacheSize:   64,               // 64MB default for metadata
			SyncWrites:       false,            // Async writes for better performance
		},
		Volumes: []VolumeConfig{
			{
				VolumeId:      "default-volume",
				MountPath:     "./venue-storage/default",
				VolumeType:    "LocalFileSystem",
				ShardingDepth: 2,
			},
		},
		Tenants:                 []TenantConfig{},
		FileWatchers:            []FileWatcherConfig{},
		EnableBackgroundCleanup: false,
		CleanupOptions: CleanupOptionsConfig{
			CleanupInterval:                1 * time.Hour,
			InitialDelay:                   1 * time.Minute,
			CleanupEmptyDirectories:        true,
			CleanupTimedOutFiles:           true,
			ProcessingTimeout:              30 * time.Minute,
			CleanupPermanentlyFailedFiles:  true,
			FailedFileRetentionPeriod:      7 * 24 * time.Hour,
			CleanupCompletedRecords:        false,
			CompletedRecordRetentionPeriod: 30 * 24 * time.Hour,
			OptimizeDatabases:              true,
			DatabaseOptimizationInterval:   24 * time.Hour,
		},
		DatabaseHealthCheckOptions: DatabaseHealthCheckOptionsConfig{
			InitialDelay:          2 * time.Second,
			MaxRetries:            3,
			RetryDelay:            1 * time.Second,
			CheckOnStartupOnly:    true,
			PeriodicCheckInterval: 1 * time.Hour,
		},
	}
}

// ApplyDefaults applies default values to empty configuration fields.
func (c *Config) ApplyDefaults() {
	defaults := DefaultConfig()

	if c.MetadataDirectory == "" {
		c.MetadataDirectory = defaults.MetadataDirectory
	}
	if c.QuotaDirectory == "" {
		c.QuotaDirectory = defaults.QuotaDirectory
	}
	if c.FileWatcherConfigurationDirectory == "" {
		c.FileWatcherConfigurationDirectory = defaults.FileWatcherConfigurationDirectory
	}

	// Apply MetadataOptions defaults
	if c.MetadataOptions.CacheTTL == 0 {
		c.MetadataOptions.CacheTTL = defaults.MetadataOptions.CacheTTL
	}
	if c.MetadataOptions.MaxCacheEntries == 0 {
		c.MetadataOptions.MaxCacheEntries = defaults.MetadataOptions.MaxCacheEntries
	}

	// Apply BadgerDBOptions defaults
	if c.BadgerDBOptions.GCInterval == 0 {
		c.BadgerDBOptions.GCInterval = defaults.BadgerDBOptions.GCInterval
	}
	if c.BadgerDBOptions.GCDiscardRatio == 0 {
		c.BadgerDBOptions.GCDiscardRatio = defaults.BadgerDBOptions.GCDiscardRatio
	}
	if c.BadgerDBOptions.MemTableSize == 0 {
		c.BadgerDBOptions.MemTableSize = defaults.BadgerDBOptions.MemTableSize
	}
	if c.BadgerDBOptions.ValueLogFileSize == 0 {
		c.BadgerDBOptions.ValueLogFileSize = defaults.BadgerDBOptions.ValueLogFileSize
	}
	if c.BadgerDBOptions.BlockCacheSize == 0 {
		c.BadgerDBOptions.BlockCacheSize = defaults.BadgerDBOptions.BlockCacheSize
	}

	// Apply RetryPolicy defaults
	if c.RetryPolicy.MaxRetryCount == 0 {
		c.RetryPolicy.MaxRetryCount = defaults.RetryPolicy.MaxRetryCount
	}
	if c.RetryPolicy.InitialRetryDelay == 0 {
		c.RetryPolicy.InitialRetryDelay = defaults.RetryPolicy.InitialRetryDelay
	}
	if c.RetryPolicy.MaxRetryDelay == 0 {
		c.RetryPolicy.MaxRetryDelay = defaults.RetryPolicy.MaxRetryDelay
	}

	// Apply CleanupOptions defaults
	if c.CleanupOptions.CleanupInterval == 0 {
		c.CleanupOptions.CleanupInterval = defaults.CleanupOptions.CleanupInterval
	}
	if c.CleanupOptions.InitialDelay == 0 {
		c.CleanupOptions.InitialDelay = defaults.CleanupOptions.InitialDelay
	}
	if c.CleanupOptions.ProcessingTimeout == 0 {
		c.CleanupOptions.ProcessingTimeout = defaults.CleanupOptions.ProcessingTimeout
	}
	if c.CleanupOptions.FailedFileRetentionPeriod == 0 {
		c.CleanupOptions.FailedFileRetentionPeriod = defaults.CleanupOptions.FailedFileRetentionPeriod
	}
	if c.CleanupOptions.CompletedRecordRetentionPeriod == 0 {
		c.CleanupOptions.CompletedRecordRetentionPeriod = defaults.CleanupOptions.CompletedRecordRetentionPeriod
	}
	if c.CleanupOptions.DatabaseOptimizationInterval == 0 {
		c.CleanupOptions.DatabaseOptimizationInterval = defaults.CleanupOptions.DatabaseOptimizationInterval
	}

	// Apply DatabaseHealthCheckOptions defaults
	if c.DatabaseHealthCheckOptions.InitialDelay == 0 {
		c.DatabaseHealthCheckOptions.InitialDelay = defaults.DatabaseHealthCheckOptions.InitialDelay
	}
	if c.DatabaseHealthCheckOptions.MaxRetries == 0 {
		c.DatabaseHealthCheckOptions.MaxRetries = defaults.DatabaseHealthCheckOptions.MaxRetries
	}
	if c.DatabaseHealthCheckOptions.RetryDelay == 0 {
		c.DatabaseHealthCheckOptions.RetryDelay = defaults.DatabaseHealthCheckOptions.RetryDelay
	}
	if c.DatabaseHealthCheckOptions.PeriodicCheckInterval == 0 {
		c.DatabaseHealthCheckOptions.PeriodicCheckInterval = defaults.DatabaseHealthCheckOptions.PeriodicCheckInterval
	}

	// Apply defaults to volumes
	for i := range c.Volumes {
		if c.Volumes[i].VolumeType == "" {
			c.Volumes[i].VolumeType = "LocalFileSystem"
		}
	}

	// Apply defaults to file watchers
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
	if c.MetadataDirectory == "" {
		return fmt.Errorf("MetadataDirectory is required")
	}
	if c.QuotaDirectory == "" {
		return fmt.Errorf("QuotaDirectory is required")
	}

	// Validate volumes
	if len(c.Volumes) == 0 {
		return fmt.Errorf("at least one volume is required")
	}
	volumeIds := make(map[string]bool)
	for i, vol := range c.Volumes {
		if vol.VolumeId == "" {
			return fmt.Errorf("volume[%d]: VolumeId is required", i)
		}
		if volumeIds[vol.VolumeId] {
			return fmt.Errorf("volume[%d]: duplicate VolumeId: %s", i, vol.VolumeId)
		}
		volumeIds[vol.VolumeId] = true

		if vol.MountPath == "" {
			return fmt.Errorf("volume[%d]: MountPath is required", i)
		}
		if vol.ShardingDepth < 0 || vol.ShardingDepth > 3 {
			return fmt.Errorf("volume[%d]: ShardingDepth must be between 0 and 3", i)
		}
	}

	// Validate tenants
	tenantIds := make(map[string]bool)
	for i, tenant := range c.Tenants {
		if tenant.TenantId == "" {
			return fmt.Errorf("tenant[%d]: TenantId is required", i)
		}
		if tenantIds[tenant.TenantId] {
			return fmt.Errorf("tenant[%d]: duplicate TenantId: %s", i, tenant.TenantId)
		}
		tenantIds[tenant.TenantId] = true
	}

	// Validate file watchers
	watcherIds := make(map[string]bool)
	for i, watcher := range c.FileWatchers {
		if watcher.WatcherId == "" {
			return fmt.Errorf("fileWatcher[%d]: WatcherId is required", i)
		}
		if watcherIds[watcher.WatcherId] {
			return fmt.Errorf("fileWatcher[%d]: duplicate WatcherId: %s", i, watcher.WatcherId)
		}
		watcherIds[watcher.WatcherId] = true

		if watcher.WatchPath == "" {
			return fmt.Errorf("fileWatcher[%d]: WatchPath is required", i)
		}
		if !watcher.MultiTenantMode && watcher.TenantId == "" {
			return fmt.Errorf("fileWatcher[%d]: TenantId is required when MultiTenantMode is false", i)
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

// LoadFromFile loads configuration from a file (JSON or YAML based on extension).
// It attempts to load from the "venue" node first, and falls back to root level if not found.
func LoadFromFile(filePath string) (*Config, error) {
	return LoadFromFileWithNode(filePath, "venue")
}

// LoadFromFileWithNode loads configuration from a file with a specific root node.
// If nodeName is empty, loads from root level directly.
// If nodeName is specified but not found, falls back to root level.
func LoadFromFileWithNode(filePath string, nodeName string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	var config Config

	switch ext {
	case ".json":
		if nodeName != "" {
			// Try to parse with node
			var wrapper map[string]json.RawMessage
			if err := json.Unmarshal(data, &wrapper); err != nil {
				return nil, fmt.Errorf("failed to parse JSON config: %w", err)
			}
			if nodeData, ok := wrapper[nodeName]; ok {
				if err := json.Unmarshal(nodeData, &config); err != nil {
					return nil, fmt.Errorf("failed to parse JSON config from node '%s': %w", nodeName, err)
				}
			} else {
				// Fallback to root level
				if err := json.Unmarshal(data, &config); err != nil {
					return nil, fmt.Errorf("failed to parse JSON config: %w", err)
				}
			}
		} else {
			if err := json.Unmarshal(data, &config); err != nil {
				return nil, fmt.Errorf("failed to parse JSON config: %w", err)
			}
		}
	case ".yaml", ".yml":
		if nodeName != "" {
			// Try to parse with node
			var wrapper map[string]interface{}
			if err := yaml.Unmarshal(data, &wrapper); err != nil {
				return nil, fmt.Errorf("failed to parse YAML config: %w", err)
			}
			if nodeData, ok := wrapper[nodeName]; ok {
				// Re-marshal the node data and unmarshal into config
				nodeBytes, err := yaml.Marshal(nodeData)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal node data: %w", err)
				}
				if err := yaml.Unmarshal(nodeBytes, &config); err != nil {
					return nil, fmt.Errorf("failed to parse YAML config from node '%s': %w", nodeName, err)
				}
			} else {
				// Fallback to root level
				if err := yaml.Unmarshal(data, &config); err != nil {
					return nil, fmt.Errorf("failed to parse YAML config: %w", err)
				}
			}
		} else {
			if err := yaml.Unmarshal(data, &config); err != nil {
				return nil, fmt.Errorf("failed to parse YAML config: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported config file extension: %s (use .json, .yaml, or .yml)", ext)
	}

	// Apply defaults for missing values
	config.ApplyDefaults()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// SaveToFile saves the configuration to a file (JSON or YAML based on extension).
func (c *Config) SaveToFile(filePath string) error {
	ext := strings.ToLower(filepath.Ext(filePath))

	var data []byte
	var err error

	switch ext {
	case ".json":
		data, err = json.MarshalIndent(c, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON config: %w", err)
		}
	case ".yaml", ".yml":
		data, err = yaml.Marshal(c)
		if err != nil {
			return fmt.Errorf("failed to marshal YAML config: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config file extension: %s (use .json, .yaml, or .yml)", ext)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
