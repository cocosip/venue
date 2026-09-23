// Package config defines Venue configuration independently from configuration
// files, environment variables, and binding libraries.
package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// Statistics series bounds. The default keeps a bounded amount of in-memory
// state for a busy runtime; the range matches the in-memory recorder's own
// validation so a configuration cannot ask for something the recorder rejects.
const (
	defaultStatisticsMaxSeries = 16384
	minStatisticsMaxSeries     = 1024
	maxStatisticsMaxSeries     = 262144
)

// Volume startup health-check defaults. The initial delay exists so a network
// volume can finish mounting; a volume that answers the first probe is never
// delayed.
const (
	defaultVolumeInitialDelay     = 2 * time.Second
	defaultVolumeHealthCheckDelay = 500 * time.Millisecond
)

// Advanced watcher defaults, applied by ApplyDefaults only to zero values.
const (
	defaultAutoCreateTenantDirectoriesCacheTTL = 60 * time.Second
	defaultFileStabilityCheckDelay             = 100 * time.Millisecond
	defaultSkipStabilityCheckAfterAge          = time.Minute
	defaultImportedFilesPruneInterval          = 5 * time.Minute
	defaultImportedFilesHistoryFlushInterval   = 2 * time.Second
	defaultMaxPostImportActionRetryCount       = 5
	defaultPostImportActionRetryInitialDelay   = 5 * time.Second
	defaultPostImportActionRetryMaxDelay       = 5 * time.Minute
	defaultSourceCleanupDatabasePath           = "source-cleanup.db"
	defaultSourceCleanupPollingInterval        = 5 * time.Second
	defaultSourceCleanupMaxConcurrentActions   = 2
	defaultSourceCleanupMaxActiveJobs          = 10000
	defaultSourceCleanupTerminalRetention      = 24 * time.Hour
	defaultSourceCleanupReservationTimeout     = 10 * time.Minute
	defaultSourceCleanupOptimizationInterval   = 24 * time.Hour
	defaultSourceCleanupTerminalPruneBatch     = 5000
	defaultSourceCleanupFailureDirectory       = "./locus-source-failed"
)

// Config configures one Venue runtime instance.
type Config struct {
	MetadataDirectory                 string `json:"metadataDirectory" yaml:"metadataDirectory" mapstructure:"metadataDirectory"`
	QuotaDirectory                    string `json:"quotaDirectory" yaml:"quotaDirectory" mapstructure:"quotaDirectory"`
	FileWatcherConfigurationDirectory string `json:"fileWatcherConfigurationDirectory" yaml:"fileWatcherConfigurationDirectory" mapstructure:"fileWatcherConfigurationDirectory"`
	AutoCreateTenants                 bool   `json:"autoCreateTenants" yaml:"autoCreateTenants" mapstructure:"autoCreateTenants"`
	DefaultTenantQuota                int64  `json:"defaultTenantQuota" yaml:"defaultTenantQuota" mapstructure:"defaultTenantQuota"`
	EnableDatabaseHealthCheck         bool   `json:"enableDatabaseHealthCheck" yaml:"enableDatabaseHealthCheck" mapstructure:"enableDatabaseHealthCheck"`

	// FailFastOnStartupRecoveryFailure makes startup fail when a database had to
	// be quarantined and could not be restored from a backup. When false (the
	// default), the runtime continues in a degraded state and reports it.
	FailFastOnStartupRecoveryFailure bool                      `json:"failFastOnStartupRecoveryFailure" yaml:"failFastOnStartupRecoveryFailure" mapstructure:"failFastOnStartupRecoveryFailure"`
	RetryPolicy                      RetryPolicyConfig         `json:"retryPolicy" yaml:"retryPolicy" mapstructure:"retryPolicy"`
	TenantManager                    TenantManagerConfig       `json:"tenantManagerOptions" yaml:"tenantManagerOptions" mapstructure:"tenantManagerOptions"`
	Metadata                         MetadataConfig            `json:"metadataOptions" yaml:"metadataOptions" mapstructure:"metadataOptions"`
	Sqlite                           SqliteConfig              `json:"sqliteOptions" yaml:"sqliteOptions" mapstructure:"sqliteOptions"`
	Volumes                          []VolumeConfig            `json:"volumes" yaml:"volumes" mapstructure:"volumes"`
	Tenants                          []TenantConfig            `json:"tenants" yaml:"tenants" mapstructure:"tenants"`
	FileWatchers                     []FileWatcherConfig       `json:"fileWatchers" yaml:"fileWatchers" mapstructure:"fileWatchers"`
	FileWatcherRoots                 []FileWatcherRootConfig   `json:"watcherRoots" yaml:"watcherRoots" mapstructure:"watcherRoots"`
	FileWatcherService               FileWatcherServiceConfig  `json:"watcherServiceOptions" yaml:"watcherServiceOptions" mapstructure:"watcherServiceOptions"`
	SourceCleanup                    SourceCleanupConfig       `json:"sourceCleanup" yaml:"sourceCleanup" mapstructure:"sourceCleanup"`
	EnableBackgroundCleanup          bool                      `json:"enableBackgroundCleanup" yaml:"enableBackgroundCleanup" mapstructure:"enableBackgroundCleanup"`
	Cleanup                          CleanupConfig             `json:"cleanupOptions" yaml:"cleanupOptions" mapstructure:"cleanupOptions"`
	OrphanRecovery                   OrphanRecoveryConfig      `json:"orphanRecoveryOptions" yaml:"orphanRecoveryOptions" mapstructure:"orphanRecoveryOptions"`
	DatabaseHealthCheck              DatabaseHealthCheckConfig `json:"databaseHealthCheckOptions" yaml:"databaseHealthCheckOptions" mapstructure:"databaseHealthCheckOptions"`
	Statistics                       StatisticsConfig          `json:"statisticsOptions" yaml:"statisticsOptions" mapstructure:"statisticsOptions"`
	Logging                          *logging.Config           `json:"-" yaml:"-" mapstructure:"-"`
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

// SqliteConfig configures the SQLite metadata and directory-quota repositories.
//
// One database file is created per tenant below the metadata and quota roots:
// {metadataDirectory}/{tenantId}/metadata.db and
// {quotaDirectory}/{tenantId}/quotas.db.
//
// The engine is a pure-Go SQLite binding, so the runtime needs no cgo and builds
// with CGO_ENABLED=0.
type SqliteConfig struct {
	// JournalMode is the SQLite journal mode. WAL supports concurrent readers
	// during a write and provides crash recovery. Allowed values:
	// DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF (case-insensitive).
	JournalMode string `json:"journalMode" yaml:"journalMode" mapstructure:"journalMode"`

	// SynchronousMode is the SQLite synchronous mode. NORMAL is safe against
	// process crashes; a power failure can lose the most recent commits. FULL
	// fsyncs every commit. Allowed values: OFF, NORMAL, FULL, EXTRA, 0, 1, 2, 3.
	SynchronousMode string `json:"synchronousMode" yaml:"synchronousMode" mapstructure:"synchronousMode"`

	// CacheSizeKb is the per-connection page cache. Negative values are
	// kilobytes; positive values are pages (4 KB each). Zero is rejected: it
	// would disable the page cache and is almost always a configuration error.
	CacheSizeKb int `json:"cacheSizeKb" yaml:"cacheSizeKb" mapstructure:"cacheSizeKb"`

	// BusyTimeoutMs is how long a statement waits for a lock before failing.
	BusyTimeoutMs int `json:"busyTimeoutMs" yaml:"busyTimeoutMs" mapstructure:"busyTimeoutMs"`

	// CheckpointAfterBatch runs PRAGMA wal_checkpoint(PASSIVE) after every
	// committed write batch. It bounds WAL growth at the cost of extra I/O.
	CheckpointAfterBatch bool `json:"checkpointAfterBatch" yaml:"checkpointAfterBatch" mapstructure:"checkpointAfterBatch"`

	// MaxOpenConns is the connection limit of each tenant's pool. Keep it at 1:
	// the repository serializes every statement of a tenant on one connection,
	// mirroring Locus's single long-lived connection per tenant.
	MaxOpenConns int `json:"maxOpenConns" yaml:"maxOpenConns" mapstructure:"maxOpenConns"`

	// MaxOpenDatabases bounds how many tenant database handles stay open at the
	// same time. Zero means unlimited: every tenant that was touched keeps its
	// handle until Close.
	//
	// Raise the limit only with benchmark evidence; see the storage design.
	MaxOpenDatabases int `json:"maxOpenDatabases" yaml:"maxOpenDatabases" mapstructure:"maxOpenDatabases"`

	// OpenDatabaseIdleTimeout closes a tenant handle that has been idle for this
	// long. Zero disables idle eviction. A non-zero value trades reopen cost for
	// a smaller file-handle footprint.
	OpenDatabaseIdleTimeout time.Duration `json:"openDatabaseIdleTimeout" yaml:"openDatabaseIdleTimeout" mapstructure:"openDatabaseIdleTimeout"`

	// RecoverCorruptedDatabase quarantines a database file that cannot be opened
	// because it is corrupted, and recreates an empty one instead of failing
	// startup. The quarantined file is kept for CorruptedDatabaseRetention.
	//
	// This is a destructive repair: the quarantined data is not re-imported
	// automatically. A lock or permission failure is never treated as
	// corruption, because the file may hold a healthy database.
	RecoverCorruptedDatabase bool `json:"recoverCorruptedDatabase" yaml:"recoverCorruptedDatabase" mapstructure:"recoverCorruptedDatabase"`

	// CorruptedDatabaseRetention is how long a quarantined database file is
	// kept before it is pruned during startup. Zero selects 72h; a negative
	// value disables pruning.
	CorruptedDatabaseRetention time.Duration `json:"corruptedDatabaseRetention" yaml:"corruptedDatabaseRetention" mapstructure:"corruptedDatabaseRetention"`

	// BackupDirectory is the root of the per-tenant backup tree. Each tenant
	// writes to {BackupDirectory}/{tenantId}/metadata.<stamp>.bak. An empty
	// value disables periodic backups and automatic restore.
	BackupDirectory string `json:"backupDirectory" yaml:"backupDirectory" mapstructure:"backupDirectory"`

	// BackupInterval is the delay between two backup cycles. A non-positive
	// value disables the periodic runner.
	BackupInterval time.Duration `json:"backupInterval" yaml:"backupInterval" mapstructure:"backupInterval"`

	// BackupRetention is how long a backup file is kept inside each tenant
	// directory. A non-positive value disables pruning.
	BackupRetention time.Duration `json:"backupRetention" yaml:"backupRetention" mapstructure:"backupRetention"`

	// AutoRestoreFromBackup loads the newest readable backup of a tenant whose
	// database had to be quarantined. It requires RecoverCorruptedDatabase and
	// BackupDirectory. A backup that cannot be loaded leaves the tenant empty
	// and degraded rather than failing startup, unless
	// FailFastOnStartupRecoveryFailure is set.
	AutoRestoreFromBackup bool `json:"autoRestoreFromBackup" yaml:"autoRestoreFromBackup" mapstructure:"autoRestoreFromBackup"`

	// SkipBackupVerification disables the PRAGMA integrity_check(1) that runs on
	// every produced backup before it is accepted. Verification is ON by
	// default, so the zero value keeps it on: the field is deliberately inverted
	// because a boolean cannot distinguish "unset" from an explicit false.
	SkipBackupVerification bool `json:"skipBackupVerification" yaml:"skipBackupVerification" mapstructure:"skipBackupVerification"`

	// OptimizeIdleTenantDatabases lets the maintenance cycle open, VACUUM and
	// close tenants that currently have no open handle. Disabled by default
	// because it multiplies file-handle churn during maintenance.
	OptimizeIdleTenantDatabases bool `json:"optimizeIdleTenantDatabases" yaml:"optimizeIdleTenantDatabases" mapstructure:"optimizeIdleTenantDatabases"`
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

	// HealthCheckCacheTTL caches the volume health probe for this long, so a busy
	// write path does not perform a probe write per volume per operation.
	// Zero selects the runtime default (30s); a negative value disables caching.
	HealthCheckCacheTTL time.Duration `json:"healthCheckCacheTTL" yaml:"healthCheckCacheTTL" mapstructure:"healthCheckCacheTTL"`

	// InitialDelay is how long startup waits before the first health-check retry
	// for this volume, so a network volume such as a Kubernetes PVC can finish
	// mounting. A volume that is healthy immediately is never delayed; the wait
	// only happens before a retry. Zero disables the delay.
	InitialDelay time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`

	// HealthCheckDelay is the delay between health-check attempts at startup.
	// Zero disables the inter-attempt delay.
	HealthCheckDelay time.Duration `json:"healthCheckDelay" yaml:"healthCheckDelay" mapstructure:"healthCheckDelay"`

	// WarmupOnStartup performs one throwaway write after the volume passed its
	// startup health checks, so the first real write does not pay for a cold
	// write path.
	WarmupOnStartup bool `json:"warmupOnStartup" yaml:"warmupOnStartup" mapstructure:"warmupOnStartup"`
}

// TenantConfig configures one tenant.
type TenantConfig struct {
	TenantID string `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`
	Enabled  bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Quota    *int64 `json:"quota" yaml:"quota" mapstructure:"quota"`
}

// FileWatcherConfig configures one import watcher.
type FileWatcherConfig struct {
	WatcherID                         string        `json:"watcherId" yaml:"watcherId" mapstructure:"watcherId"`
	TenantID                          string        `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`
	MultiTenantMode                   bool          `json:"multiTenantMode" yaml:"multiTenantMode" mapstructure:"multiTenantMode"`
	AutoCreateTenantDirectories       bool          `json:"autoCreateTenantDirectories" yaml:"autoCreateTenantDirectories" mapstructure:"autoCreateTenantDirectories"`
	WatchPath                         string        `json:"watchPath" yaml:"watchPath" mapstructure:"watchPath"`
	Enabled                           bool          `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	IncludeSubdirectories             bool          `json:"includeSubdirectories" yaml:"includeSubdirectories" mapstructure:"includeSubdirectories"`
	FilePatterns                      []string      `json:"filePatterns" yaml:"filePatterns" mapstructure:"filePatterns"`
	PostImportAction                  string        `json:"postImportAction" yaml:"postImportAction" mapstructure:"postImportAction"`
	MoveToDirectory                   string        `json:"moveToDirectory" yaml:"moveToDirectory" mapstructure:"moveToDirectory"`
	PollingInterval                   time.Duration `json:"pollingInterval" yaml:"pollingInterval" mapstructure:"pollingInterval"`
	MaxFileSizeBytes                  int64         `json:"maxFileSizeBytes" yaml:"maxFileSizeBytes" mapstructure:"maxFileSizeBytes"`
	MinFileAge                        time.Duration `json:"minFileAge" yaml:"minFileAge" mapstructure:"minFileAge"`
	MaxConcurrentImports              int           `json:"maxConcurrentImports" yaml:"maxConcurrentImports" mapstructure:"maxConcurrentImports"`
	MaxPostImportActionRetryCount     int           `json:"maxPostImportActionRetryCount" yaml:"maxPostImportActionRetryCount" mapstructure:"maxPostImportActionRetryCount"`
	PostImportActionRetryInitialDelay time.Duration `json:"postImportActionRetryInitialDelay" yaml:"postImportActionRetryInitialDelay" mapstructure:"postImportActionRetryInitialDelay"`
	PostImportActionRetryMaxDelay     time.Duration `json:"postImportActionRetryMaxDelay" yaml:"postImportActionRetryMaxDelay" mapstructure:"postImportActionRetryMaxDelay"`
	// SourceCleanupFailureDirectory quarantines exhausted source cleanup jobs.
	// An empty value leaves the source in place after the final failed attempt.
	SourceCleanupFailureDirectory string `json:"sourceCleanupFailureDirectory" yaml:"sourceCleanupFailureDirectory" mapstructure:"sourceCleanupFailureDirectory"`

	// AutoCreateTenantDirectoriesCacheTTL caches the tenant list used by
	// AutoCreateTenantDirectories. Zero selects the default (60s).
	AutoCreateTenantDirectoriesCacheTTL time.Duration `json:"autoCreateTenantDirectoriesCacheTtl" yaml:"autoCreateTenantDirectoriesCacheTtl" mapstructure:"autoCreateTenantDirectoriesCacheTtl"`

	// FileStabilityCheckDelay is the delay before the second stability probe.
	// Zero selects the default (100ms); a negative value disables the probe.
	FileStabilityCheckDelay time.Duration `json:"fileStabilityCheckDelay" yaml:"fileStabilityCheckDelay" mapstructure:"fileStabilityCheckDelay"`

	// SkipStabilityCheckAfterAge skips the second stability probe for files at
	// least this old. Zero selects the default (1m); a negative value always
	// probes.
	SkipStabilityCheckAfterAge time.Duration `json:"skipStabilityCheckAfterAge" yaml:"skipStabilityCheckAfterAge" mapstructure:"skipStabilityCheckAfterAge"`

	// DisableImportedFilesPruneThrottle turns off the throttle that limits how
	// often stale import-history pruning rewrites the history file. The throttle
	// is enabled by default, so the zero value keeps the default behavior.
	DisableImportedFilesPruneThrottle bool `json:"disableImportedFilesPruneThrottle" yaml:"disableImportedFilesPruneThrottle" mapstructure:"disableImportedFilesPruneThrottle"`

	// ImportedFilesPruneInterval is the minimum delay between prune runs while
	// the throttle is enabled. Zero selects the default (5m).
	ImportedFilesPruneInterval time.Duration `json:"importedFilesPruneInterval" yaml:"importedFilesPruneInterval" mapstructure:"importedFilesPruneInterval"`

	// DisableImportedFilesHistoryFlushDebounce turns off the debounce that
	// coalesces import-history writes. The debounce is enabled by default, so
	// the zero value keeps the default behavior.
	DisableImportedFilesHistoryFlushDebounce bool `json:"disableImportedFilesHistoryFlushDebounce" yaml:"disableImportedFilesHistoryFlushDebounce" mapstructure:"disableImportedFilesHistoryFlushDebounce"`

	// ImportedFilesHistoryFlushInterval is the minimum delay between
	// import-history persistence writes while the debounce is enabled. Zero
	// selects the default (2s).
	ImportedFilesHistoryFlushInterval time.Duration `json:"importedFilesHistoryFlushInterval" yaml:"importedFilesHistoryFlushInterval" mapstructure:"importedFilesHistoryFlushInterval"`
}

// FileWatcherRootConfig is a template that derives one watcher per tenant
// directory under RootPath, so a multi-tenant deployment does not have to
// enumerate every tenant by hand.
type FileWatcherRootConfig struct {
	// RootPath is the directory whose immediate subdirectories are tenants.
	RootPath string `json:"rootPath" yaml:"rootPath" mapstructure:"rootPath"`

	// MultiTenantMode derives one watcher per immediate subdirectory. When
	// false, RootPath itself becomes a single-tenant watcher.
	MultiTenantMode bool `json:"multiTenantMode" yaml:"multiTenantMode" mapstructure:"multiTenantMode"`

	// Enabled applies to every derived watcher.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// IncludeSubdirectories applies to every derived watcher.
	IncludeSubdirectories bool `json:"includeSubdirectories" yaml:"includeSubdirectories" mapstructure:"includeSubdirectories"`

	// FilePatterns filters imported files.
	FilePatterns []string `json:"filePatterns" yaml:"filePatterns" mapstructure:"filePatterns"`

	// PostImportAction is "Delete", "Move", or "Keep".
	PostImportAction string `json:"postImportAction" yaml:"postImportAction" mapstructure:"postImportAction"`

	// MoveToDirectory is the target directory for PostImportAction "Move".
	MoveToDirectory string `json:"moveToDirectory" yaml:"moveToDirectory" mapstructure:"moveToDirectory"`

	// PollingInterval is the scan interval for derived watchers.
	PollingInterval time.Duration `json:"pollingInterval" yaml:"pollingInterval" mapstructure:"pollingInterval"`

	// MaxFileSizeBytes limits the imported file size; 0 means unlimited.
	MaxFileSizeBytes int64 `json:"maxFileSizeBytes" yaml:"maxFileSizeBytes" mapstructure:"maxFileSizeBytes"`

	// MinFileAge is the minimum file age before import.
	MinFileAge time.Duration `json:"minFileAge" yaml:"minFileAge" mapstructure:"minFileAge"`

	// MaxConcurrentImports limits concurrent imports per derived watcher.
	MaxConcurrentImports int `json:"maxConcurrentImports" yaml:"maxConcurrentImports" mapstructure:"maxConcurrentImports"`

	// MaxPostImportActionRetryCount limits delete or move attempts after import.
	MaxPostImportActionRetryCount int `json:"maxPostImportActionRetryCount" yaml:"maxPostImportActionRetryCount" mapstructure:"maxPostImportActionRetryCount"`

	// PostImportActionRetryInitialDelay is the first action retry delay.
	PostImportActionRetryInitialDelay time.Duration `json:"postImportActionRetryInitialDelay" yaml:"postImportActionRetryInitialDelay" mapstructure:"postImportActionRetryInitialDelay"`

	// PostImportActionRetryMaxDelay caps exponential action retry backoff.
	PostImportActionRetryMaxDelay time.Duration `json:"postImportActionRetryMaxDelay" yaml:"postImportActionRetryMaxDelay" mapstructure:"postImportActionRetryMaxDelay"`

	// SourceCleanupFailureDirectory quarantines exhausted source cleanup jobs
	// for every watcher derived from this root template.
	SourceCleanupFailureDirectory string `json:"sourceCleanupFailureDirectory" yaml:"sourceCleanupFailureDirectory" mapstructure:"sourceCleanupFailureDirectory"`

	// AutoCreateTenantDirectoriesCacheTTL caches the tenant list used by
	// AutoCreateTenantDirectories. Zero selects the default (60s).
	AutoCreateTenantDirectoriesCacheTTL time.Duration `json:"autoCreateTenantDirectoriesCacheTtl" yaml:"autoCreateTenantDirectoriesCacheTtl" mapstructure:"autoCreateTenantDirectoriesCacheTtl"`

	// FileStabilityCheckDelay is the delay before the second stability probe.
	// Zero selects the default (100ms); a negative value disables the probe.
	FileStabilityCheckDelay time.Duration `json:"fileStabilityCheckDelay" yaml:"fileStabilityCheckDelay" mapstructure:"fileStabilityCheckDelay"`

	// SkipStabilityCheckAfterAge skips the second stability probe for files at
	// least this old. Zero selects the default (1m); a negative value always
	// probes.
	SkipStabilityCheckAfterAge time.Duration `json:"skipStabilityCheckAfterAge" yaml:"skipStabilityCheckAfterAge" mapstructure:"skipStabilityCheckAfterAge"`

	// DisableImportedFilesPruneThrottle turns off the import-history prune
	// throttle for every derived watcher. It is enabled by default.
	DisableImportedFilesPruneThrottle bool `json:"disableImportedFilesPruneThrottle" yaml:"disableImportedFilesPruneThrottle" mapstructure:"disableImportedFilesPruneThrottle"`

	// ImportedFilesPruneInterval is the minimum delay between prune runs while
	// the throttle is enabled. Zero selects the default (5m).
	ImportedFilesPruneInterval time.Duration `json:"importedFilesPruneInterval" yaml:"importedFilesPruneInterval" mapstructure:"importedFilesPruneInterval"`

	// DisableImportedFilesHistoryFlushDebounce turns off the import-history
	// write debounce for every derived watcher. It is enabled by default.
	DisableImportedFilesHistoryFlushDebounce bool `json:"disableImportedFilesHistoryFlushDebounce" yaml:"disableImportedFilesHistoryFlushDebounce" mapstructure:"disableImportedFilesHistoryFlushDebounce"`

	// ImportedFilesHistoryFlushInterval is the minimum delay between
	// import-history persistence writes while the debounce is enabled. Zero
	// selects the default (2s).
	ImportedFilesHistoryFlushInterval time.Duration `json:"importedFilesHistoryFlushInterval" yaml:"importedFilesHistoryFlushInterval" mapstructure:"importedFilesHistoryFlushInterval"`
}

// FileWatcherServiceConfig configures the background file watcher service
// globally, independently of the individual watchers.
type FileWatcherServiceConfig struct {
	// Enabled controls whether the background service scans at all. When false,
	// every watcher is dormant until the service is re-enabled.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// DefaultPollingInterval applies when a watcher does not set one.
	DefaultPollingInterval time.Duration `json:"defaultPollingInterval" yaml:"defaultPollingInterval" mapstructure:"defaultPollingInterval"`

	// MinimumPollingInterval clamps short per-watcher intervals.
	MinimumPollingInterval time.Duration `json:"minimumPollingInterval" yaml:"minimumPollingInterval" mapstructure:"minimumPollingInterval"`

	// MaximumPollingInterval clamps long per-watcher intervals.
	MaximumPollingInterval time.Duration `json:"maximumPollingInterval" yaml:"maximumPollingInterval" mapstructure:"maximumPollingInterval"`

	// DisabledCheckInterval is how often a disabled service rechecks whether it
	// should resume.
	DisabledCheckInterval time.Duration `json:"disabledCheckInterval" yaml:"disabledCheckInterval" mapstructure:"disabledCheckInterval"`

	// MaxParallelWatcherScans bounds how many watchers one cycle scans in
	// parallel. Zero selects the runtime default; a negative value means
	// sequential scanning.
	MaxParallelWatcherScans int `json:"maxParallelWatcherScans" yaml:"maxParallelWatcherScans" mapstructure:"maxParallelWatcherScans"`
}

// SourceCleanupConfig configures the durable post-import source cleanup worker.
// DatabasePath is resolved relative to FileWatcherConfigurationDirectory unless
// it is absolute.
type SourceCleanupConfig struct {
	Enabled                      bool          `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	DatabasePath                 string        `json:"databasePath" yaml:"databasePath" mapstructure:"databasePath"`
	PollingInterval              time.Duration `json:"pollingInterval" yaml:"pollingInterval" mapstructure:"pollingInterval"`
	MaxConcurrentActions         int           `json:"maxConcurrentActions" yaml:"maxConcurrentActions" mapstructure:"maxConcurrentActions"`
	MaxActiveJobs                int           `json:"maxActiveJobs" yaml:"maxActiveJobs" mapstructure:"maxActiveJobs"`
	TerminalJobRetentionPeriod   time.Duration `json:"terminalJobRetentionPeriod" yaml:"terminalJobRetentionPeriod" mapstructure:"terminalJobRetentionPeriod"`
	ImportReservationTimeout     time.Duration `json:"importReservationTimeout" yaml:"importReservationTimeout" mapstructure:"importReservationTimeout"`
	EnableDatabaseOptimization   bool          `json:"enableDatabaseOptimization" yaml:"enableDatabaseOptimization" mapstructure:"enableDatabaseOptimization"`
	DatabaseOptimizationInterval time.Duration `json:"databaseOptimizationInterval" yaml:"databaseOptimizationInterval" mapstructure:"databaseOptimizationInterval"`
	TerminalPruneBatchSize       int           `json:"terminalPruneBatchSize" yaml:"terminalPruneBatchSize" mapstructure:"terminalPruneBatchSize"`
}

// StatisticsConfig configures in-process runtime statistics.
//
// Statistics are disabled by default. When enabled, Venue records bounded
// windowed counters for the storage, metadata, and watcher paths, and callers
// can read aggregated snapshots through Venue.Statistics().
type StatisticsConfig struct {
	// Enabled turns in-process statistics collection on. When false, recording
	// costs one call per instrumented operation and allocates nothing.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// WindowSize is the aggregation bucket size.
	WindowSize time.Duration `json:"windowSize" yaml:"windowSize" mapstructure:"windowSize"`

	// Retention is how long in-memory buckets are kept.
	Retention time.Duration `json:"retention" yaml:"retention" mapstructure:"retention"`

	// MaxSeries bounds the number of retained time-bucket and dimension series
	// so a high-cardinality workload cannot grow memory without limit.
	// Valid range: 1024 to 262144.
	MaxSeries int `json:"maxSeries" yaml:"maxSeries" mapstructure:"maxSeries"`

	// Dimensions selects which low-cardinality dimensions are retained.
	Dimensions StatisticsDimensionConfig `json:"dimensions" yaml:"dimensions" mapstructure:"dimensions"`

	// Output configures optional periodic log output of a statistics summary.
	Output StatisticsOutputConfig `json:"output" yaml:"output" mapstructure:"output"`
}

// StatisticsDimensionConfig selects which statistics dimensions are retained.
// Retaining tenant_id is disabled by default because it is high-cardinality.
type StatisticsDimensionConfig struct {
	// TenantID retains the tenant_id dimension.
	TenantID bool `json:"tenantId" yaml:"tenantId" mapstructure:"tenantId"`

	// VolumeID retains the volume_id dimension.
	VolumeID bool `json:"volumeId" yaml:"volumeId" mapstructure:"volumeId"`

	// WatcherID retains the watcher_id dimension.
	WatcherID bool `json:"watcherId" yaml:"watcherId" mapstructure:"watcherId"`

	// Operation retains the operation dimension.
	Operation bool `json:"operation" yaml:"operation" mapstructure:"operation"`
}

// StatisticsOutputConfig configures periodic log output of statistics.
type StatisticsOutputConfig struct {
	// Enabled turns periodic output on. It requires Statistics.Enabled.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// Sink is the output kind. Only "Logging" is supported.
	Sink string `json:"sink" yaml:"sink" mapstructure:"sink"`

	// Interval is the delay between summaries.
	Interval time.Duration `json:"interval" yaml:"interval" mapstructure:"interval"`

	// QueryWindow is the time range included in each summary.
	QueryWindow time.Duration `json:"queryWindow" yaml:"queryWindow" mapstructure:"queryWindow"`

	// IncludeEmptySnapshots logs a summary even when every counter is zero.
	IncludeEmptySnapshots bool `json:"includeEmptySnapshots" yaml:"includeEmptySnapshots" mapstructure:"includeEmptySnapshots"`
}

// CleanupConfig configures background cleanup.
type CleanupConfig struct {
	CleanupInterval                time.Duration         `json:"cleanupInterval" yaml:"cleanupInterval" mapstructure:"cleanupInterval"`
	InitialDelay                   time.Duration         `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`
	CleanupEmptyDirectories        bool                  `json:"cleanupEmptyDirectories" yaml:"cleanupEmptyDirectories" mapstructure:"cleanupEmptyDirectories"`
	CleanupTimedOutFiles           bool                  `json:"cleanupTimedOutFiles" yaml:"cleanupTimedOutFiles" mapstructure:"cleanupTimedOutFiles"`
	ProcessingTimeout              time.Duration         `json:"processingTimeout" yaml:"processingTimeout" mapstructure:"processingTimeout"`
	RecoverTimedOutOnEmptyQueue    bool                  `json:"recoverTimedOutOnEmptyQueue" yaml:"recoverTimedOutOnEmptyQueue" mapstructure:"recoverTimedOutOnEmptyQueue"`
	TimedOutReclaimCooldown        time.Duration         `json:"timedOutReclaimCooldown" yaml:"timedOutReclaimCooldown" mapstructure:"timedOutReclaimCooldown"`
	CleanupPermanentlyFailedFiles  bool                  `json:"cleanupPermanentlyFailedFiles" yaml:"cleanupPermanentlyFailedFiles" mapstructure:"cleanupPermanentlyFailedFiles"`
	PermanentlyFailedDisposition   string                `json:"permanentlyFailedDisposition" yaml:"permanentlyFailedDisposition" mapstructure:"permanentlyFailedDisposition"`
	DeadLetter                     DeadLetterConfig      `json:"deadLetter" yaml:"deadLetter" mapstructure:"deadLetter"`
	FailedFileRetentionPeriod      time.Duration         `json:"failedFileRetentionPeriod" yaml:"failedFileRetentionPeriod" mapstructure:"failedFileRetentionPeriod"`
	CleanupCompletedRecords        bool                  `json:"cleanupCompletedRecords" yaml:"cleanupCompletedRecords" mapstructure:"cleanupCompletedRecords"`
	CompletedRecordRetentionPeriod time.Duration         `json:"completedRecordRetentionPeriod" yaml:"completedRecordRetentionPeriod" mapstructure:"completedRecordRetentionPeriod"`
	CleanupOrphanedMetadata        bool                  `json:"cleanupOrphanedMetadata" yaml:"cleanupOrphanedMetadata" mapstructure:"cleanupOrphanedMetadata"`
	CleanupJunkFiles               bool                  `json:"cleanupJunkFiles" yaml:"cleanupJunkFiles" mapstructure:"cleanupJunkFiles"`
	JunkFileCleanupInterval        time.Duration         `json:"junkFileCleanupInterval" yaml:"junkFileCleanupInterval" mapstructure:"junkFileCleanupInterval"`
	CleanupInvalidDatabaseBackups  bool                  `json:"cleanupInvalidDatabaseBackups" yaml:"cleanupInvalidDatabaseBackups" mapstructure:"cleanupInvalidDatabaseBackups"`
	RetiredVolumes                 []RetiredVolumeConfig `json:"retiredVolumes" yaml:"retiredVolumes" mapstructure:"retiredVolumes"`
	OptimizeDatabases              bool                  `json:"optimizeDatabases" yaml:"optimizeDatabases" mapstructure:"optimizeDatabases"`
	DatabaseOptimizationInterval   time.Duration         `json:"databaseOptimizationInterval" yaml:"databaseOptimizationInterval" mapstructure:"databaseOptimizationInterval"`

	// EmptyQueueReclaimBatchSize bounds how many timed-out files are reclaimed
	// synchronously when a claim finds an empty queue. Zero selects the runtime
	// default (32); a negative value disables immediate reclaim.
	EmptyQueueReclaimBatchSize int `json:"emptyQueueReclaimBatchSize" yaml:"emptyQueueReclaimBatchSize" mapstructure:"emptyQueueReclaimBatchSize"`

	// EnableBackgroundTimedOutReclaim lets a successful claim opportunistically
	// reclaim timed-out files in the background, so timed-out records are
	// recovered even when cleanup is disabled or its interval is long.
	EnableBackgroundTimedOutReclaim bool `json:"enableBackgroundTimedOutReclaim" yaml:"enableBackgroundTimedOutReclaim" mapstructure:"enableBackgroundTimedOutReclaim"`

	// BackgroundTimedOutReclaimBatchSize bounds how many timed-out files one
	// background reclaim pass recovers. Zero selects the runtime default (8); a
	// negative value disables the background pass.
	BackgroundTimedOutReclaimBatchSize int `json:"backgroundTimedOutReclaimBatchSize" yaml:"backgroundTimedOutReclaimBatchSize" mapstructure:"backgroundTimedOutReclaimBatchSize"`
}

// DeadLetterConfig configures where permanently failed payloads are moved when// PermanentlyFailedDisposition is "MoveToDeadLetter".
//
// Relative paths are resolved under the owning volume's mount path, so a
// dead-letter area never leaves the volume that holds the payload.
type DeadLetterConfig struct {
	// RootPath is the dead-letter root, relative to the volume mount path.
	RootPath string `json:"rootPath" yaml:"rootPath" mapstructure:"rootPath"`

	// IncludeTenantInPath inserts the tenant ID below the root.
	IncludeTenantInPath bool `json:"includeTenantInPath" yaml:"includeTenantInPath" mapstructure:"includeTenantInPath"`

	// IncludeDatePartition inserts a yyyyMMdd partition directory.
	IncludeDatePartition bool `json:"includeDatePartition" yaml:"includeDatePartition" mapstructure:"includeDatePartition"`

	// ShardingDepth is the number of two-character hexadecimal shard segments
	// inserted below the (optional) partition directory.
	ShardingDepth int `json:"shardingDepth" yaml:"shardingDepth" mapstructure:"shardingDepth"`
}

// RetiredVolumeConfig declares a storage volume that was intentionally retired
// and how cleanup should treat metadata that still references it.
type RetiredVolumeConfig struct {
	// VolumeID is the retired volume identifier.
	VolumeID string `json:"volumeId" yaml:"volumeId" mapstructure:"volumeId"`

	// Disposition is "Keep" (default) or "PurgeMetadataOnly".
	Disposition string `json:"disposition" yaml:"disposition" mapstructure:"disposition"`
}

// DatabaseHealthCheckConfig configures startup and periodic database checks.
type DatabaseHealthCheckConfig struct {
	InitialDelay          time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`
	MaxRetries            int           `json:"maxRetries" yaml:"maxRetries" mapstructure:"maxRetries"`
	RetryDelay            time.Duration `json:"retryDelay" yaml:"retryDelay" mapstructure:"retryDelay"`
	CheckOnStartupOnly    bool          `json:"checkOnStartupOnly" yaml:"checkOnStartupOnly" mapstructure:"checkOnStartupOnly"`
	PeriodicCheckInterval time.Duration `json:"periodicCheckInterval" yaml:"periodicCheckInterval" mapstructure:"periodicCheckInterval"`
}

// OrphanRecoveryConfig configures the optional orphan-file recovery service.
// Recovery scans storage volumes for physical files that have no metadata
// record (for example after a crash between the physical write and the metadata
// commit) and re-registers them as Pending so they re-enter the queue.
//
// Recovery is disabled by default: it must be enabled explicitly.
type OrphanRecoveryConfig struct {
	// Enabled starts the orphan recovery service.
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// RunOnStartup runs one recovery scan after InitialDelay during startup.
	RunOnStartup bool `json:"runOnStartup" yaml:"runOnStartup" mapstructure:"runOnStartup"`

	// RecoveryInterval is the delay between periodic recovery scans.
	RecoveryInterval time.Duration `json:"recoveryInterval" yaml:"recoveryInterval" mapstructure:"recoveryInterval"`

	// InitialDelay delays the first scan so storage volumes can finish mounting.
	InitialDelay time.Duration `json:"initialDelay" yaml:"initialDelay" mapstructure:"initialDelay"`
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
		Sqlite: SqliteConfig{
			// The defaults mirror the Locus SQLite options so an engine swap does
			// not silently change durability or cache behaviour.
			JournalMode:          "WAL",
			SynchronousMode:      "NORMAL",
			CacheSizeKb:          -4000,
			BusyTimeoutMs:        5000,
			CheckpointAfterBatch: false,
			MaxOpenConns:         1,
			// Zero keeps every touched tenant handle open, as Locus does.
			MaxOpenDatabases:            0,
			OpenDatabaseIdleTimeout:     0,
			RecoverCorruptedDatabase:    false,
			CorruptedDatabaseRetention:  72 * time.Hour,
			BackupDirectory:             "",
			BackupInterval:              time.Hour,
			BackupRetention:             7 * 24 * time.Hour,
			AutoRestoreFromBackup:       false,
			SkipBackupVerification:      false,
			OptimizeIdleTenantDatabases: false,
		},
		Volumes: []VolumeConfig{{
			VolumeID: "default-volume", MountPath: "./venue-storage/default",
			VolumeType: "LocalFileSystem", ShardingDepth: 2, EnableFsync: true,
			InitialDelay: 2 * time.Second, HealthCheckDelay: 500 * time.Millisecond,
		}},
		Tenants:          []TenantConfig{},
		FileWatchers:     []FileWatcherConfig{},
		FileWatcherRoots: []FileWatcherRootConfig{},
		FileWatcherService: FileWatcherServiceConfig{
			Enabled:                 true,
			DefaultPollingInterval:  30 * time.Second,
			MinimumPollingInterval:  5 * time.Second,
			MaximumPollingInterval:  time.Hour,
			DisabledCheckInterval:   time.Minute,
			MaxParallelWatcherScans: 4,
		},
		SourceCleanup: SourceCleanupConfig{
			Enabled:                      true,
			DatabasePath:                 defaultSourceCleanupDatabasePath,
			PollingInterval:              defaultSourceCleanupPollingInterval,
			MaxConcurrentActions:         defaultSourceCleanupMaxConcurrentActions,
			MaxActiveJobs:                defaultSourceCleanupMaxActiveJobs,
			TerminalJobRetentionPeriod:   defaultSourceCleanupTerminalRetention,
			ImportReservationTimeout:     defaultSourceCleanupReservationTimeout,
			EnableDatabaseOptimization:   true,
			DatabaseOptimizationInterval: defaultSourceCleanupOptimizationInterval,
			TerminalPruneBatchSize:       defaultSourceCleanupTerminalPruneBatch,
		},
		EnableBackgroundCleanup: true,
		Cleanup: CleanupConfig{
			CleanupInterval:               time.Hour,
			InitialDelay:                  time.Minute,
			CleanupEmptyDirectories:       true,
			CleanupTimedOutFiles:          true,
			ProcessingTimeout:             30 * time.Minute,
			RecoverTimedOutOnEmptyQueue:   true,
			TimedOutReclaimCooldown:       30 * time.Second,
			CleanupPermanentlyFailedFiles: true,
			PermanentlyFailedDisposition:  "MoveToDeadLetter",
			DeadLetter: DeadLetterConfig{
				RootPath:             ".deadletter",
				IncludeTenantInPath:  true,
				IncludeDatePartition: true,
				ShardingDepth:        2,
			},
			FailedFileRetentionPeriod:      3 * 24 * time.Hour,
			CleanupCompletedRecords:        true,
			CompletedRecordRetentionPeriod: 0,
			// Opt-in: the orphaned-metadata sweep performs one file-existence
			// check per tracked record.
			CleanupOrphanedMetadata:       false,
			CleanupJunkFiles:              true,
			JunkFileCleanupInterval:       20 * time.Minute,
			CleanupInvalidDatabaseBackups: true,
			RetiredVolumes:                []RetiredVolumeConfig{},
			OptimizeDatabases:             true,
			DatabaseOptimizationInterval:  24 * time.Hour,
			// Locus bounds both the immediate and the background reclaim passes;
			// the background pass is what recovers timed-out records when the
			// cleanup interval is long or cleanup is disabled.
			EmptyQueueReclaimBatchSize:         32,
			EnableBackgroundTimedOutReclaim:    true,
			BackgroundTimedOutReclaimBatchSize: 8,
		},
		DatabaseHealthCheck: DatabaseHealthCheckConfig{
			InitialDelay:          2 * time.Second,
			MaxRetries:            3,
			RetryDelay:            time.Second,
			CheckOnStartupOnly:    true,
			PeriodicCheckInterval: time.Hour,
		},
		OrphanRecovery: OrphanRecoveryConfig{
			Enabled:          false,
			RunOnStartup:     false,
			RecoveryInterval: 6 * time.Hour,
			InitialDelay:     10 * time.Second,
		},
		Statistics: StatisticsConfig{
			// Statistics stay off unless a caller opts in.
			Enabled:    false,
			WindowSize: 5 * time.Minute,
			Retention:  time.Hour,
			MaxSeries:  defaultStatisticsMaxSeries,
			Dimensions: StatisticsDimensionConfig{
				// tenant_id is high-cardinality and stays off by default.
				TenantID:  false,
				VolumeID:  true,
				WatcherID: true,
				Operation: true,
			},
			Output: StatisticsOutputConfig{
				Enabled:               false,
				Sink:                  "Logging",
				Interval:              time.Minute,
				QueryWindow:           15 * time.Minute,
				IncludeEmptySnapshots: false,
			},
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
	if c.SourceCleanup == (SourceCleanupConfig{}) {
		c.SourceCleanup = d.SourceCleanup
	} else {
		if c.SourceCleanup.DatabasePath == "" && c.SourceCleanup.Enabled {
			c.SourceCleanup.DatabasePath = d.SourceCleanup.DatabasePath
		}
		if c.SourceCleanup.PollingInterval == 0 {
			c.SourceCleanup.PollingInterval = d.SourceCleanup.PollingInterval
		}
		if c.SourceCleanup.MaxConcurrentActions == 0 {
			c.SourceCleanup.MaxConcurrentActions = d.SourceCleanup.MaxConcurrentActions
		}
		if c.SourceCleanup.MaxActiveJobs == 0 {
			c.SourceCleanup.MaxActiveJobs = d.SourceCleanup.MaxActiveJobs
		}
		if c.SourceCleanup.TerminalJobRetentionPeriod == 0 {
			c.SourceCleanup.TerminalJobRetentionPeriod = d.SourceCleanup.TerminalJobRetentionPeriod
		}
		if c.SourceCleanup.ImportReservationTimeout == 0 {
			c.SourceCleanup.ImportReservationTimeout = d.SourceCleanup.ImportReservationTimeout
		}
		if c.SourceCleanup.DatabaseOptimizationInterval == 0 {
			c.SourceCleanup.DatabaseOptimizationInterval = d.SourceCleanup.DatabaseOptimizationInterval
		}
		if c.SourceCleanup.TerminalPruneBatchSize == 0 {
			c.SourceCleanup.TerminalPruneBatchSize = d.SourceCleanup.TerminalPruneBatchSize
		}
	}
	if c.Cleanup.DeadLetter == (DeadLetterConfig{}) {
		c.Cleanup.DeadLetter = d.Cleanup.DeadLetter
	}
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
	if c.Cleanup.CleanupInterval == 0 {
		c.Cleanup.CleanupInterval = d.Cleanup.CleanupInterval
	}
	if c.Cleanup.InitialDelay == 0 {
		c.Cleanup.InitialDelay = d.Cleanup.InitialDelay
	}
	if c.Cleanup.ProcessingTimeout == 0 {
		c.Cleanup.ProcessingTimeout = d.Cleanup.ProcessingTimeout
	}
	if c.Cleanup.TimedOutReclaimCooldown == 0 {
		c.Cleanup.TimedOutReclaimCooldown = d.Cleanup.TimedOutReclaimCooldown
	}
	if c.Cleanup.FailedFileRetentionPeriod == 0 {
		c.Cleanup.FailedFileRetentionPeriod = d.Cleanup.FailedFileRetentionPeriod
	}
	if c.Cleanup.DatabaseOptimizationInterval == 0 {
		c.Cleanup.DatabaseOptimizationInterval = d.Cleanup.DatabaseOptimizationInterval
	}
	if c.Cleanup.JunkFileCleanupInterval == 0 {
		c.Cleanup.JunkFileCleanupInterval = d.Cleanup.JunkFileCleanupInterval
	}
	if c.Cleanup.EmptyQueueReclaimBatchSize == 0 {
		c.Cleanup.EmptyQueueReclaimBatchSize = d.Cleanup.EmptyQueueReclaimBatchSize
	}
	if c.Cleanup.BackgroundTimedOutReclaimBatchSize == 0 {
		c.Cleanup.BackgroundTimedOutReclaimBatchSize = d.Cleanup.BackgroundTimedOutReclaimBatchSize
	}
	if c.Cleanup.PermanentlyFailedDisposition == "" {
		c.Cleanup.PermanentlyFailedDisposition = d.Cleanup.PermanentlyFailedDisposition
	}
	if c.Cleanup.DeadLetter.RootPath == "" {
		c.Cleanup.DeadLetter.RootPath = d.Cleanup.DeadLetter.RootPath
	}
	if c.Cleanup.DeadLetter.ShardingDepth == 0 {
		c.Cleanup.DeadLetter.ShardingDepth = d.Cleanup.DeadLetter.ShardingDepth
	}
	if c.Sqlite.JournalMode == "" {
		c.Sqlite.JournalMode = d.Sqlite.JournalMode
	}
	if c.Sqlite.SynchronousMode == "" {
		c.Sqlite.SynchronousMode = d.Sqlite.SynchronousMode
	}
	if c.Sqlite.CacheSizeKb == 0 {
		c.Sqlite.CacheSizeKb = d.Sqlite.CacheSizeKb
	}
	if c.Sqlite.BusyTimeoutMs == 0 {
		c.Sqlite.BusyTimeoutMs = d.Sqlite.BusyTimeoutMs
	}
	if c.Sqlite.MaxOpenConns == 0 {
		c.Sqlite.MaxOpenConns = d.Sqlite.MaxOpenConns
	}
	if c.Sqlite.CorruptedDatabaseRetention == 0 {
		c.Sqlite.CorruptedDatabaseRetention = d.Sqlite.CorruptedDatabaseRetention
	}
	if c.Sqlite.BackupInterval == 0 {
		c.Sqlite.BackupInterval = d.Sqlite.BackupInterval
	}
	if c.Sqlite.BackupRetention == 0 {
		c.Sqlite.BackupRetention = d.Sqlite.BackupRetention
	}
	if c.Statistics.WindowSize == 0 {
		c.Statistics.WindowSize = d.Statistics.WindowSize
	}
	if c.Statistics.Retention == 0 {
		c.Statistics.Retention = d.Statistics.Retention
	}
	if c.Statistics.MaxSeries == 0 {
		c.Statistics.MaxSeries = d.Statistics.MaxSeries
	}
	if c.Statistics.Output.Sink == "" {
		c.Statistics.Output.Sink = d.Statistics.Output.Sink
	}
	if c.Statistics.Output.Interval == 0 {
		c.Statistics.Output.Interval = d.Statistics.Output.Interval
	}
	if c.Statistics.Output.QueryWindow == 0 {
		c.Statistics.Output.QueryWindow = d.Statistics.Output.QueryWindow
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
	if c.OrphanRecovery.RecoveryInterval == 0 {
		c.OrphanRecovery.RecoveryInterval = d.OrphanRecovery.RecoveryInterval
	}
	if c.OrphanRecovery.InitialDelay == 0 {
		c.OrphanRecovery.InitialDelay = d.OrphanRecovery.InitialDelay
	}
	if c.FileWatcherService.DefaultPollingInterval == 0 {
		c.FileWatcherService.DefaultPollingInterval = d.FileWatcherService.DefaultPollingInterval
	}
	if c.FileWatcherService.MinimumPollingInterval == 0 {
		c.FileWatcherService.MinimumPollingInterval = d.FileWatcherService.MinimumPollingInterval
	}
	if c.FileWatcherService.MaximumPollingInterval == 0 {
		c.FileWatcherService.MaximumPollingInterval = d.FileWatcherService.MaximumPollingInterval
	}
	if c.FileWatcherService.DisabledCheckInterval == 0 {
		c.FileWatcherService.DisabledCheckInterval = d.FileWatcherService.DisabledCheckInterval
	}
	if c.FileWatcherService.MaxParallelWatcherScans == 0 {
		c.FileWatcherService.MaxParallelWatcherScans = d.FileWatcherService.MaxParallelWatcherScans
	}
	for i := range c.FileWatcherRoots {
		root := &c.FileWatcherRoots[i]
		if root.PollingInterval == 0 {
			root.PollingInterval = 30 * time.Second
		}
		if root.MinFileAge == 0 {
			root.MinFileAge = 5 * time.Second
		}
		if root.MaxConcurrentImports == 0 {
			root.MaxConcurrentImports = 4
		}
		if len(root.FilePatterns) == 0 {
			root.FilePatterns = []string{"*.*"}
		}
		if root.PostImportAction == "" {
			root.PostImportAction = "Delete"
		}
		if root.SourceCleanupFailureDirectory == "" {
			root.SourceCleanupFailureDirectory = defaultSourceCleanupFailureDirectory
		}
		applyFileWatcherRootAdvancedDefaults(root)
	}
	for i := range c.Volumes {
		if c.Volumes[i].VolumeType == "" {
			c.Volumes[i].VolumeType = "LocalFileSystem"
		}
		if c.Volumes[i].InitialDelay == 0 {
			c.Volumes[i].InitialDelay = defaultVolumeInitialDelay
		}
		if c.Volumes[i].HealthCheckDelay == 0 {
			c.Volumes[i].HealthCheckDelay = defaultVolumeHealthCheckDelay
		}
	}
	for i := range c.FileWatchers {
		if c.FileWatchers[i].PollingInterval == 0 {
			c.FileWatchers[i].PollingInterval = 30 * time.Second
		}
		if c.FileWatchers[i].MinFileAge == 0 {
			c.FileWatchers[i].MinFileAge = 5 * time.Second
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
		if c.FileWatchers[i].SourceCleanupFailureDirectory == "" {
			c.FileWatchers[i].SourceCleanupFailureDirectory = defaultSourceCleanupFailureDirectory
		}
		applyFileWatcherAdvancedDefaults(&c.FileWatchers[i])
	}
}

// applyFileWatcherAdvancedDefaults fills the zero-valued advanced watcher
// settings. A negative duration is preserved: it selects the documented
// "disabled" behavior rather than the default.
func applyFileWatcherAdvancedDefaults(watcher *FileWatcherConfig) {
	if watcher.MaxPostImportActionRetryCount == 0 {
		watcher.MaxPostImportActionRetryCount = defaultMaxPostImportActionRetryCount
	}
	if watcher.PostImportActionRetryInitialDelay == 0 {
		watcher.PostImportActionRetryInitialDelay = defaultPostImportActionRetryInitialDelay
	}
	if watcher.PostImportActionRetryMaxDelay == 0 {
		watcher.PostImportActionRetryMaxDelay = defaultPostImportActionRetryMaxDelay
	}
	if watcher.AutoCreateTenantDirectoriesCacheTTL == 0 {
		watcher.AutoCreateTenantDirectoriesCacheTTL = defaultAutoCreateTenantDirectoriesCacheTTL
	}
	if watcher.FileStabilityCheckDelay == 0 {
		watcher.FileStabilityCheckDelay = defaultFileStabilityCheckDelay
	}
	if watcher.SkipStabilityCheckAfterAge == 0 {
		watcher.SkipStabilityCheckAfterAge = defaultSkipStabilityCheckAfterAge
	}
	if watcher.ImportedFilesPruneInterval == 0 {
		watcher.ImportedFilesPruneInterval = defaultImportedFilesPruneInterval
	}
	if watcher.ImportedFilesHistoryFlushInterval == 0 {
		watcher.ImportedFilesHistoryFlushInterval = defaultImportedFilesHistoryFlushInterval
	}
}

// applyFileWatcherRootAdvancedDefaults fills the zero-valued advanced settings of
// a root template, using the same defaults as a single watcher.
func applyFileWatcherRootAdvancedDefaults(root *FileWatcherRootConfig) {
	if root.MaxPostImportActionRetryCount == 0 {
		root.MaxPostImportActionRetryCount = defaultMaxPostImportActionRetryCount
	}
	if root.PostImportActionRetryInitialDelay == 0 {
		root.PostImportActionRetryInitialDelay = defaultPostImportActionRetryInitialDelay
	}
	if root.PostImportActionRetryMaxDelay == 0 {
		root.PostImportActionRetryMaxDelay = defaultPostImportActionRetryMaxDelay
	}
	if root.AutoCreateTenantDirectoriesCacheTTL == 0 {
		root.AutoCreateTenantDirectoriesCacheTTL = defaultAutoCreateTenantDirectoriesCacheTTL
	}
	if root.FileStabilityCheckDelay == 0 {
		root.FileStabilityCheckDelay = defaultFileStabilityCheckDelay
	}
	if root.SkipStabilityCheckAfterAge == 0 {
		root.SkipStabilityCheckAfterAge = defaultSkipStabilityCheckAfterAge
	}
	if root.ImportedFilesPruneInterval == 0 {
		root.ImportedFilesPruneInterval = defaultImportedFilesPruneInterval
	}
	if root.ImportedFilesHistoryFlushInterval == 0 {
		root.ImportedFilesHistoryFlushInterval = defaultImportedFilesHistoryFlushInterval
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
	if err := validateRetryPolicy(&c.RetryPolicy); err != nil {
		return err
	}
	if err := validateSqlite(&c.Sqlite); err != nil {
		return err
	}
	if err := validateCaches(&c.TenantManager, &c.Metadata); err != nil {
		return err
	}
	if err := validateCleanup(&c.Cleanup); err != nil {
		return err
	}
	if err := validateOrphanRecovery(&c.OrphanRecovery); err != nil {
		return err
	}
	if err := validateDatabaseHealthCheck(&c.DatabaseHealthCheck); err != nil {
		return err
	}
	if err := validateFileWatcherService(&c.FileWatcherService); err != nil {
		return err
	}
	if err := validateSourceCleanup(&c.SourceCleanup); err != nil {
		return err
	}
	if err := validateFileWatcherRoots(c.FileWatcherRoots); err != nil {
		return err
	}
	if err := validateStatistics(&c.Statistics); err != nil {
		return err
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
		if volume.InitialDelay < 0 {
			return fmt.Errorf("volume[%d]: InitialDelay cannot be negative", i)
		}
		if volume.HealthCheckDelay < 0 {
			return fmt.Errorf("volume[%d]: HealthCheckDelay cannot be negative", i)
		}
	}

	tenantIDs := make(map[string]bool)
	for i, tenant := range c.Tenants {
		if tenant.TenantID == "" {
			return fmt.Errorf("tenant[%d]: TenantID is required", i)
		}
		// Tenant IDs become path segments for tenant metadata and physical
		// storage, so they must be validated before runtime construction.
		if err := core.ValidateTenantID(tenant.TenantID); err != nil {
			return fmt.Errorf("tenant[%d]: %w", i, err)
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
		if watcher.MultiTenantMode && watcher.TenantID != "" {
			return fmt.Errorf("fileWatcher[%d]: TenantID must be empty when MultiTenantMode is true", i)
		}
		if !watcher.MultiTenantMode {
			if err := core.ValidateTenantID(watcher.TenantID); err != nil {
				return fmt.Errorf("fileWatcher[%d]: %w", i, err)
			}
		}
		if watcher.PostImportAction != "Delete" && watcher.PostImportAction != "Move" && watcher.PostImportAction != "Keep" {
			return fmt.Errorf("fileWatcher[%d]: PostImportAction must be 'Delete', 'Move', or 'Keep'", i)
		}
		if watcher.PostImportAction == "Move" && watcher.MoveToDirectory == "" {
			return fmt.Errorf("fileWatcher[%d]: MoveToDirectory is required when PostImportAction is 'Move'", i)
		}
		if watcher.MaxConcurrentImports < 0 {
			return fmt.Errorf("fileWatcher[%d]: MaxConcurrentImports cannot be negative", i)
		}
		if watcher.MaxPostImportActionRetryCount < 0 {
			return fmt.Errorf("fileWatcher[%d]: MaxPostImportActionRetryCount cannot be negative", i)
		}
		if watcher.PostImportActionRetryInitialDelay < 0 {
			return fmt.Errorf("fileWatcher[%d]: PostImportActionRetryInitialDelay cannot be negative", i)
		}
		if watcher.PostImportActionRetryMaxDelay < 0 {
			return fmt.Errorf("fileWatcher[%d]: PostImportActionRetryMaxDelay cannot be negative", i)
		}
		if watcher.MinFileAge < 0 {
			return fmt.Errorf("fileWatcher[%d]: MinFileAge cannot be negative", i)
		}
		if watcher.MaxFileSizeBytes < 0 {
			return fmt.Errorf("fileWatcher[%d]: MaxFileSizeBytes cannot be negative", i)
		}
		if watcher.PollingInterval < 0 {
			return fmt.Errorf("fileWatcher[%d]: PollingInterval cannot be negative", i)
		}
		if err := validateSourceCleanupFailureDirectory(
			fmt.Sprintf("fileWatcher[%d]", i), watcher.SourceCleanupFailureDirectory,
		); err != nil {
			return err
		}
		if err := validateWatcherAdvancedSettings(
			"fileWatcher["+fmt.Sprint(i)+"]",
			watcher.AutoCreateTenantDirectoriesCacheTTL,
			watcher.ImportedFilesPruneInterval,
			watcher.ImportedFilesHistoryFlushInterval,
		); err != nil {
			return err
		}
		for patternIndex, pattern := range watcher.FilePatterns {
			if pattern == "" || pattern != strings.TrimSpace(pattern) {
				return fmt.Errorf("fileWatcher[%d]: FilePatterns[%d] must be a non-blank pattern", i, patternIndex)
			}
			if _, err := filepath.Match(pattern, "probe"); err != nil {
				return fmt.Errorf("fileWatcher[%d]: FilePatterns[%d] is not a valid pattern: %w", i, patternIndex, err)
			}
		}
	}
	return nil
}

// sqliteJournalModes and sqliteSynchronousModes are the value whitelists Locus
// applies to its SQLite pragmas (SqliteOptions.BuildPragmaSql). They are matched
// case-insensitively.
var (
	sqliteJournalModes = map[string]bool{
		"DELETE": true, "TRUNCATE": true, "PERSIST": true,
		"MEMORY": true, "WAL": true, "OFF": true,
	}

	sqliteSynchronousModes = map[string]bool{
		"OFF": true, "NORMAL": true, "FULL": true, "EXTRA": true,
		"0": true, "1": true, "2": true, "3": true,
	}
)

// validateSqlite checks the SQLite engine configuration, including the pragma
// value whitelists, so an injection or a typo fails at construction instead of
// reaching a PRAGMA statement.
func validateSqlite(sqlite *SqliteConfig) error {
	if !sqliteJournalModes[strings.ToUpper(sqlite.JournalMode)] {
		return fmt.Errorf("Sqlite.JournalMode %q is not one of DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF", sqlite.JournalMode)
	}
	if !sqliteSynchronousModes[strings.ToUpper(sqlite.SynchronousMode)] {
		return fmt.Errorf("Sqlite.SynchronousMode %q is not one of OFF, NORMAL, FULL, EXTRA, 0, 1, 2, 3", sqlite.SynchronousMode)
	}
	if sqlite.CacheSizeKb == 0 {
		return fmt.Errorf("Sqlite.CacheSizeKb cannot be 0: it would disable the page cache")
	}
	if sqlite.BusyTimeoutMs < 0 {
		return fmt.Errorf("Sqlite.BusyTimeoutMs cannot be negative")
	}
	if sqlite.MaxOpenConns < 1 {
		return fmt.Errorf("Sqlite.MaxOpenConns must be at least 1")
	}
	if sqlite.MaxOpenDatabases < 0 {
		return fmt.Errorf("Sqlite.MaxOpenDatabases cannot be negative")
	}
	if sqlite.OpenDatabaseIdleTimeout < 0 {
		return fmt.Errorf("Sqlite.OpenDatabaseIdleTimeout cannot be negative")
	}
	if sqlite.CorruptedDatabaseRetention < 0 {
		return fmt.Errorf("Sqlite.CorruptedDatabaseRetention cannot be negative")
	}
	if sqlite.BackupInterval < 0 {
		return fmt.Errorf("Sqlite.BackupInterval cannot be negative")
	}
	if sqlite.BackupRetention < 0 {
		return fmt.Errorf("Sqlite.BackupRetention cannot be negative")
	}
	if sqlite.AutoRestoreFromBackup && sqlite.BackupDirectory == "" {
		return fmt.Errorf("Sqlite.AutoRestoreFromBackup requires Sqlite.BackupDirectory")
	}
	// An empty BackupDirectory disables periodic backups, so BackupInterval and
	// BackupRetention are simply unused in that case. They are not rejected: the
	// defaults carry a positive interval, and requiring the directory would make
	// DefaultConfig itself invalid.
	return nil
}

func validateRetryPolicy(policy *RetryPolicyConfig) error {
	if policy.MaxRetryCount < 0 {
		return fmt.Errorf("RetryPolicy.MaxRetryCount cannot be negative")
	}
	if policy.InitialRetryDelay < 0 {
		return fmt.Errorf("RetryPolicy.InitialRetryDelay cannot be negative")
	}
	if policy.MaxRetryDelay < 0 {
		return fmt.Errorf("RetryPolicy.MaxRetryDelay cannot be negative")
	}
	return nil
}

// validateStatistics mirrors the in-memory recorder's own limits so an invalid
// configuration fails at construction rather than at first record.
func validateStatistics(statistics *StatisticsConfig) error {
	if statistics.WindowSize < 0 {
		return fmt.Errorf("Statistics.WindowSize cannot be negative")
	}
	if statistics.Retention < 0 {
		return fmt.Errorf("Statistics.Retention cannot be negative")
	}
	if statistics.WindowSize > 0 && statistics.Retention > 0 && statistics.Retention < statistics.WindowSize {
		return fmt.Errorf("Statistics.Retention must be greater than or equal to Statistics.WindowSize")
	}
	if statistics.MaxSeries < 0 {
		return fmt.Errorf("Statistics.MaxSeries cannot be negative")
	}
	// A zero value means "use the default", which ApplyDefaults fills in.
	if statistics.MaxSeries > 0 && (statistics.MaxSeries < minStatisticsMaxSeries || statistics.MaxSeries > maxStatisticsMaxSeries) {
		return fmt.Errorf(
			"Statistics.MaxSeries must be between %d and %d",
			minStatisticsMaxSeries,
			maxStatisticsMaxSeries,
		)
	}
	if statistics.Output.Sink != "" && !strings.EqualFold(statistics.Output.Sink, "Logging") {
		return fmt.Errorf("Statistics.Output.Sink must be 'Logging'")
	}
	if statistics.Output.Interval < 0 {
		return fmt.Errorf("Statistics.Output.Interval cannot be negative")
	}
	if statistics.Output.QueryWindow < 0 {
		return fmt.Errorf("Statistics.Output.QueryWindow cannot be negative")
	}
	if statistics.Output.Enabled && !statistics.Enabled {
		return fmt.Errorf("Statistics.Output.Enabled requires Statistics.Enabled")
	}
	return nil
}

func validateCaches(tenantManager *TenantManagerConfig, metadata *MetadataConfig) error {
	if tenantManager.CacheTTL < 0 {
		return fmt.Errorf("TenantManager.CacheTTL cannot be negative")
	}
	if metadata.CacheTTL < 0 {
		return fmt.Errorf("Metadata.CacheTTL cannot be negative")
	}
	if metadata.MaxCacheEntries < 0 {
		return fmt.Errorf("Metadata.MaxCacheEntries cannot be negative")
	}
	return nil
}

func validateCleanup(cleanup *CleanupConfig) error {
	if cleanup.CleanupInterval < 0 {
		return fmt.Errorf("Cleanup.CleanupInterval cannot be negative")
	}
	if cleanup.InitialDelay < 0 {
		return fmt.Errorf("Cleanup.InitialDelay cannot be negative")
	}
	if cleanup.ProcessingTimeout < 0 {
		return fmt.Errorf("Cleanup.ProcessingTimeout cannot be negative")
	}
	if cleanup.TimedOutReclaimCooldown < 0 {
		return fmt.Errorf("Cleanup.TimedOutReclaimCooldown cannot be negative")
	}
	if cleanup.FailedFileRetentionPeriod < 0 {
		return fmt.Errorf("Cleanup.FailedFileRetentionPeriod cannot be negative")
	}
	if cleanup.CompletedRecordRetentionPeriod < 0 {
		return fmt.Errorf("Cleanup.CompletedRecordRetentionPeriod cannot be negative")
	}
	if cleanup.DatabaseOptimizationInterval < 0 {
		return fmt.Errorf("Cleanup.DatabaseOptimizationInterval cannot be negative")
	}
	if cleanup.JunkFileCleanupInterval < 0 {
		return fmt.Errorf("Cleanup.JunkFileCleanupInterval cannot be negative")
	}
	if _, err := core.ParsePermanentlyFailedDisposition(cleanup.PermanentlyFailedDisposition); err != nil {
		return fmt.Errorf("Cleanup.PermanentlyFailedDisposition: %w", err)
	}
	if err := validateRetiredVolumes(cleanup.RetiredVolumes); err != nil {
		return err
	}
	return validateDeadLetter(&cleanup.DeadLetter)
}

// validateDeadLetter ensures the dead-letter area stays inside a volume root and
// describes a usable layout.
func validateDeadLetter(deadLetter *DeadLetterConfig) error {
	if deadLetter.RootPath == "" {
		return fmt.Errorf("Cleanup.DeadLetter.RootPath is required")
	}
	if filepath.IsAbs(deadLetter.RootPath) || filepath.VolumeName(deadLetter.RootPath) != "" {
		return fmt.Errorf("Cleanup.DeadLetter.RootPath must be relative to the volume mount path")
	}
	for _, segment := range strings.FieldsFunc(filepath.ToSlash(deadLetter.RootPath), func(r rune) bool { return r == '/' }) {
		if segment == ".." {
			return fmt.Errorf("Cleanup.DeadLetter.RootPath must not contain %q segments", "..")
		}
	}
	if deadLetter.ShardingDepth < 0 || deadLetter.ShardingDepth > 3 {
		return fmt.Errorf("Cleanup.DeadLetter.ShardingDepth must be between 0 and 3")
	}
	return nil
}

func validateRetiredVolumes(volumes []RetiredVolumeConfig) error {
	seen := make(map[string]bool, len(volumes))
	for i, volume := range volumes {
		if volume.VolumeID == "" {
			return fmt.Errorf("retiredVolumes[%d]: VolumeID is required", i)
		}
		if seen[volume.VolumeID] {
			return fmt.Errorf("retiredVolumes[%d]: duplicate VolumeID: %s", i, volume.VolumeID)
		}
		seen[volume.VolumeID] = true
		if _, err := core.ParseRetiredVolumeDisposition(volume.Disposition); err != nil {
			return fmt.Errorf("retiredVolumes[%d]: %w", i, err)
		}
	}
	return nil
}

func validateOrphanRecovery(recovery *OrphanRecoveryConfig) error {
	if recovery.RecoveryInterval < 0 {
		return fmt.Errorf("OrphanRecovery.RecoveryInterval cannot be negative")
	}
	if recovery.InitialDelay < 0 {
		return fmt.Errorf("OrphanRecovery.InitialDelay cannot be negative")
	}
	return nil
}

func validateDatabaseHealthCheck(check *DatabaseHealthCheckConfig) error {
	if check.InitialDelay < 0 {
		return fmt.Errorf("DatabaseHealthCheck.InitialDelay cannot be negative")
	}
	if check.MaxRetries < 0 {
		return fmt.Errorf("DatabaseHealthCheck.MaxRetries cannot be negative")
	}
	if check.RetryDelay < 0 {
		return fmt.Errorf("DatabaseHealthCheck.RetryDelay cannot be negative")
	}
	if check.PeriodicCheckInterval < 0 {
		return fmt.Errorf("DatabaseHealthCheck.PeriodicCheckInterval cannot be negative")
	}
	return nil
}

// validateWatcherAdvancedSettings rejects negative values for the advanced
// watcher settings whose zero value means "use the default". The two stability
// delays are excluded: a negative value there means "disable the extra probe".
func validateWatcherAdvancedSettings(label string, cacheTTL, pruneInterval, flushInterval time.Duration) error {
	if cacheTTL < 0 {
		return fmt.Errorf("%s: AutoCreateTenantDirectoriesCacheTTL cannot be negative", label)
	}
	if pruneInterval < 0 {
		return fmt.Errorf("%s: ImportedFilesPruneInterval cannot be negative", label)
	}
	if flushInterval < 0 {
		return fmt.Errorf("%s: ImportedFilesHistoryFlushInterval cannot be negative", label)
	}
	return nil
}

func validateFileWatcherService(service *FileWatcherServiceConfig) error {
	if service.DefaultPollingInterval < 0 {
		return fmt.Errorf("FileWatcherService.DefaultPollingInterval cannot be negative")
	}
	if service.MinimumPollingInterval < 0 {
		return fmt.Errorf("FileWatcherService.MinimumPollingInterval cannot be negative")
	}
	if service.MaximumPollingInterval < 0 {
		return fmt.Errorf("FileWatcherService.MaximumPollingInterval cannot be negative")
	}
	if service.MinimumPollingInterval > 0 && service.MaximumPollingInterval > 0 &&
		service.MinimumPollingInterval > service.MaximumPollingInterval {
		return fmt.Errorf("FileWatcherService.MinimumPollingInterval must not exceed MaximumPollingInterval")
	}
	if service.DisabledCheckInterval < 0 {
		return fmt.Errorf("FileWatcherService.DisabledCheckInterval cannot be negative")
	}
	if service.MaxParallelWatcherScans < 0 {
		return fmt.Errorf("FileWatcherService.MaxParallelWatcherScans cannot be negative")
	}
	return nil
}

func validateSourceCleanup(cleanup *SourceCleanupConfig) error {
	if cleanup.Enabled && strings.TrimSpace(cleanup.DatabasePath) == "" {
		return fmt.Errorf("SourceCleanup.DatabasePath is required when enabled")
	}
	if cleanup.PollingInterval <= 0 {
		return fmt.Errorf("SourceCleanup.PollingInterval must be positive")
	}
	if cleanup.MaxConcurrentActions <= 0 {
		return fmt.Errorf("SourceCleanup.MaxConcurrentActions must be positive")
	}
	if cleanup.MaxActiveJobs <= 0 {
		return fmt.Errorf("SourceCleanup.MaxActiveJobs must be positive")
	}
	if cleanup.TerminalJobRetentionPeriod <= 0 {
		return fmt.Errorf("SourceCleanup.TerminalJobRetentionPeriod must be positive")
	}
	if cleanup.ImportReservationTimeout <= 0 {
		return fmt.Errorf("SourceCleanup.ImportReservationTimeout must be positive")
	}
	if cleanup.DatabaseOptimizationInterval <= 0 {
		return fmt.Errorf("SourceCleanup.DatabaseOptimizationInterval must be positive")
	}
	if cleanup.TerminalPruneBatchSize <= 0 {
		return fmt.Errorf("SourceCleanup.TerminalPruneBatchSize must be positive")
	}
	return nil
}

func validateSourceCleanupFailureDirectory(label, path string) error {
	if path == "" {
		return nil
	}
	if filepath.IsAbs(path) {
		return fmt.Errorf("%s.SourceCleanupFailureDirectory must be relative", label)
	}
	clean := filepath.Clean(path)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%s.SourceCleanupFailureDirectory must remain below its configuration root", label)
	}
	return nil
}

func validateFileWatcherRoots(roots []FileWatcherRootConfig) error {
	seen := make(map[string]bool, len(roots))
	for i, root := range roots {
		if root.RootPath == "" {
			return fmt.Errorf("watcherRoots[%d]: RootPath is required", i)
		}
		if seen[root.RootPath] {
			return fmt.Errorf("watcherRoots[%d]: duplicate RootPath: %s", i, root.RootPath)
		}
		seen[root.RootPath] = true
		if !root.MultiTenantMode {
			// A single-tenant root has no tenant directory to infer a tenant
			// from, so the runtime cannot generate a usable watcher.
			return fmt.Errorf("watcherRoots[%d]: MultiTenantMode must be true for a derived watcher", i)
		}
		if root.PostImportAction != "Delete" && root.PostImportAction != "Move" && root.PostImportAction != "Keep" {
			return fmt.Errorf("watcherRoots[%d]: PostImportAction must be 'Delete', 'Move', or 'Keep'", i)
		}
		if root.PostImportAction == "Move" && root.MoveToDirectory == "" {
			return fmt.Errorf("watcherRoots[%d]: MoveToDirectory is required when PostImportAction is 'Move'", i)
		}
		if root.MaxConcurrentImports < 0 {
			return fmt.Errorf("watcherRoots[%d]: MaxConcurrentImports cannot be negative", i)
		}
		if root.MaxPostImportActionRetryCount < 0 {
			return fmt.Errorf("watcherRoots[%d]: MaxPostImportActionRetryCount cannot be negative", i)
		}
		if root.PostImportActionRetryInitialDelay < 0 {
			return fmt.Errorf("watcherRoots[%d]: PostImportActionRetryInitialDelay cannot be negative", i)
		}
		if root.PostImportActionRetryMaxDelay < 0 {
			return fmt.Errorf("watcherRoots[%d]: PostImportActionRetryMaxDelay cannot be negative", i)
		}
		if root.MinFileAge < 0 {
			return fmt.Errorf("watcherRoots[%d]: MinFileAge cannot be negative", i)
		}
		if root.MaxFileSizeBytes < 0 {
			return fmt.Errorf("watcherRoots[%d]: MaxFileSizeBytes cannot be negative", i)
		}
		if root.PollingInterval < 0 {
			return fmt.Errorf("watcherRoots[%d]: PollingInterval cannot be negative", i)
		}
		if err := validateSourceCleanupFailureDirectory(
			fmt.Sprintf("watcherRoots[%d]", i), root.SourceCleanupFailureDirectory,
		); err != nil {
			return err
		}
		if err := validateWatcherAdvancedSettings(
			"watcherRoots["+fmt.Sprint(i)+"]",
			root.AutoCreateTenantDirectoriesCacheTTL,
			root.ImportedFilesPruneInterval,
			root.ImportedFilesHistoryFlushInterval,
		); err != nil {
			return err
		}
		for patternIndex, pattern := range root.FilePatterns {
			if pattern == "" || pattern != strings.TrimSpace(pattern) {
				return fmt.Errorf("watcherRoots[%d]: FilePatterns[%d] must be a non-blank pattern", i, patternIndex)
			}
			if _, err := filepath.Match(pattern, "probe"); err != nil {
				return fmt.Errorf("watcherRoots[%d]: FilePatterns[%d] is not a valid pattern: %w", i, patternIndex, err)
			}
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
	clone.FileWatcherRoots = make([]FileWatcherRootConfig, len(c.FileWatcherRoots))
	for i, root := range c.FileWatcherRoots {
		clone.FileWatcherRoots[i] = root
		clone.FileWatcherRoots[i].FilePatterns = append([]string(nil), root.FilePatterns...)
	}
	if c.Logging != nil {
		loggingConfig := *c.Logging
		clone.Logging = &loggingConfig
	}
	return &clone
}
