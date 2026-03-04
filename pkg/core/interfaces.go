package core

import (
	"context"
	"io"
	"time"
)

// StoragePool is the main interface for file storage and queue-based processing.
// It provides a unified API that combines file storage operations with queue management.
// Storage volumes are configured at startup and managed internally.
type StoragePool interface {
	// WriteFile stores a file in the storage pool and returns a system-generated fileKey.
	// The file is initially in Pending status and will be available for processing.
	//
	// originalFileName is optional but recommended to preserve file extensions for debugging.
	// Returns the generated fileKey (UUID format without dashes).
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	// - ErrTenantNotFound if tenant doesn't exist and auto-create is disabled
	// - ErrTenantQuotaExceeded if tenant quota is exceeded
	// - ErrDirectoryQuotaExceeded if directory quota is exceeded
	// - ErrInsufficientStorage if no volumes have available space
	WriteFile(ctx context.Context, tenant TenantContext, content io.Reader, originalFileName *string) (string, error)

	// ReadFile retrieves a file by its fileKey.
	// Returns an io.ReadCloser that must be closed by the caller.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	// - ErrFileNotFound if file doesn't exist
	ReadFile(ctx context.Context, tenant TenantContext, fileKey string) (io.ReadCloser, error)

	// GetFileInfo returns basic file information.
	// Returns nil if file doesn't exist.
	GetFileInfo(ctx context.Context, tenant TenantContext, fileKey string) (*FileInfo, error)

	// GetFileLocation returns detailed file location information for diagnostics.
	// Returns nil if file doesn't exist.
	GetFileLocation(ctx context.Context, tenant TenantContext, fileKey string) (*FileLocation, error)

	// GetNextFileForProcessing retrieves the next pending file for processing.
	// This operation is thread-safe and atomic - no two calls will return the same file.
	// The file status is automatically transitioned from Pending to Processing.
	//
	// Returns nil if no files are available for processing.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	GetNextFileForProcessing(ctx context.Context, tenant TenantContext) (*FileLocation, error)

	// GetNextBatchForProcessing retrieves a batch of pending files for processing.
	// Each file's status is automatically transitioned from Pending to Processing.
	// This operation is thread-safe - no file will be returned more than once.
	//
	// Returns an empty slice if no files are available.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	GetNextBatchForProcessing(ctx context.Context, tenant TenantContext, batchSize int) ([]*FileLocation, error)

	// MarkAsCompleted marks a file as successfully processed.
	// This deletes both the physical file and its metadata.
	MarkAsCompleted(ctx context.Context, fileKey string) error

	// MarkAsFailed marks a file as failed and schedules it for retry.
	// If retry count exceeds the maximum, the file is marked as PermanentlyFailed.
	//
	// The retry delay uses exponential backoff:
	//   delay = InitialDelay * 2^(retryCount-1), capped at MaxRetryDelay
	MarkAsFailed(ctx context.Context, fileKey string, errorMessage string) error

	// GetFileStatus returns the current processing status of a file.
	GetFileStatus(ctx context.Context, fileKey string) (FileProcessingStatus, error)

	// GetTotalCapacity returns the total capacity across all mounted volumes.
	GetTotalCapacity(ctx context.Context) (int64, error)

	// GetAvailableSpace returns the available space across all mounted volumes.
	GetAvailableSpace(ctx context.Context) (int64, error)
}

// TenantManager manages tenant lifecycle and multi-tenant isolation.
type TenantManager interface {
	// GetTenant retrieves a tenant context by ID.
	// If auto-create is enabled and tenant doesn't exist, creates it automatically.
	//
	// Errors:
	// - ErrTenantNotFound if tenant doesn't exist and auto-create is disabled
	GetTenant(ctx context.Context, tenantID string) (TenantContext, error)

	// IsTenantEnabled checks if a tenant is enabled.
	// Returns false if tenant doesn't exist.
	IsTenantEnabled(ctx context.Context, tenantID string) (bool, error)

	// CreateTenant creates a new tenant.
	//
	// Errors:
	// - ErrTenantAlreadyExists if tenant already exists
	CreateTenant(ctx context.Context, tenantID string) error

	// EnableTenant enables a disabled tenant.
	//
	// Errors:
	// - ErrTenantNotFound if tenant doesn't exist
	EnableTenant(ctx context.Context, tenantID string) error

	// DisableTenant disables a tenant.
	// All subsequent operations on this tenant will fail with ErrTenantDisabled.
	//
	// Errors:
	// - ErrTenantNotFound if tenant doesn't exist
	DisableTenant(ctx context.Context, tenantID string) error

	// GetAllTenants returns all tenants.
	GetAllTenants(ctx context.Context) ([]TenantContext, error)
}

// FileScheduler manages file processing queue with concurrency control.
type FileScheduler interface {
	// GetNextFileForProcessing retrieves the next pending file atomically.
	// Status is transitioned from Pending to Processing within a transaction.
	GetNextFileForProcessing(ctx context.Context, tenant TenantContext) (*FileLocation, error)

	// GetNextBatchForProcessing retrieves multiple files atomically.
	GetNextBatchForProcessing(ctx context.Context, tenant TenantContext, batchSize int) ([]*FileLocation, error)

	// MarkAsCompleted marks a file as completed and schedules it for deletion.
	MarkAsCompleted(ctx context.Context, fileKey string) error

	// MarkAsFailed marks a file as failed and schedules retry or permanent failure.
	MarkAsFailed(ctx context.Context, fileKey string, errorMessage string) error

	// GetFileStatus returns the current status of a file.
	GetFileStatus(ctx context.Context, fileKey string) (FileProcessingStatus, error)

	// ResetTimedOutFiles finds files in Processing status that exceed timeout
	// and resets them to Pending status for retry.
	ResetTimedOutFiles(ctx context.Context, timeout time.Duration) (int, error)
}

// StorageVolume represents a storage backend (local filesystem, network drive, cloud storage).
type StorageVolume interface {
	// VolumeID returns the unique identifier for this volume.
	VolumeID() string

	// MountPath returns the root path where this volume is mounted.
	MountPath() string

	// IsHealthy checks if the volume is healthy and available for operations.
	IsHealthy(ctx context.Context) bool

	// TotalCapacity returns the total capacity in bytes.
	TotalCapacity(ctx context.Context) (int64, error)

	// AvailableSpace returns the available space in bytes.
	AvailableSpace(ctx context.Context) (int64, error)

	// WriteFile writes a file to the specified path within the volume.
	// Returns the number of bytes written.
	WriteFile(ctx context.Context, relativePath string, content io.Reader) (int64, error)

	// ReadFile reads a file from the specified path within the volume.
	// Returns an io.ReadCloser that must be closed by the caller.
	ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error)

	// DeleteFile deletes a file at the specified path within the volume.
	DeleteFile(ctx context.Context, relativePath string) error

	// FileExists checks if a file exists at the specified path.
	FileExists(ctx context.Context, relativePath string) (bool, error)
}

// DirectoryQuotaManager manages file count quotas at the directory level.
type DirectoryQuotaManager interface {
	// CanAddFile checks if a file can be added to a directory without exceeding quota.
	// Returns true if quota allows, false otherwise.
	CanAddFile(ctx context.Context, tenantID string, directoryPath string) (bool, error)

	// IncrementFileCount atomically increments the file count for a directory.
	//
	// Errors:
	// - ErrDirectoryQuotaExceeded if quota would be exceeded
	IncrementFileCount(ctx context.Context, tenantID string, directoryPath string) error

	// DecrementFileCount atomically decrements the file count for a directory.
	DecrementFileCount(ctx context.Context, tenantID string, directoryPath string) error

	// GetFileCount returns the current file count for a directory.
	GetFileCount(ctx context.Context, tenantID string, directoryPath string) (int, error)

	// SetQuota sets the maximum file count for a directory (0 = unlimited).
	SetQuota(ctx context.Context, tenantID string, directoryPath string, maxCount int) error

	// GetQuota returns the quota configuration for a directory.
	GetQuota(ctx context.Context, tenantID string, directoryPath string) (*DirectoryQuota, error)
}

// TenantQuotaManager manages file count quotas at the tenant level.
type TenantQuotaManager interface {
	// CanAddFile checks if a file can be added to a tenant without exceeding quota.
	CanAddFile(ctx context.Context, tenantID string) (bool, error)

	// IncrementFileCount atomically increments the file count for a tenant.
	//
	// Errors:
	// - ErrTenantQuotaExceeded if quota would be exceeded
	IncrementFileCount(ctx context.Context, tenantID string) error

	// DecrementFileCount atomically decrements the file count for a tenant.
	DecrementFileCount(ctx context.Context, tenantID string) error

	// GetFileCount returns the current file count for a tenant.
	GetFileCount(ctx context.Context, tenantID string) (int, error)

	// SetQuota sets the maximum file count for a tenant (0 = unlimited).
	SetQuota(ctx context.Context, tenantID string, maxCount int) error
}

// CleanupService handles cleanup of orphaned resources.
type CleanupService interface {
	// CleanupEmptyDirectories removes empty directories recursively.
	CleanupEmptyDirectories(ctx context.Context) (*CleanupStatistics, error)

	// CleanupTimedOutProcessingFiles resets files that have been in Processing status too long.
	CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*CleanupStatistics, error)

	// CleanupPermanentlyFailedFiles deletes files that have permanently failed.
	CleanupPermanentlyFailedFiles(ctx context.Context) (*CleanupStatistics, error)

	// CleanupOrphanedMetadata removes metadata for files that no longer exist physically.
	CleanupOrphanedMetadata(ctx context.Context) (*CleanupStatistics, error)
}

// MetadataRepository manages file metadata storage with caching.
type MetadataRepository interface {
	// AddOrUpdate adds or updates file metadata atomically.
	AddOrUpdate(ctx context.Context, metadata *FileMetadata) error

	// AddOrUpdateBatch adds or updates multiple file metadata atomically in a single transaction.
	// More efficient than calling AddOrUpdate multiple times for bulk operations.
	AddOrUpdateBatch(ctx context.Context, metadata []*FileMetadata) error

	// Get retrieves file metadata by key.
	// Returns nil if not found.
	Get(ctx context.Context, fileKey string) (*FileMetadata, error)

	// Delete removes file metadata.
	Delete(ctx context.Context, fileKey string) error

	// DeleteBatch removes multiple file metadata atomically in a single transaction.
	DeleteBatch(ctx context.Context, fileKeys []string) error

	// GetByStatus retrieves files by status with optional limit.
	GetByStatus(ctx context.Context, tenantID string, status FileProcessingStatus, limit int) ([]*FileMetadata, error)

	// GetPendingFiles retrieves files ready for processing (Pending status + available time passed).
	GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*FileMetadata, error)

	// UpdateStatus atomically updates file status.
	UpdateStatus(ctx context.Context, fileKey string, newStatus FileProcessingStatus) error

	// CompareAndTransitionToProcessing atomically transitions a file to Processing status
	// if and only if it is currently in Pending status.
	// Returns the updated metadata on success, or an error if:
	// - File not found
	// - File is not in Pending status
	// - Database error
	// This is a compare-and-swap operation to prevent duplicate processing.
	CompareAndTransitionToProcessing(ctx context.Context, fileKey string) (*FileMetadata, error)

	// GetTimedOutProcessingFiles retrieves files in Processing status that exceed timeout.
	GetTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) ([]*FileMetadata, error)

	// Close closes the repository and releases all resources.
	Close() error
}

// DirectoryQuotaRepository manages directory quota persistence.
type DirectoryQuotaRepository interface {
	// GetOrCreate retrieves directory quota or creates with defaults.
	GetOrCreate(ctx context.Context, directoryPath string) (*DirectoryQuota, error)

	// Update updates directory quota atomically.
	Update(ctx context.Context, quota *DirectoryQuota) error

	// IncrementCount atomically increments the file count.
	IncrementCount(ctx context.Context, directoryPath string) error

	// DecrementCount atomically decrements the file count.
	DecrementCount(ctx context.Context, directoryPath string) error

	// Close closes the repository and releases all resources.
	Close() error
}

// RetryPolicy defines the retry policy for failed file operations.
type RetryPolicy struct {
	// MaxRetryCount is the maximum number of retries for failed files.
	MaxRetryCount int

	// InitialRetryDelay is the initial delay before the first retry.
	InitialRetryDelay time.Duration

	// UseExponentialBackoff enables exponential backoff for retry delays.
	UseExponentialBackoff bool

	// MaxRetryDelay is the maximum delay between retries.
	MaxRetryDelay time.Duration
}

// PostImportAction defines what to do with files after successful import.
type PostImportAction int

const (
	// PostImportActionDelete deletes the original file after import.
	PostImportActionDelete PostImportAction = iota

	// PostImportActionMove moves the file to a different directory after import.
	PostImportActionMove

	// PostImportActionKeep keeps the original file in place after import.
	PostImportActionKeep
)

// ParsePostImportAction converts a string to PostImportAction.
func ParsePostImportAction(s string) PostImportAction {
	switch s {
	case "Move":
		return PostImportActionMove
	case "Keep":
		return PostImportActionKeep
	default:
		return PostImportActionDelete
	}
}

// FileWatcherConfiguration defines the configuration for a file watcher.
type FileWatcherConfiguration struct {
	// WatcherID is a unique identifier for this watcher.
	WatcherID string

	// TenantID is the tenant to import files into.
	// Leave empty for multi-tenant mode.
	TenantID string

	// WatchPath is the directory path to monitor.
	WatchPath string

	// MultiTenantMode enables multi-tenant mode.
	// In multi-tenant mode, each subdirectory name is treated as a tenant ID.
	MultiTenantMode bool

	// AutoCreateTenantDirectories automatically creates subdirectories for all tenants.
	// Only works in multi-tenant mode.
	AutoCreateTenantDirectories bool

	// IncludeSubdirectories enables recursive directory watching.
	IncludeSubdirectories bool

	// PollingInterval is the interval between scans.
	PollingInterval time.Duration

	// MinFileAge is the minimum age a file must be before import.
	// This prevents importing files that are still being written.
	MinFileAge time.Duration

	// FilePatterns are glob patterns to filter files (e.g., "*.pdf", "*.txt").
	// Empty means all files.
	FilePatterns []string

	// MaxFileSizeBytes is the maximum file size to import (0 = unlimited).
	MaxFileSizeBytes int64

	// MaxConcurrentImports is the maximum number of concurrent file imports.
	MaxConcurrentImports int

	// PostImportAction defines what to do after successful import.
	PostImportAction PostImportAction

	// MoveToDirectory is the target directory for PostImportActionMove.
	MoveToDirectory string

	// Enabled indicates if this watcher is active.
	Enabled bool
}

// FileWatcherScanResult contains the results of a file watcher scan.
type FileWatcherScanResult struct {
	// FilesDiscovered is the number of files found during scan.
	FilesDiscovered int

	// FilesImported is the number of files successfully imported.
	FilesImported int

	// FilesSkipped is the number of files skipped (too young, wrong pattern, etc.).
	FilesSkipped int

	// FilesFailed is the number of files that failed to import.
	FilesFailed int

	// BytesImported is the total bytes imported.
	BytesImported int64

	// Errors contains error messages for failed imports.
	Errors []string

	// ScanDuration is how long the scan took.
	ScanDuration time.Duration
}

// FileWatcher manages automatic file import from monitored directories.
type FileWatcher interface {
	// RegisterWatcher adds a new file watcher configuration.
	RegisterWatcher(ctx context.Context, config *FileWatcherConfiguration) error

	// UnregisterWatcher removes a file watcher.
	UnregisterWatcher(ctx context.Context, watcherID string) error

	// GetWatcher retrieves a watcher configuration by ID.
	GetWatcher(ctx context.Context, watcherID string) (*FileWatcherConfiguration, error)

	// GetAllWatchers retrieves all watcher configurations.
	GetAllWatchers(ctx context.Context) ([]*FileWatcherConfiguration, error)

	// EnableWatcher enables a watcher.
	EnableWatcher(ctx context.Context, watcherID string) error

	// DisableWatcher disables a watcher.
	DisableWatcher(ctx context.Context, watcherID string) error

	// ScanNow manually triggers a scan for the specified watcher.
	// Returns the scan result.
	ScanNow(ctx context.Context, watcherID string) (*FileWatcherScanResult, error)

	// ScanAllWatchers scans all enabled watchers.
	ScanAllWatchers(ctx context.Context) (map[string]*FileWatcherScanResult, error)
}

// DatabaseType represents the type of database.
type DatabaseType string

const (
	// DatabaseTypeMetadata is the metadata database type.
	DatabaseTypeMetadata DatabaseType = "metadata"

	// DatabaseTypeDirectoryQuota is the directory quota database type.
	DatabaseTypeDirectoryQuota DatabaseType = "directory_quota"
)

// DatabaseHealthStatus represents the health status of a database.
type DatabaseHealthStatus struct {
	// DatabaseType is the type of database.
	DatabaseType DatabaseType

	// TenantID is the tenant ID (empty for shared databases).
	TenantID string

	// DatabasePath is the path to the database.
	DatabasePath string

	// IsHealthy indicates if the database is healthy.
	IsHealthy bool

	// Error contains the error message if not healthy.
	Error string
}

// DatabaseHealthReport contains the results of a database health check.
type DatabaseHealthReport struct {
	// HealthyDatabases is the count of healthy databases.
	HealthyDatabases int

	// CorruptedDatabases contains information about corrupted databases.
	CorruptedDatabases []*DatabaseHealthStatus

	// OrphanedTenants lists tenants with physical files but no metadata.
	OrphanedTenants []string

	// AllHealthy is true if all databases are healthy.
	AllHealthy bool
}

// DatabaseHealthChecker checks database health.
type DatabaseHealthChecker interface {
	// CheckAllDatabases checks the health of all databases.
	CheckAllDatabases(ctx context.Context) (*DatabaseHealthReport, error)

	// CheckMetadataDatabase checks a specific metadata database.
	CheckMetadataDatabase(ctx context.Context, tenantID string) (*DatabaseHealthStatus, error)

	// CheckDirectoryQuotaDatabase checks the directory quota database.
	CheckDirectoryQuotaDatabase(ctx context.Context) (*DatabaseHealthStatus, error)

	// DetectOrphanedFiles detects tenants with physical files but no metadata.
	DetectOrphanedFiles(ctx context.Context) ([]string, error)
}
