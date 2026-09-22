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

	// WriteFileToDirectory stores a file and associates it with a logical directory.
	// The logical directory is used for quota accounting and is independent of the
	// volume's physical sharding layout.
	WriteFileToDirectory(
		ctx context.Context,
		tenant TenantContext,
		content io.Reader,
		originalFileName *string,
		logicalDirectoryPath string,
	) (string, error)

	// ReadFile retrieves a file by its fileKey.
	// Returns an io.ReadCloser that must be closed by the caller.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	// - ErrFileNotFound if file doesn't exist
	ReadFile(ctx context.Context, tenant TenantContext, fileKey string) (io.ReadCloser, error)

	// GetFileInfo returns basic file information.
	//
	// Errors:
	// - ErrFileNotFound if the file doesn't exist for this tenant
	GetFileInfo(ctx context.Context, tenant TenantContext, fileKey string) (*FileInfo, error)

	// GetFileLocation returns detailed file location information for diagnostics.
	//
	// Errors:
	// - ErrFileNotFound if the file doesn't exist for this tenant
	GetFileLocation(ctx context.Context, tenant TenantContext, fileKey string) (*FileLocation, error)

	// GetNextFileForProcessing retrieves the next pending file for processing.
	// This operation is thread-safe and atomic - no two calls will return the same file.
	// The file status is automatically transitioned from Pending to Processing.
	//
	// It returns (nil, nil) when no file is available for processing. An empty
	// queue is not an error: callers should treat a nil location as "no work
	// right now" and poll again later.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	// - a failed claim is reported to the caller instead of being masked as an
	//   empty queue (for example a closed or failing metadata repository)
	GetNextFileForProcessing(ctx context.Context, tenant TenantContext) (*FileLocation, error)

	// GetNextBatchForProcessing retrieves a batch of pending files for processing.
	// Each file's status is automatically transitioned from Pending to Processing.
	// This operation is thread-safe - no file will be returned more than once.
	//
	// It returns an empty slice and a nil error when no file is available.
	//
	// Errors:
	// - ErrTenantDisabled if tenant is disabled
	// - a failed claim is reported to the caller instead of being masked as an
	//   empty queue
	GetNextBatchForProcessing(ctx context.Context, tenant TenantContext, batchSize int) ([]*FileLocation, error)

	// MarkAsCompleted marks a file as successfully processed.
	// Physical deletion and final metadata removal are performed by cleanup.
	MarkAsCompleted(ctx context.Context, lease FileProcessingLease) error

	// MarkAsFailed marks a file as failed and schedules it for retry.
	// If retry count exceeds the maximum, the file is marked as PermanentlyFailed.
	//
	// The retry delay uses exponential backoff:
	//   delay = InitialDelay * 2^(retryCount-1), capped at MaxRetryDelay
	MarkAsFailed(ctx context.Context, lease FileProcessingLease, errorMessage string) error

	// GetFileStatus returns the current processing status of a file.
	GetFileStatus(ctx context.Context, tenant TenantContext, fileKey string) (FileProcessingStatus, error)

	// GetTotalCapacity returns the total capacity across all mounted volumes.
	GetTotalCapacity(ctx context.Context) (int64, error)

	// GetAvailableSpace returns the available space across all mounted volumes.
	GetAvailableSpace(ctx context.Context) (int64, error)
}

// IdempotentStoragePool is an optional storage capability used by importers
// that may repeat the same logical write after a crash or a failed follow-up
// action. operationID is scoped to the tenant and remains valid across process
// restarts while the corresponding metadata record exists.
type IdempotentStoragePool interface {
	WriteFileIdempotently(
		ctx context.Context,
		tenant TenantContext,
		content io.Reader,
		originalFileName *string,
		operationID string,
	) (string, error)
}

// TenantManager manages tenant lifecycle and multi-tenant isolation.
type TenantManager interface {
	// GetTenant retrieves a tenant context by ID.
	// If auto-create is enabled and tenant doesn't exist, creates it automatically.
	//
	// Errors:
	// - ErrTenantNotFound if tenant doesn't exist and auto-create is disabled
	GetTenant(ctx context.Context, tenantID string) (TenantContext, error)

	// TryGetTenant retrieves a tenant context by ID without creating it.
	//
	// It reports ok=false when no tenant with that ID exists. Unlike GetTenant it
	// never writes tenant state or creates a tenant directory as a side effect,
	// so a read-only caller can probe a tenant without materializing it.
	//
	// Errors describe only a failed read; a missing tenant is not an error.
	TryGetTenant(ctx context.Context, tenantID string) (tenant TenantContext, ok bool, err error)

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
	//
	// It returns (nil, nil) when no file is available; an empty queue is not an
	// error. Errors describe a failed claim, not an exhausted queue.
	GetNextFileForProcessing(ctx context.Context, tenant TenantContext) (*FileLocation, error)

	// GetNextBatchForProcessing retrieves multiple files atomically.
	// Each file's status is transitioned from Pending to Processing within a
	// transaction.
	//
	// It returns an empty slice and a nil error when no file is available.
	GetNextBatchForProcessing(ctx context.Context, tenant TenantContext, batchSize int) ([]*FileLocation, error)

	// MarkAsCompleted marks a file as completed and schedules it for deletion.
	// The lease must still identify the active Processing state of the file.
	MarkAsCompleted(ctx context.Context, lease FileProcessingLease) error

	// MarkAsFailed marks a file as failed and schedules retry or permanent failure.
	// The lease must still identify the active Processing state of the file.
	MarkAsFailed(ctx context.Context, lease FileProcessingLease, errorMessage string) error

	// GetFileStatus returns the current status of a file.
	// On failure the returned status is the zero FileProcessingStatus and the
	// error preserves core.ErrFileNotFound for missing files.
	GetFileStatus(ctx context.Context, tenant TenantContext, fileKey string) (FileProcessingStatus, error)

	// ResetTimedOutFiles finds files in Processing status that exceed timeout
	// and resets them to Pending status for retry. A reset file becomes
	// available for processing immediately: AvailableForProcessingAt is rewritten
	// to the recovery instant, so a stale or future availability value can never
	// keep a recovered file out of the queue.
	ResetTimedOutFiles(ctx context.Context, tenant TenantContext, timeout time.Duration) (int, error)
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

// StorageVolumePathBuilder exposes a volume-specific physical layout strategy.
// StoragePool uses it after volume selection so sharding settings are honored.
type StorageVolumePathBuilder interface {
	BuildPhysicalPath(tenantID string, fileKey string, fileExtension string) (string, error)
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

	// SetFileCount replaces the current count during startup reconciliation.
	SetFileCount(ctx context.Context, tenantID string, directoryPath string, count int) error
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

	// SetFileCount replaces the current count during startup reconciliation.
	SetFileCount(ctx context.Context, tenantID string, count int) error
}

// FileMover is an optional StorageVolume capability that moves a file inside
// the volume without copying its bytes.
//
// Cleanup uses it to dead-letter a payload cheaply and atomically. A volume that
// does not implement it is handled by a read/write/delete copy instead, so the
// capability is an optimization plus an atomicity improvement, never a
// correctness requirement.
type FileMover interface {
	// MoveFile moves fromRelativePath to toRelativePath inside the volume.
	// Both paths are volume-relative and are sanitized by the volume. Moving a
	// missing source returns ErrFileNotFound.
	MoveFile(ctx context.Context, fromRelativePath string, toRelativePath string) error
}

// ShardingDepthProvider is an optional StorageVolume capability that reports the
// configured physical sharding depth.
//
// Cleanup uses it to protect shard directories from empty-directory removal. A
// volume that does not implement it falls back to structural detection.
type ShardingDepthProvider interface {
	ShardingDepth() int
}

// StatisticsRecorder records low-overhead in-process statistics deltas.
//
// Implementations must never block the caller on I/O and must be safe for
// concurrent use: every recorder is called from storage, scheduler, and watcher
// code paths at once. A recorder that is disabled drops every delta, and a nil
// recorder is treated as disabled by the components that accept one.
type StatisticsRecorder interface {
	// Record adds value to the named measurement for the time bucket that
	// contains timestamp. A zero value and an empty name are ignored. Dimensions
	// are optional; an implementation retains only the dimension keys it was
	// configured to keep.
	Record(name string, value int64, timestamp time.Time, dimensions map[string]string)
}

// StatisticsReader reads aggregated in-process statistics snapshots.
type StatisticsReader interface {
	// Snapshot aggregates the measurements whose bucket overlaps
	// [query.From, query.To) and returns them with the known totals filled in.
	// A query with To not after From returns an empty snapshot.
	Snapshot(query StatisticsQuery) StatisticsSnapshot
}

// TenantQuotaAdministrator is an optional TenantQuotaManager capability that
// administers the fallback and per-tenant file-count limits.
//
// Locus exposes these operations on its tenant quota manager; Venue keeps them
// in a separate capability so a read-only or test TenantQuotaManager
// implementation is not forced to implement limit administration. Callers that
// hold only TenantQuotaManager should type-assert:
//
//	if admin, ok := mgr.(core.TenantQuotaAdministrator); ok { ... }
type TenantQuotaAdministrator interface {
	// GetEffectiveLimit returns the limit that currently applies to a tenant:
	// the per-tenant limit when one is set, otherwise the global limit.
	// A zero limit means unlimited.
	GetEffectiveLimit(ctx context.Context, tenantID string) (int, error)

	// SetTenantLimit sets a per-tenant limit, overriding the global limit.
	// A negative limit is rejected with ErrInvalidArgument; zero means
	// unlimited.
	SetTenantLimit(ctx context.Context, tenantID string, maxCount int) error

	// RemoveTenantLimit removes a per-tenant limit so the tenant falls back to
	// the global limit. Removing a limit that was never set is not an error.
	RemoveTenantLimit(ctx context.Context, tenantID string) error

	// SetGlobalLimit sets the fallback limit for tenants without their own
	// limit. A negative limit is rejected with ErrInvalidArgument; zero means
	// unlimited.
	SetGlobalLimit(ctx context.Context, maxCount int) error

	// GetGlobalLimit returns the fallback limit (0 means unlimited).
	GetGlobalLimit(ctx context.Context) (int, error)
}

// MetadataBackupService is an optional MetadataRepository capability that writes
// a consistent online backup of the whole repository.
//
// The backup is engine-defined but self-describing: a stream produced by one
// repository can be restored into an empty repository of the same engine, which
// restores every tenant's records exactly as of the backup instant. A backup is
// taken while the repository serves reads and writes; it does not stop the
// runtime.
type MetadataBackupService interface {
	// Backup writes a consistent snapshot of the repository to w and returns the
	// engine sequence number the snapshot was taken at. It returns
	// ErrInvalidArgument for a nil writer, and any error from w is returned
	// unchanged.
	Backup(ctx context.Context, w io.Writer) (since uint64, err error)
}

// StorageVolumeHealthProbe is an optional StorageVolume capability that probes
// the volume immediately and refreshes whatever health cache the volume keeps.
//
// A plain IsHealthy call may answer from a cache; ProbeHealth is the forced
// variant a startup path uses before it decides whether a volume is usable.
type StorageVolumeHealthProbe interface {
	// ProbeHealth performs an immediate health probe, refreshes the volume's
	// cached health state, and reports whether the volume is usable.
	ProbeHealth(ctx context.Context) bool
}

// StorageVolumeWritePathWarmup is an optional StorageVolume capability that
// performs one throwaway write so the first real write does not pay for cold
// path or filesystem caches.
type StorageVolumeWritePathWarmup interface {
	// WarmWritePathCache performs a best-effort warmup write. An error means the
	// warmup did not complete; callers must treat it as advisory and not as a
	// reason to unmount or disable the volume.
	WarmWritePathCache(ctx context.Context) error
}

// StorageVolumeWritePathDiagnostics is an optional StorageVolume capability that
// exposes aggregated write-path observations for operations.
type StorageVolumeWritePathDiagnostics interface {
	// WritePathStatistics returns a snapshot of the volume's write-path
	// observations. The returned value is a copy: later writes do not mutate it.
	WritePathStatistics() StorageVolumeWritePathStatistics
}

// DatabaseOptimizationService is an optional CleanupService capability that
// reports how much space one optimization pass reclaimed.
//
// Callers that hold only CleanupService can keep using OptimizeDatabases, which
// returns aggregate counters; this capability adds the size detail.
type DatabaseOptimizationService interface {
	// OptimizeDatabasesDetailed reclaims database space and reports the reclaimed
	// byte totals.
	OptimizeDatabasesDetailed(ctx context.Context) (*DatabaseOptimizationResult, error)
}

// TenantCleanupService is an optional CleanupService capability that restricts
// one maintenance operation to a single tenant.
//
// Locus exposes the tenant-scoped variants alongside the all-tenant sweeps;
// Venue keeps them in a separate capability so an existing CleanupService
// implementation or test double remains valid without implementing them.
type TenantCleanupService interface {
	// CleanupEmptyDirectoriesForTenant removes empty directories for one tenant
	// only and does not touch another tenant's directories.
	//
	// Errors:
	// - ErrTenantNotFound if the tenant does not exist
	// - ErrInvalidArgument if the tenant ID is empty or unsafe as a path segment
	CleanupEmptyDirectoriesForTenant(ctx context.Context, tenantID string) (*CleanupStatistics, error)
}

// TenantOrphanRecoveryService is an optional OrphanRecoveryService capability
// that recovers orphaned physical files for one tenant only.
type TenantOrphanRecoveryService interface {
	// RecoverOrphanedFilesForTenant scans the volumes for physical files that
	// belong to tenantID and rebuilds their metadata.
	//
	// Errors:
	// - ErrTenantNotFound if the tenant does not exist
	// - ErrInvalidArgument if the tenant ID is empty or unsafe as a path segment
	RecoverOrphanedFilesForTenant(ctx context.Context, tenantID string) (*OrphanRecoveryReport, error)
}

// CleanupService handles cleanup of orphaned resources.
type CleanupService interface {
	// CleanupEmptyDirectories removes empty directories recursively.
	CleanupEmptyDirectories(ctx context.Context) (*CleanupStatistics, error)

	// CleanupTimedOutProcessingFiles resets files that have been in Processing status too long.
	CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*CleanupStatistics, error)

	// CleanupPermanentlyFailedFiles applies the configured permanently-failed
	// disposition to files whose retention has elapsed: keep, move to the
	// dead-letter area, or delete.
	CleanupPermanentlyFailedFiles(ctx context.Context, retention time.Duration) (*CleanupStatistics, error)

	// CleanupCompletedFiles deletes completed files after the retention period.
	CleanupCompletedFiles(ctx context.Context, retention time.Duration) (*CleanupStatistics, error)

	// CleanupOrphanedMetadata removes metadata for files that no longer exist physically.
	CleanupOrphanedMetadata(ctx context.Context) (*CleanupStatistics, error)

	// CleanupJunkFiles removes OS junk files (Thumbs.db, .DS_Store, desktop.ini)
	// from the configured storage volumes.
	CleanupJunkFiles(ctx context.Context) (*CleanupStatistics, error)

	// CleanupInvalidDatabaseBackups removes quarantined database directories
	// that exceeded their retention.
	CleanupInvalidDatabaseBackups(ctx context.Context) (*CleanupStatistics, error)

	// OptimizeDatabases triggers database space reclamation and compaction work.
	OptimizeDatabases(ctx context.Context) (*CleanupStatistics, error)

	// CumulativeStatistics returns process-lifetime totals of every cleanup
	// operation. Counters are monotonic and never reset while the runtime lives.
	CumulativeStatistics() *CleanupStatistics
}

// MetadataPage is one page of a status-ordered metadata scan.
type MetadataPage struct {
	// Records are the page entries in queue order.
	Records []*FileMetadata

	// NextCursor is an opaque continuation token. An empty value means the scan
	// is complete. Callers must pass the returned cursor back unchanged.
	NextCursor string
}

// StatusPageReader is an optional MetadataRepository capability that enumerates
// records by status in bounded pages.
//
// Long-running maintenance (cleanup, reconciliation, orphan scans) should use
// this capability when it is available so a large store is never loaded into
// memory at once. Callers that only hold the MetadataRepository interface should
// type-assert and fall back to the unbounded query otherwise:
//
//	if reader, ok := repo.(StatusPageReader); ok { ... } else { ... }
type StatusPageReader interface {
	// GetByStatusPage returns up to limit records with the given status, ordered
	// like GetPendingFiles (availability, then arrival, then file key).
	// An empty cursor starts at the beginning. It returns an empty page with an
	// empty cursor when the scan is complete.
	//
	// Errors:
	// - ErrInvalidArgument when tenantID is empty or limit is not positive
	GetByStatusPage(ctx context.Context, tenantID string, status FileProcessingStatus, cursor string, limit int) (*MetadataPage, error)
}

// MetadataRepository manages file metadata storage with caching.
type MetadataRepository interface {
	// AddOrUpdate adds or updates file metadata atomically.
	AddOrUpdate(ctx context.Context, metadata *FileMetadata) error

	// AddOrUpdateBatch adds or updates multiple file metadata atomically in a single transaction.
	// More efficient than calling AddOrUpdate multiple times for bulk operations.
	AddOrUpdateBatch(ctx context.Context, metadata []*FileMetadata) error

	// Get retrieves file metadata by key.
	//
	// Errors:
	// - ErrFileNotFound if no metadata exists for (tenantID, fileKey)
	Get(ctx context.Context, tenantID string, fileKey string) (*FileMetadata, error)

	// Delete removes file metadata.
	Delete(ctx context.Context, tenantID string, fileKey string) error

	// DeleteBatch removes multiple file metadata atomically in a single transaction.
	DeleteBatch(ctx context.Context, tenantID string, fileKeys []string) error

	// GetByStatus retrieves files by status with optional limit.
	GetByStatus(ctx context.Context, tenantID string, status FileProcessingStatus, limit int) ([]*FileMetadata, error)

	// GetPendingFiles retrieves files ready for processing (Pending status + available time passed).
	GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*FileMetadata, error)

	// UpdateStatus atomically updates file status.
	//
	// This is an administrative/repair operation: it applies the new status
	// unconditionally and performs no lease or availability checks. Public
	// operations that manage the processing queue must instead use
	// CompareAndTransitionToProcessing to claim work and CompareAndUpdateProcessing
	// to complete, fail, or recover a claimed file, because those methods verify
	// that the caller still owns the active lease.
	UpdateStatus(ctx context.Context, tenantID string, fileKey string, newStatus FileProcessingStatus) error

	// CompareAndTransitionToProcessing atomically transitions a file to Processing status
	// if and only if it is currently in Pending status.
	// Returns the updated metadata on success, or an error if:
	// - File not found
	// - File is not in Pending status
	// - Database error
	// This is a compare-and-swap operation to prevent duplicate processing.
	CompareAndTransitionToProcessing(ctx context.Context, tenantID string, fileKey string) (*FileMetadata, error)

	// CompareAndUpdateProcessing atomically updates a Processing file only when
	// its tenant, file key, and processing start time match the supplied lease.
	CompareAndUpdateProcessing(
		ctx context.Context,
		lease FileProcessingLease,
		update func(*FileMetadata) error,
	) (*FileMetadata, error)

	// GetTimedOutProcessingFiles retrieves files in Processing status that exceed timeout.
	GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*FileMetadata, error)

	// Optimize triggers repository-level garbage collection / compaction work.
	Optimize(ctx context.Context) error

	// Close closes the repository and releases all resources.
	Close() error
}

// ImportOperationRepository is the optional persistent lookup used by an
// IdempotentStoragePool. Implementations must scope operation IDs by tenant and
// remove the mapping when the owning metadata record is deleted.
type ImportOperationRepository interface {
	GetByImportOperationID(ctx context.Context, tenantID string, operationID string) (*FileMetadata, error)
}

// DirectoryQuotaRepository manages directory quota persistence.
type DirectoryQuotaRepository interface {
	// GetOrCreate retrieves directory quota or creates with defaults.
	GetOrCreate(ctx context.Context, tenantID string, directoryPath string) (*DirectoryQuota, error)

	// Update updates directory quota atomically.
	Update(ctx context.Context, tenantID string, quota *DirectoryQuota) error

	// IncrementCount atomically increments the file count.
	IncrementCount(ctx context.Context, tenantID string, directoryPath string) error

	// DecrementCount atomically decrements the file count.
	DecrementCount(ctx context.Context, tenantID string, directoryPath string) error

	// GetAll returns all directory quotas for one tenant.
	GetAll(ctx context.Context, tenantID string) ([]*DirectoryQuota, error)

	// Optimize triggers repository-level garbage collection / compaction work.
	Optimize(ctx context.Context) error

	// Close closes the repository and releases all resources.
	Close() error
}

// RetryPolicy defines the retry policy for failed file operations.
//
// Deprecated: use FileRetryPolicy. RetryPolicy duplicates it and is not
// referenced by the runtime.
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

	// MaxPostImportActionRetryCount is the maximum number of delete or move
	// attempts after storage succeeds. Zero selects the default of 5.
	MaxPostImportActionRetryCount int

	// PostImportActionRetryInitialDelay is the delay after the first failed
	// delete or move. Zero retries on the next scan.
	PostImportActionRetryInitialDelay time.Duration

	// PostImportActionRetryMaxDelay caps exponential action retry backoff. Zero
	// selects the default of 5 minutes.
	PostImportActionRetryMaxDelay time.Duration

	// PostImportAction defines what to do after successful import.
	PostImportAction PostImportAction

	// MoveToDirectory is the target directory for PostImportActionMove.
	MoveToDirectory string

	// Enabled indicates if this watcher is active.
	Enabled bool

	// AutoCreateTenantDirectoriesCacheTTL caches the tenant list used by
	// AutoCreateTenantDirectories so repeated scans do not rescan the tenant
	// store. Zero selects the runtime default (60s).
	AutoCreateTenantDirectoriesCacheTTL time.Duration

	// FileStabilityCheckDelay is the delay before the second stability probe.
	// The probe re-reads a candidate's size and modification time after this
	// delay so a file that is still being written is not imported early.
	// Zero or negative disables the delayed second probe.
	FileStabilityCheckDelay time.Duration

	// SkipStabilityCheckAfterAge skips the delayed second probe for files at
	// least this old: they can only be complete. Zero disables the shortcut.
	SkipStabilityCheckAfterAge time.Duration

	// EnableImportedFilesPruneThrottle throttles stale import-history pruning so
	// a scan does not rewrite the history file on every pass.
	EnableImportedFilesPruneThrottle bool

	// ImportedFilesPruneInterval is the minimum delay between prune runs when
	// EnableImportedFilesPruneThrottle is set. Zero selects the runtime default
	// (5 minutes).
	ImportedFilesPruneInterval time.Duration

	// EnableImportedFilesHistoryFlushDebounce coalesces import-history writes so
	// a burst of imports persists once instead of once per file.
	EnableImportedFilesHistoryFlushDebounce bool

	// ImportedFilesHistoryFlushInterval is the minimum delay between
	// import-history persistence writes when debounce is enabled. Zero selects
	// the runtime default (2 seconds).
	ImportedFilesHistoryFlushInterval time.Duration
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

	// PostImportActionsRetried is the number of previously persisted delete or
	// move actions attempted by this scan.
	PostImportActionsRetried int

	// FilesQuarantined is the number of sources suppressed after exhausting
	// their post-import action attempts.
	FilesQuarantined int

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

	// UpdateWatcher replaces the configuration of an existing watcher.
	//
	// Errors:
	// - ErrWatcherNotFound if no watcher has that ID
	// - ErrInvalidArgument if the configuration is invalid
	UpdateWatcher(ctx context.Context, config *FileWatcherConfiguration) error

	// UnregisterWatcher removes a file watcher.
	UnregisterWatcher(ctx context.Context, watcherID string) error

	// GetWatcher retrieves a watcher configuration by ID.
	GetWatcher(ctx context.Context, watcherID string) (*FileWatcherConfiguration, error)

	// GetAllWatchers retrieves all watcher configurations.
	GetAllWatchers(ctx context.Context) ([]*FileWatcherConfiguration, error)

	// GetWatchersForTenant retrieves the watchers that import for one tenant.
	// Multi-tenant watchers are not attributed to a single tenant and are
	// therefore not returned.
	GetWatchersForTenant(ctx context.Context, tenantID string) ([]*FileWatcherConfiguration, error)

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

// FileWatcherAutoManager derives per-tenant file watchers from a root
// configuration, so a multi-tenant deployment does not have to enumerate every
// tenant directory by hand.
//
// Generated watchers are ordinary watchers: they are registered through
// FileWatcher and carry the tenant ID as their WatcherID suffix. Applying a root
// configuration is idempotent, and removing every watcher only removes the
// watchers generated from roots.
type FileWatcherAutoManager interface {
	// ApplyRootConfiguration creates or updates one watcher per tenant
	// directory found under root.RootPath and returns how many watchers it
	// created or updated. When root.MultiTenantMode is false, the root itself
	// becomes a single watcher.
	ApplyRootConfiguration(ctx context.Context, root *FileWatcherRootConfiguration) (int, error)

	// DiscoverAndCreateWatchers re-applies every configured root and returns the
	// number of watchers created or updated.
	DiscoverAndCreateWatchers(ctx context.Context) (int, error)

	// RemoveAllWatchers removes every watcher that was derived from a root.
	RemoveAllWatchers(ctx context.Context) error

	// GetRootConfiguration returns the root configuration currently in use, or
	// nil when no root is configured.
	GetRootConfiguration(ctx context.Context) (*FileWatcherRootConfiguration, error)
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

	// DatabaseSizes maps a database path to its on-disk size in bytes.
	// Directories that do not exist are omitted. The map is used for
	// operational reporting only and never affects AllHealthy.
	DatabaseSizes map[string]int64

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
