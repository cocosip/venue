package core

import (
	"fmt"
	"time"
)

// TenantContext represents a tenant's identity and status.
type TenantContext struct {
	// ID is the unique tenant identifier.
	ID string

	// Status is the current tenant status.
	Status TenantStatus

	// CreatedAt is when the tenant was created.
	CreatedAt time.Time
}

// IsEnabled returns true if the tenant is enabled.
func (t TenantContext) IsEnabled() bool {
	return t.Status == TenantStatusEnabled
}

// TenantStatus represents the current status of a tenant.
type TenantStatus int

const (
	// TenantStatusEnabled indicates the tenant is active and can perform operations.
	TenantStatusEnabled TenantStatus = 0

	// TenantStatusDisabled indicates the tenant is disabled and all operations will fail.
	TenantStatusDisabled TenantStatus = 1

	// TenantStatusSuspended indicates the tenant is temporarily suspended.
	TenantStatusSuspended TenantStatus = 2
)

// String returns the string representation of TenantStatus.
func (s TenantStatus) String() string {
	switch s {
	case TenantStatusEnabled:
		return "Enabled"
	case TenantStatusDisabled:
		return "Disabled"
	case TenantStatusSuspended:
		return "Suspended"
	default:
		return "Unknown"
	}
}

// FileProcessingStatus represents the current processing status of a file.
type FileProcessingStatus int

const (
	// FileStatusPending indicates the file is waiting to be processed.
	FileStatusPending FileProcessingStatus = 0

	// FileStatusProcessing indicates the file is currently being processed.
	FileStatusProcessing FileProcessingStatus = 1

	// FileStatusCompleted indicates the file was successfully processed and awaits cleanup.
	FileStatusCompleted FileProcessingStatus = 2

	// FileStatusFailed indicates processing failed but the file can be retried.
	//
	// The scheduler retries a failed file directly as Pending, so this status is
	// currently only observable on records written by an external caller or
	// restored from legacy metadata. It exists for Locus compatibility.
	FileStatusFailed FileProcessingStatus = 3

	// FileStatusPermanentlyFailed indicates the file exceeded max retries.
	FileStatusPermanentlyFailed FileProcessingStatus = 4

	// FileStatusDeleteRequested is a reserved Locus lifecycle status.
	//
	// Venue does not implement the two-phase delete: cleanup removes the
	// physical file and then the metadata record directly from Completed or
	// PermanentlyFailed. The value exists so Locus-compatible status numbers
	// stay stable, and no runtime path produces it.
	FileStatusDeleteRequested FileProcessingStatus = 5

	// FileStatusDeleteSucceeded is a reserved Locus lifecycle status.
	//
	// See FileStatusDeleteRequested: no runtime path produces this value.
	FileStatusDeleteSucceeded FileProcessingStatus = 6

	// FileStatusDeadLettered indicates that a permanently failed payload was
	// moved to the dead-letter area and released from tenant and directory
	// quotas. It is produced by cleanup when
	// PermanentlyFailedDisposition is MoveToDeadLetter (the default).
	FileStatusDeadLettered FileProcessingStatus = 7
)

// String returns the string representation of FileProcessingStatus.
func (s FileProcessingStatus) String() string {
	switch s {
	case FileStatusPending:
		return "Pending"
	case FileStatusProcessing:
		return "Processing"
	case FileStatusCompleted:
		return "Completed"
	case FileStatusFailed:
		return "Failed"
	case FileStatusPermanentlyFailed:
		return "PermanentlyFailed"
	case FileStatusDeleteRequested:
		return "DeleteRequested"
	case FileStatusDeleteSucceeded:
		return "DeleteSucceeded"
	case FileStatusDeadLettered:
		return "DeadLettered"
	default:
		return "Unknown"
	}
}

// FileProcessingLease identifies one worker's active claim on a file.
// Callers must return the lease when completing or failing processing.
type FileProcessingLease struct {
	// TenantID is the tenant that owns the leased file.
	TenantID string

	// FileKey is the leased file identifier.
	FileKey string

	// ProcessingStartTimeUTC uniquely identifies the processing attempt.
	ProcessingStartTimeUTC time.Time
}

// FileLocation contains detailed information about a file's location and status.
// This is returned by queue processing methods and diagnostics.
type FileLocation struct {
	// FileKey is the unique identifier for the file.
	FileKey string

	// TenantID is the tenant that owns this file.
	TenantID string

	// VolumeID is the storage volume where the file is located.
	VolumeID string

	// PhysicalPath is the full path to the file on the storage volume.
	PhysicalPath string

	// DirectoryPath is the normalized logical directory used for quota accounting.
	DirectoryPath string

	// FileSize is the size of the file in bytes.
	FileSize int64

	// FileExtension is the file extension (including the dot, e.g., ".pdf").
	FileExtension string

	// OriginalFileName is the original filename provided during write.
	OriginalFileName string

	// Status is the current processing status.
	Status FileProcessingStatus

	// RetryCount is the number of times processing has been retried.
	RetryCount int

	// AvailableForProcessingAt is when the file becomes available for retry (nil if immediately available).
	AvailableForProcessingAt *time.Time

	// ProcessingStartTime is when processing started (nil if not processing).
	ProcessingStartTime *time.Time

	// CompletedAt is when processing completed successfully.
	CompletedAt *time.Time

	// Lease identifies the active processing claim, if one exists.
	Lease *FileProcessingLease

	// LastFailedAt is when the last failure occurred.
	LastFailedAt *time.Time

	// LastError is the error message from the last failure.
	LastError string

	// CreatedAt is when the file was created.
	CreatedAt time.Time

	// UpdatedAt is when the file metadata was last updated.
	UpdatedAt time.Time
}

// FileInfo contains basic information about a file.
// This is a lightweight version of FileLocation for simple queries.
type FileInfo struct {
	// FileKey is the unique identifier for the file.
	FileKey string

	// TenantID is the tenant that owns the file.
	TenantID string

	// FileSize is the size of the file in bytes.
	FileSize int64

	// Status is the current processing status.
	Status FileProcessingStatus

	// RetryCount is how many times processing has been retried.
	RetryCount int

	// CreatedAt is when the file was created.
	CreatedAt time.Time
}

// FileMetadata is the internal representation of file metadata stored in the database.
// This extends FileLocation with additional tracking fields.
type FileMetadata struct {
	FileKey  string
	TenantID string
	// ImportOperationID identifies a caller's logical write for durable,
	// tenant-scoped idempotency. Empty means the write was not idempotent.
	ImportOperationID        string
	VolumeID                 string
	PhysicalPath             string
	DirectoryPath            string
	FileSize                 int64
	FileExtension            string
	OriginalFileName         string
	Status                   FileProcessingStatus
	RetryCount               int
	AvailableForProcessingAt *time.Time
	ProcessingStartTime      *time.Time
	CompletedAt              *time.Time

	// ReleasedProcessingStartTimeUTC identifies the processing lease that was
	// released most recently. It lets a repeated release of the same lease
	// (for example a worker retrying a completion call) succeed as a no-op,
	// while a lease superseded by a newer claim is still rejected.
	ReleasedProcessingStartTimeUTC *time.Time

	LastFailedAt *time.Time
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time

	// DeadLetteredAt is when a permanently failed payload was moved to the
	// dead-letter area. PhysicalPath then points at the dead-letter location,
	// and the record no longer counts toward tenant or directory quotas.
	DeadLetteredAt *time.Time
}

// ToFileLocation converts FileMetadata to FileLocation.
func (m *FileMetadata) ToFileLocation() *FileLocation {
	var lease *FileProcessingLease
	if m.ProcessingStartTime != nil {
		lease = &FileProcessingLease{
			TenantID:               m.TenantID,
			FileKey:                m.FileKey,
			ProcessingStartTimeUTC: *m.ProcessingStartTime,
		}
	}

	return &FileLocation{
		FileKey:                  m.FileKey,
		TenantID:                 m.TenantID,
		VolumeID:                 m.VolumeID,
		PhysicalPath:             m.PhysicalPath,
		DirectoryPath:            m.DirectoryPath,
		FileSize:                 m.FileSize,
		FileExtension:            m.FileExtension,
		OriginalFileName:         m.OriginalFileName,
		Status:                   m.Status,
		RetryCount:               m.RetryCount,
		AvailableForProcessingAt: m.AvailableForProcessingAt,
		ProcessingStartTime:      m.ProcessingStartTime,
		CompletedAt:              m.CompletedAt,
		Lease:                    lease,
		LastFailedAt:             m.LastFailedAt,
		LastError:                m.LastError,
		CreatedAt:                m.CreatedAt,
		UpdatedAt:                m.UpdatedAt,
	}
}

// ToFileInfo converts FileMetadata to FileInfo.
func (m *FileMetadata) ToFileInfo() *FileInfo {
	return &FileInfo{
		FileKey:    m.FileKey,
		TenantID:   m.TenantID,
		FileSize:   m.FileSize,
		Status:     m.Status,
		RetryCount: m.RetryCount,
		CreatedAt:  m.CreatedAt,
	}
}

// DirectoryQuota represents quota configuration for a directory.
type DirectoryQuota struct {
	// DirectoryPath is the unique identifier for the directory.
	DirectoryPath string

	// CurrentCount is the current number of files in the directory.
	CurrentCount int

	// MaxCount is the maximum allowed files (0 = unlimited).
	MaxCount int

	// Enabled indicates whether quota enforcement is enabled.
	Enabled bool

	// CreatedAt is when the quota was created.
	CreatedAt time.Time

	// UpdatedAt is when the quota was last updated.
	UpdatedAt time.Time
}

// IsUnlimited returns true if the quota is unlimited (MaxCount = 0).
func (q *DirectoryQuota) IsUnlimited() bool {
	return q.MaxCount == 0
}

// CanAddFile returns true if a file can be added without exceeding quota.
func (q *DirectoryQuota) CanAddFile() bool {
	if !q.Enabled || q.IsUnlimited() {
		return true
	}
	return q.CurrentCount < q.MaxCount
}

// PermanentlyFailedDisposition defines what cleanup does with a permanently
// failed file once its retention period elapses.
type PermanentlyFailedDisposition int

const (
	// PermanentlyFailedKeep leaves the payload and its metadata in place for
	// manual intervention. It keeps consuming quota.
	PermanentlyFailedKeep PermanentlyFailedDisposition = iota

	// PermanentlyFailedMoveToDeadLetter moves the payload into the dead-letter
	// area, records DeadLetteredAt, and releases its quota. This is the Locus
	// default.
	PermanentlyFailedMoveToDeadLetter

	// PermanentlyFailedDelete deletes the payload and its metadata.
	PermanentlyFailedDelete
)

// String returns the configuration spelling of the disposition.
func (d PermanentlyFailedDisposition) String() string {
	switch d {
	case PermanentlyFailedKeep:
		return "Keep"
	case PermanentlyFailedMoveToDeadLetter:
		return "MoveToDeadLetter"
	case PermanentlyFailedDelete:
		return "Delete"
	default:
		return "Unknown"
	}
}

// ParsePermanentlyFailedDisposition converts a configuration value to a
// disposition. An empty value selects the Locus default
// (PermanentlyFailedMoveToDeadLetter); an unrecognized value is reported as an
// error so a typo cannot silently change how failed payloads are handled.
func ParsePermanentlyFailedDisposition(value string) (PermanentlyFailedDisposition, error) {
	switch value {
	case "":
		return PermanentlyFailedMoveToDeadLetter, nil
	case "Keep":
		return PermanentlyFailedKeep, nil
	case "MoveToDeadLetter":
		return PermanentlyFailedMoveToDeadLetter, nil
	case "Delete":
		return PermanentlyFailedDelete, nil
	default:
		return PermanentlyFailedMoveToDeadLetter, fmt.Errorf("unknown permanently failed disposition %q: %w", value, ErrInvalidArgument)
	}
}

// RetiredVolumeDisposition defines how metadata that references an explicitly
// retired volume is handled.
type RetiredVolumeDisposition int

const (
	// RetiredVolumeKeep keeps the metadata and keeps logging skipped cleanup.
	RetiredVolumeKeep RetiredVolumeDisposition = iota

	// RetiredVolumePurgeMetadataOnly removes metadata and quota rows without
	// touching physical storage.
	RetiredVolumePurgeMetadataOnly
)

// String returns the configuration spelling of the disposition.
func (d RetiredVolumeDisposition) String() string {
	switch d {
	case RetiredVolumeKeep:
		return "Keep"
	case RetiredVolumePurgeMetadataOnly:
		return "PurgeMetadataOnly"
	default:
		return "Unknown"
	}
}

// ParseRetiredVolumeDisposition converts a configuration value to a retired
// volume disposition. An empty value selects the default (RetiredVolumeKeep).
func ParseRetiredVolumeDisposition(value string) (RetiredVolumeDisposition, error) {
	switch value {
	case "":
		return RetiredVolumeKeep, nil
	case "Keep":
		return RetiredVolumeKeep, nil
	case "PurgeMetadataOnly":
		return RetiredVolumePurgeMetadataOnly, nil
	default:
		return RetiredVolumeKeep, fmt.Errorf("unknown retired volume disposition %q: %w", value, ErrInvalidArgument)
	}
}

// FileRetryPolicy defines the retry behavior for failed file processing.
type FileRetryPolicy struct {
	// MaxRetryCount is the maximum number of retries before permanent failure.
	MaxRetryCount int

	// InitialRetryDelay is the delay before the first retry.
	InitialRetryDelay time.Duration

	// UseExponentialBackoff enables exponential backoff for retry delays.
	UseExponentialBackoff bool

	// MaxRetryDelay is the maximum delay between retries.
	MaxRetryDelay time.Duration
}

// DefaultFileRetryPolicy returns the default retry policy.
func DefaultFileRetryPolicy() *FileRetryPolicy {
	return &FileRetryPolicy{
		MaxRetryCount:         3,
		InitialRetryDelay:     5 * time.Second,
		UseExponentialBackoff: true,
		MaxRetryDelay:         5 * time.Minute,
	}
}

// CalculateRetryDelay calculates the delay before the next retry.
func (p *FileRetryPolicy) CalculateRetryDelay(retryCount int) time.Duration {
	if !p.UseExponentialBackoff {
		return p.InitialRetryDelay
	}

	// Exponential backoff: delay = InitialDelay * 2^(retryCount-1)
	delay := p.InitialRetryDelay
	for i := 1; i < retryCount; i++ {
		delay *= 2
		if delay > p.MaxRetryDelay {
			return p.MaxRetryDelay
		}
	}

	if delay > p.MaxRetryDelay {
		return p.MaxRetryDelay
	}

	return delay
}

// CleanupStatistics tracks cleanup operation results.
type CleanupStatistics struct {
	// EmptyDirectoriesRemoved is the number of empty directories removed.
	EmptyDirectoriesRemoved int

	// CompletedRecordsRemoved is the number of completed files and metadata records removed.
	CompletedRecordsRemoved int

	// PermanentlyFailedFilesRemoved is the number of permanently failed files removed.
	PermanentlyFailedFilesRemoved int

	// DeadLetteredFiles is the number of permanently failed files moved to the
	// dead-letter area instead of being deleted.
	DeadLetteredFiles int

	// JunkFilesRemoved is the number of OS junk files removed from storage
	// volumes (for example Thumbs.db, .DS_Store, desktop.ini).
	JunkFilesRemoved int

	// InvalidDatabaseBackupsRemoved is the number of expired quarantined
	// database directories removed.
	InvalidDatabaseBackupsRemoved int

	// TimedOutFilesReset is the number of timed-out files reset to pending.
	TimedOutFilesReset int

	// OrphanedMetadataRemoved is the number of orphaned metadata records removed.
	OrphanedMetadataRemoved int

	// MetadataDatabasesOptimized is the number of metadata databases optimized.
	MetadataDatabasesOptimized int

	// QuotaDatabasesOptimized is the number of quota databases optimized.
	QuotaDatabasesOptimized int

	// SpaceFreed is the total space freed in bytes.
	SpaceFreed int64
}

// TenantMetadata stores persistent tenant information.
type TenantMetadata struct {
	// TenantID is the unique tenant identifier.
	TenantID string

	// Status is the current tenant status.
	Status TenantStatus

	// StoragePath is the root storage path for this tenant.
	StoragePath string

	// CreatedAt is when the tenant was created.
	CreatedAt time.Time

	// UpdatedAt is when the tenant was last updated.
	UpdatedAt time.Time
}

// ToTenantContext converts TenantMetadata to TenantContext.
func (m *TenantMetadata) ToTenantContext() TenantContext {
	return TenantContext{
		ID:        m.TenantID,
		Status:    m.Status,
		CreatedAt: m.CreatedAt,
	}
}

// FileWatcherRootConfiguration is a template for deriving one watcher per tenant
// directory under RootPath.
//
// It carries the same import settings as FileWatcherConfiguration but no watcher
// ID: the generated watcher ID and tenant ID come from the directory name.
type FileWatcherRootConfiguration struct {
	// RootPath is the directory whose immediate subdirectories are tenants.
	RootPath string

	// MultiTenantMode treats each immediate subdirectory of RootPath as a
	// tenant. When false, RootPath itself becomes a single-tenant watcher.
	MultiTenantMode bool

	// Enabled applies to every generated watcher.
	Enabled bool

	// IncludeSubdirectories applies to every generated watcher.
	IncludeSubdirectories bool

	// FilePatterns filters imported files.
	FilePatterns []string

	// PostImportAction applies after a successful import.
	PostImportAction PostImportAction

	// MoveToDirectory is the target directory for PostImportActionMove.
	MoveToDirectory string

	// PollingInterval is the scan interval for generated watchers.
	PollingInterval time.Duration

	// MaxFileSizeBytes limits the imported file size; 0 means unlimited.
	MaxFileSizeBytes int64

	// MinFileAge is the minimum file age before import.
	MinFileAge time.Duration

	// MaxConcurrentImports limits concurrent imports per generated watcher.
	MaxConcurrentImports int

	// MaxPostImportActionRetryCount limits delete or move attempts after import.
	MaxPostImportActionRetryCount int

	// PostImportActionRetryInitialDelay is the first action retry delay.
	PostImportActionRetryInitialDelay time.Duration

	// PostImportActionRetryMaxDelay caps exponential action retry backoff.
	PostImportActionRetryMaxDelay time.Duration

	// AutoCreateTenantDirectoriesCacheTTL caches the tenant list used by
	// automatic tenant-directory creation. Zero selects the runtime default.
	AutoCreateTenantDirectoriesCacheTTL time.Duration

	// FileStabilityCheckDelay is the delay before the second stability probe.
	// Zero selects the runtime default; a negative value disables the probe.
	FileStabilityCheckDelay time.Duration

	// SkipStabilityCheckAfterAge skips the second stability probe for older
	// files. Zero selects the runtime default; a negative value always probes.
	SkipStabilityCheckAfterAge time.Duration

	// EnableImportedFilesPruneThrottle throttles stale import-history pruning.
	EnableImportedFilesPruneThrottle bool

	// ImportedFilesPruneInterval is the minimum delay between prune runs while
	// the throttle is enabled. Zero selects the runtime default.
	ImportedFilesPruneInterval time.Duration

	// EnableImportedFilesHistoryFlushDebounce coalesces import-history writes.
	EnableImportedFilesHistoryFlushDebounce bool

	// ImportedFilesHistoryFlushInterval is the minimum delay between
	// import-history writes while the debounce is enabled. Zero selects the
	// runtime default.
	ImportedFilesHistoryFlushInterval time.Duration
}

// FileWatcherServiceOptions configures the background file watcher service
// globally, independently of the individual watchers.
type FileWatcherServiceOptions struct {
	// Enabled controls whether the background service scans at all.
	Enabled bool

	// DefaultPollingInterval is used when a watcher does not set one.
	DefaultPollingInterval time.Duration

	// MinimumPollingInterval clamps short watcher intervals.
	MinimumPollingInterval time.Duration

	// MaximumPollingInterval clamps long watcher intervals.
	MaximumPollingInterval time.Duration

	// DisabledCheckInterval is how often a globally disabled service rechecks
	// whether it should resume.
	DisabledCheckInterval time.Duration

	// MaxParallelWatcherScans bounds how many watchers one cycle scans in
	// parallel.
	MaxParallelWatcherScans int
}

// VolumeInfo contains information about a storage volume.
type VolumeInfo struct {
	// VolumeID is the unique volume identifier.
	VolumeID string

	// MountPath is the root path where the volume is mounted.
	MountPath string

	// IsHealthy indicates if the volume is healthy.
	IsHealthy bool

	// TotalCapacity is the total capacity in bytes.
	TotalCapacity int64

	// AvailableSpace is the available space in bytes.
	AvailableSpace int64
}

// Stable in-process statistics measurement names.
//
// The names are part of the observable statistics contract: recorders,
// dimension filters, and downstream consumers all key on them. The write
// throughput name is derived by a reader from the write-byte total and the
// queried duration, so it is never recorded directly.
const (
	// StatisticStorageWriteSuccessCount counts files written successfully.
	StatisticStorageWriteSuccessCount = "storage.write.success.count"

	// StatisticStorageWriteBytes counts bytes written successfully.
	StatisticStorageWriteBytes = "storage.write.bytes"

	// StatisticStorageWriteMegabytesPerSecond is the derived write throughput.
	StatisticStorageWriteMegabytesPerSecond = "storage.write.megabytes_per_second"

	// StatisticStorageFileDequeuedCount counts files claimed for processing.
	StatisticStorageFileDequeuedCount = "storage.file.dequeued.count"

	// StatisticStorageFileReadCount counts files read directly by a caller.
	StatisticStorageFileReadCount = "storage.file.read.count"

	// StatisticStorageFileCompletedCount counts files marked completed.
	StatisticStorageFileCompletedCount = "storage.file.completed.count"

	// StatisticMetadataPersistedBatchCount counts committed metadata batches.
	StatisticMetadataPersistedBatchCount = "metadata.persisted.batch.count"

	// StatisticMetadataPersistedOperationCount counts metadata operations
	// persisted by committed batches.
	StatisticMetadataPersistedOperationCount = "metadata.persisted.operation.count"

	// StatisticWatcherScanCount counts watcher scan cycles.
	StatisticWatcherScanCount = "watcher.scan.count"

	// StatisticWatcherFilesDiscovered counts files discovered by scans.
	StatisticWatcherFilesDiscovered = "watcher.files.discovered"

	// StatisticWatcherFilesImported counts files imported by scans.
	StatisticWatcherFilesImported = "watcher.files.imported"

	// StatisticWatcherFilesSkipped counts files skipped by scans.
	StatisticWatcherFilesSkipped = "watcher.files.skipped"

	// StatisticWatcherFilesFailed counts files that failed to import.
	StatisticWatcherFilesFailed = "watcher.files.failed"

	// StatisticWatcherBytesImported counts bytes imported by scans.
	StatisticWatcherBytesImported = "watcher.bytes.imported"
)

// Stable statistics dimension keys. A recorder retains a dimension only when it
// was configured to keep it.
const (
	// StatisticsDimensionTenantID is the tenant dimension key.
	StatisticsDimensionTenantID = "tenant_id"

	// StatisticsDimensionVolumeID is the volume dimension key.
	StatisticsDimensionVolumeID = "volume_id"

	// StatisticsDimensionWatcherID is the watcher dimension key.
	StatisticsDimensionWatcherID = "watcher_id"

	// StatisticsDimensionOperation is the operation dimension key.
	StatisticsDimensionOperation = "operation"
)

// StatisticsQuery selects the time range and dimension filters of a snapshot.
//
// From is inclusive and To is exclusive. Buckets are summed when they overlap
// the range. A query with To not after From returns an empty snapshot. An empty
// filter string matches every value for that dimension.
type StatisticsQuery struct {
	// From is the inclusive lower bound of the queried range.
	From time.Time

	// To is the exclusive upper bound of the queried range.
	To time.Time

	// TenantID optionally filters by tenant.
	TenantID string

	// VolumeID optionally filters by volume.
	VolumeID string

	// WatcherID optionally filters by watcher.
	WatcherID string

	// Operation optionally filters by operation.
	Operation string
}

// StatisticsMeasurement is one aggregated statistics measurement.
type StatisticsMeasurement struct {
	// Name is the measurement name, for example
	// StatisticStorageWriteBytes.
	Name string

	// Value is the aggregated value over the queried range.
	Value float64

	// Dimensions contains the retained dimension keys and values for this
	// measurement. It is never nil and may be empty.
	Dimensions map[string]string
}

// StatisticsSnapshot is the aggregated statistics of a queried range.
//
// A snapshot is an immutable value returned by a reader: callers may retain it
// after the runtime moves on.
type StatisticsSnapshot struct {
	// WriteFileCount is the number of successful file writes.
	WriteFileCount int64

	// WriteBytes is the number of bytes written.
	WriteBytes int64

	// WriteMegabytesPerSecond is the write throughput in MiB/s over the queried
	// range. It is zero when the range is empty.
	WriteMegabytesPerSecond float64

	// DequeuedFileCount is the number of files claimed for processing.
	DequeuedFileCount int64

	// ReadFileCount is the number of files read directly.
	ReadFileCount int64

	// CompletedFileCount is the number of files marked completed.
	CompletedFileCount int64

	// MetadataPersistedOperationCount is the number of metadata operations
	// persisted by committed batches.
	MetadataPersistedOperationCount int64

	// WatcherImportedFileCount is the number of files imported by watchers.
	WatcherImportedFileCount int64

	// WatcherImportedBytes is the number of bytes imported by watchers.
	WatcherImportedBytes int64

	// Measurements are every aggregated measurement in the range, including the
	// derived throughput measurement. It is never nil.
	Measurements []StatisticsMeasurement
}

// MetadataBackupInfo describes the newest backup of a metadata database.
//
// Venue's recoverability guarantee is backup-period granularity rather than
// Locus's per-event queue journal, so this type is the observable "closest
// recoverable point" of a runtime.
type MetadataBackupInfo struct {
	// BackupDirectory is the directory that was inspected.
	BackupDirectory string

	// LatestBackupPath is the newest backup file, or empty when none exists.
	LatestBackupPath string

	// LatestBackupAt is when the newest backup was written.
	LatestBackupAt time.Time

	// BackupCount is how many backup files were found.
	BackupCount int

	// TotalBytes is the combined size of the backup files in bytes.
	TotalBytes int64
}

// DatabaseOptimizationResult reports what one database optimization pass
// reclaimed.
//
// It is the detailed counterpart of the aggregate CleanupStatistics counters:
// the counters say how many databases were optimized, this result says how much
// space that freed.
type DatabaseOptimizationResult struct {
	// MetadataDatabasesOptimized is the number of metadata databases optimized.
	MetadataDatabasesOptimized int

	// QuotaDatabasesOptimized is the number of quota databases optimized.
	QuotaDatabasesOptimized int

	// SpaceReclaimed is the total space reclaimed in bytes.
	SpaceReclaimed int64

	// SizeBefore is the combined on-disk size before optimization, in bytes.
	SizeBefore int64

	// SizeAfter is the combined on-disk size after optimization, in bytes.
	SizeAfter int64
}

// SpaceReclaimedMegabytes returns SpaceReclaimed in MiB.
func (r DatabaseOptimizationResult) SpaceReclaimedMegabytes() float64 {
	return float64(r.SpaceReclaimed) / 1024 / 1024
}

// PercentageReclaimed returns the share of the pre-optimization size that was
// reclaimed, in percent. It is zero when SizeBefore is not positive.
func (r DatabaseOptimizationResult) PercentageReclaimed() float64 {
	if r.SizeBefore <= 0 {
		return 0
	}
	return float64(r.SpaceReclaimed) * 100 / float64(r.SizeBefore)
}

// StorageVolumeWritePathStatistics is a point-in-time snapshot of one volume's
// write-path observations.
//
// Locus reports a .NET-specific mix of source-stream kinds (MemoryStream fast
// paths, seekable FileStream copies, and so on) that has no Go analogue. Venue
// reports the mechanism-independent observations of its own write path, so the
// snapshot answers the same operational question ("where is write time going,
// and how much did this volume actually write?") without inventing counters for
// stream types Go does not have.
type StorageVolumeWritePathStatistics struct {
	// TotalWrites is the number of successful writes.
	TotalWrites int64

	// TotalBytes is the number of bytes written successfully.
	TotalBytes int64

	// FailedWrites is the number of writes that returned an error.
	FailedWrites int64

	// DirectoryPreparationCount is the number of observed directory-preparation
	// phases (creating the destination directory tree for a write).
	DirectoryPreparationCount int64

	// DirectoryPreparationDuration is the total time spent preparing directories.
	DirectoryPreparationDuration time.Duration

	// CopyOperationCount is the number of observed payload copy operations.
	CopyOperationCount int64

	// CopyDuration is the total time spent copying payload bytes into the
	// destination file.
	CopyDuration time.Duration

	// FsyncCount is the number of observed fsync calls.
	FsyncCount int64

	// FsyncDuration is the total time spent in fsync calls.
	FsyncDuration time.Duration
}
