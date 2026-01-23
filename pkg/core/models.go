package core

import "time"

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

	// FileStatusCompleted indicates the file was successfully processed (file deleted).
	FileStatusCompleted FileProcessingStatus = 2

	// FileStatusFailed is a legacy/unused status.
	FileStatusFailed FileProcessingStatus = 3

	// FileStatusPermanentlyFailed indicates the file exceeded max retries.
	FileStatusPermanentlyFailed FileProcessingStatus = 4
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
	default:
		return "Unknown"
	}
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

	// FileSize is the size of the file in bytes.
	FileSize int64

	// Status is the current processing status.
	Status FileProcessingStatus

	// CreatedAt is when the file was created.
	CreatedAt time.Time
}

// FileMetadata is the internal representation of file metadata stored in the database.
// This extends FileLocation with additional tracking fields.
type FileMetadata struct {
	FileKey                  string
	TenantID                 string
	VolumeID                 string
	PhysicalPath             string
	FileSize                 int64
	FileExtension            string
	OriginalFileName         string
	Status                   FileProcessingStatus
	RetryCount               int
	AvailableForProcessingAt *time.Time
	ProcessingStartTime      *time.Time
	LastFailedAt             *time.Time
	LastError                string
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// ToFileLocation converts FileMetadata to FileLocation.
func (m *FileMetadata) ToFileLocation() *FileLocation {
	return &FileLocation{
		FileKey:                  m.FileKey,
		TenantID:                 m.TenantID,
		VolumeID:                 m.VolumeID,
		PhysicalPath:             m.PhysicalPath,
		FileSize:                 m.FileSize,
		FileExtension:            m.FileExtension,
		OriginalFileName:         m.OriginalFileName,
		Status:                   m.Status,
		RetryCount:               m.RetryCount,
		AvailableForProcessingAt: m.AvailableForProcessingAt,
		ProcessingStartTime:      m.ProcessingStartTime,
		LastFailedAt:             m.LastFailedAt,
		LastError:                m.LastError,
		CreatedAt:                m.CreatedAt,
		UpdatedAt:                m.UpdatedAt,
	}
}

// ToFileInfo converts FileMetadata to FileInfo.
func (m *FileMetadata) ToFileInfo() *FileInfo {
	return &FileInfo{
		FileKey:   m.FileKey,
		FileSize:  m.FileSize,
		Status:    m.Status,
		CreatedAt: m.CreatedAt,
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

	// PermanentlyFailedFilesRemoved is the number of permanently failed files removed.
	PermanentlyFailedFilesRemoved int

	// TimedOutFilesReset is the number of timed-out files reset to pending.
	TimedOutFilesReset int

	// OrphanedMetadataRemoved is the number of orphaned metadata records removed.
	OrphanedMetadataRemoved int

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
