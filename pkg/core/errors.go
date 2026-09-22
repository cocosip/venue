package core

import (
	"errors"
	"fmt"
	"time"
)

// Tenant-related errors

// ErrTenantNotFound is returned when a tenant doesn't exist.
var ErrTenantNotFound = errors.New("tenant not found")

// ErrTenantDisabled is returned when attempting to operate on a disabled tenant.
var ErrTenantDisabled = errors.New("tenant is disabled")

// ErrTenantSuspended is returned when attempting to operate on a suspended tenant.
var ErrTenantSuspended = errors.New("tenant is suspended")

// ErrTenantAlreadyExists is returned when attempting to create a tenant that already exists.
var ErrTenantAlreadyExists = errors.New("tenant already exists")

// Quota-related errors

// ErrTenantQuotaExceeded is returned when tenant-level file quota is exceeded.
var ErrTenantQuotaExceeded = errors.New("tenant quota exceeded")

// ErrDirectoryQuotaExceeded is returned when directory-level file quota is exceeded.
var ErrDirectoryQuotaExceeded = errors.New("directory quota exceeded")

// Storage-related errors

// ErrInsufficientStorage is returned when no storage volumes have available space.
var ErrInsufficientStorage = errors.New("insufficient storage space")

// ErrStorageVolumeUnavailable is returned when a storage volume is not healthy or not mounted.
var ErrStorageVolumeUnavailable = errors.New("storage volume unavailable")

// ErrVolumeAlreadyMounted is returned when attempting to mount a volume that's already mounted.
var ErrVolumeAlreadyMounted = errors.New("volume already mounted")

// File-related errors

// ErrFileNotFound is returned when a file doesn't exist.
var ErrFileNotFound = errors.New("file not found")

// ErrFileAlreadyProcessing is returned when attempting to process a file that's already processing.
var ErrFileAlreadyProcessing = errors.New("file is already being processed")

// ErrNoFilesAvailable is retained for compatibility.
//
// Deprecated: the runtime no longer returns this error. Queue allocation
// reports an empty queue as (nil, nil) and an empty slice respectively; use
// ErrFileNotClaimable to detect that a specific candidate could not be claimed.
var ErrNoFilesAvailable = errors.New("no files available for processing")

// ErrWatcherNotFound is returned when a file watcher ID is unknown.
var ErrWatcherNotFound = errors.New("file watcher not found")

// ErrInvalidFileKey is returned when a file key is invalid.
var ErrInvalidFileKey = errors.New("invalid file key")

// ErrFileAlreadyExists is returned when attempting to create a file that already exists.
var ErrFileAlreadyExists = errors.New("file already exists")

// ErrFileNotClaimable is returned when a candidate file cannot be claimed
// because it is no longer Pending or is not yet available for processing.
//
// It classifies contention: a competing worker or an unfinished backoff window
// owns the candidate, so a caller such as the file scheduler can continue with
// the next candidate instead of aborting. Infrastructure failures are reported
// separately (see ErrDatabaseError) and must not be swallowed as contention.
var ErrFileNotClaimable = errors.New("file is not claimable")

// ErrProcessingLeaseMismatch is returned when a processing transition uses a stale lease.
var ErrProcessingLeaseMismatch = errors.New("processing lease mismatch")

// FileProcessingLeaseMismatchError describes the active state that rejected a lease.
type FileProcessingLeaseMismatchError struct {
	TenantID                       string
	FileKey                        string
	ExpectedProcessingStartTimeUTC time.Time
	ActualProcessingStartTimeUTC   *time.Time
	ActualStatus                   *FileProcessingStatus
}

// Error returns a diagnostic description of the rejected processing lease.
func (e *FileProcessingLeaseMismatchError) Error() string {
	actualStatus := "Unknown"
	if e.ActualStatus != nil {
		actualStatus = e.ActualStatus.String()
	}
	if e.ActualProcessingStartTimeUTC != nil {
		return fmt.Sprintf(
			"processing lease for tenant %q file %q no longer matches: expected start %s, active start %s, actual status %s",
			e.TenantID,
			e.FileKey,
			e.ExpectedProcessingStartTimeUTC.Format(time.RFC3339Nano),
			e.ActualProcessingStartTimeUTC.Format(time.RFC3339Nano),
			actualStatus,
		)
	}
	return fmt.Sprintf(
		"processing lease for tenant %q file %q no longer matches active metadata: expected start %s, actual status %s",
		e.TenantID,
		e.FileKey,
		e.ExpectedProcessingStartTimeUTC.Format(time.RFC3339Nano),
		actualStatus,
	)
}

// Unwrap supports errors.Is with ErrProcessingLeaseMismatch.
func (e *FileProcessingLeaseMismatchError) Unwrap() error {
	return ErrProcessingLeaseMismatch
}

// General errors

// ErrInvalidArgument is returned when an argument is invalid.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrOperationCanceled is returned when an operation is canceled via context.
var ErrOperationCanceled = errors.New("operation canceled")

// ErrDatabaseError is returned when a database operation fails.
var ErrDatabaseError = errors.New("database error")

// ErrPathTraversalAttempt is returned when a path traversal attempt is detected.
var ErrPathTraversalAttempt = errors.New("path traversal attempt detected")
