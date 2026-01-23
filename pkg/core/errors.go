package core

import "errors"

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

// ErrNoFilesAvailable is returned when no files are available for processing.
var ErrNoFilesAvailable = errors.New("no files available for processing")

// ErrInvalidFileKey is returned when a file key is invalid.
var ErrInvalidFileKey = errors.New("invalid file key")

// ErrFileAlreadyExists is returned when attempting to create a file that already exists.
var ErrFileAlreadyExists = errors.New("file already exists")

// General errors

// ErrInvalidArgument is returned when an argument is invalid.
var ErrInvalidArgument = errors.New("invalid argument")

// ErrOperationCanceled is returned when an operation is canceled via context.
var ErrOperationCanceled = errors.New("operation canceled")

// ErrDatabaseError is returned when a database operation fails.
var ErrDatabaseError = errors.New("database error")

// ErrPathTraversalAttempt is returned when a path traversal attempt is detected.
var ErrPathTraversalAttempt = errors.New("path traversal attempt detected")
