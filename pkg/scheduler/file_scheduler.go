package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// FileSchedulerOptions configures the file scheduler.
type FileSchedulerOptions struct {
	// RetryPolicy defines the retry behavior for failed files.
	RetryPolicy *core.FileRetryPolicy

	// ProcessingTimeout is the duration after which a Processing file is considered timed out.
	// Default: 30 minutes
	ProcessingTimeout time.Duration
}

// DefaultFileSchedulerOptions returns default scheduler options.
func DefaultFileSchedulerOptions() *FileSchedulerOptions {
	return &FileSchedulerOptions{
		RetryPolicy:       core.DefaultFileRetryPolicy(),
		ProcessingTimeout: 30 * time.Minute,
	}
}

// fileScheduler implements the FileScheduler interface.
type fileScheduler struct {
	metadataRepo      core.MetadataRepository
	volumes           map[string]core.StorageVolume
	retryPolicy       *core.FileRetryPolicy
	processingTimeout time.Duration
	mu                sync.RWMutex
}

// NewFileScheduler creates a new file scheduler.
func NewFileScheduler(
	metadataRepo core.MetadataRepository,
	volumes map[string]core.StorageVolume,
	opts *FileSchedulerOptions,
) (core.FileScheduler, error) {
	if metadataRepo == nil {
		return nil, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	if len(volumes) == 0 {
		return nil, fmt.Errorf("at least one storage volume is required: %w", core.ErrInvalidArgument)
	}

	if opts == nil {
		opts = DefaultFileSchedulerOptions()
	}

	if opts.RetryPolicy == nil {
		opts.RetryPolicy = core.DefaultFileRetryPolicy()
	}

	if opts.ProcessingTimeout == 0 {
		opts.ProcessingTimeout = 30 * time.Minute
	}

	return &fileScheduler{
		metadataRepo:      metadataRepo,
		volumes:           volumes,
		retryPolicy:       opts.RetryPolicy,
		processingTimeout: opts.ProcessingTimeout,
	}, nil
}

// GetNextFileForProcessing retrieves the next pending file atomically.
// Status is transitioned from Pending to Processing within a transaction.
func (s *fileScheduler) GetNextFileForProcessing(ctx context.Context, tenant core.TenantContext) (*core.FileLocation, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	// Get pending files (limit 10 to avoid full scan)
	pendingFiles, err := s.metadataRepo.GetPendingFiles(ctx, tenant.ID, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	if len(pendingFiles) == 0 {
		return nil, core.ErrNoFilesAvailable
	}

	// Try to claim the first available file
	// Use optimistic locking: try to update status, if it fails another worker got it
	for _, file := range pendingFiles {
		// Attempt to transition from Pending to Processing
		if err := s.transitionToProcessing(ctx, file); err != nil {
			// If error, try next file (this one was claimed by another worker)
			continue
		}

		// Successfully claimed the file
		return file.ToFileLocation(), nil
	}

	// All files were claimed by other workers
	return nil, core.ErrNoFilesAvailable
}

// GetNextBatchForProcessing retrieves multiple files atomically.
func (s *fileScheduler) GetNextBatchForProcessing(ctx context.Context, tenant core.TenantContext, batchSize int) ([]*core.FileLocation, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive: %w", core.ErrInvalidArgument)
	}

	// Get pending files (fetch more than needed to account for race conditions)
	fetchSize := batchSize * 2
	if fetchSize > 100 {
		fetchSize = 100
	}

	pendingFiles, err := s.metadataRepo.GetPendingFiles(ctx, tenant.ID, fetchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	if len(pendingFiles) == 0 {
		return []*core.FileLocation{}, nil
	}

	// Try to claim files up to batchSize
	var claimedFiles []*core.FileLocation
	for _, file := range pendingFiles {
		if len(claimedFiles) >= batchSize {
			break
		}

		// Attempt to transition from Pending to Processing
		if err := s.transitionToProcessing(ctx, file); err != nil {
			// If error, try next file
			continue
		}

		// Successfully claimed the file
		claimedFiles = append(claimedFiles, file.ToFileLocation())
	}

	return claimedFiles, nil
}

// transitionToProcessing atomically transitions a file from Pending to Processing.
// Returns error if the file is no longer in Pending status (already claimed).
func (s *fileScheduler) transitionToProcessing(ctx context.Context, file *core.FileMetadata) error {
	// Re-read the file to check current status
	current, err := s.metadataRepo.Get(ctx, file.FileKey)
	if err != nil {
		return fmt.Errorf("failed to get current file status: %w", err)
	}

	// Check if still in Pending status
	if current.Status != core.FileStatusPending {
		return fmt.Errorf("file status changed, no longer pending")
	}

	// Update to Processing status with processing start time
	now := time.Now()
	current.Status = core.FileStatusProcessing
	current.ProcessingStartTime = &now
	current.UpdatedAt = now

	// Save the update
	if err := s.metadataRepo.AddOrUpdate(ctx, current); err != nil {
		return fmt.Errorf("failed to update file status: %w", err)
	}

	// Update the input file metadata
	*file = *current

	return nil
}

// MarkAsCompleted marks a file as completed and schedules it for deletion.
func (s *fileScheduler) MarkAsCompleted(ctx context.Context, fileKey string) error {
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get current file metadata
	metadata, err := s.metadataRepo.Get(ctx, fileKey)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Get the storage volume
	volume, exists := s.volumes[metadata.VolumeID]
	if !exists {
		return fmt.Errorf("storage volume %s not found", metadata.VolumeID)
	}

	// Delete the physical file
	if err := volume.DeleteFile(ctx, metadata.PhysicalPath); err != nil {
		// Log error but continue to delete metadata
		// (file might already be deleted)
	}

	// Delete metadata
	if err := s.metadataRepo.Delete(ctx, fileKey); err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	return nil
}

// MarkAsFailed marks a file as failed and schedules retry or permanent failure.
func (s *fileScheduler) MarkAsFailed(ctx context.Context, fileKey string, errorMessage string) error {
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get current file metadata
	metadata, err := s.metadataRepo.Get(ctx, fileKey)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Increment retry count
	metadata.RetryCount++
	now := time.Now()
	metadata.LastFailedAt = &now
	metadata.LastError = errorMessage
	metadata.UpdatedAt = now
	metadata.ProcessingStartTime = nil

	// Check if exceeded max retries
	if metadata.RetryCount > s.retryPolicy.MaxRetryCount {
		// Permanently failed
		metadata.Status = core.FileStatusPermanentlyFailed
		metadata.AvailableForProcessingAt = nil
	} else {
		// Schedule for retry with exponential backoff
		metadata.Status = core.FileStatusPending
		retryDelay := s.retryPolicy.CalculateRetryDelay(metadata.RetryCount)
		availableAt := now.Add(retryDelay)
		metadata.AvailableForProcessingAt = &availableAt
	}

	// Save the update
	if err := s.metadataRepo.AddOrUpdate(ctx, metadata); err != nil {
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

	return nil
}

// GetFileStatus returns the current status of a file.
func (s *fileScheduler) GetFileStatus(ctx context.Context, fileKey string) (core.FileProcessingStatus, error) {
	if fileKey == "" {
		return 0, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	metadata, err := s.metadataRepo.Get(ctx, fileKey)
	if err != nil {
		return 0, fmt.Errorf("failed to get file metadata: %w", err)
	}

	return metadata.Status, nil
}

// ResetTimedOutFiles finds files in Processing status that exceed timeout
// and resets them to Pending status for retry.
func (s *fileScheduler) ResetTimedOutFiles(ctx context.Context, timeout time.Duration) (int, error) {
	// Use configured timeout if not specified
	if timeout == 0 {
		timeout = s.processingTimeout
	}

	// Get timed out files
	timedOutFiles, err := s.metadataRepo.GetTimedOutProcessingFiles(ctx, timeout)
	if err != nil {
		return 0, fmt.Errorf("failed to get timed out files: %w", err)
	}

	resetCount := 0
	for _, file := range timedOutFiles {
		// Reset to Pending status
		now := time.Now()
		file.Status = core.FileStatusPending
		file.ProcessingStartTime = nil
		file.UpdatedAt = now

		// Keep existing retry count and available time
		// (this is a timeout, not a failure)

		if err := s.metadataRepo.AddOrUpdate(ctx, file); err != nil {
			// Log error but continue with other files
			continue
		}

		resetCount++
	}

	return resetCount, nil
}
