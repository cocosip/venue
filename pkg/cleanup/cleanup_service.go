package cleanup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// CleanupServiceOptions configures the cleanup service.
type CleanupServiceOptions struct {
	// MetadataRepository stores file metadata.
	MetadataRepository core.MetadataRepository

	// FileScheduler manages file processing queue.
	FileScheduler core.FileScheduler

	// Volumes is the collection of storage volumes.
	Volumes map[string]core.StorageVolume

	// TenantQuotaManager manages tenant-level quotas.
	// Optional: used to decrement counts when deleting files.
	TenantQuotaManager core.TenantQuotaManager

	// DirectoryQuotaManager manages directory-level quotas.
	// Optional: used to decrement counts when deleting files.
	DirectoryQuotaManager core.DirectoryQuotaManager

	// DirectoryQuotaRepository stores directory quota data.
	// Optional: used for database optimization.
	DirectoryQuotaRepository core.DirectoryQuotaRepository

	// DefaultProcessingTimeout is the default timeout for processing files.
	// Default: 30 minutes
	DefaultProcessingTimeout time.Duration
}

// cleanupService implements the CleanupService interface.
type cleanupService struct {
	metadataRepo             core.MetadataRepository
	scheduler                core.FileScheduler
	volumes                  map[string]core.StorageVolume
	tenantQuotaMgr           core.TenantQuotaManager
	dirQuotaMgr              core.DirectoryQuotaManager
	dirQuotaRepo             core.DirectoryQuotaRepository
	defaultProcessingTimeout time.Duration
	mu                       sync.RWMutex
}

// NewCleanupService creates a new cleanup service.
func NewCleanupService(opts *CleanupServiceOptions) (core.CleanupService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.MetadataRepository == nil {
		return nil, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.FileScheduler == nil {
		return nil, fmt.Errorf("file scheduler cannot be nil: %w", core.ErrInvalidArgument)
	}

	if len(opts.Volumes) == 0 {
		return nil, fmt.Errorf("at least one storage volume is required: %w", core.ErrInvalidArgument)
	}

	defaultTimeout := opts.DefaultProcessingTimeout
	if defaultTimeout == 0 {
		defaultTimeout = 30 * time.Minute
	}

	return &cleanupService{
		metadataRepo:             opts.MetadataRepository,
		scheduler:                opts.FileScheduler,
		volumes:                  opts.Volumes,
		tenantQuotaMgr:           opts.TenantQuotaManager,
		dirQuotaMgr:              opts.DirectoryQuotaManager,
		dirQuotaRepo:             opts.DirectoryQuotaRepository,
		defaultProcessingTimeout: defaultTimeout,
	}, nil
}

// CleanupEmptyDirectories removes empty directories recursively.
func (s *cleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}

	s.mu.RLock()
	volumes := make(map[string]core.StorageVolume)
	for k, v := range s.volumes {
		volumes[k] = v
	}
	s.mu.RUnlock()

	// For each volume, scan and remove empty directories
	for _, volume := range volumes {
		removed, err := s.cleanupEmptyDirsInVolume(ctx, volume)
		if err != nil {
			// Log error but continue with other volumes
			continue
		}
		stats.EmptyDirectoriesRemoved += removed
	}

	return stats, nil
}

// cleanupEmptyDirsInVolume removes empty directories in a specific volume.
func (s *cleanupService) cleanupEmptyDirsInVolume(ctx context.Context, volume core.StorageVolume) (int, error) {
	mountPath := volume.MountPath()
	removed := 0

	// Walk the directory tree and remove only non-system empty directories.
	// Parent directories may become removable on a later cleanup pass.
	err := filepath.Walk(mountPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if !info.IsDir() {
			return nil // Skip files
		}

		if isProtectedSystemDirectory(mountPath, path) {
			return nil // Preserve mount path and system-managed directory structure
		}

		// Check if directory is empty
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil // Skip on error
		}

		if len(entries) == 0 {
			// Directory is empty, remove it
			if err := os.Remove(path); err == nil {
				removed++
			}
		}

		return nil
	})

	if err != nil {
		return removed, err
	}

	return removed, nil
}

func isProtectedSystemDirectory(mountPath string, path string) bool {
	cleanMountPath := filepath.Clean(mountPath)
	cleanPath := filepath.Clean(path)

	if cleanPath == cleanMountPath {
		return true
	}

	relativePath, err := filepath.Rel(cleanMountPath, cleanPath)
	if err != nil {
		return true
	}

	if relativePath == "." {
		return true
	}

	parts := strings.Split(filepath.ToSlash(relativePath), "/")
	if len(parts) == 0 {
		return true
	}

	// The first level under the volume mount is system-managed in Venue.
	// With the default generators this is the tenant directory, and with
	// sharded storage it can also be the first shard directory.
	if len(parts) == 1 {
		return true
	}

	return isProtectedDateHierarchy(parts) || isProtectedShardHierarchy(parts)
}

func isProtectedDateHierarchy(parts []string) bool {
	if len(parts) < 2 || len(parts) > 5 {
		return false
	}

	if !isNDigits(parts[1], 4) {
		return false
	}

	if len(parts) >= 3 && !isNumericRange(parts[2], 2, 1, 12) {
		return false
	}

	if len(parts) >= 4 && !isNumericRange(parts[3], 2, 1, 31) {
		return false
	}

	if len(parts) == 5 && !isNumericRange(parts[4], 2, 0, 23) {
		return false
	}

	return true
}

func isProtectedShardHierarchy(parts []string) bool {
	if len(parts) < 2 || len(parts) > 3 {
		return false
	}

	for _, part := range parts {
		if !isLowerHexByte(part) {
			return false
		}
	}

	return true
}

func isNDigits(value string, width int) bool {
	if len(value) != width {
		return false
	}

	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

func isNumericRange(value string, width int, min int, max int) bool {
	if !isNDigits(value, width) {
		return false
	}

	number := 0
	for _, r := range value {
		number = number*10 + int(r-'0')
	}

	return number >= min && number <= max
}

func isLowerHexByte(value string) bool {
	if len(value) != 2 {
		return false
	}

	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}

	return true
}

// CleanupTimedOutProcessingFiles resets files that have been in Processing status too long.
func (s *cleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}

	// Use configured timeout if not specified
	if timeout == 0 {
		timeout = s.defaultProcessingTimeout
	}

	// Use scheduler to reset timed out files
	count, err := s.scheduler.ResetTimedOutFiles(ctx, timeout)
	if err != nil {
		return stats, fmt.Errorf("failed to reset timed out files: %w", err)
	}

	stats.TimedOutFilesReset = count

	return stats, nil
}

// CleanupPermanentlyFailedFiles deletes files that have permanently failed.
func (s *cleanupService) CleanupPermanentlyFailedFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}

	// Get all permanently failed files
	// Note: We don't have a tenant filter here, so we need to scan all tenants
	// This is a limitation - in production you'd want to iterate through known tenants
	failedFiles, err := s.metadataRepo.GetByStatus(ctx, "", core.FileStatusPermanentlyFailed, 0)
	if err != nil {
		return stats, fmt.Errorf("failed to get permanently failed files: %w", err)
	}

	for _, file := range failedFiles {
		// Delete physical file
		volume, exists := s.volumes[file.VolumeID]
		if exists {
			if err := volume.DeleteFile(ctx, file.PhysicalPath); err == nil {
				stats.SpaceFreed += file.FileSize
			}
		}

		// Delete metadata
		if err := s.metadataRepo.Delete(ctx, file.FileKey); err != nil {
			// Log error but continue
			continue
		}

		// Decrement quotas
		if s.tenantQuotaMgr != nil {
			_ = s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID)
		}
		if s.dirQuotaMgr != nil {
			directoryPath := filepath.Dir(file.PhysicalPath)
			_ = s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, directoryPath)
		}

		stats.PermanentlyFailedFilesRemoved++
	}

	return stats, nil
}

// CleanupOrphanedMetadata removes metadata for files that no longer exist physically.
func (s *cleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}

	// Get all file metadata (we'll scan by status)
	// This is not efficient for large systems - in production you'd want pagination
	allStatuses := []core.FileProcessingStatus{
		core.FileStatusPending,
		core.FileStatusProcessing,
		core.FileStatusFailed,
		core.FileStatusPermanentlyFailed,
	}

	for _, status := range allStatuses {
		files, err := s.metadataRepo.GetByStatus(ctx, "", status, 0)
		if err != nil {
			continue
		}

		for _, file := range files {
			// Check if physical file exists
			volume, exists := s.volumes[file.VolumeID]
			if !exists {
				// Volume doesn't exist, metadata is orphaned
				_ = s.metadataRepo.Delete(ctx, file.FileKey)
				stats.OrphanedMetadataRemoved++
				continue
			}

			// Check if file exists on volume
			fileExists, err := volume.FileExists(ctx, file.PhysicalPath)
			if err != nil || !fileExists {
				// File doesn't exist, metadata is orphaned
				_ = s.metadataRepo.Delete(ctx, file.FileKey)

				// Decrement quotas
				if s.tenantQuotaMgr != nil {
					_ = s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID)
				}
				if s.dirQuotaMgr != nil {
					directoryPath := filepath.Dir(file.PhysicalPath)
					_ = s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, directoryPath)
				}

				stats.OrphanedMetadataRemoved++
			}
		}
	}

	return stats, nil
}

// OptimizeDatabases triggers repository-level garbage collection / compaction work.
func (s *cleanupService) OptimizeDatabases(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}

	if err := s.metadataRepo.Optimize(ctx); err != nil {
		return stats, fmt.Errorf("failed to optimize metadata repository: %w", err)
	}
	stats.MetadataDatabasesOptimized++

	if s.dirQuotaRepo != nil {
		if err := s.dirQuotaRepo.Optimize(ctx); err != nil {
			return stats, fmt.Errorf("failed to optimize directory quota repository: %w", err)
		}
		stats.QuotaDatabasesOptimized++
	}

	return stats, nil
}
