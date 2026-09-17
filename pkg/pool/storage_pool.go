package pool

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/internal/directorypath"
	"github.com/cocosip/venue/pkg/core"
	"github.com/google/uuid"
)

// StoragePoolOptions configures the storage pool.
type StoragePoolOptions struct {
	// TenantManager manages tenant lifecycle.
	TenantManager core.TenantManager

	// MetadataRepository stores file metadata.
	MetadataRepository core.MetadataRepository

	// FileScheduler manages file processing queue.
	FileScheduler core.FileScheduler

	// Volumes is the collection of storage volumes.
	// Key is volumeID, value is the volume.
	Volumes map[string]core.StorageVolume

	// PathGenerator generates storage paths for files.
	// If nil, uses default date-based path generator.
	PathGenerator PathGenerator

	// TenantQuotaManager manages tenant-level quotas.
	// If nil, quota checks are skipped.
	TenantQuotaManager core.TenantQuotaManager

	// DirectoryQuotaManager manages directory-level quotas.
	// If nil, quota checks are skipped.
	DirectoryQuotaManager core.DirectoryQuotaManager
}

// PathGenerator generates storage paths for files.
type PathGenerator interface {
	// GeneratePath generates a storage path for a file.
	// Returns a relative path like "2024/01/22/file.ext"
	GeneratePath(tenantID string, fileKey string, fileExtension string) string
}

// storagePool implements the StoragePool interface.
type storagePool struct {
	tenantManager  core.TenantManager
	metadataRepo   core.MetadataRepository
	scheduler      core.FileScheduler
	volumes        map[string]core.StorageVolume
	pathGenerator  PathGenerator
	tenantQuotaMgr core.TenantQuotaManager
	dirQuotaMgr    core.DirectoryQuotaManager
	mu             sync.RWMutex
	volumeSelector VolumeSelector
}

// NewStoragePool creates a new storage pool.
func NewStoragePool(opts *StoragePoolOptions) (core.StoragePool, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.TenantManager == nil {
		return nil, fmt.Errorf("tenant manager cannot be nil: %w", core.ErrInvalidArgument)
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

	pool := &storagePool{
		tenantManager:  opts.TenantManager,
		metadataRepo:   opts.MetadataRepository,
		scheduler:      opts.FileScheduler,
		volumes:        opts.Volumes,
		pathGenerator:  opts.PathGenerator,
		tenantQuotaMgr: opts.TenantQuotaManager,
		dirQuotaMgr:    opts.DirectoryQuotaManager,
		volumeSelector: &MostAvailableSpaceSelector{},
	}

	return pool, nil
}

// WriteFile stores a file in the storage pool and returns a system-generated fileKey.
func (p *storagePool) WriteFile(ctx context.Context, tenant core.TenantContext, content io.Reader, originalFileName *string) (string, error) {
	return p.writeFile(ctx, tenant, content, originalFileName, "/")
}

// WriteFileToDirectory stores a file with a normalized logical directory.
func (p *storagePool) WriteFileToDirectory(
	ctx context.Context,
	tenant core.TenantContext,
	content io.Reader,
	originalFileName *string,
	logicalDirectoryPath string,
) (string, error) {
	return p.writeFile(ctx, tenant, content, originalFileName, directorypath.Normalize(logicalDirectoryPath))
}

func (p *storagePool) writeFile(
	ctx context.Context,
	tenant core.TenantContext,
	content io.Reader,
	originalFileName *string,
	logicalDirectoryPath string,
) (string, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return "", core.ErrTenantDisabled
	}

	// Generate unique file key
	fileKey := generateFileKey()

	// Extract file extension
	fileExtension := ""
	if originalFileName != nil {
		fileExtension = filepath.Ext(*originalFileName)
	}

	// Check tenant quota
	if p.tenantQuotaMgr != nil {
		if err := p.tenantQuotaMgr.IncrementFileCount(ctx, tenant.ID); err != nil {
			return "", err
		}
		// Defer decrement in case of error
		defer func() {
			if err := recover(); err != nil {
				_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
				panic(err)
			}
		}()
	}

	// Select storage volume
	volume, err := p.volumeSelector.SelectVolume(ctx, p.volumes)
	if err != nil {
		if p.tenantQuotaMgr != nil {
			_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
		}
		return "", err
	}

	relativePath, err := p.buildPhysicalPath(volume, tenant.ID, fileKey, fileExtension)
	if err != nil {
		if p.tenantQuotaMgr != nil {
			_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
		}
		return "", fmt.Errorf("failed to build physical path: %w", err)
	}
	// Check directory quota
	if p.dirQuotaMgr != nil {
		if err := p.dirQuotaMgr.IncrementFileCount(ctx, tenant.ID, logicalDirectoryPath); err != nil {
			if p.tenantQuotaMgr != nil {
				_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
			}
			return "", err
		}
	}

	// Write file to volume
	fileSize, err := volume.WriteFile(ctx, relativePath, content)
	if err != nil {
		// Rollback quotas
		if p.dirQuotaMgr != nil {
			_ = p.dirQuotaMgr.DecrementFileCount(ctx, tenant.ID, logicalDirectoryPath)
		}
		if p.tenantQuotaMgr != nil {
			_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
		}
		return "", fmt.Errorf("failed to write file to volume: %w", err)
	}

	// Create file metadata
	now := time.Now()
	metadata := &core.FileMetadata{
		FileKey:          fileKey,
		TenantID:         tenant.ID,
		VolumeID:         volume.VolumeID(),
		PhysicalPath:     relativePath,
		DirectoryPath:    logicalDirectoryPath,
		FileSize:         fileSize,
		FileExtension:    fileExtension,
		OriginalFileName: stringValue(originalFileName),
		Status:           core.FileStatusPending,
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	// Save metadata
	if err := p.metadataRepo.AddOrUpdate(ctx, metadata); err != nil {
		// Try to delete the physical file (best effort)
		_ = volume.DeleteFile(ctx, relativePath)
		// Rollback quotas
		if p.dirQuotaMgr != nil {
			_ = p.dirQuotaMgr.DecrementFileCount(ctx, tenant.ID, logicalDirectoryPath)
		}
		if p.tenantQuotaMgr != nil {
			_ = p.tenantQuotaMgr.DecrementFileCount(ctx, tenant.ID)
		}
		return "", fmt.Errorf("failed to save file metadata: %w", err)
	}

	return fileKey, nil
}

func (p *storagePool) buildPhysicalPath(volume core.StorageVolume, tenantID, fileKey, fileExtension string) (string, error) {
	if p.pathGenerator != nil {
		return p.pathGenerator.GeneratePath(tenantID, fileKey, fileExtension), nil
	}
	if builder, ok := volume.(core.StorageVolumePathBuilder); ok {
		return builder.BuildPhysicalPath(tenantID, fileKey, fileExtension)
	}
	return (&DateBasedPathGenerator{}).GeneratePath(tenantID, fileKey, fileExtension), nil
}

// ReadFile retrieves a file by its fileKey.
func (p *storagePool) ReadFile(ctx context.Context, tenant core.TenantContext, fileKey string) (io.ReadCloser, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get file metadata
	metadata, err := p.metadataRepo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		return nil, err
	}

	// Verify tenant matches
	if metadata.TenantID != tenant.ID {
		return nil, core.ErrFileNotFound
	}

	// Get storage volume
	volume, exists := p.volumes[metadata.VolumeID]
	if !exists {
		return nil, fmt.Errorf("storage volume %s not found", metadata.VolumeID)
	}

	// Read file from volume
	reader, err := volume.ReadFile(ctx, metadata.PhysicalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file from volume: %w", err)
	}

	return reader, nil
}

// GetFileInfo returns basic file information.
func (p *storagePool) GetFileInfo(ctx context.Context, tenant core.TenantContext, fileKey string) (*core.FileInfo, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get file metadata
	metadata, err := p.metadataRepo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		return nil, err
	}

	// Verify tenant matches
	if metadata.TenantID != tenant.ID {
		return nil, core.ErrFileNotFound
	}

	return metadata.ToFileInfo(), nil
}

// GetFileLocation returns detailed file location information for diagnostics.
func (p *storagePool) GetFileLocation(ctx context.Context, tenant core.TenantContext, fileKey string) (*core.FileLocation, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get file metadata
	metadata, err := p.metadataRepo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		return nil, err
	}

	// Verify tenant matches
	if metadata.TenantID != tenant.ID {
		return nil, core.ErrFileNotFound
	}

	return metadata.ToFileLocation(), nil
}

// GetNextFileForProcessing retrieves the next pending file for processing.
func (p *storagePool) GetNextFileForProcessing(ctx context.Context, tenant core.TenantContext) (*core.FileLocation, error) {
	return p.scheduler.GetNextFileForProcessing(ctx, tenant)
}

// GetNextBatchForProcessing retrieves a batch of pending files for processing.
func (p *storagePool) GetNextBatchForProcessing(ctx context.Context, tenant core.TenantContext, batchSize int) ([]*core.FileLocation, error) {
	return p.scheduler.GetNextBatchForProcessing(ctx, tenant, batchSize)
}

// MarkAsCompleted marks a file as successfully processed.
func (p *storagePool) MarkAsCompleted(ctx context.Context, lease core.FileProcessingLease) error {
	return p.scheduler.MarkAsCompleted(ctx, lease)
}

// MarkAsFailed marks a file as failed and schedules it for retry.
func (p *storagePool) MarkAsFailed(ctx context.Context, lease core.FileProcessingLease, errorMessage string) error {
	return p.scheduler.MarkAsFailed(ctx, lease, errorMessage)
}

// GetFileStatus returns the current processing status of a file.
func (p *storagePool) GetFileStatus(ctx context.Context, tenant core.TenantContext, fileKey string) (core.FileProcessingStatus, error) {
	return p.scheduler.GetFileStatus(ctx, tenant, fileKey)
}

// GetTotalCapacity returns the total capacity across all mounted volumes.
func (p *storagePool) GetTotalCapacity(ctx context.Context) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var totalCapacity int64
	for _, volume := range p.volumes {
		capacity, err := volume.TotalCapacity(ctx)
		if err != nil {
			// Skip volumes with errors
			continue
		}
		totalCapacity += capacity
	}

	return totalCapacity, nil
}

// GetAvailableSpace returns the available space across all mounted volumes.
func (p *storagePool) GetAvailableSpace(ctx context.Context) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var availableSpace int64
	for _, volume := range p.volumes {
		space, err := volume.AvailableSpace(ctx)
		if err != nil {
			// Skip volumes with errors
			continue
		}
		availableSpace += space
	}

	return availableSpace, nil
}

// Helper functions

// generateFileKey generates a unique file key (UUID without dashes).
func generateFileKey() string {
	id := uuid.New()
	return hex.EncodeToString(id[:])
}

// stringValue returns the string value or empty string if nil.
func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
