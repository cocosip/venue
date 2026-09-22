package pool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/internal/directorypath"
	"github.com/cocosip/venue/pkg/core"
	"github.com/google/uuid"
)

// defaultCapacityCacheTTL bounds how often capacity reporting probes the
// volumes. It is short so operational callers still observe near-live values.
const defaultCapacityCacheTTL = time.Second

const idempotentWriteGuardCount = 256

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

	// StatisticsRecorder optionally receives in-process operation statistics.
	// Nil means recording is disabled.
	StatisticsRecorder core.StatisticsRecorder
}

// PathGenerator generates storage paths for files.
type PathGenerator interface {
	// GeneratePath generates a storage path for a file.
	// Returns a relative path like "2024/01/22/file.ext"
	GeneratePath(tenantID string, fileKey string, fileExtension string) string
}

// storagePool implements the StoragePool interface.
//
// volumes, pathGenerator and volumeSelector are construction-time dependencies:
// NewStoragePool assigns them once and they are never replaced afterwards, so
// the volumes map can be read without synchronization.
type storagePool struct {
	tenantManager  core.TenantManager
	metadataRepo   core.MetadataRepository
	scheduler      core.FileScheduler
	volumes        map[string]core.StorageVolume
	pathGenerator  PathGenerator
	tenantQuotaMgr core.TenantQuotaManager
	dirQuotaMgr    core.DirectoryQuotaManager
	volumeSelector VolumeSelector

	// statistics optionally receives operation statistics. Nil disables
	// recording, so every call site goes through recordStatistic.
	statistics core.StatisticsRecorder

	// mu guards the capacity cache. now is a clock seam for tests; nil means
	// time.Now. capacityTTL overrides defaultCapacityCacheTTL when positive.
	mu          sync.Mutex
	now         func() time.Time
	capacityTTL time.Duration
	capacity    capacitySnapshot

	// idempotentWriteGuards serialize equal tenant/operation pairs without
	// retaining operation IDs. The fixed array keeps memory bounded while the
	// SQLite unique index remains the durable source of truth.
	idempotentWriteGuards [idempotentWriteGuardCount]sync.Mutex
}

// capacitySnapshot is the cached aggregate capacity of the healthy volumes.
type capacitySnapshot struct {
	total     int64
	available int64
	expiresAt time.Time
	valid     bool
}

// writeResources tracks which write-path resources were acquired so a single
// deferred cleanup can roll back exactly the ones that were taken.
type writeResources struct {
	tenantQuotaTaken    bool
	directoryQuotaTaken bool

	// physicalFileWritten means the volume may hold bytes not covered by
	// durable metadata; physicalVolume and physicalPath identify them.
	physicalFileWritten bool
	physicalVolume      core.StorageVolume
	physicalPath        string
}

// volumeCleanupError reports a failed physical rollback without exposing the
// underlying volume error, which may embed a physical path. The cause stays
// reachable through errors.Is/errors.As for programmatic handling.
type volumeCleanupError struct {
	volumeID string
	cause    error
}

func (e *volumeCleanupError) Error() string {
	return fmt.Sprintf(
		"physical file rollback failed on volume %s (details withheld because volume errors may contain physical paths)",
		e.volumeID,
	)
}

func (e *volumeCleanupError) Unwrap() error { return e.cause }

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
		statistics:     opts.StatisticsRecorder,
	}

	return pool, nil
}

// recordStatistic forwards one statistics delta when a recorder is configured.
//
// The recorder is optional: a nil recorder is the documented "recording
// disabled" state, and recording never affects the storage operation.
func (p *storagePool) recordStatistic(name string, value int64, tenantID, volumeID string) {
	if p.statistics == nil {
		return
	}

	dimensions := map[string]string{
		core.StatisticsDimensionTenantID: tenantID,
		core.StatisticsDimensionVolumeID: volumeID,
	}

	p.statistics.Record(name, value, time.Now().UTC(), dimensions)
}

// WriteFile stores a file in the storage pool and returns a system-generated fileKey.
func (p *storagePool) WriteFile(ctx context.Context, tenant core.TenantContext, content io.Reader, originalFileName *string) (string, error) {
	return p.writeFile(ctx, tenant, content, originalFileName, "/", "")
}

// WriteFileIdempotently stores one logical write at most once for a tenant.
// SQLite owns the durable mapping; the fixed striped lock only closes the
// in-process check/write race and never grows with the number of imports.
func (p *storagePool) WriteFileIdempotently(
	ctx context.Context,
	tenant core.TenantContext,
	content io.Reader,
	originalFileName *string,
	operationID string,
) (string, error) {
	if strings.TrimSpace(operationID) == "" {
		return "", fmt.Errorf("import operation ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	operationRepo, ok := p.metadataRepo.(core.ImportOperationRepository)
	if !ok {
		return "", fmt.Errorf("metadata repository does not support import operation lookup: %w", core.ErrInvalidArgument)
	}

	guard := p.idempotentWriteGuard(tenant.ID, operationID)
	guard.Lock()
	defer guard.Unlock()

	existing, err := operationRepo.GetByImportOperationID(ctx, tenant.ID, operationID)
	if err == nil {
		return existing.FileKey, nil
	}
	if !errors.Is(err, core.ErrFileNotFound) {
		return "", err
	}

	fileKey, err := p.writeFile(ctx, tenant, content, originalFileName, "/", operationID)
	if err == nil {
		return fileKey, nil
	}

	// A second process may have won the database unique constraint after our
	// lookup. The failed write has already rolled its physical file and quotas
	// back, so returning the durable winner is safe.
	existing, lookupErr := operationRepo.GetByImportOperationID(ctx, tenant.ID, operationID)
	if lookupErr == nil {
		return existing.FileKey, nil
	}
	return "", err
}

func (p *storagePool) idempotentWriteGuard(tenantID string, operationID string) *sync.Mutex {
	var hash uint32 = 2166136261
	for _, value := range tenantID + "\n" + operationID {
		hash ^= uint32(value)
		hash *= 16777619
	}
	return &p.idempotentWriteGuards[hash%idempotentWriteGuardCount]
}

// WriteFileToDirectory stores a file with a normalized logical directory.
func (p *storagePool) WriteFileToDirectory(
	ctx context.Context,
	tenant core.TenantContext,
	content io.Reader,
	originalFileName *string,
	logicalDirectoryPath string,
) (string, error) {
	return p.writeFile(ctx, tenant, content, originalFileName, directorypath.Normalize(logicalDirectoryPath), "")
}

func (p *storagePool) writeFile(
	ctx context.Context,
	tenant core.TenantContext,
	content io.Reader,
	originalFileName *string,
	logicalDirectoryPath string,
	operationID string,
) (fileKey string, err error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return "", core.ErrTenantDisabled
	}

	// The tenant identifier becomes a physical directory segment. Reject an
	// unsafe value before it can take any quota, volume, or metadata resource.
	if err := core.ValidateTenantID(tenant.ID); err != nil {
		return "", fmt.Errorf("invalid tenant identifier: %w", err)
	}

	// Generate unique file key
	fileKey = generateFileKey()

	// Extract file extension
	fileExtension := ""
	if originalFileName != nil {
		fileExtension = filepath.Ext(*originalFileName)
	}

	// Physical file creation and metadata persistence form one logical
	// operation. Every resource acquired below is tracked so that a single
	// deferred cleanup rolls back exactly what was taken, on error and on
	// panic, before the panic keeps propagating.
	var resources writeResources
	defer func() {
		recovered := recover()
		if recovered == nil && err == nil {
			// Physical bytes and metadata are durable: keep the quotas.
			return
		}

		cleanupErr := p.rollbackWrite(ctx, tenant.ID, logicalDirectoryPath, &resources)
		if recovered != nil {
			panic(recovered)
		}
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	// Check tenant quota
	if p.tenantQuotaMgr != nil {
		if incErr := p.tenantQuotaMgr.IncrementFileCount(ctx, tenant.ID); incErr != nil {
			return "", incErr
		}
		resources.tenantQuotaTaken = true
	}

	// Select storage volume candidates in preference order
	candidates, err := p.selectWriteCandidates(ctx, content)
	if err != nil {
		return "", err
	}

	// Check directory quota
	if p.dirQuotaMgr != nil {
		if incErr := p.dirQuotaMgr.IncrementFileCount(ctx, tenant.ID, logicalDirectoryPath); incErr != nil {
			return "", incErr
		}
		resources.directoryQuotaTaken = true
	}

	// Write file to volume
	volume, relativePath, fileSize, err := p.writeToVolumes(ctx, candidates, tenant.ID, fileKey, fileExtension, content, &resources)
	if err != nil {
		return "", err
	}

	// Create file metadata
	now := time.Now()
	metadata := &core.FileMetadata{
		FileKey:           fileKey,
		TenantID:          tenant.ID,
		ImportOperationID: operationID,
		VolumeID:          volume.VolumeID(),
		PhysicalPath:      relativePath,
		DirectoryPath:     logicalDirectoryPath,
		FileSize:          fileSize,
		FileExtension:     fileExtension,
		OriginalFileName:  stringValue(originalFileName),
		Status:            core.FileStatusPending,
		RetryCount:        0,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	// Save metadata
	if err := p.metadataRepo.AddOrUpdate(ctx, metadata); err != nil {
		return "", fmt.Errorf("failed to save file metadata: %w", err)
	}

	// Success path only: a rollback above means nothing durable was written, so
	// the write statistics must not be inflated by a failed attempt.
	p.recordStatistic(core.StatisticStorageWriteSuccessCount, 1, tenant.ID, volume.VolumeID())
	p.recordStatistic(core.StatisticStorageWriteBytes, fileSize, tenant.ID, volume.VolumeID())

	return fileKey, nil
}

// writeToVolumes writes content to the first candidate volume that accepts it.
// A failed attempt deletes its partial file; the write is retried on the next
// candidate only when the content can be replayed. When a partial file cannot be
// deleted the loop stops and leaves it tracked in resources so the deferred
// rollback owns the cleanup and the quota accounting.
func (p *storagePool) writeToVolumes(
	ctx context.Context,
	candidates []core.StorageVolume,
	tenantID string,
	fileKey string,
	fileExtension string,
	content io.Reader,
	resources *writeResources,
) (core.StorageVolume, string, int64, error) {
	seeker, seekable := content.(io.Seeker)
	initialPosition := int64(0)
	if seekable {
		position, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			seekable = false
		} else {
			initialPosition = position
		}
	}

	var lastErr error
	for index, candidate := range candidates {
		relativePath, err := p.buildPhysicalPath(candidate, tenantID, fileKey, fileExtension)
		if err != nil {
			return nil, "", 0, fmt.Errorf("failed to build physical path: %w", err)
		}

		if seekable && index > 0 {
			if _, err := seeker.Seek(initialPosition, io.SeekStart); err != nil {
				return nil, "", 0, fmt.Errorf("failed to rewind content for retry: %w", err)
			}
		}

		// Track the target pessimistically: the volume may create a partial file
		// and then fail (or panic) before returning.
		resources.physicalFileWritten = true
		resources.physicalVolume = candidate
		resources.physicalPath = relativePath

		written, err := candidate.WriteFile(ctx, relativePath, content)
		if err == nil {
			return candidate, relativePath, written, nil
		}

		lastErr = err

		if cleanupErr := candidate.DeleteFile(ctx, relativePath); cleanupErr != nil {
			// Stop retrying: an unaccounted partial file must not be forgotten
			// while another copy is written elsewhere.
			break
		}

		resources.physicalFileWritten = false
		resources.physicalVolume = nil
		resources.physicalPath = ""

		// Forget cached selection state so the failing volume is re-evaluated
		// before it is chosen again.
		p.invalidateCapacityCache()

		if !seekable || index+1 >= len(candidates) {
			break
		}
	}

	return nil, "", 0, fmt.Errorf("failed to write file to volume: %w", lastErr)
}

// rollbackWrite releases the resources tracked by a failed write. Quotas are
// only released once no bytes are left on disk, so a failed physical delete
// keeps the accounting honest instead of hiding an orphan file.
func (p *storagePool) rollbackWrite(
	ctx context.Context,
	tenantID string,
	logicalDirectoryPath string,
	resources *writeResources,
) error {
	var cleanupErr error

	if resources.physicalFileWritten && resources.physicalVolume != nil {
		if err := resources.physicalVolume.DeleteFile(ctx, resources.physicalPath); err != nil {
			cleanupErr = &volumeCleanupError{volumeID: resources.physicalVolume.VolumeID(), cause: err}
		} else {
			resources.physicalFileWritten = false
		}
	}

	if cleanupErr != nil {
		return cleanupErr
	}

	if resources.directoryQuotaTaken && p.dirQuotaMgr != nil {
		if err := p.dirQuotaMgr.DecrementFileCount(ctx, tenantID, logicalDirectoryPath); err != nil {
			cleanupErr = fmt.Errorf("failed to roll back directory quota for tenant %s: %w", tenantID, err)
		}
		resources.directoryQuotaTaken = false
	}

	if resources.tenantQuotaTaken && p.tenantQuotaMgr != nil {
		if err := p.tenantQuotaMgr.DecrementFileCount(ctx, tenantID); err != nil && cleanupErr == nil {
			cleanupErr = fmt.Errorf("failed to roll back tenant quota for tenant %s: %w", tenantID, err)
		}
		resources.tenantQuotaTaken = false
	}

	return cleanupErr
}

// selectWriteCandidates resolves the ordered list of volumes to try. A seekable
// content reader constrains the selection to volumes that can hold the
// remaining payload; the reader position is restored before returning.
func (p *storagePool) selectWriteCandidates(ctx context.Context, content io.Reader) ([]core.StorageVolume, error) {
	requiredBytes := remainingContentSize(content)

	if selector, ok := p.volumeSelector.(VolumeCandidateSelector); ok {
		candidates, err := selector.SelectVolumeCandidates(ctx, p.volumes, requiredBytes)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("no writable volumes available: %w", core.ErrInsufficientStorage)
		}
		return candidates, nil
	}

	volume, err := p.volumeSelector.SelectVolume(ctx, p.volumes)
	if err != nil {
		return nil, err
	}
	return []core.StorageVolume{volume}, nil
}

// remainingContentSize reports how many bytes are left in a seekable reader
// without consuming it. It returns 0 when the size is unknown.
func remainingContentSize(content io.Reader) int64 {
	seeker, ok := content.(io.Seeker)
	if !ok {
		return 0
	}

	current, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0
	}

	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		_, _ = seeker.Seek(current, io.SeekStart)
		return 0
	}

	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return 0
	}

	if end <= current {
		return 0
	}
	return end - current
}

func (p *storagePool) buildPhysicalPath(volume core.StorageVolume, tenantID, fileKey, fileExtension string) (string, error) {
	// Every derived path is tenant-scoped, so validate the identifier even when
	// a custom path generator is configured.
	if err := core.ValidateTenantID(tenantID); err != nil {
		return "", fmt.Errorf("invalid tenant identifier: %w", err)
	}
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
		// The stored physical path can drift from the canonical layout. Rebuild
		// the canonical path, verify the bytes are really there, persist the
		// correction, and retry the read once (Locus
		// TryCorrectMetadataPhysicalPathAsync).
		if correctedPath, corrected := p.tryCorrectPhysicalPath(ctx, volume, metadata); corrected {
			if retryReader, retryErr := volume.ReadFile(ctx, correctedPath); retryErr == nil {
				p.recordStatistic(core.StatisticStorageFileReadCount, 1, metadata.TenantID, metadata.VolumeID)

				return retryReader, nil
			}
		}
		return nil, fmt.Errorf("failed to read file from volume: %w", err)
	}

	p.recordStatistic(core.StatisticStorageFileReadCount, 1, metadata.TenantID, metadata.VolumeID)

	return reader, nil
}

// tryCorrectPhysicalPath rebuilds the canonical relative path for a file and, if
// the bytes are actually there, persists the corrected path. It reports whether
// the metadata was corrected.
func (p *storagePool) tryCorrectPhysicalPath(
	ctx context.Context,
	volume core.StorageVolume,
	metadata *core.FileMetadata,
) (string, bool) {
	canonicalPath, err := p.buildPhysicalPath(volume, metadata.TenantID, metadata.FileKey, metadata.FileExtension)
	if err != nil || canonicalPath == "" || canonicalPath == metadata.PhysicalPath {
		return "", false
	}

	exists, err := volume.FileExists(ctx, canonicalPath)
	if err != nil || !exists {
		return "", false
	}

	// Persist a copy so a failed write cannot mutate shared metadata state.
	corrected := *metadata
	corrected.PhysicalPath = canonicalPath
	corrected.UpdatedAt = time.Now()
	if err := p.metadataRepo.AddOrUpdate(ctx, &corrected); err != nil {
		return "", false
	}

	return canonicalPath, true
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
	location, err := p.scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		return nil, err
	}

	p.recordDequeue(location)

	return location, nil
}

// GetNextBatchForProcessing retrieves a batch of pending files for processing.
func (p *storagePool) GetNextBatchForProcessing(ctx context.Context, tenant core.TenantContext, batchSize int) ([]*core.FileLocation, error) {
	locations, err := p.scheduler.GetNextBatchForProcessing(ctx, tenant, batchSize)
	if err != nil {
		return nil, err
	}

	for _, location := range locations {
		p.recordDequeue(location)
	}

	return locations, nil
}

// recordDequeue reports one claimed file. A claim that returned no location is
// not a dequeue and is not recorded.
func (p *storagePool) recordDequeue(location *core.FileLocation) {
	if location == nil {
		return
	}

	p.recordStatistic(core.StatisticStorageFileDequeuedCount, 1, location.TenantID, location.VolumeID)
}

// MarkAsCompleted marks a file as successfully processed.
func (p *storagePool) MarkAsCompleted(ctx context.Context, lease core.FileProcessingLease) error {
	if err := p.scheduler.MarkAsCompleted(ctx, lease); err != nil {
		return err
	}

	// A completed file is still durable metadata, so its volume is resolved
	// after the successful transition. A failed resolution never affects the
	// completion; it only leaves the volume dimension empty.
	volumeID := ""
	if metadata, err := p.metadataRepo.Get(ctx, lease.TenantID, lease.FileKey); err == nil {
		volumeID = metadata.VolumeID
	}

	p.recordStatistic(core.StatisticStorageFileCompletedCount, 1, lease.TenantID, volumeID)

	return nil
}

// MarkAsFailed marks a file as failed and schedules it for retry.
func (p *storagePool) MarkAsFailed(ctx context.Context, lease core.FileProcessingLease, errorMessage string) error {
	return p.scheduler.MarkAsFailed(ctx, lease, errorMessage)
}

// GetFileStatus returns the current processing status of a file.
func (p *storagePool) GetFileStatus(ctx context.Context, tenant core.TenantContext, fileKey string) (core.FileProcessingStatus, error) {
	return p.scheduler.GetFileStatus(ctx, tenant, fileKey)
}

// GetTotalCapacity returns the total capacity across all healthy mounted
// volumes. Unhealthy volumes and volumes that fail to report capacity are
// skipped, so the result may be a partial sum. The aggregate is cached for a
// short window.
func (p *storagePool) GetTotalCapacity(ctx context.Context) (int64, error) {
	return p.capacitySnapshot(ctx).total, nil
}

// GetAvailableSpace returns the available space across all healthy mounted
// volumes. Unhealthy volumes and volumes that fail to report space are skipped,
// so the result may be a partial sum. The aggregate is cached for a short
// window.
func (p *storagePool) GetAvailableSpace(ctx context.Context) (int64, error) {
	return p.capacitySnapshot(ctx).available, nil
}

// capacitySnapshot returns the cached aggregate capacity of the healthy
// volumes, recomputing it when the cache window has expired.
func (p *storagePool) capacitySnapshot(ctx context.Context) capacitySnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.timeNow()
	if p.capacity.valid && now.Before(p.capacity.expiresAt) {
		return p.capacity
	}

	snapshot := capacitySnapshot{expiresAt: now.Add(p.capacityCacheDuration()), valid: true}

	complete := true
	for _, volume := range p.volumes {
		if err := ctx.Err(); err != nil {
			// Report the partial sum but never cache a cancelled computation.
			complete = false
			break
		}
		if !volume.IsHealthy(ctx) {
			continue
		}
		if capacity, err := volume.TotalCapacity(ctx); err == nil {
			snapshot.total += capacity
		}
		if space, err := volume.AvailableSpace(ctx); err == nil {
			snapshot.available += space
		}
	}

	if complete {
		p.capacity = snapshot
	}

	return snapshot
}

// invalidateCapacityCache discards the cached aggregate so the next capacity or
// selection decision re-probes the volumes.
func (p *storagePool) invalidateCapacityCache() {
	p.mu.Lock()
	p.capacity.valid = false
	p.mu.Unlock()
}

func (p *storagePool) capacityCacheDuration() time.Duration {
	if p.capacityTTL > 0 {
		return p.capacityTTL
	}
	return defaultCapacityCacheTTL
}

// timeNow returns the pool clock, which tests may replace.
func (p *storagePool) timeNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
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
