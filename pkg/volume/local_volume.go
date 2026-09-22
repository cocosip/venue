package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/google/uuid"
)

// Optional core.StorageVolume capabilities implemented by this volume. Callers
// type-assert them, so an implementation drift must fail the build here rather
// than silently degrade a caller to its fallback path.
var (
	_ core.FileMover             = (*LocalFileSystemVolume)(nil)
	_ core.ShardingDepthProvider = (*LocalFileSystemVolume)(nil)
)

// DefaultHealthCheckCacheTTL is the default lifetime of a cached health probe
// result. Health probes touch the filesystem, so the result is reused for this
// window instead of writing and deleting a probe file on every call.
const DefaultHealthCheckCacheTTL = 30 * time.Second

// LocalFileSystemVolumeOptions configures a local file system volume.
type LocalFileSystemVolumeOptions struct {
	// VolumeID is the unique identifier for this volume.
	VolumeID string

	// VolumeType is the type of volume (should be "LocalFileSystem").
	VolumeType string

	// MountPath is the root directory where files are stored.
	MountPath string

	// ShardDepth is the directory sharding depth (0-3).
	// 0 = no sharding (all files in root)
	// 1 = one level (ab/file)
	// 2 = two levels (ab/cd/file)
	// 3 = three levels (ab/cd/ef/file)
	ShardDepth int

	// EnableFsync enables fsync after file writes for durability.
	// Disable for better write performance at the cost of durability.
	EnableFsync bool

	// HealthCheckCacheTTL is how long a health probe result is reused.
	// Zero selects DefaultHealthCheckCacheTTL; a negative value disables the
	// cache so every IsHealthy call probes the volume.
	HealthCheckCacheTTL time.Duration
}

// LocalFileSystemVolume implements StorageVolume for local filesystem.
type LocalFileSystemVolume struct {
	volumeID    string
	mountPath   string
	shardDepth  int
	enableFsync bool
	sanitizer   *PathSanitizer

	// healthCacheTTL is the lifetime of a cached probe result. Negative values
	// disable caching.
	healthCacheTTL time.Duration
	// now and probeHealth are seams for deterministic tests. They default to
	// time.Now and performHealthProbe.
	now         func() time.Time
	probeHealth func(context.Context) bool

	// healthMu makes the probe single-flight: callers that arrive while a probe
	// is running wait for it and then reuse its cached result.
	healthMu        sync.Mutex
	healthKnown     bool
	healthValue     bool
	healthExpiresAt time.Time
}

// NewLocalFileSystemVolume creates a new local file system volume.
func NewLocalFileSystemVolume(opts *LocalFileSystemVolumeOptions) (core.StorageVolume, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.VolumeID == "" {
		return nil, fmt.Errorf("volume ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Validate VolumeType (if specified, must be "LocalFileSystem")
	if opts.VolumeType != "" && opts.VolumeType != "LocalFileSystem" {
		return nil, fmt.Errorf("invalid volume type %s for LocalFileSystemVolume: %w", opts.VolumeType, core.ErrInvalidArgument)
	}

	if opts.MountPath == "" {
		return nil, fmt.Errorf("mount path cannot be empty: %w", core.ErrInvalidArgument)
	}

	if opts.ShardDepth < 0 || opts.ShardDepth > 3 {
		return nil, fmt.Errorf("shard depth must be between 0 and 3: %w", core.ErrInvalidArgument)
	}

	// Ensure mount path exists
	if err := os.MkdirAll(opts.MountPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create mount path: %w", err)
	}

	healthCacheTTL := opts.HealthCheckCacheTTL
	if healthCacheTTL == 0 {
		healthCacheTTL = DefaultHealthCheckCacheTTL
	}

	volume := &LocalFileSystemVolume{
		volumeID:       opts.VolumeID,
		mountPath:      opts.MountPath,
		shardDepth:     opts.ShardDepth,
		enableFsync:    opts.EnableFsync,
		sanitizer:      NewPathSanitizer(opts.MountPath),
		healthCacheTTL: healthCacheTTL,
		now:            time.Now,
	}
	volume.probeHealth = volume.performHealthProbe

	return volume, nil
}

// VolumeID returns the unique identifier for this volume.
func (v *LocalFileSystemVolume) VolumeID() string {
	return v.volumeID
}

// MountPath returns the root path where this volume is mounted.
func (v *LocalFileSystemVolume) MountPath() string {
	return v.mountPath
}

// ShardingDepth returns the configured physical directory sharding depth (0-3).
//
// It implements core.ShardingDepthProvider so callers that must reason about the
// directory layout, such as cleanup, can protect shard directories instead of
// guessing which directories are structural.
func (v *LocalFileSystemVolume) ShardingDepth() int {
	return v.shardDepth
}

// IsHealthy checks if the volume is healthy and available for operations.
//
// The probe (mount-path stat plus a small write/delete round trip) is cached
// for HealthCheckCacheTTL, and a negative TTL disables the cache. Failed probes
// are cached for the same window. Concurrent callers share a single probe.
func (v *LocalFileSystemVolume) IsHealthy(ctx context.Context) bool {
	if v.healthCacheTTL < 0 {
		return v.probeHealth(ctx)
	}

	v.healthMu.Lock()
	defer v.healthMu.Unlock()

	now := v.now()
	if v.healthKnown && now.Before(v.healthExpiresAt) {
		return v.healthValue
	}

	// Only the lock holder probes; callers blocked here reuse the result that
	// this probe caches before releasing the lock.
	healthy := v.probeHealth(ctx)
	v.healthKnown = true
	v.healthValue = healthy
	v.healthExpiresAt = now.Add(v.healthCacheTTL)

	return healthy
}

// performHealthProbe runs the uncached health probe.
func (v *LocalFileSystemVolume) performHealthProbe(ctx context.Context) bool {
	// Check if mount path exists
	if _, err := os.Stat(v.mountPath); err != nil {
		return false
	}

	// Try to write a test file with random GUID to avoid concurrency issues
	testFileName := fmt.Sprintf(".health_check_%s", uuid.New().String())
	testPath := filepath.Join(v.mountPath, testFileName)
	if err := os.WriteFile(testPath, []byte("ok"), 0644); err != nil {
		return false
	}

	// Clean up test file
	_ = os.Remove(testPath)

	return true
}

// TotalCapacity and AvailableSpace are implemented in platform-specific files:
// - local_volume_unix.go for Linux/macOS
// - local_volume_windows.go for Windows

// WriteFile writes a file to the specified path within the volume.
// Returns the number of bytes written.
func (v *LocalFileSystemVolume) WriteFile(ctx context.Context, relativePath string, content io.Reader) (int64, error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return 0, err
	}

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Copy content
	written, err := io.Copy(file, content)
	if err != nil {
		// Clean up on error
		_ = os.Remove(fullPath)
		return 0, fmt.Errorf("failed to write file content: %w", err)
	}

	// Sync to disk if enabled (for durability)
	// Note: Full sync can be expensive for high-throughput scenarios.
	if v.enableFsync {
		if err := file.Sync(); err != nil {
			return written, fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return written, nil
}

// ReadFile reads a file from the specified path within the volume.
// Returns an io.ReadCloser that must be closed by the caller.
func (v *LocalFileSystemVolume) ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return nil, err
	}

	// Check if file exists
	if _, err := os.Stat(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil, core.ErrFileNotFound
		}
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Open file
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

// DeleteFile deletes a file at the specified path within the volume.
func (v *LocalFileSystemVolume) DeleteFile(ctx context.Context, relativePath string) error {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return err
	}

	// Delete file
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, consider it success
			return nil
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}

	return nil
}

// MoveFile moves a file inside the volume without copying its bytes.
//
// Both paths are volume-relative and are sanitized exactly like every other path
// in this package, so neither of them can leave the volume root. The destination
// parent directory chain is created when it is missing. The move is attempted
// with os.Rename, which is atomic and O(1) within one filesystem; when the source
// and destination live on different filesystems the bytes are staged into a
// temporary file next to the destination and committed over it before the source
// is removed.
//
// A move onto the same sanitized path is a successful no-op, and is reported
// without touching the filesystem even when the source is absent. An empty or
// escaping destination, and a destination that already exists, are rejected with
// ErrInvalidArgument/ErrPathTraversalAttempt instead of overwriting another
// payload. A missing source reports ErrFileNotFound.
func (v *LocalFileSystemVolume) MoveFile(ctx context.Context, fromRelativePath string, toRelativePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Sanitizing both sides is what keeps a relative path from escaping the
	// volume root; the sanitizer is the single source of truth for that check.
	sourcePath, err := v.sanitizer.SanitizeAndJoin(fromRelativePath)
	if err != nil {
		return err
	}

	destinationPath, err := v.sanitizer.SanitizeAndJoin(toRelativePath)
	if err != nil {
		return err
	}

	// The requested end state already holds, so there is nothing to do.
	if sourcePath == destinationPath {
		return nil
	}

	// os.Rename replaces an existing target (on Windows it always does), which
	// would silently destroy another payload. Refuse instead.
	if _, err := os.Stat(destinationPath); err == nil {
		return fmt.Errorf("move destination already exists: %w", core.ErrInvalidArgument)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to inspect move destination: %w", err)
	}

	if _, err := os.Stat(sourcePath); err != nil {
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to inspect move source: %w", err)
	}

	// Ensure the destination directory exists.
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	if err := os.Rename(sourcePath, destinationPath); err != nil {
		if isCrossDeviceError(err) {
			return v.copyFileAndRemoveSource(ctx, sourcePath, destinationPath)
		}
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to move file: %w", err)
	}

	return nil
}

// copyFileAndRemoveSource is the cross-device fallback for MoveFile: the bytes
// are written to a temporary file in the destination directory so the final
// rename stays inside one filesystem and therefore publishes the destination
// atomically. The source is only removed after that rename succeeded.
//
// The caller owns the temporary file; every failure path removes it best-effort,
// because a partially written staging file must never be mistaken for a payload.
// Full physical paths are deliberately absent from the returned messages.
func (v *LocalFileSystemVolume) copyFileAndRemoveSource(ctx context.Context, sourceFullPath string, destinationFullPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	sourceInfo, err := os.Stat(sourceFullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to inspect move source: %w", err)
	}

	source, err := os.Open(sourceFullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to open move source: %w", err)
	}
	defer func() { _ = source.Close() }()

	staged, err := os.CreateTemp(filepath.Dir(destinationFullPath), ".venue-move-*")
	if err != nil {
		return fmt.Errorf("failed to stage move destination: %w", err)
	}
	stagedPath := staged.Name()

	// Best-effort release of the staging file: it only exists to be renamed onto
	// the destination, so it must not outlive a failed move. Closing twice is
	// harmless and intentionally ignored for the same reason.
	committed := false
	defer func() {
		_ = staged.Close()
		if !committed {
			_ = os.Remove(stagedPath)
		}
	}()

	copied, err := io.Copy(staged, source)
	if err != nil {
		return fmt.Errorf("failed to copy move source: %w", err)
	}
	if copied != sourceInfo.Size() {
		return fmt.Errorf("move copy is incomplete, wrote %d of %d bytes: %w", copied, sourceInfo.Size(), io.ErrUnexpectedEOF)
	}

	// Release the source handle before it is removed: Windows refuses to delete a
	// file that is still open, and the deferred Close cannot run early enough.
	if err := source.Close(); err != nil {
		return fmt.Errorf("failed to close move source: %w", err)
	}

	if err := staged.Sync(); err != nil {
		return fmt.Errorf("failed to sync staged move copy: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("failed to close staged move copy: %w", err)
	}

	if err := os.Rename(stagedPath, destinationFullPath); err != nil {
		return fmt.Errorf("failed to commit move copy: %w", err)
	}
	committed = true

	if err := os.Remove(sourceFullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to remove move source: %w", err)
	}

	return nil
}

// isCrossDeviceError reports whether a rename failed only because the source and
// the destination are on different filesystems, which is the one rename failure
// that a copy can still satisfy. Windows reports ERROR_NOT_SAME_DEVICE instead of
// EXDEV, and its message text is localized, so the platform errno is preferred
// and the stable English wordings are only a backstop for wrapped errors.
func isCrossDeviceError(err error) bool {
	if isNotSameDeviceErrno(err) || errors.Is(err, syscall.EXDEV) {
		return true
	}

	message := strings.ToLower(err.Error())

	return strings.Contains(message, "not same device") ||
		strings.Contains(message, "cross-device link") ||
		strings.Contains(message, "different disk drive")
}

// FileExists checks if a file exists at the specified path.
func (v *LocalFileSystemVolume) FileExists(ctx context.Context, relativePath string) (bool, error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return false, err
	}

	// Check existence
	_, err = os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, fmt.Errorf("failed to check file existence: %w", err)
}

// BuildFilePath builds a file path with sharding.
// fileKey: the file key (UUID without dashes)
// extension: optional file extension (including the dot, e.g., ".pdf")
func (v *LocalFileSystemVolume) BuildFilePath(fileKey string, extension string) (string, error) {
	// Build sharded path
	shardedPath, err := BuildShardedPath(fileKey, v.shardDepth)
	if err != nil {
		return "", err
	}

	// Add extension if provided
	if extension != "" {
		shardedPath += extension
	}

	return shardedPath, nil
}

// BuildPhysicalPath builds the tenant-scoped relative path used by the storage pool.
func (v *LocalFileSystemVolume) BuildPhysicalPath(tenantID string, fileKey string, extension string) (string, error) {
	// The tenant ID becomes the first physical directory segment, so it must be
	// a safe single path segment before it is joined onto the volume root.
	if err := core.ValidateTenantID(tenantID); err != nil {
		return "", fmt.Errorf("invalid tenant identifier: %w", err)
	}
	shardedPath, err := v.BuildFilePath(fileKey, extension)
	if err != nil {
		return "", err
	}
	return filepath.Join(tenantID, shardedPath), nil
}

// GetFileSize returns the size of a file in bytes.
func (v *LocalFileSystemVolume) GetFileSize(ctx context.Context, relativePath string) (int64, error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return 0, err
	}

	// Get file info
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, core.ErrFileNotFound
		}
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	return info.Size(), nil
}
