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
	_ core.FileMover                    = (*LocalFileSystemVolume)(nil)
	_ core.ShardingDepthProvider        = (*LocalFileSystemVolume)(nil)
	_ core.StorageVolumeHealthProbe     = (*LocalFileSystemVolume)(nil)
	_ core.StorageVolumeWritePathWarmup = (*LocalFileSystemVolume)(nil)
)

// DefaultHealthCheckCacheTTL is the default lifetime of a cached health probe
// result. Health probes touch the filesystem, so the result is reused for this
// window instead of writing and deleting a probe file on every call.
const DefaultHealthCheckCacheTTL = 30 * time.Second

// writeWarmupFilePrefix is the volume-internal name prefix of the throwaway file
// WarmWritePathCache creates. The leading dot keeps it out of every tenant
// namespace, and the prefix makes a file left behind by a crashed process
// recognizable to cleanup and orphan scans as internal.
const writeWarmupFilePrefix = ".venue-write-warmup-"

// writeWarmupPayload is the content of the throwaway warmup file. It is
// deliberately tiny: the warmup exists to exercise the write path, not to move
// data.
const writeWarmupPayload = "venue-write-path-warmup"

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

	// warmupName is a seam for deterministic tests. It defaults to a unique
	// volume-internal name.
	warmupName func() string

	// syncFile, syncDirectory and openFileForRead are seams for deterministic
	// tests: they default to (*os.File).Sync, syncing a directory handle, and
	// os.Open. Production never replaces them.
	syncFile        func(*os.File) error
	syncDirectory   func(string) error
	openFileForRead func(string) (*os.File, error)

	// healthMu makes the probe single-flight: callers that arrive while a probe
	// is running wait for it and then reuse its cached result. It guards only the
	// cached probe state; the write path never takes it.
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
	volume.warmupName = func() string {
		return writeWarmupFilePrefix + uuid.NewString()
	}
	volume.syncFile = func(file *os.File) error { return file.Sync() }
	volume.syncDirectory = syncDirectory
	volume.openFileForRead = os.Open

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
//
// A cancelled or expired context never poisons the cache: a context failure
// says nothing about the volume, and caching it would make one cancelled
// request reject every write for the whole TTL. The verdict is returned
// uncached, so the next caller with a live context probes for real.
func (v *LocalFileSystemVolume) IsHealthy(ctx context.Context) bool {
	if v.healthCacheTTL < 0 {
		return v.probeHealth(ctx)
	}

	if contextFailure(ctx) != nil {
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

// ProbeHealth performs an immediate health probe and refreshes the cached health
// state that IsHealthy reads, so the forced outcome is the one later health
// checks report for the rest of the cache window.
//
// The probe runs outside the cache mutex: WriteFile never takes healthMu, so a
// forced probe can never block the write path, and the mutex is held only for the
// short critical section that publishes the outcome. Concurrent ProbeHealth
// calls therefore each run their own probe; a forced probe is an explicit caller
// decision and is deliberately not de-duplicated (unlike the implicit probes
// that IsHealthy shares through the single-flight lock path).
//
// A cancelled context is treated as an unsuccessful probe and is cached like any
// other outcome; a probe never panics on an unusable context.
func (v *LocalFileSystemVolume) ProbeHealth(ctx context.Context) bool {
	healthy := v.probeHealth(ctx)

	v.healthMu.Lock()
	v.healthKnown = true
	v.healthValue = healthy
	v.healthExpiresAt = v.now().Add(v.healthCacheTTL)
	v.healthMu.Unlock()

	return healthy
}

// WarmWritePathCache performs one throwaway write through the real volume write
// path, so the first production write does not pay for a cold path or cold
// filesystem caches.
//
// The probe file is created through WriteFile under a volume-internal dotted name
// in the volume root, which keeps it subject to the same path sanitization as any
// payload, and it is deleted again on every outcome. The cleanup runs on a
// cancellation-detached context, because a cancelled warmup must not leave the
// probe file behind. Warmup is advisory for callers, but it still reports a write
// or cleanup failure wrapped around its cause so the caller can decide whether to
// retry.
//
// A context that is already unusable returns the context error without touching
// the filesystem.
func (v *LocalFileSystemVolume) WarmWritePathCache(ctx context.Context) error {
	if err := contextFailure(ctx); err != nil {
		return fmt.Errorf("write path warmup skipped: %w", err)
	}

	// The cleanup is the part that must always run, so it is detached from caller
	// cancellation while the write itself still observes the caller's context.
	cleanupCtx := context.WithoutCancel(ctx)

	relativePath := v.warmupName()
	if _, err := v.WriteFile(ctx, relativePath, strings.NewReader(writeWarmupPayload)); err != nil {
		if removeErr := v.DeleteFile(cleanupCtx, relativePath); removeErr != nil {
			// Both halves failed: report both causes rather than hiding the
			// cleanup failure behind the write failure.
			return errors.Join(
				fmt.Errorf("write path warmup write failed: %w", err),
				fmt.Errorf("write path warmup cleanup failed: %w", removeErr),
			)
		}
		return fmt.Errorf("write path warmup write failed: %w", err)
	}

	if err := v.DeleteFile(cleanupCtx, relativePath); err != nil {
		return fmt.Errorf("write path warmup cleanup failed: %w", err)
	}

	return nil
}

// performHealthProbe runs the uncached health probe.
//
// The probe file reuses the write-warmup name (writeWarmupFilePrefix), so a probe
// that dies before its cleanup leaves a file that every other component already
// recognizes as volume-internal: the dotted single-segment name never matches the
// managed payload layout, so orphan recovery skips it instead of registering it
// as a recoverable file. The probe deliberately writes with os.WriteFile rather
// than through WriteFile, because it is a health check and not a stored payload.
func (v *LocalFileSystemVolume) performHealthProbe(ctx context.Context) bool {
	// An unusable context cannot vouch for the volume and must not panic.
	if contextFailure(ctx) != nil {
		return false
	}

	// Check if mount path exists
	if _, err := os.Stat(v.mountPath); err != nil {
		return false
	}

	// Try to write a test file with a unique volume-internal name to avoid
	// concurrency issues
	testPath := filepath.Join(v.mountPath, v.warmupName())
	if err := os.WriteFile(testPath, []byte("ok"), 0644); err != nil {
		return false
	}

	// Clean up test file
	_ = os.Remove(testPath)

	return true
}

// errNilContext is reported when a caller passes no context at all, which cannot
// be honoured and must never panic a probe.
var errNilContext = errors.New("nil context")

// contextFailure returns the reason ctx cannot be honoured, or nil when it can.
// A nil context cannot be used at all, and a cancelled or expired one must not be
// treated as permission to perform I/O.
func contextFailure(ctx context.Context) error {
	if ctx == nil {
		return errNilContext
	}
	return ctx.Err()
}

// TotalCapacity and AvailableSpace are implemented in platform-specific files:
// - local_volume_unix.go for Linux/macOS
// - local_volume_windows.go for Windows

// WriteFile writes a file to the specified path within the volume.
// Returns the number of bytes written.
//
// The payload is copied through a context-aware reader, so a request whose
// context is cancelled mid-copy stops at the next chunk and the partial file is
// removed instead of being streamed to completion.
//
// With EnableFsync the file is synced and then its containing directory is synced
// best-effort, so the new name survives a crash as reliably as its contents; a
// failed file sync removes the file rather than leaving an unaccounted payload.
func (v *LocalFileSystemVolume) WriteFile(ctx context.Context, relativePath string, content io.Reader) (written int64, err error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return 0, err
	}

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if mkdirErr := os.MkdirAll(dir, 0755); mkdirErr != nil {
		return 0, fmt.Errorf("failed to create directory: %w", mkdirErr)
	}

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}

	// committed stays false until every step that makes the payload valid has
	// run. The deferred cleanup then covers paths a plain error check cannot: a
	// panic in the caller's reader unwinds through here too, and the half-written
	// file plus its open handle must not survive the unwind.
	//
	// The close inside the cleanup is deliberately unconditional and idempotent:
	// on Windows os.Remove fails with a sharing violation while this process
	// still holds the handle, so the handle must go before the unlink.
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = file.Close()
		_ = os.Remove(fullPath)
	}()

	// Copy content through a context-aware reader so a cancelled request stops
	// at the next chunk instead of streaming the whole payload.
	written, copyErr := io.Copy(file, &contextReader{ctx: ctx, reader: content})
	if copyErr != nil {
		return 0, fmt.Errorf("failed to write file content: %w", copyErr)
	}

	// Sync to disk if enabled (for durability)
	// Note: Full sync can be expensive for high-throughput scenarios.
	if v.enableFsync {
		if syncErr := v.syncFile(file); syncErr != nil {
			// The bytes are not durable, so the file must not survive: leaving
			// it would publish a payload whose metadata write is about to fail.
			return written, fmt.Errorf("failed to sync file: %w", syncErr)
		}

		// Syncing the file makes its contents durable, but the directory entry
		// that names it is separate. Without this the file can be missing after
		// a crash while its SQLite metadata row is already durable. A failure is
		// best-effort: the directory cannot be opened for fsync on every
		// platform (Windows rejects it), and the payload itself is already
		// synced.
		_ = v.syncDirectory(dir)
	}

	if closeErr := file.Close(); closeErr != nil {
		return written, fmt.Errorf("failed to close file: %w", closeErr)
	}

	committed = true

	return written, nil
}

// ReadFile reads a file from the specified path within the volume.
// Returns an io.ReadCloser that must be closed by the caller.
//
// A missing file is reported as core.ErrFileNotFound. The check is the open
// itself rather than a separate stat: a stat followed by an open would leave a
// window in which a concurrent delete turns a not-found into a generic error.
func (v *LocalFileSystemVolume) ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	// Sanitize and get full path
	fullPath, err := v.sanitizer.SanitizeAndJoin(relativePath)
	if err != nil {
		return nil, err
	}

	// Open file
	file, err := v.openFileForRead(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, core.ErrFileNotFound
		}
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
//
// The destination check is a stat followed by the rename, which is not atomic, so
// a writer that creates the destination between the two is overwritten. That is
// accepted under the volume's trusted-local-filesystem model (see
// PathSanitizer.SanitizeAndJoin): within Venue every destination is derived from
// a system-generated fileKey that no other write targets, so the only way to hit
// the window is to create the destination out of band, which already requires
// write access to the volume. A portable no-replace rename does not exist —
// os.Rename replaces unconditionally and Windows exposes no
// MoveFileEx-without-replace through the standard library — so an atomic
// no-replace commit is not implemented.
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

	// A same-filesystem rename preserves the source's mode, so the fallback
	// copy must too: os.CreateTemp hands back a private 0600 file, and renaming
	// it unmodified would publish the payload with permissions the payload
	// never had.
	if err := staged.Chmod(sourceInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("failed to preserve the source mode on the move destination: %w", err)
	}

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

	// The staging file is only synced when durability is configured: with
	// EnableFsync off this volume does not promise durability for any write, and
	// the source file is still intact until the rename below, so an unsynced copy
	// cannot lose data that a synced one would keep. Gating on the same setting
	// keeps the move path's cost consistent with the write path's.
	if v.enableFsync {
		if err := v.syncFile(staged); err != nil {
			return fmt.Errorf("failed to sync staged move copy: %w", err)
		}
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
//
// The extension is validated through SanitizeExtension before it is concatenated
// into the path, so a caller-supplied value cannot introduce a separator, a null
// byte, a control character, or an unusable length. Trailing dots and spaces are
// trimmed, and a remainder that carries no extension is dropped. A rejected
// extension returns the SanitizeExtension error and an empty path.
func (v *LocalFileSystemVolume) BuildFilePath(fileKey string, extension string) (string, error) {
	// Build sharded path
	shardedPath, err := BuildShardedPath(fileKey, v.shardDepth)
	if err != nil {
		return "", err
	}

	sanitizedExtension, err := SanitizeExtension(extension)
	if err != nil {
		return "", err
	}

	// Add extension if provided
	if sanitizedExtension != "" {
		shardedPath += sanitizedExtension
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
