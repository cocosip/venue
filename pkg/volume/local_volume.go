package volume

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cocosip/venue/pkg/core"
	"github.com/google/uuid"
)

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
}

// LocalFileSystemVolume implements StorageVolume for local filesystem.
type LocalFileSystemVolume struct {
	volumeID   string
	mountPath  string
	shardDepth int
	sanitizer  *PathSanitizer
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

	return &LocalFileSystemVolume{
		volumeID:   opts.VolumeID,
		mountPath:  opts.MountPath,
		shardDepth: opts.ShardDepth,
		sanitizer:  NewPathSanitizer(opts.MountPath),
	}, nil
}

// VolumeID returns the unique identifier for this volume.
func (v *LocalFileSystemVolume) VolumeID() string {
	return v.volumeID
}

// MountPath returns the root path where this volume is mounted.
func (v *LocalFileSystemVolume) MountPath() string {
	return v.mountPath
}

// IsHealthy checks if the volume is healthy and available for operations.
func (v *LocalFileSystemVolume) IsHealthy(ctx context.Context) bool {
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

	// Sync to disk
	if err := file.Sync(); err != nil {
		return written, fmt.Errorf("failed to sync file: %w", err)
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
