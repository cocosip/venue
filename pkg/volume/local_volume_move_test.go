package volume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// platformSanitizerError reports the error class path_sanitizer.go produces for
// an input whose classification depends on the host platform: on Windows a drive
// or verbatim (\\?\) prefix is an absolute path, while a non-Windows build
// rejects the backslash it contains as a path traversal attempt.
func platformSanitizerError(windowsError error, otherError error) error {
	if runtime.GOOS == "windows" {
		return windowsError
	}
	return otherError
}

// writeVolumeFile writes content through the public volume API.
func writeVolumeFile(t *testing.T, volume *LocalFileSystemVolume, relativePath string, content string) {
	t.Helper()

	if _, err := volume.WriteFile(context.Background(), relativePath, bytes.NewReader([]byte(content))); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", relativePath, err)
	}
}

// readVolumeFile reads a whole file through the public volume API.
func readVolumeFile(t *testing.T, volume *LocalFileSystemVolume, relativePath string) string {
	t.Helper()

	reader, err := volume.ReadFile(context.Background(), relativePath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", relativePath, err)
	}
	defer func() { _ = reader.Close() }()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading %q failed: %v", relativePath, err)
	}

	return string(content)
}

// assertVolumeFileExists asserts the volume reports a path as present or absent.
func assertVolumeFileExists(t *testing.T, volume *LocalFileSystemVolume, relativePath string, want bool) {
	t.Helper()

	exists, err := volume.FileExists(context.Background(), relativePath)
	if err != nil {
		t.Fatalf("FileExists(%q) error = %v", relativePath, err)
	}
	if exists != want {
		t.Fatalf("FileExists(%q) = %t, want %t", relativePath, exists, want)
	}
}

// assertPhysicalFileUnderRoot asserts a volume-relative path resolves to a real
// file that stays inside the mount root, and returns its physical path.
func assertPhysicalFileUnderRoot(t *testing.T, mountPath string, relativePath string) string {
	t.Helper()

	fullPath := filepath.Join(mountPath, filepath.FromSlash(relativePath))
	relative, err := filepath.Rel(mountPath, fullPath)
	if err != nil {
		t.Fatalf("filepath.Rel(%q, %q) error = %v", mountPath, fullPath, err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		t.Fatalf("path %q escapes mount root %q", fullPath, mountPath)
	}
	if _, err := os.Stat(fullPath); err != nil {
		t.Fatalf("expected physical file at %q: %v", fullPath, err)
	}

	return fullPath
}

// TestLocalFileSystemVolume_MoveFileWithinVolume tests an ordinary move.
func TestLocalFileSystemVolume_MoveFileWithinVolume(t *testing.T) {
	ctx := context.Background()
	volume, mountPath := createTestVolume(t, 2)

	writeVolumeFile(t, volume, "src/payload.txt", "payload-content")
	assertPhysicalFileUnderRoot(t, mountPath, "src/payload.txt")

	if err := volume.MoveFile(ctx, "src/payload.txt", "dst/nested/payload.txt"); err != nil {
		t.Fatalf("MoveFile() error = %v", err)
	}

	if got := readVolumeFile(t, volume, "dst/nested/payload.txt"); got != "payload-content" {
		t.Errorf("destination content = %q, want %q", got, "payload-content")
	}
	assertPhysicalFileUnderRoot(t, mountPath, "dst/nested/payload.txt")

	if _, err := os.Stat(filepath.Join(mountPath, "src", "payload.txt")); !os.IsNotExist(err) {
		t.Errorf("source file still present after move: %v", err)
	}
}

// TestLocalFileSystemVolume_MoveFileAcrossShards tests moves of sharded paths
// and that FileExists reports the move for every configured shard depth.
func TestLocalFileSystemVolume_MoveFileAcrossShards(t *testing.T) {
	ctx := context.Background()

	fileKey := "550e8400e29b41d4a716446655440000"
	destKey := "a1b2c3d4e5f647899abcdef01234567"

	testCases := []struct {
		name       string
		shardDepth int
	}{
		{"No sharding", 0},
		{"One level sharding", 1},
		{"Two level sharding", 2},
		{"Three level sharding", 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, mountPath := createTestVolume(t, tc.shardDepth)

			source, err := volume.BuildFilePath(fileKey, ".bin")
			if err != nil {
				t.Fatalf("BuildFilePath(source) error = %v", err)
			}
			destination, err := volume.BuildFilePath(destKey, ".bin")
			if err != nil {
				t.Fatalf("BuildFilePath(destination) error = %v", err)
			}

			writeVolumeFile(t, volume, source, "sharded-content")

			if err := volume.MoveFile(ctx, source, destination); err != nil {
				t.Fatalf("MoveFile() error = %v", err)
			}

			assertVolumeFileExists(t, volume, destination, true)
			assertVolumeFileExists(t, volume, source, false)
			assertPhysicalFileUnderRoot(t, mountPath, destination)

			if got := readVolumeFile(t, volume, destination); got != "sharded-content" {
				t.Errorf("destination content = %q, want %q", got, "sharded-content")
			}
		})
	}
}

// TestLocalFileSystemVolume_MoveFileRejectsUnsafePaths tests that both the
// source and the destination keep going through the path sanitizer.
func TestLocalFileSystemVolume_MoveFileRejectsUnsafePaths(t *testing.T) {
	ctx := context.Background()

	unsafePaths := []struct {
		name string
		path string
		want error
	}{
		{
			name: "Parent traversal",
			path: "../escape.txt",
			want: core.ErrPathTraversalAttempt,
		},
		{
			name: "Backslash parent traversal",
			path: `..\escape.txt`,
			want: core.ErrPathTraversalAttempt,
		},
		{
			name: "Traversal below a valid directory",
			path: "sub/../../escape.txt",
			want: core.ErrPathTraversalAttempt,
		},
		{
			name: "Three consecutive dots",
			path: "file...txt",
			want: core.ErrPathTraversalAttempt,
		},
		{
			name: "Absolute drive path",
			path: `C:\Windows\win.ini`,
			want: platformSanitizerError(core.ErrInvalidArgument, core.ErrPathTraversalAttempt),
		},
		{
			name: "Verbatim absolute path",
			path: `\\?\C:\Windows\win.ini`,
			want: platformSanitizerError(core.ErrInvalidArgument, core.ErrPathTraversalAttempt),
		},
		{
			name: "Verbatim UNC path",
			path: `\\?\UNC\server\share\payload.txt`,
			want: platformSanitizerError(core.ErrInvalidArgument, core.ErrPathTraversalAttempt),
		},
	}

	for _, tc := range unsafePaths {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("As source", func(t *testing.T) {
				volume, _ := createTestVolume(t, 2)
				writeVolumeFile(t, volume, "valid/payload.txt", "content")

				err := volume.MoveFile(ctx, tc.path, "valid/moved.txt")
				if !errors.Is(err, tc.want) {
					t.Fatalf("MoveFile(%q, %q) error = %v, want %v", tc.path, "valid/moved.txt", err, tc.want)
				}
				assertVolumeFileExists(t, volume, "valid/payload.txt", true)
			})

			t.Run("As destination", func(t *testing.T) {
				volume, _ := createTestVolume(t, 2)
				writeVolumeFile(t, volume, "valid/payload.txt", "content")

				err := volume.MoveFile(ctx, "valid/payload.txt", tc.path)
				if !errors.Is(err, tc.want) {
					t.Fatalf("MoveFile(%q, %q) error = %v, want %v", "valid/payload.txt", tc.path, err, tc.want)
				}
				assertVolumeFileExists(t, volume, "valid/payload.txt", true)

				if got := readVolumeFile(t, volume, "valid/payload.txt"); got != "content" {
					t.Errorf("source content = %q, want %q", got, "content")
				}
			})
		})
	}
}

// TestLocalFileSystemVolume_MoveFileMissingSource tests the not-found contract.
func TestLocalFileSystemVolume_MoveFileMissingSource(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 2)

	err := volume.MoveFile(ctx, "missing/payload.txt", "dst/payload.txt")
	if !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("MoveFile() error = %v, want %v", err, core.ErrFileNotFound)
	}

	assertVolumeFileExists(t, volume, "dst/payload.txt", false)
}

// TestLocalFileSystemVolume_MoveFileExistingDestination tests that an existing
// destination is refused instead of being silently overwritten.
func TestLocalFileSystemVolume_MoveFileExistingDestination(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 2)

	writeVolumeFile(t, volume, "src/payload.txt", "source-content")
	writeVolumeFile(t, volume, "dst/payload.txt", "existing-content")

	err := volume.MoveFile(ctx, "src/payload.txt", "dst/payload.txt")
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("MoveFile() error = %v, want %v", err, core.ErrInvalidArgument)
	}

	if got := readVolumeFile(t, volume, "dst/payload.txt"); got != "existing-content" {
		t.Errorf("existing destination content = %q, want %q", got, "existing-content")
	}
	if got := readVolumeFile(t, volume, "src/payload.txt"); got != "source-content" {
		t.Errorf("source content after rejected move = %q, want %q", got, "source-content")
	}
}

// TestLocalFileSystemVolume_MoveFileSamePathIsNoOp tests the no-op contract.
func TestLocalFileSystemVolume_MoveFileSamePathIsNoOp(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 2)

	writeVolumeFile(t, volume, "payload.txt", "same-content")

	testCases := []struct {
		name       string
		from       string
		to         string
		createFile bool
	}{
		{
			name:       "Identical paths",
			from:       "payload.txt",
			to:         "payload.txt",
			createFile: true,
		},
		{
			name:       "Equivalent relative spellings",
			from:       "./payload.txt",
			to:         "payload.txt",
			createFile: true,
		},
		{
			name:       "Identical missing paths",
			from:       "absent.txt",
			to:         "absent.txt",
			createFile: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := volume.MoveFile(ctx, tc.from, tc.to); err != nil {
				t.Fatalf("MoveFile(%q, %q) error = %v, want nil", tc.from, tc.to, err)
			}
		})
	}

	if got := readVolumeFile(t, volume, "payload.txt"); got != "same-content" {
		t.Errorf("content after no-op moves = %q, want %q", got, "same-content")
	}
}

// TestLocalFileSystemVolume_MoveFileCreatesDestinationParentDirectories tests
// that the destination parent chain is created before the move.
func TestLocalFileSystemVolume_MoveFileCreatesDestinationParentDirectories(t *testing.T) {
	ctx := context.Background()
	volume, mountPath := createTestVolume(t, 3)

	writeVolumeFile(t, volume, "payload.txt", "content")

	destinationParent := filepath.Join(mountPath, "new", "nested", "dir")
	if _, err := os.Stat(destinationParent); !os.IsNotExist(err) {
		t.Fatalf("precondition failed: %q already exists: %v", destinationParent, err)
	}

	if err := volume.MoveFile(ctx, "payload.txt", "new/nested/dir/payload.txt"); err != nil {
		t.Fatalf("MoveFile() error = %v", err)
	}

	assertVolumeFileExists(t, volume, "new/nested/dir/payload.txt", true)
	assertVolumeFileExists(t, volume, "payload.txt", false)
	assertPhysicalFileUnderRoot(t, mountPath, "new/nested/dir/payload.txt")
}

// TestLocalFileSystemVolume_MoveFileRejectsEmptyDestination tests the empty
// destination contract.
func TestLocalFileSystemVolume_MoveFileRejectsEmptyDestination(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 2)

	writeVolumeFile(t, volume, "payload.txt", "content")

	err := volume.MoveFile(ctx, "payload.txt", "")
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("MoveFile() error = %v, want %v", err, core.ErrInvalidArgument)
	}

	assertVolumeFileExists(t, volume, "payload.txt", true)
}

// TestLocalFileSystemVolume_MoveFileHonoursCancelledContext tests that no
// filesystem work happens for an already-cancelled context.
func TestLocalFileSystemVolume_MoveFileHonoursCancelledContext(t *testing.T) {
	volume, _ := createTestVolume(t, 2)

	writeVolumeFile(t, volume, "src/payload.txt", "content")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := volume.MoveFile(ctx, "src/payload.txt", "dst/payload.txt")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MoveFile() error = %v, want %v", err, context.Canceled)
	}

	assertVolumeFileExists(t, volume, "src/payload.txt", true)
	assertVolumeFileExists(t, volume, "dst/payload.txt", false)
}

// TestLocalFileSystemVolume_MoveFileCopyFallback tests the cross-device fallback
// directly: a temporary directory cannot span two filesystems, so the helper that
// os.Rename failures delegate to is exercised on its own.
func TestLocalFileSystemVolume_MoveFileCopyFallback(t *testing.T) {
	volume, mountPath := createTestVolume(t, 0)

	content := strings.Repeat("fallback-content-", 4096)
	sourcePath := filepath.Join(mountPath, "src", "payload.bin")
	destinationPath := filepath.Join(mountPath, "dst", "payload.bin")

	t.Run("Copies then removes the source", func(t *testing.T) {
		writeVolumeFile(t, volume, "src/payload.bin", content)

		if err := os.MkdirAll(filepath.Dir(destinationPath), 0755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(destinationPath), err)
		}

		if err := volume.copyFileAndRemoveSource(context.Background(), sourcePath, destinationPath); err != nil {
			t.Fatalf("copyFileAndRemoveSource() error = %v", err)
		}

		if got := readVolumeFile(t, volume, "dst/payload.bin"); got != content {
			t.Errorf("destination length = %d, want %d", len(got), len(content))
		}
		assertVolumeFileExists(t, volume, "src/payload.bin", false)

		entries, err := os.ReadDir(filepath.Dir(destinationPath))
		if err != nil {
			t.Fatalf("ReadDir(%q) error = %v", filepath.Dir(destinationPath), err)
		}
		if len(entries) != 1 || entries[0].Name() != "payload.bin" {
			t.Errorf("destination directory contents = %v, want exactly payload.bin", entries)
		}
	})

	t.Run("Honours a cancelled context", func(t *testing.T) {
		writeVolumeFile(t, volume, "src/payload.bin", content)
		_ = os.Remove(destinationPath)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := volume.copyFileAndRemoveSource(ctx, sourcePath, destinationPath)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("copyFileAndRemoveSource() error = %v, want %v", err, context.Canceled)
		}

		assertVolumeFileExists(t, volume, "src/payload.bin", true)
		if _, err := os.Stat(destinationPath); !os.IsNotExist(err) {
			t.Errorf("destination created for a cancelled context: %v", err)
		}
	})
}

// TestIsCrossDeviceError tests the classification that decides whether a failed
// rename can still be satisfied by a copy instead of being reported.
func TestIsCrossDeviceError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "EXDEV link error",
			err:  &os.LinkError{Op: "rename", Old: "src", New: "dst", Err: syscall.EXDEV},
			want: true,
		},
		{
			name: "Wrapped cross-device wording",
			err:  fmt.Errorf("rename failed: %w", errors.New("invalid cross-device link")),
			want: true,
		},
		{
			name: "Permission failure",
			err:  &os.LinkError{Op: "rename", Old: "src", New: "dst", Err: os.ErrPermission},
			want: false,
		},
		{
			name: "Missing source",
			err:  &os.LinkError{Op: "rename", Old: "src", New: "dst", Err: os.ErrNotExist},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCrossDeviceError(tc.err); got != tc.want {
				t.Errorf("isCrossDeviceError(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// TestLocalFileSystemVolume_ShardingDepth tests the configured depth is reported.
func TestLocalFileSystemVolume_ShardingDepth(t *testing.T) {
	testCases := []struct {
		name       string
		shardDepth int
	}{
		{"No sharding", 0},
		{"Two level sharding", 2},
		{"Three level sharding", 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, _ := createTestVolume(t, tc.shardDepth)

			if got := volume.ShardingDepth(); got != tc.shardDepth {
				t.Errorf("ShardingDepth() = %d, want %d", got, tc.shardDepth)
			}
		})
	}
}

// TestLocalFileSystemVolume_ImplementsOptionalCapabilities tests that the two
// optional capabilities are discoverable through the core.StorageVolume value
// that callers such as cleanup actually hold.
func TestLocalFileSystemVolume_ImplementsOptionalCapabilities(t *testing.T) {
	volume, _ := createTestVolume(t, 2)

	var storageVolume core.StorageVolume = volume

	if _, ok := storageVolume.(core.FileMover); !ok {
		t.Error("LocalFileSystemVolume does not implement core.FileMover")
	}
	if _, ok := storageVolume.(core.ShardingDepthProvider); !ok {
		t.Error("LocalFileSystemVolume does not implement core.ShardingDepthProvider")
	}
}
