package volume

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// Helper function to create a temporary test volume
func createTestVolume(t *testing.T, shardDepth int) (*LocalFileSystemVolume, string) {
	t.Helper()

	tempDir := t.TempDir()
	opts := &LocalFileSystemVolumeOptions{
		VolumeID:   "test-volume",
		MountPath:  tempDir,
		ShardDepth: shardDepth,
	}

	volume, err := NewLocalFileSystemVolume(opts)
	if err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Type assert to concrete type for testing
	concreteVolume, ok := volume.(*LocalFileSystemVolume)
	if !ok {
		t.Fatalf("Expected *LocalFileSystemVolume, got %T", volume)
	}

	return concreteVolume, tempDir
}

// TestNewLocalFileSystemVolume tests creating new volumes.
func TestNewLocalFileSystemVolume(t *testing.T) {
	testCases := []struct {
		name      string
		opts      *LocalFileSystemVolumeOptions
		wantError bool
	}{
		{
			name: "Valid options",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test-volume",
				MountPath:  t.TempDir(),
				ShardDepth: 2,
			},
			wantError: false,
		},
		{
			name:      "Nil options",
			opts:      nil,
			wantError: true,
		},
		{
			name: "Empty volume ID",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "",
				MountPath:  t.TempDir(),
				ShardDepth: 0,
			},
			wantError: true,
		},
		{
			name: "Empty mount path",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				MountPath:  "",
				ShardDepth: 0,
			},
			wantError: true,
		},
		{
			name: "Invalid shard depth (negative)",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				MountPath:  t.TempDir(),
				ShardDepth: -1,
			},
			wantError: true,
		},
		{
			name: "Invalid shard depth (too large)",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				MountPath:  t.TempDir(),
				ShardDepth: 4,
			},
			wantError: true,
		},
		{
			name: "Shard depth 0",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				MountPath:  t.TempDir(),
				ShardDepth: 0,
			},
			wantError: false,
		},
		{
			name: "Shard depth 3",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				MountPath:  t.TempDir(),
				ShardDepth: 3,
			},
			wantError: false,
		},
		{
			name: "Valid VolumeType (LocalFileSystem)",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				VolumeType: "LocalFileSystem",
				MountPath:  t.TempDir(),
				ShardDepth: 2,
			},
			wantError: false,
		},
		{
			name: "Empty VolumeType (default)",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				VolumeType: "",
				MountPath:  t.TempDir(),
				ShardDepth: 2,
			},
			wantError: false,
		},
		{
			name: "Invalid VolumeType (S3)",
			opts: &LocalFileSystemVolumeOptions{
				VolumeID:   "test",
				VolumeType: "S3",
				MountPath:  t.TempDir(),
				ShardDepth: 2,
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, err := NewLocalFileSystemVolume(tc.opts)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if volume == nil {
					t.Fatal("Expected non-nil volume")
				}
			}
		})
	}
}

// TestLocalFileSystemVolume_VolumeID tests VolumeID method.
func TestLocalFileSystemVolume_VolumeID(t *testing.T) {
	volume, _ := createTestVolume(t, 0)

	if volume.VolumeID() != "test-volume" {
		t.Errorf("Expected volume ID 'test-volume', got %s", volume.VolumeID())
	}
}

// TestLocalFileSystemVolume_MountPath tests MountPath method.
func TestLocalFileSystemVolume_MountPath(t *testing.T) {
	volume, tempDir := createTestVolume(t, 0)

	mountPath := volume.MountPath()
	if mountPath != tempDir {
		t.Errorf("Expected mount path %s, got %s", tempDir, mountPath)
	}
}

// TestLocalFileSystemVolume_IsHealthy tests health check.
func TestLocalFileSystemVolume_IsHealthy(t *testing.T) {
	ctx := context.Background()

	t.Run("Healthy volume", func(t *testing.T) {
		volume, _ := createTestVolume(t, 0)

		if !volume.IsHealthy(ctx) {
			t.Error("Expected volume to be healthy")
		}
	})

	t.Run("Unhealthy volume (deleted mount path)", func(t *testing.T) {
		volume, tempDir := createTestVolume(t, 0)

		// Remove the mount path
		os.RemoveAll(tempDir)

		if volume.IsHealthy(ctx) {
			t.Error("Expected volume to be unhealthy after mount path deletion")
		}
	})
}

// TestLocalFileSystemVolume_TotalCapacity tests capacity retrieval.
func TestLocalFileSystemVolume_TotalCapacity(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	capacity, err := volume.TotalCapacity(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if capacity <= 0 {
		t.Errorf("Expected positive capacity, got %d", capacity)
	}

	t.Logf("Total capacity: %d bytes", capacity)
}

// TestLocalFileSystemVolume_AvailableSpace tests available space retrieval.
func TestLocalFileSystemVolume_AvailableSpace(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	available, err := volume.AvailableSpace(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if available <= 0 {
		t.Errorf("Expected positive available space, got %d", available)
	}

	t.Logf("Available space: %d bytes", available)
}

// TestLocalFileSystemVolume_WriteFile tests file writing.
func TestLocalFileSystemVolume_WriteFile(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	testCases := []struct {
		name         string
		relativePath string
		content      string
		wantError    bool
	}{
		{
			name:         "Simple file",
			relativePath: "test.txt",
			content:      "Hello, World!",
			wantError:    false,
		},
		{
			name:         "File in subdirectory",
			relativePath: "subdir/test.txt",
			content:      "Test content",
			wantError:    false,
		},
		{
			name:         "File in deep subdirectory",
			relativePath: "a/b/c/test.txt",
			content:      "Deep content",
			wantError:    false,
		},
		{
			name:         "Empty content",
			relativePath: "empty.txt",
			content:      "",
			wantError:    false,
		},
		{
			name:         "Large content",
			relativePath: "large.txt",
			content:      strings.Repeat("A", 10000),
			wantError:    false,
		},
		{
			name:         "Path traversal attempt",
			relativePath: "../escape.txt",
			content:      "Malicious",
			wantError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader([]byte(tc.content))
			written, err := volume.WriteFile(ctx, tc.relativePath, reader)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				expectedSize := int64(len(tc.content))
				if written != expectedSize {
					t.Errorf("Expected %d bytes written, got %d", expectedSize, written)
				}
			}
		})
	}
}

// TestLocalFileSystemVolume_ReadFile tests file reading.
func TestLocalFileSystemVolume_ReadFile(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	// Write a test file first
	testContent := "Test file content"
	relativePath := "read-test.txt"
	reader := bytes.NewReader([]byte(testContent))
	_, err := volume.WriteFile(ctx, relativePath, reader)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	t.Run("Read existing file", func(t *testing.T) {
		readCloser, err := volume.ReadFile(ctx, relativePath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		defer readCloser.Close()

		content, err := io.ReadAll(readCloser)
		if err != nil {
			t.Fatalf("Failed to read content: %v", err)
		}

		if string(content) != testContent {
			t.Errorf("Expected content %s, got %s", testContent, string(content))
		}
	})

	t.Run("Read non-existent file", func(t *testing.T) {
		_, err := volume.ReadFile(ctx, "non-existent.txt")
		if err == nil {
			t.Fatal("Expected error for non-existent file, got nil")
		}

		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Path traversal attempt", func(t *testing.T) {
		_, err := volume.ReadFile(ctx, "../escape.txt")
		if err == nil {
			t.Fatal("Expected error for path traversal, got nil")
		}
	})
}

// TestLocalFileSystemVolume_DeleteFile tests file deletion.
func TestLocalFileSystemVolume_DeleteFile(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	// Write a test file
	relativePath := "delete-test.txt"
	reader := bytes.NewReader([]byte("Test content"))
	_, err := volume.WriteFile(ctx, relativePath, reader)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	t.Run("Delete existing file", func(t *testing.T) {
		err := volume.DeleteFile(ctx, relativePath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file is deleted
		exists, _ := volume.FileExists(ctx, relativePath)
		if exists {
			t.Error("File should not exist after deletion")
		}
	})

	t.Run("Delete non-existent file", func(t *testing.T) {
		// Should not error when deleting non-existent file
		err := volume.DeleteFile(ctx, "non-existent.txt")
		if err != nil {
			t.Errorf("Expected no error for deleting non-existent file, got %v", err)
		}
	})

	t.Run("Path traversal attempt", func(t *testing.T) {
		err := volume.DeleteFile(ctx, "../escape.txt")
		if err == nil {
			t.Fatal("Expected error for path traversal, got nil")
		}
	})
}

// TestLocalFileSystemVolume_FileExists tests file existence check.
func TestLocalFileSystemVolume_FileExists(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	// Write a test file
	relativePath := "exists-test.txt"
	reader := bytes.NewReader([]byte("Test content"))
	_, err := volume.WriteFile(ctx, relativePath, reader)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	t.Run("Existing file", func(t *testing.T) {
		exists, err := volume.FileExists(ctx, relativePath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if !exists {
			t.Error("File should exist")
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		exists, err := volume.FileExists(ctx, "non-existent.txt")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if exists {
			t.Error("File should not exist")
		}
	})

	t.Run("Path traversal attempt", func(t *testing.T) {
		_, err := volume.FileExists(ctx, "../escape.txt")
		if err == nil {
			t.Fatal("Expected error for path traversal, got nil")
		}
	})
}

// TestLocalFileSystemVolume_BuildFilePath tests sharded path building.
func TestLocalFileSystemVolume_BuildFilePath(t *testing.T) {
	testCases := []struct {
		name       string
		shardDepth int
		fileKey    string
		extension  string
		wantPath   string
		wantError  bool
	}{
		{
			name:       "No sharding, no extension",
			shardDepth: 0,
			fileKey:    "a1b2c3d4e5f6",
			extension:  "",
			wantPath:   "a1b2c3d4e5f6",
			wantError:  false,
		},
		{
			name:       "No sharding, with extension",
			shardDepth: 0,
			fileKey:    "a1b2c3d4e5f6",
			extension:  ".pdf",
			wantPath:   "a1b2c3d4e5f6.pdf",
			wantError:  false,
		},
		{
			name:       "One level sharding",
			shardDepth: 1,
			fileKey:    "a1b2c3d4e5f6",
			extension:  ".txt",
			wantPath:   filepath.Join("a1", "a1b2c3d4e5f6.txt"),
			wantError:  false,
		},
		{
			name:       "Two level sharding",
			shardDepth: 2,
			fileKey:    "a1b2c3d4e5f6",
			extension:  ".jpg",
			wantPath:   filepath.Join("a1", "b2", "a1b2c3d4e5f6.jpg"),
			wantError:  false,
		},
		{
			name:       "Three level sharding",
			shardDepth: 3,
			fileKey:    "a1b2c3d4e5f6",
			extension:  "",
			wantPath:   filepath.Join("a1", "b2", "c3", "a1b2c3d4e5f6"),
			wantError:  false,
		},
		{
			name:       "UUID without dashes",
			shardDepth: 2,
			fileKey:    "550e8400e29b41d4a716446655440000",
			extension:  ".pdf",
			wantPath:   filepath.Join("55", "0e", "550e8400e29b41d4a716446655440000.pdf"),
			wantError:  false,
		},
		{
			name:       "Empty file key",
			shardDepth: 1,
			fileKey:    "",
			extension:  ".txt",
			wantError:  true,
		},
		{
			name:       "File key too short",
			shardDepth: 2,
			fileKey:    "a1",
			extension:  "",
			wantError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, _ := createTestVolume(t, tc.shardDepth)

			path, err := volume.BuildFilePath(tc.fileKey, tc.extension)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if path != tc.wantPath {
					t.Errorf("Expected path %s, got %s", tc.wantPath, path)
				}
			}
		})
	}
}

// TestLocalFileSystemVolume_GetFileSize tests file size retrieval.
func TestLocalFileSystemVolume_GetFileSize(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	testCases := []struct {
		name     string
		content  string
		wantSize int64
	}{
		{
			name:     "Empty file",
			content:  "",
			wantSize: 0,
		},
		{
			name:     "Small file",
			content:  "Hello",
			wantSize: 5,
		},
		{
			name:     "Large file",
			content:  strings.Repeat("A", 10000),
			wantSize: 10000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			relativePath := "size-test.txt"
			reader := bytes.NewReader([]byte(tc.content))
			_, err := volume.WriteFile(ctx, relativePath, reader)
			if err != nil {
				t.Fatalf("Failed to write test file: %v", err)
			}

			size, err := volume.GetFileSize(ctx, relativePath)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if size != tc.wantSize {
				t.Errorf("Expected size %d, got %d", tc.wantSize, size)
			}

			// Clean up
			volume.DeleteFile(ctx, relativePath)
		})
	}

	t.Run("Non-existent file", func(t *testing.T) {
		_, err := volume.GetFileSize(ctx, "non-existent.txt")
		if err == nil {
			t.Fatal("Expected error for non-existent file, got nil")
		}
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})
}

// TestLocalFileSystemVolume_ShardedFileOperations tests file operations with sharding.
func TestLocalFileSystemVolume_ShardedFileOperations(t *testing.T) {
	ctx := context.Background()

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
			volume, _ := createTestVolume(t, tc.shardDepth)

			// Build sharded path
			fileKey := "550e8400e29b41d4a716446655440000"
			relativePath, err := volume.BuildFilePath(fileKey, ".txt")
			if err != nil {
				t.Fatalf("Failed to build file path: %v", err)
			}

			// Write file
			testContent := "Sharded file content"
			reader := bytes.NewReader([]byte(testContent))
			written, err := volume.WriteFile(ctx, relativePath, reader)
			if err != nil {
				t.Fatalf("Failed to write file: %v", err)
			}

			if written != int64(len(testContent)) {
				t.Errorf("Expected %d bytes written, got %d", len(testContent), written)
			}

			// Check existence
			exists, err := volume.FileExists(ctx, relativePath)
			if err != nil {
				t.Fatalf("Failed to check file existence: %v", err)
			}
			if !exists {
				t.Error("File should exist")
			}

			// Read file
			readCloser, err := volume.ReadFile(ctx, relativePath)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			content, err := io.ReadAll(readCloser)
			readCloser.Close() // Close before deletion on Windows
			if err != nil {
				t.Fatalf("Failed to read content: %v", err)
			}

			if string(content) != testContent {
				t.Errorf("Expected content %s, got %s", testContent, string(content))
			}

			// Get file size
			size, err := volume.GetFileSize(ctx, relativePath)
			if err != nil {
				t.Fatalf("Failed to get file size: %v", err)
			}

			if size != int64(len(testContent)) {
				t.Errorf("Expected size %d, got %d", len(testContent), size)
			}

			// Delete file
			err = volume.DeleteFile(ctx, relativePath)
			if err != nil {
				t.Fatalf("Failed to delete file: %v", err)
			}

			// Verify deletion
			exists, err = volume.FileExists(ctx, relativePath)
			if err != nil {
				t.Fatalf("Failed to check file existence: %v", err)
			}
			if exists {
				t.Error("File should not exist after deletion")
			}
		})
	}
}

// TestLocalFileSystemVolume_ConcurrentWrites tests concurrent file writes.
func TestLocalFileSystemVolume_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 2)

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			fileKey := fmt.Sprintf("concurrent-%d-file", id)
			relativePath, _ := volume.BuildFilePath(fileKey, ".txt")
			content := fmt.Sprintf("Content from goroutine %d", id)
			reader := bytes.NewReader([]byte(content))

			_, err := volume.WriteFile(ctx, relativePath, reader)
			if err != nil {
				t.Errorf("Goroutine %d failed to write file: %v", id, err)
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
