package metadata

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// Helper function to create a test repository
func createTestRepository(t *testing.T) (core.MetadataRepository, string) {
	t.Helper()

	tempDir := t.TempDir()
	opts := &BadgerRepositoryOptions{
		TenantID:       "test-tenant",
		DataPath:       tempDir,
		CacheTTL:       1 * time.Second, // Short TTL for testing
		GCInterval:     1 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := NewBadgerMetadataRepository(opts)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	return repo, tempDir
}

// Helper function to create test metadata
func createTestMetadata(fileKey string, status core.FileProcessingStatus) *core.FileMetadata {
	now := time.Now()
	return &core.FileMetadata{
		FileKey:          fileKey,
		TenantID:         "test-tenant",
		OriginalFileName: fileKey + ".txt",
		FileSize:         1024,
		VolumeID:         "volume-1",
		PhysicalPath:     "path/to/" + fileKey,
		Status:           status,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

// TestNewBadgerMetadataRepository tests creating new repositories.
func TestNewBadgerMetadataRepository(t *testing.T) {
	testCases := []struct {
		name      string
		opts      *BadgerRepositoryOptions
		wantError bool
	}{
		{
			name: "Valid options",
			opts: &BadgerRepositoryOptions{
				TenantID: "test-tenant",
				DataPath: t.TempDir(),
			},
			wantError: false,
		},
		{
			name:      "Nil options",
			opts:      nil,
			wantError: true,
		},
		{
			name: "Empty tenant ID",
			opts: &BadgerRepositoryOptions{
				TenantID: "",
				DataPath: t.TempDir(),
			},
			wantError: true,
		},
		{
			name: "Empty data path",
			opts: &BadgerRepositoryOptions{
				TenantID: "test",
				DataPath: "",
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := NewBadgerMetadataRepository(tc.opts)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if repo == nil {
					t.Fatal("Expected non-nil repository")
				}

				// Clean up
				if closer, ok := repo.(*BadgerMetadataRepository); ok {
					_ = closer.Close()
				}
			}
		})
	}
}

// TestBadgerRepository_AddOrUpdate tests adding and updating metadata.
func TestBadgerRepository_AddOrUpdate(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	t.Run("Add new metadata", func(t *testing.T) {
		metadata := createTestMetadata("file1", core.FileStatusPending)

		err := repo.AddOrUpdate(ctx, metadata)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify it was saved
		retrieved, err := repo.Get(ctx, "file1")
		if err != nil {
			t.Fatalf("Failed to get metadata: %v", err)
		}

		if retrieved.FileKey != "file1" {
			t.Errorf("Expected file key 'file1', got %s", retrieved.FileKey)
		}
	})

	t.Run("Update existing metadata", func(t *testing.T) {
		metadata := createTestMetadata("file1", core.FileStatusProcessing)

		err := repo.AddOrUpdate(ctx, metadata)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify it was updated
		retrieved, err := repo.Get(ctx, "file1")
		if err != nil {
			t.Fatalf("Failed to get metadata: %v", err)
		}

		if retrieved.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", retrieved.Status)
		}
	})

	t.Run("Nil metadata", func(t *testing.T) {
		err := repo.AddOrUpdate(ctx, nil)
		if err == nil {
			t.Fatal("Expected error for nil metadata")
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		metadata := createTestMetadata("", core.FileStatusPending)

		err := repo.AddOrUpdate(ctx, metadata)
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

// TestBadgerRepository_Get tests retrieving metadata.
func TestBadgerRepository_Get(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Add test data
	metadata := createTestMetadata("file1", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, metadata)

	t.Run("Get existing file", func(t *testing.T) {
		retrieved, err := repo.Get(ctx, "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if retrieved.FileKey != "file1" {
			t.Errorf("Expected file key 'file1', got %s", retrieved.FileKey)
		}
	})

	t.Run("Get non-existent file", func(t *testing.T) {
		_, err := repo.Get(ctx, "non-existent")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		_, err := repo.Get(ctx, "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

// TestBadgerRepository_Delete tests deleting metadata.
func TestBadgerRepository_Delete(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Add test data
	metadata := createTestMetadata("file1", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, metadata)

	t.Run("Delete existing file", func(t *testing.T) {
		err := repo.Delete(ctx, "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify it was deleted
		_, err = repo.Get(ctx, "file1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound after deletion, got %v", err)
		}
	})

	t.Run("Delete non-existent file", func(t *testing.T) {
		// Should not error
		err := repo.Delete(ctx, "non-existent")
		if err != nil {
			t.Errorf("Expected no error for deleting non-existent file, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := repo.Delete(ctx, "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

// TestBadgerRepository_GetByStatus tests querying by status.
func TestBadgerRepository_GetByStatus(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Add test data with different statuses
	_ = repo.AddOrUpdate(ctx, createTestMetadata("pending1", core.FileStatusPending))
	_ = repo.AddOrUpdate(ctx, createTestMetadata("pending2", core.FileStatusPending))
	_ = repo.AddOrUpdate(ctx, createTestMetadata("processing1", core.FileStatusProcessing))
	_ = repo.AddOrUpdate(ctx, createTestMetadata("completed1", core.FileStatusCompleted))
	_ = repo.AddOrUpdate(ctx, createTestMetadata("failed1", core.FileStatusFailed))

	t.Run("Get pending files", func(t *testing.T) {
		results, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 0)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 pending files, got %d", len(results))
		}
	})

	t.Run("Get processing files", func(t *testing.T) {
		results, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusProcessing, 0)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 processing file, got %d", len(results))
		}
	})

	t.Run("Get with limit", func(t *testing.T) {
		results, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 file with limit, got %d", len(results))
		}
	})
}

// TestBadgerRepository_GetPendingFiles tests getting pending files.
func TestBadgerRepository_GetPendingFiles(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	now := time.Now()
	future := now.Add(1 * time.Hour)
	past := now.Add(-1 * time.Hour)

	// Add files with different availability times
	file1 := createTestMetadata("available-now", core.FileStatusPending)
	file1.AvailableForProcessingAt = nil // Available immediately
	_ = repo.AddOrUpdate(ctx, file1)

	file2 := createTestMetadata("available-past", core.FileStatusPending)
	file2.AvailableForProcessingAt = &past
	_ = repo.AddOrUpdate(ctx, file2)

	file3 := createTestMetadata("not-available-yet", core.FileStatusPending)
	file3.AvailableForProcessingAt = &future
	_ = repo.AddOrUpdate(ctx, file3)

	file4 := createTestMetadata("processing", core.FileStatusProcessing)
	_ = repo.AddOrUpdate(ctx, file4)

	t.Run("Get available pending files", func(t *testing.T) {
		results, err := repo.GetPendingFiles(ctx, "test-tenant", 0)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Should get 2 files: available-now and available-past
		if len(results) != 2 {
			t.Errorf("Expected 2 available files, got %d", len(results))
		}
	})

	t.Run("Get with limit", func(t *testing.T) {
		results, err := repo.GetPendingFiles(ctx, "test-tenant", 1)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 file with limit, got %d", len(results))
		}
	})
}

// TestBadgerRepository_UpdateStatus tests updating status.
func TestBadgerRepository_UpdateStatus(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Add test data
	metadata := createTestMetadata("file1", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, metadata)

	t.Run("Update to processing", func(t *testing.T) {
		err := repo.UpdateStatus(ctx, "file1", core.FileStatusProcessing)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify status was updated
		retrieved, _ := repo.Get(ctx, "file1")
		if retrieved.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", retrieved.Status)
		}
	})

	t.Run("Update non-existent file", func(t *testing.T) {
		err := repo.UpdateStatus(ctx, "non-existent", core.FileStatusCompleted)
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := repo.UpdateStatus(ctx, "", core.FileStatusCompleted)
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

// TestBadgerRepository_GetTimedOutProcessingFiles tests getting timed out files.
func TestBadgerRepository_GetTimedOutProcessingFiles(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	now := time.Now()
	longAgo := now.Add(-2 * time.Hour)
	recent := now.Add(-30 * time.Second)

	// Add files with different processing start times
	file1 := createTestMetadata("timed-out", core.FileStatusProcessing)
	file1.ProcessingStartTime = &longAgo
	_ = repo.AddOrUpdate(ctx, file1)

	file2 := createTestMetadata("still-processing", core.FileStatusProcessing)
	file2.ProcessingStartTime = &recent
	_ = repo.AddOrUpdate(ctx, file2)

	file3 := createTestMetadata("pending", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, file3)

	t.Run("Get timed out files", func(t *testing.T) {
		timeout := 1 * time.Hour
		results, err := repo.GetTimedOutProcessingFiles(ctx, timeout)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Should get 1 file: timed-out
		if len(results) != 1 {
			t.Errorf("Expected 1 timed out file, got %d", len(results))
		}

		if len(results) > 0 && results[0].FileKey != "timed-out" {
			t.Errorf("Expected timed-out file, got %s", results[0].FileKey)
		}
	})

	t.Run("Short timeout gets more files", func(t *testing.T) {
		timeout := 10 * time.Second
		results, err := repo.GetTimedOutProcessingFiles(ctx, timeout)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Should get 2 files: both timed-out and still-processing
		if len(results) != 2 {
			t.Errorf("Expected 2 timed out files, got %d", len(results))
		}
	})
}

// TestBadgerRepository_Cache tests caching behavior.
func TestBadgerRepository_Cache(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	t.Run("Active files are cached", func(t *testing.T) {
		// Add a pending file
		metadata := createTestMetadata("cached-file", core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)

		// First get - should cache it
		_, _ = repo.Get(ctx, "cached-file")

		// Check cache stats
		concreteRepo := repo.(*BadgerMetadataRepository)
		stats := concreteRepo.GetCacheStats()

		totalEntries := stats["total_entries"].(int)
		if totalEntries < 1 {
			t.Error("Expected at least 1 cached entry")
		}
	})

	t.Run("Completed files are not cached", func(t *testing.T) {
		// Add a completed file
		metadata := createTestMetadata("completed-file", core.FileStatusCompleted)
		_ = repo.AddOrUpdate(ctx, metadata)

		// Get it
		_, _ = repo.Get(ctx, "completed-file")

		// It should not be in cache (only active files)
		concreteRepo := repo.(*BadgerMetadataRepository)
		cached := concreteRepo.cache.get("completed-file")
		if cached != nil {
			t.Error("Completed file should not be cached")
		}
	})

	t.Run("Cache expires", func(t *testing.T) {
		// Add a pending file
		metadata := createTestMetadata("expiring-file", core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)

		// Get it to cache
		_, _ = repo.Get(ctx, "expiring-file")

		// Wait for cache to expire (TTL is 1 second in test)
		time.Sleep(1500 * time.Millisecond)

		// Should not be in cache anymore
		concreteRepo := repo.(*BadgerMetadataRepository)
		cached := concreteRepo.cache.get("expiring-file")
		if cached != nil {
			t.Error("Cache should have expired")
		}
	})
}

// TestBadgerRepository_ConcurrentAccess tests concurrent operations.
func TestBadgerRepository_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			metadata := createTestMetadata(
				fmt.Sprintf("concurrent-file-%d", id),
				core.FileStatusPending,
			)
			_ = repo.AddOrUpdate(ctx, metadata)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all files were written
	results, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 0)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(results) < numGoroutines {
		t.Errorf("Expected at least %d files, got %d", numGoroutines, len(results))
	}
}

// TestBadgerRepository_Close tests closing the repository.
func TestBadgerRepository_Close(t *testing.T) {
	ctx := context.Background()
	repo, tempDir := createTestRepository(t)

	// Add some data
	metadata := createTestMetadata("file1", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, metadata)

	// Close the repository
	concreteRepo := repo.(*BadgerMetadataRepository)
	err := concreteRepo.Close()
	if err != nil {
		t.Fatalf("Expected no error on close, got %v", err)
	}

	// Operations after close should fail
	err = repo.AddOrUpdate(ctx, metadata)
	if err == nil {
		t.Error("Expected error for operation after close")
	}

	// Clean up temp directory
	_ = os.RemoveAll(tempDir)
}
