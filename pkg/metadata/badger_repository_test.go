package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
		wantIs    error
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
			wantIs:    core.ErrInvalidArgument,
		},
		{
			name: "Tenant ID cannot escape the data path",
			opts: &BadgerRepositoryOptions{
				TenantID: "../../escaped",
				DataPath: t.TempDir(),
			},
			wantError: true,
			wantIs:    core.ErrPathTraversalAttempt,
		},
		{
			name: "Tenant ID cannot contain a path separator",
			opts: &BadgerRepositoryOptions{
				TenantID: "tenant/sub",
				DataPath: t.TempDir(),
			},
			wantError: true,
			wantIs:    core.ErrPathTraversalAttempt,
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
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Errorf("error = %v, want %v", err, tc.wantIs)
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
		retrieved, err := repo.Get(ctx, "test-tenant", "file1")
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
		retrieved, err := repo.Get(ctx, "test-tenant", "file1")
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
		retrieved, err := repo.Get(ctx, "test-tenant", "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if retrieved.FileKey != "file1" {
			t.Errorf("Expected file key 'file1', got %s", retrieved.FileKey)
		}
	})

	t.Run("Get non-existent file", func(t *testing.T) {
		_, err := repo.Get(ctx, "test-tenant", "non-existent")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		_, err := repo.Get(ctx, "test-tenant", "")
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
		err := repo.Delete(ctx, "test-tenant", "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify it was deleted
		_, err = repo.Get(ctx, "test-tenant", "file1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound after deletion, got %v", err)
		}
	})

	t.Run("Delete non-existent file", func(t *testing.T) {
		// Should not error
		err := repo.Delete(ctx, "test-tenant", "non-existent")
		if err != nil {
			t.Errorf("Expected no error for deleting non-existent file, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := repo.Delete(ctx, "test-tenant", "")
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
		err := repo.UpdateStatus(ctx, "test-tenant", "file1", core.FileStatusProcessing)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify status was updated
		retrieved, _ := repo.Get(ctx, "test-tenant", "file1")
		if retrieved.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", retrieved.Status)
		}
	})

	t.Run("Update non-existent file", func(t *testing.T) {
		err := repo.UpdateStatus(ctx, "test-tenant", "non-existent", core.FileStatusCompleted)
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := repo.UpdateStatus(ctx, "test-tenant", "", core.FileStatusCompleted)
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
		results, err := repo.GetTimedOutProcessingFiles(ctx, "test-tenant", timeout)
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
		results, err := repo.GetTimedOutProcessingFiles(ctx, "test-tenant", timeout)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Should get 2 files: both timed-out and still-processing
		if len(results) != 2 {
			t.Errorf("Expected 2 timed out files, got %d", len(results))
		}
	})
}

func TestBadgerRepository_CompareAndUpdateProcessing(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	t.Run("matching lease updates metadata and status index", func(t *testing.T) {
		leaseStart := time.Date(2026, time.September, 17, 11, 0, 0, 0, time.UTC)
		file := createTestMetadata("matching-lease", core.FileStatusProcessing)
		file.ProcessingStartTime = &leaseStart
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata: %v", err)
		}

		updated, err := repo.CompareAndUpdateProcessing(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "matching-lease",
			ProcessingStartTimeUTC: leaseStart,
		}, func(current *core.FileMetadata) error {
			current.Status = core.FileStatusPending
			current.ProcessingStartTime = nil
			current.RetryCount = 1
			return nil
		})
		if err != nil {
			t.Fatalf("compare and update: %v", err)
		}
		if updated.Status != core.FileStatusPending || updated.RetryCount != 1 {
			t.Errorf("updated metadata = status %s retry %d, want Pending retry 1", updated.Status, updated.RetryCount)
		}

		processing, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusProcessing, 0)
		if err != nil {
			t.Fatalf("get processing index: %v", err)
		}
		if len(processing) != 0 {
			t.Errorf("processing index contains %d records, want 0", len(processing))
		}
		pending, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 0)
		if err != nil {
			t.Fatalf("get pending index: %v", err)
		}
		if len(pending) != 1 || pending[0].FileKey != "matching-lease" {
			t.Errorf("pending index = %#v, want matching-lease", pending)
		}
	})

	t.Run("stale timestamp returns details and leaves metadata unchanged", func(t *testing.T) {
		activeStart := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
		file := createTestMetadata("stale-lease", core.FileStatusProcessing)
		file.ProcessingStartTime = &activeStart
		file.RetryCount = 2
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata: %v", err)
		}

		_, err := repo.CompareAndUpdateProcessing(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "stale-lease",
			ProcessingStartTimeUTC: activeStart.Add(-time.Minute),
		}, func(current *core.FileMetadata) error {
			current.Status = core.FileStatusPending
			current.RetryCount++
			return nil
		})
		if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
			t.Fatalf("error = %v, want ErrProcessingLeaseMismatch", err)
		}
		var mismatch *core.FileProcessingLeaseMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("error type = %T, want FileProcessingLeaseMismatchError", err)
		}
		if mismatch.ActualProcessingStartTimeUTC == nil || !mismatch.ActualProcessingStartTimeUTC.Equal(activeStart) {
			t.Errorf("actual lease start = %v, want %v", mismatch.ActualProcessingStartTimeUTC, activeStart)
		}
		if mismatch.ActualStatus == nil || *mismatch.ActualStatus != core.FileStatusProcessing {
			t.Errorf("actual status = %v, want Processing", mismatch.ActualStatus)
		}

		current, getErr := repo.Get(ctx, "test-tenant", "stale-lease")
		if getErr != nil {
			t.Fatalf("get metadata: %v", getErr)
		}
		if current.Status != core.FileStatusProcessing || current.RetryCount != 2 {
			t.Errorf("metadata changed to status %s retry %d", current.Status, current.RetryCount)
		}
	})

	t.Run("wrong tenant cannot mutate the owning tenant", func(t *testing.T) {
		activeStart := time.Date(2026, time.September, 17, 13, 0, 0, 0, time.UTC)
		file := createTestMetadata("tenant-isolated-lease", core.FileStatusProcessing)
		file.ProcessingStartTime = &activeStart
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata: %v", err)
		}

		_, err := repo.CompareAndUpdateProcessing(ctx, core.FileProcessingLease{
			TenantID:               "other-tenant",
			FileKey:                "tenant-isolated-lease",
			ProcessingStartTimeUTC: activeStart,
		}, func(current *core.FileMetadata) error {
			current.Status = core.FileStatusPending
			return nil
		})
		if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
			t.Fatalf("error = %v, want ErrProcessingLeaseMismatch", err)
		}

		current, getErr := repo.Get(ctx, "test-tenant", "tenant-isolated-lease")
		if getErr != nil {
			t.Fatalf("get owning metadata: %v", getErr)
		}
		if current.Status != core.FileStatusProcessing {
			t.Errorf("owning metadata status = %s, want Processing", current.Status)
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
		_, _ = repo.Get(ctx, "test-tenant", "cached-file")

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
		_, _ = repo.Get(ctx, "test-tenant", "completed-file")

		// It should not be in cache (only active files)
		concreteRepo := repo.(*BadgerMetadataRepository)
		cached := concreteRepo.cache.get("test-tenant", "completed-file")
		if cached != nil {
			t.Error("Completed file should not be cached")
		}
	})

	t.Run("Cache expires", func(t *testing.T) {
		// Add a pending file
		metadata := createTestMetadata("expiring-file", core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)

		// Get it to cache
		_, _ = repo.Get(ctx, "test-tenant", "expiring-file")

		// Wait for cache to expire (TTL is 1 second in test)
		time.Sleep(1500 * time.Millisecond)

		// Should not be in cache anymore
		concreteRepo := repo.(*BadgerMetadataRepository)
		cached := concreteRepo.cache.get("test-tenant", "expiring-file")
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

// TestBadgerRepositoryCloseIsIdempotentAndConcurrencySafe guards the shutdown
// contract: every Close caller either performs the shutdown or waits for it, so
// no caller can report success while the database handle is still open.
func TestBadgerRepositoryCloseIsIdempotentAndConcurrencySafe(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	concrete := repo.(*BadgerMetadataRepository)

	if err := repo.AddOrUpdate(ctx, createTestMetadata("close-guard", core.FileStatusPending)); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	const closers = 8
	errs := make([]error, closers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = concrete.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Close #%d = %v, want nil", i, err)
		}
	}
	if !concrete.db.IsClosed() {
		t.Error("database handle still open after every Close returned")
	}
	if err := concrete.Close(); err != nil {
		t.Errorf("repeated Close = %v, want nil", err)
	}
}

// TestGetByStatusReturnsEveryRecordWhenTheCacheIsSmall is the cache
// short-circuit regression test: an active-status query must read the secondary
// index rather than an eviction-bounded, unordered cache subset.
func TestGetByStatusReturnsEveryRecordWhenTheCacheIsSmall(t *testing.T) {
	ctx := context.Background()
	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID:        "test-tenant",
		DataPath:        t.TempDir(),
		CacheTTL:        time.Minute,
		MaxCacheEntries: 2,
	})
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	base := time.Date(2026, time.January, 5, 6, 7, 8, 0, time.UTC)
	const total = 5
	for i := 0; i < total; i++ {
		metadata := createTestMetadata(fmt.Sprintf("cached-%02d", i), core.FileStatusPending)
		metadata.CreatedAt = base.Add(time.Duration(i) * time.Second)
		metadata.UpdatedAt = metadata.CreatedAt
		if err := repo.AddOrUpdate(ctx, metadata); err != nil {
			t.Fatalf("add record %d: %v", i, err)
		}
	}

	concrete := repo.(*BadgerMetadataRepository)
	if cached := concrete.cache.getStats()["total_entries"].(int); cached > 2 {
		t.Fatalf("test setup cached %d entries, want at most 2", cached)
	}

	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{"unlimited", 0, total},
		{"limited", 3, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, tt.limit)
			if err != nil {
				t.Fatalf("GetByStatus failed: %v", err)
			}
			if len(results) != tt.want {
				t.Fatalf("got %d records, want %d", len(results), tt.want)
			}
			for i, result := range results {
				if want := fmt.Sprintf("cached-%02d", i); result.FileKey != want {
					t.Errorf("result[%d] = %q, want %q (index order)", i, result.FileKey, want)
				}
			}
		})
	}
}

// TestAddOrUpdateBatchValidatesTheWholeSlice verifies that batch writes reject
// the same records as AddOrUpdate, name the offending index, and write nothing.
func TestAddOrUpdateBatchValidatesTheWholeSlice(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	tenantless := createTestMetadata("batch-tenantless", core.FileStatusPending)
	tenantless.TenantID = ""

	tests := []struct {
		name      string
		metadata  []*core.FileMetadata
		wantIndex string
	}{
		{
			name:      "nil entry",
			metadata:  []*core.FileMetadata{createTestMetadata("batch-nil", core.FileStatusPending), nil},
			wantIndex: "metadata[1]",
		},
		{
			name: "empty file key",
			metadata: []*core.FileMetadata{
				createTestMetadata("batch-empty-key", core.FileStatusPending),
				createTestMetadata("", core.FileStatusPending),
			},
			wantIndex: "metadata[1]",
		},
		{
			name:      "empty tenant ID",
			metadata:  []*core.FileMetadata{createTestMetadata("batch-empty-tenant", core.FileStatusPending), tenantless},
			wantIndex: "metadata[1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := repo.AddOrUpdateBatch(ctx, tt.metadata)
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("AddOrUpdateBatch error = %v, want ErrInvalidArgument", err)
			}
			if !strings.Contains(err.Error(), tt.wantIndex) {
				t.Errorf("error %q does not name %s", err, tt.wantIndex)
			}
			if _, getErr := repo.Get(ctx, "test-tenant", tt.metadata[0].FileKey); !errors.Is(getErr, core.ErrFileNotFound) {
				t.Errorf("batch wrote %q despite an invalid entry: %v", tt.metadata[0].FileKey, getErr)
			}
		})
	}

	t.Run("valid slice is written", func(t *testing.T) {
		batch := []*core.FileMetadata{
			createTestMetadata("batch-valid-1", core.FileStatusPending),
			createTestMetadata("batch-valid-2", core.FileStatusProcessing),
		}
		if err := repo.AddOrUpdateBatch(ctx, batch); err != nil {
			t.Fatalf("AddOrUpdateBatch error = %v, want nil", err)
		}
		for _, metadata := range batch {
			got, err := repo.Get(ctx, "test-tenant", metadata.FileKey)
			if err != nil {
				t.Fatalf("get %q: %v", metadata.FileKey, err)
			}
			if got.Status != metadata.Status {
				t.Errorf("%q status = %s, want %s", metadata.FileKey, got.Status, metadata.Status)
			}
		}
	})
}

// TestDeleteBatchValidatesKeysAndPropagatesDeleteFailures verifies batch delete
// input validation and that a failed primary delete is reported instead of
// being silently swallowed.
func TestDeleteBatchValidatesKeysAndPropagatesDeleteFailures(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	for _, fileKey := range []string{"delete-1", "delete-2"} {
		if err := repo.AddOrUpdate(ctx, createTestMetadata(fileKey, core.FileStatusPending)); err != nil {
			t.Fatalf("add %q: %v", fileKey, err)
		}
	}

	t.Run("empty tenant ID is rejected", func(t *testing.T) {
		err := repo.DeleteBatch(ctx, "", []string{"delete-1"})
		if !errors.Is(err, core.ErrInvalidArgument) {
			t.Fatalf("DeleteBatch error = %v, want ErrInvalidArgument", err)
		}
		if !strings.Contains(err.Error(), "tenant ID") {
			t.Errorf("error %q does not name the tenant ID", err)
		}
	})

	t.Run("empty file key names the offending index", func(t *testing.T) {
		err := repo.DeleteBatch(ctx, "test-tenant", []string{"delete-1", ""})
		if !errors.Is(err, core.ErrInvalidArgument) {
			t.Fatalf("DeleteBatch error = %v, want ErrInvalidArgument", err)
		}
		if !strings.Contains(err.Error(), "fileKeys[1]") {
			t.Errorf("error %q does not name fileKeys[1]", err)
		}
	})

	t.Run("primary delete failure is propagated", func(t *testing.T) {
		oversized := strings.Repeat("k", 70000)
		err := repo.DeleteBatch(ctx, "test-tenant", []string{"delete-1", oversized})
		if err == nil {
			t.Fatal("DeleteBatch error = nil, want the oversized key failure")
		}
		if _, getErr := repo.Get(ctx, "test-tenant", "delete-1"); getErr != nil {
			t.Errorf("failed batch delete removed delete-1: %v", getErr)
		}
	})

	t.Run("valid batch deletes every key", func(t *testing.T) {
		if err := repo.DeleteBatch(ctx, "test-tenant", []string{"delete-1", "delete-2"}); err != nil {
			t.Fatalf("DeleteBatch error = %v, want nil", err)
		}
		for _, fileKey := range []string{"delete-1", "delete-2"} {
			if _, err := repo.Get(ctx, "test-tenant", fileKey); !errors.Is(err, core.ErrFileNotFound) {
				t.Errorf("%q still present after batch delete: %v", fileKey, err)
			}
		}
	})
}

// TestCompareAndTransitionToProcessingClassifiesClaimFailures verifies that
// contention is distinguishable from infrastructure failure so a scheduler can
// continue on contention only.
func TestCompareAndTransitionToProcessingClassifiesClaimFailures(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Minute)
	notAvailable := createTestMetadata("claim-not-available", core.FileStatusPending)
	notAvailable.AvailableForProcessingAt = &future
	alreadyProcessing := createTestMetadata("claim-already-processing", core.FileStatusProcessing)
	available := createTestMetadata("claim-available", core.FileStatusPending)
	available.AvailableForProcessingAt = &past
	for _, metadata := range []*core.FileMetadata{notAvailable, alreadyProcessing, available} {
		if err := repo.AddOrUpdate(ctx, metadata); err != nil {
			t.Fatalf("add %s: %v", metadata.FileKey, err)
		}
	}

	tests := []struct {
		name    string
		fileKey string
		want    error
	}{
		{"not pending is contention", "claim-already-processing", core.ErrFileNotClaimable},
		{"not yet available is contention", "claim-not-available", core.ErrFileNotClaimable},
		{"missing record stays ErrFileNotFound", "claim-missing", core.ErrFileNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.CompareAndTransitionToProcessing(ctx, "test-tenant", tt.fileKey)
			if !errors.Is(err, tt.want) {
				t.Errorf("error = %v, want %v", err, tt.want)
			}
		})
	}

	t.Run("available pending record is claimed", func(t *testing.T) {
		updated, err := repo.CompareAndTransitionToProcessing(ctx, "test-tenant", "claim-available")
		if err != nil {
			t.Fatalf("claim error = %v, want nil", err)
		}
		if updated.Status != core.FileStatusProcessing {
			t.Errorf("claimed status = %s, want Processing", updated.Status)
		}
	})

	t.Run("closed repository is an infrastructure failure", func(t *testing.T) {
		closed, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
			TenantID: "test-tenant",
			DataPath: t.TempDir(),
		})
		if err != nil {
			t.Fatalf("create repository: %v", err)
		}
		if err := closed.Close(); err != nil {
			t.Fatalf("close repository: %v", err)
		}
		if _, err := closed.CompareAndTransitionToProcessing(ctx, "test-tenant", "claim-missing"); !errors.Is(err, core.ErrDatabaseError) {
			t.Errorf("error = %v, want ErrDatabaseError", err)
		}
	})
}

// TestConcurrentClaimsOnlyReportClaimContention verifies that racing workers see
// contention (including BadgerDB write conflicts) rather than an infrastructure
// failure, and that exactly one worker wins the claim.
func TestConcurrentClaimsOnlyReportClaimContention(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	if err := repo.AddOrUpdate(ctx, createTestMetadata("contended-claim", core.FileStatusPending)); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = repo.CompareAndTransitionToProcessing(ctx, "test-tenant", "contended-claim")
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, core.ErrFileNotClaimable):
			// Lost the race: contention is expected and skippable.
		default:
			t.Errorf("claimer %d error = %v, want nil or ErrFileNotClaimable", i, err)
		}
	}
	if winners != 1 {
		t.Errorf("%d claimers succeeded, want exactly 1", winners)
	}
}

// TestCompareAndUpdateProcessingRepeatedReleaseIsIdempotent verifies that
// releasing the same lease twice is a no-op while a superseded lease is still
// rejected.
func TestCompareAndUpdateProcessingRepeatedReleaseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	leaseStart := time.Date(2026, time.September, 17, 10, 0, 0, 0, time.UTC)
	lease := core.FileProcessingLease{
		TenantID:               "test-tenant",
		FileKey:                "idempotent-lease",
		ProcessingStartTimeUTC: leaseStart,
	}
	seed := func(t *testing.T) {
		t.Helper()
		file := createTestMetadata(lease.FileKey, core.FileStatusProcessing)
		file.ProcessingStartTime = &leaseStart
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("seed metadata: %v", err)
		}
	}

	t.Run("repeated complete is a no-op", func(t *testing.T) {
		seed(t)
		completedAt := time.Date(2026, time.September, 17, 10, 5, 0, 0, time.UTC)
		release := func() (*core.FileMetadata, error) {
			return repo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
				current.Status = core.FileStatusCompleted
				current.ProcessingStartTime = nil
				current.CompletedAt = &completedAt
				current.UpdatedAt = completedAt
				return nil
			})
		}

		if _, err := release(); err != nil {
			t.Fatalf("first release: %v", err)
		}
		second, err := release()
		if err != nil {
			t.Fatalf("repeated release = %v, want success", err)
		}
		if second.Status != core.FileStatusCompleted || second.CompletedAt == nil || !second.CompletedAt.Equal(completedAt) {
			t.Errorf("repeated release changed metadata: status %s completedAt %v", second.Status, second.CompletedAt)
		}

		current, err := repo.Get(ctx, lease.TenantID, lease.FileKey)
		if err != nil {
			t.Fatalf("get metadata: %v", err)
		}
		if current.ReleasedProcessingStartTimeUTC == nil || !current.ReleasedProcessingStartTimeUTC.Equal(leaseStart) {
			t.Errorf("released lease marker = %v, want %v", current.ReleasedProcessingStartTimeUTC, leaseStart)
		}
	})

	t.Run("repeated failure does not retry again", func(t *testing.T) {
		seed(t)
		failedAt := time.Date(2026, time.September, 17, 11, 0, 0, 0, time.UTC)
		release := func() (*core.FileMetadata, error) {
			return repo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
				current.RetryCount++
				current.Status = core.FileStatusPermanentlyFailed
				current.ProcessingStartTime = nil
				current.LastFailedAt = &failedAt
				current.UpdatedAt = failedAt
				return nil
			})
		}

		if _, err := release(); err != nil {
			t.Fatalf("first release: %v", err)
		}
		second, err := release()
		if err != nil {
			t.Fatalf("repeated release = %v, want success", err)
		}
		if second.RetryCount != 1 {
			t.Errorf("RetryCount = %d after repeated failure release, want 1", second.RetryCount)
		}
		if second.Status != core.FileStatusPermanentlyFailed {
			t.Errorf("status = %s, want PermanentlyFailed", second.Status)
		}
	})

	t.Run("superseded lease is rejected", func(t *testing.T) {
		seed(t)
		if _, err := repo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
			current.Status = core.FileStatusPending
			current.ProcessingStartTime = nil
			return nil
		}); err != nil {
			t.Fatalf("release: %v", err)
		}

		newLeaseStart := leaseStart.Add(time.Minute)
		reclaimed, err := repo.Get(ctx, lease.TenantID, lease.FileKey)
		if err != nil {
			t.Fatalf("get metadata: %v", err)
		}
		reclaimed.Status = core.FileStatusProcessing
		reclaimed.ProcessingStartTime = &newLeaseStart
		reclaimed.UpdatedAt = newLeaseStart
		if err := repo.AddOrUpdate(ctx, reclaimed); err != nil {
			t.Fatalf("re-claim metadata: %v", err)
		}

		called := false
		_, err = repo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
			called = true
			return nil
		})
		if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
			t.Fatalf("stale lease error = %v, want ErrProcessingLeaseMismatch", err)
		}
		if called {
			t.Error("update callback ran for a superseded lease")
		}
	})
}

// TestMetadataQueriesHonorContextCancellation verifies that the index scans
// stop when the caller cancels.
func TestMetadataQueriesHonorContextCancellation(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	if err := repo.AddOrUpdate(ctx, createTestMetadata("ctx-pending", core.FileStatusPending)); err != nil {
		t.Fatalf("add pending metadata: %v", err)
	}
	timedOut := createTestMetadata("ctx-timed-out", core.FileStatusProcessing)
	longAgo := time.Now().Add(-2 * time.Hour)
	timedOut.ProcessingStartTime = &longAgo
	if err := repo.AddOrUpdate(ctx, timedOut); err != nil {
		t.Fatalf("add processing metadata: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	tests := []struct {
		name  string
		query func(context.Context) ([]*core.FileMetadata, error)
	}{
		{
			name: "GetByStatus",
			query: func(c context.Context) ([]*core.FileMetadata, error) {
				return repo.GetByStatus(c, "test-tenant", core.FileStatusPending, 0)
			},
		},
		{
			name: "GetPendingFiles",
			query: func(c context.Context) ([]*core.FileMetadata, error) {
				return repo.GetPendingFiles(c, "test-tenant", 0)
			},
		},
		{
			name: "GetTimedOutProcessingFiles",
			query: func(c context.Context) ([]*core.FileMetadata, error) {
				return repo.GetTimedOutProcessingFiles(c, "test-tenant", time.Minute)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := tt.query(canceled)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v (%d results), want context.Canceled", err, len(results))
			}
		})
	}
}

// TestNewBadgerMetadataRepositoryClampsGCDiscardRatio verifies that a configured
// discard ratio outside (0,1) cannot make every GC run fail with
// badger.ErrInvalidRequest.
func TestNewBadgerMetadataRepositoryClampsGCDiscardRatio(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		ratio     float64
		wantExact float64
	}{
		{"zero falls back to the default", 0, 0.5},
		{"negative falls back to the default", -0.25, 0.5},
		{"one is clamped below one", 1, 0},
		{"above one is clamped below one", 1.5, 0},
		{"valid ratio is preserved", 0.7, 0.7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
				TenantID:       "test-tenant",
				DataPath:       t.TempDir(),
				GCDiscardRatio: tt.ratio,
			})
			if err != nil {
				t.Fatalf("create repository: %v", err)
			}
			concrete := repo.(*BadgerMetadataRepository)
			t.Cleanup(func() { _ = concrete.Close() })

			if tt.wantExact != 0 && concrete.gcDiscardRatio != tt.wantExact {
				t.Errorf("gcDiscardRatio = %v, want %v", concrete.gcDiscardRatio, tt.wantExact)
			}
			if concrete.gcDiscardRatio <= 0 || concrete.gcDiscardRatio >= 1 {
				t.Fatalf("gcDiscardRatio = %v, want a value strictly inside (0,1)", concrete.gcDiscardRatio)
			}
			if err := repo.Optimize(ctx); err != nil {
				t.Errorf("Optimize with discard ratio %v: %v", concrete.gcDiscardRatio, err)
			}
		})
	}
}

// TestBadgerRepositoryInvalidArgumentErrorsNameTheArgument verifies that
// validation errors distinguish an empty tenant ID from an empty file key.
func TestBadgerRepositoryInvalidArgumentErrorsNameTheArgument(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	tests := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "Get rejects an empty tenant ID",
			call: func() error { _, err := repo.Get(ctx, "", "file-1"); return err },
			want: "tenant ID",
		},
		{
			name: "Get rejects an empty file key",
			call: func() error { _, err := repo.Get(ctx, "test-tenant", ""); return err },
			want: "file key",
		},
		{
			name: "Delete rejects an empty tenant ID",
			call: func() error { return repo.Delete(ctx, "", "file-1") },
			want: "tenant ID",
		},
		{
			name: "UpdateStatus rejects an empty tenant ID",
			call: func() error {
				return repo.UpdateStatus(ctx, "", "file-1", core.FileStatusPending)
			},
			want: "tenant ID",
		},
		{
			name: "UpdateStatus rejects an empty file key",
			call: func() error {
				return repo.UpdateStatus(ctx, "test-tenant", "", core.FileStatusPending)
			},
			want: "file key",
		},
		{
			name: "CompareAndTransitionToProcessing rejects an empty tenant ID",
			call: func() error {
				_, err := repo.CompareAndTransitionToProcessing(ctx, "", "file-1")
				return err
			},
			want: "tenant ID",
		},
		{
			name: "CompareAndTransitionToProcessing rejects an empty file key",
			call: func() error {
				_, err := repo.CompareAndTransitionToProcessing(ctx, "test-tenant", "")
				return err
			},
			want: "file key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
}
