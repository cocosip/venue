package scheduler

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/sqlite"
	"github.com/cocosip/venue/pkg/volume"
)

// TestNewFileScheduler tests creating a new file scheduler.
func TestNewFileScheduler(t *testing.T) {
	ctx := context.Background()

	t.Run("Valid configuration", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		scheduler, err := NewFileScheduler(repo, volumes, nil)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if scheduler == nil {
			t.Fatal("Expected scheduler to be created")
		}
	})

	t.Run("Nil metadata repository", func(t *testing.T) {
		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		_, err := NewFileScheduler(nil, volumes, nil)
		if err == nil {
			t.Fatal("Expected error for nil metadata repository")
		}
	})

	t.Run("Nil volumes", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		_, err := NewFileScheduler(repo, nil, nil)
		if err == nil {
			t.Fatal("Expected error for nil volumes")
		}
	})

	t.Run("Empty volumes", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		_, err := NewFileScheduler(repo, map[string]core.StorageVolume{}, nil)
		if err == nil {
			t.Fatal("Expected error for empty volumes")
		}
	})

	_ = ctx
}

// TestGetNextFileForProcessing tests getting the next file for processing.
func TestGetNextFileForProcessing(t *testing.T) {
	ctx := context.Background()

	repo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	t.Run("Get pending file", func(t *testing.T) {
		// Add a pending file
		file := createTestFileMetadata("file1", core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, file)

		// Get next file
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil || location == nil {
			t.Fatalf("Expected no error and valid location, got error: %v, location: %v", err, location)
		}

		if location.FileKey != "file1" {
			t.Errorf("Expected FileKey 'file1', got %s", location.FileKey)
		}

		if location.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", location.Status)
		}

		if location.ProcessingStartTime == nil {
			t.Error("Expected ProcessingStartTime to be set")
		}

		// Verify status was updated in repository
		updated, _ := repo.Get(ctx, "test-tenant", "file1")
		if updated.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing in repo, got %v", updated.Status)
		}
	})

	t.Run("No files available", func(t *testing.T) {
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Errorf("Expected no error for an empty queue, got %v", err)
		}
		if location != nil {
			t.Errorf("Expected nil location for an empty queue, got %v", location)
		}
	})

	t.Run("Disabled tenant", func(t *testing.T) {
		disabledTenant := core.TenantContext{
			ID:     "disabled-tenant",
			Status: core.TenantStatusDisabled,
		}

		_, err := scheduler.GetNextFileForProcessing(ctx, disabledTenant)
		if err != core.ErrTenantDisabled {
			t.Errorf("Expected ErrTenantDisabled, got %v", err)
		}
	})

	t.Run("File with future availability", func(t *testing.T) {
		// Add a file with future availability
		future := time.Now().Add(1 * time.Hour)
		file := createTestFileMetadata("file-future", core.FileStatusPending)
		file.AvailableForProcessingAt = &future
		_ = repo.AddOrUpdate(ctx, file)

		// Should not get this file
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Errorf("Expected no error for a not-yet-available file, got %v", err)
		}
		if location != nil {
			t.Errorf("Expected nil location for a not-yet-available file, got %v", location)
		}
	})
}

// TestGetNextBatchForProcessing tests getting a batch of files for processing.
func TestGetNextBatchForProcessing(t *testing.T) {
	ctx := context.Background()

	t.Run("Get batch of files", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		scheduler, _ := NewFileScheduler(repo, volumes, nil)
		tenant := createTestTenant()

		// Add 5 pending files
		for i := 1; i <= 5; i++ {
			file := createTestFileMetadata(string(rune('a'+i-1)), core.FileStatusPending)
			_ = repo.AddOrUpdate(ctx, file)
		}

		// Get batch of 3
		locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 3)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(locations) != 3 {
			t.Errorf("Expected 3 files, got %d", len(locations))
		}

		// Verify all are in Processing status
		for _, loc := range locations {
			if loc.Status != core.FileStatusProcessing {
				t.Errorf("Expected status Processing, got %v", loc.Status)
			}
		}
	})

	t.Run("Empty result when no files", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		scheduler, _ := NewFileScheduler(repo, volumes, nil)
		tenant := createTestTenant()

		locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 10)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(locations) != 0 {
			t.Errorf("Expected 0 files, got %d", len(locations))
		}
	})

	t.Run("Invalid batch size", func(t *testing.T) {
		repo, _ := createTestRepository(t)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		scheduler, _ := NewFileScheduler(repo, volumes, nil)
		tenant := createTestTenant()

		_, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 0)
		if err == nil {
			t.Fatal("Expected error for invalid batch size")
		}
	})
}

// TestMarkAsCompleted tests marking a file as completed.
func TestMarkAsCompleted(t *testing.T) {
	ctx := context.Background()

	repo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Mark file as completed", func(t *testing.T) {
		// Add a processing file
		leaseStart := time.Now().UTC()
		file := createTestFileMetadata("file1", core.FileStatusProcessing)
		file.ProcessingStartTime = &leaseStart
		_ = repo.AddOrUpdate(ctx, file)

		// Mark as completed
		err := scheduler.MarkAsCompleted(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "file1",
			ProcessingStartTimeUTC: leaseStart,
		})
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Completion is durable metadata; physical deletion happens in cleanup.
		completed, err := repo.Get(ctx, "test-tenant", "file1")
		if err != nil {
			t.Fatalf("get completed metadata: %v", err)
		}
		if completed.Status != core.FileStatusCompleted {
			t.Errorf("completed status = %s, want Completed", completed.Status)
		}
		if completed.CompletedAt == nil {
			t.Error("completed metadata has nil CompletedAt")
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := scheduler.MarkAsCompleted(ctx, core.FileProcessingLease{TenantID: "test-tenant"})
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		err := scheduler.MarkAsCompleted(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "non-existent",
			ProcessingStartTimeUTC: time.Now().UTC(),
		})
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})
}

func TestMarkAsCompletedRejectsStaleLeaseBeforeDeletingFile(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	activeStart := time.Date(2026, time.September, 17, 14, 0, 0, 0, time.UTC)
	file := createTestFileMetadata("stale-completion", core.FileStatusProcessing)
	file.PhysicalPath = "stale-completion.txt"
	file.ProcessingStartTime = &activeStart
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("add metadata: %v", err)
	}
	if _, err := volumes["test-volume"].WriteFile(ctx, file.PhysicalPath, strings.NewReader("payload")); err != nil {
		t.Fatalf("write physical file: %v", err)
	}

	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	err = scheduler.MarkAsCompleted(ctx, core.FileProcessingLease{
		TenantID:               "test-tenant",
		FileKey:                file.FileKey,
		ProcessingStartTimeUTC: activeStart.Add(-time.Minute),
	})
	if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
		t.Fatalf("error = %v, want ErrProcessingLeaseMismatch", err)
	}

	exists, err := volumes["test-volume"].FileExists(ctx, file.PhysicalPath)
	if err != nil {
		t.Fatalf("check physical file: %v", err)
	}
	if !exists {
		t.Fatal("stale completion deleted the physical file")
	}
	current, err := repo.Get(ctx, "test-tenant", file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing || !current.ProcessingStartTime.Equal(activeStart) {
		t.Errorf("metadata changed to status %s lease %v", current.Status, current.ProcessingStartTime)
	}
}

// TestMarkAsFailed tests marking a file as failed with retry logic.
func TestMarkAsFailed(t *testing.T) {
	ctx := context.Background()

	repo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	opts := &FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         3,
			InitialRetryDelay:     5 * time.Second,
			UseExponentialBackoff: true,
			MaxRetryDelay:         5 * time.Minute,
		},
		ProcessingTimeout: 30 * time.Minute,
	}

	scheduler, _ := NewFileScheduler(repo, volumes, opts)

	t.Run("First failure - schedule retry", func(t *testing.T) {
		// Add a processing file with retry count 0
		leaseStart := time.Now().UTC()
		file := createTestFileMetadata("file1", core.FileStatusProcessing)
		file.RetryCount = 0
		file.ProcessingStartTime = &leaseStart
		_ = repo.AddOrUpdate(ctx, file)

		// Mark as failed
		err := scheduler.MarkAsFailed(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "file1",
			ProcessingStartTimeUTC: leaseStart,
		}, "Test error")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file status
		updated, _ := repo.Get(ctx, "test-tenant", "file1")
		if updated.Status != core.FileStatusPending {
			t.Errorf("Expected status Pending after first failure, got %v", updated.Status)
		}

		if updated.RetryCount != 1 {
			t.Errorf("Expected RetryCount 1, got %d", updated.RetryCount)
		}

		if updated.LastError != "Test error" {
			t.Errorf("Expected LastError 'Test error', got %s", updated.LastError)
		}

		if updated.AvailableForProcessingAt == nil {
			t.Error("Expected AvailableForProcessingAt to be set")
		}

		if updated.ProcessingStartTime != nil {
			t.Error("Expected ProcessingStartTime to be cleared")
		}
	})

	t.Run("Exceed max retries - permanently failed", func(t *testing.T) {
		// Add a processing file with max retry count
		leaseStart := time.Now().UTC()
		file := createTestFileMetadata("file2", core.FileStatusProcessing)
		file.RetryCount = 2 // Third failure reaches the configured maximum
		file.ProcessingStartTime = &leaseStart
		_ = repo.AddOrUpdate(ctx, file)

		// Mark as failed (this reaches max)
		err := scheduler.MarkAsFailed(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                "file2",
			ProcessingStartTimeUTC: leaseStart,
		}, "Final error")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file is permanently failed
		updated, _ := repo.Get(ctx, "test-tenant", "file2")
		if updated.Status != core.FileStatusPermanentlyFailed {
			t.Errorf("Expected status PermanentlyFailed, got %v", updated.Status)
		}

		if updated.RetryCount != 3 {
			t.Errorf("Expected RetryCount 3, got %d", updated.RetryCount)
		}

		if updated.AvailableForProcessingAt != nil {
			t.Error("Expected AvailableForProcessingAt to be nil for permanently failed")
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := scheduler.MarkAsFailed(ctx, core.FileProcessingLease{TenantID: "test-tenant"}, "error")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

func TestMarkAsFailedRejectsStaleLeaseWithoutChangingRetryState(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	activeStart := time.Date(2026, time.September, 17, 15, 0, 0, 0, time.UTC)
	file := createTestFileMetadata("stale-failure", core.FileStatusProcessing)
	file.ProcessingStartTime = &activeStart
	file.RetryCount = 2
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	err = scheduler.MarkAsFailed(ctx, core.FileProcessingLease{
		TenantID:               "test-tenant",
		FileKey:                file.FileKey,
		ProcessingStartTimeUTC: activeStart.Add(-time.Minute),
	}, "stale worker failed")
	if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
		t.Fatalf("error = %v, want ErrProcessingLeaseMismatch", err)
	}

	current, err := repo.Get(ctx, "test-tenant", file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing || current.RetryCount != 2 {
		t.Errorf("metadata changed to status %s retry %d", current.Status, current.RetryCount)
	}
	if current.LastError != "" || current.LastFailedAt != nil {
		t.Errorf("failure fields changed to error %q at %v", current.LastError, current.LastFailedAt)
	}
}

// TestGetFileStatus tests getting file status.
func TestGetFileStatus(t *testing.T) {
	ctx := context.Background()

	repo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Get status of existing file", func(t *testing.T) {
		file := createTestFileMetadata("file1", core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, file)

		status, err := scheduler.GetFileStatus(ctx, createTestTenant(), "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", status)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		_, err := scheduler.GetFileStatus(ctx, createTestTenant(), "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		_, err := scheduler.GetFileStatus(ctx, createTestTenant(), "non-existent")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})
}

// TestResetTimedOutFiles tests resetting timed-out processing files.
func TestResetTimedOutFiles(t *testing.T) {
	ctx := context.Background()

	repo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Reset timed out files", func(t *testing.T) {
		// Add files with different processing start times
		longAgo := time.Now().Add(-2 * time.Hour)
		recent := time.Now().Add(-5 * time.Minute)

		file1 := createTestFileMetadata("file1", core.FileStatusProcessing)
		file1.ProcessingStartTime = &longAgo
		_ = repo.AddOrUpdate(ctx, file1)

		file2 := createTestFileMetadata("file2", core.FileStatusProcessing)
		file2.ProcessingStartTime = &recent
		_ = repo.AddOrUpdate(ctx, file2)

		// Reset files with timeout of 1 hour
		count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), 1*time.Hour)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 file to be reset, got %d", count)
		}

		// Verify file1 was reset to Pending
		updated1, _ := repo.Get(ctx, "test-tenant", "file1")
		if updated1.Status != core.FileStatusPending {
			t.Errorf("Expected file1 status Pending, got %v", updated1.Status)
		}

		if updated1.ProcessingStartTime != nil {
			t.Error("Expected ProcessingStartTime to be cleared")
		}

		// Verify file2 is still Processing
		updated2, _ := repo.Get(ctx, "test-tenant", "file2")
		if updated2.Status != core.FileStatusProcessing {
			t.Errorf("Expected file2 status Processing, got %v", updated2.Status)
		}
	})

	t.Run("No timed out files", func(t *testing.T) {
		count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), 1*time.Hour)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 0 {
			t.Errorf("Expected 0 files to be reset, got %d", count)
		}
	})
}

func TestResetTimedOutFilesDoesNotOverwriteReplacementLease(t *testing.T) {
	ctx := context.Background()
	baseRepo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	oldStart := time.Now().Add(-2 * time.Hour)
	newStart := time.Now().UTC()
	file := createTestFileMetadata("replacement-lease", core.FileStatusProcessing)
	file.ProcessingStartTime = &oldStart
	if err := baseRepo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	repo := &replacementLeaseRepository{
		MetadataRepository: baseRepo,
		tenantID:           "test-tenant",
		fileKey:            file.FileKey,
		replacementStart:   newStart,
	}
	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), time.Hour)
	if err != nil {
		t.Fatalf("reset timed out files: %v", err)
	}
	if count != 0 {
		t.Errorf("reset count = %d, want 0", count)
	}
	current, err := baseRepo.Get(ctx, "test-tenant", file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing || current.ProcessingStartTime == nil || !current.ProcessingStartTime.Equal(newStart) {
		t.Errorf("replacement lease overwritten: status %s start %v", current.Status, current.ProcessingStartTime)
	}
}

// Helper functions

// createTestRepository opens the SQLite metadata repository the scheduler tests
// drive, below a temporary directory owned by the test itself.
//
// t.TempDir removes the directory through its own cleanup, which was registered
// before the repository's, so LIFO ordering closes the repository — and releases
// every database file handle — before the directory is deleted.
func createTestRepository(t *testing.T) (core.MetadataRepository, string) {
	tmpDir := t.TempDir()

	repo, err := metadata.NewSQLiteMetadataRepository(&metadata.SQLiteRepositoryOptions{
		DataPath:        tmpDir,
		CacheTTL:        5 * time.Minute,
		MaxCacheEntries: 10000,
		Sqlite:          sqlite.DefaultOptions(),
	})
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Errorf("close repository: %v", err)
		}
	})

	return repo, tmpDir
}

func createTestVolumes(t *testing.T) map[string]core.StorageVolume {
	tmpDir, err := os.MkdirTemp("", "volume-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &volume.LocalFileSystemVolumeOptions{
		VolumeID:  "test-volume",
		MountPath: tmpDir,
	}

	vol, err := volume.NewLocalFileSystemVolume(opts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create volume: %v", err)
	}

	return map[string]core.StorageVolume{
		"test-volume": vol,
	}
}

func cleanupVolumes(volumes map[string]core.StorageVolume) {
	for _, vol := range volumes {
		_ = os.RemoveAll(vol.MountPath())
	}
}

func createTestTenant() core.TenantContext {
	return core.TenantContext{
		ID:        "test-tenant",
		Status:    core.TenantStatusEnabled,
		CreatedAt: time.Now(),
	}
}

func createTestFileMetadata(fileKey string, status core.FileProcessingStatus) *core.FileMetadata {
	now := time.Now()
	return &core.FileMetadata{
		FileKey:          fileKey,
		TenantID:         "test-tenant",
		VolumeID:         "test-volume",
		PhysicalPath:     "test/path/" + fileKey,
		FileSize:         1024,
		FileExtension:    ".txt",
		OriginalFileName: fileKey + ".txt",
		Status:           status,
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

type replacementLeaseRepository struct {
	core.MetadataRepository
	tenantID         string
	fileKey          string
	replacementStart time.Time
}

func (r *replacementLeaseRepository) GetTimedOutProcessingFiles(
	ctx context.Context,
	tenantID string,
	timeout time.Duration,
) ([]*core.FileMetadata, error) {
	timedOut, err := r.MetadataRepository.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
	if err != nil || len(timedOut) == 0 {
		return timedOut, err
	}
	current, err := r.Get(ctx, r.tenantID, r.fileKey)
	if err != nil {
		return nil, err
	}
	current.Status = core.FileStatusProcessing
	current.ProcessingStartTime = &r.replacementStart
	current.UpdatedAt = r.replacementStart
	if err := r.AddOrUpdate(ctx, current); err != nil {
		return nil, err
	}
	return timedOut, nil
}
