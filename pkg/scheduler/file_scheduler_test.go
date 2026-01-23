package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/volume"
)

// TestNewFileScheduler tests creating a new file scheduler.
func TestNewFileScheduler(t *testing.T) {
	ctx := context.Background()

	t.Run("Valid configuration", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)
		defer repo.(*metadata.BadgerMetadataRepository).Close()

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
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)
		defer repo.(*metadata.BadgerMetadataRepository).Close()

		_, err := NewFileScheduler(repo, nil, nil)
		if err == nil {
			t.Fatal("Expected error for nil volumes")
		}
	})

	t.Run("Empty volumes", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)
		defer repo.(*metadata.BadgerMetadataRepository).Close()

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

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)
	defer repo.(*metadata.BadgerMetadataRepository).Close()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	t.Run("Get pending file", func(t *testing.T) {
		// Add a pending file
		file := createTestFileMetadata("file1", core.FileStatusPending)
		repo.AddOrUpdate(ctx, file)

		// Get next file
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if location == nil {
			t.Fatal("Expected file location")
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
		updated, _ := repo.Get(ctx, "file1")
		if updated.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing in repo, got %v", updated.Status)
		}
	})

	t.Run("No files available", func(t *testing.T) {
		_, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != core.ErrNoFilesAvailable {
			t.Errorf("Expected ErrNoFilesAvailable, got %v", err)
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
		repo.AddOrUpdate(ctx, file)

		// Should not get this file
		_, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != core.ErrNoFilesAvailable {
			t.Errorf("Expected ErrNoFilesAvailable for future file, got %v", err)
		}
	})
}

// TestGetNextBatchForProcessing tests getting a batch of files for processing.
func TestGetNextBatchForProcessing(t *testing.T) {
	ctx := context.Background()

	t.Run("Get batch of files", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		scheduler, _ := NewFileScheduler(repo, volumes, nil)
		tenant := createTestTenant()

		// Add 5 pending files
		for i := 1; i <= 5; i++ {
			file := createTestFileMetadata(string(rune('a'+i-1)), core.FileStatusPending)
			repo.AddOrUpdate(ctx, file)
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
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)

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
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)

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

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)
	defer repo.(*metadata.BadgerMetadataRepository).Close()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Mark file as completed", func(t *testing.T) {
		// Add a processing file
		file := createTestFileMetadata("file1", core.FileStatusProcessing)
		repo.AddOrUpdate(ctx, file)

		// Mark as completed
		err := scheduler.MarkAsCompleted(ctx, "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file metadata is deleted
		_, err = repo.Get(ctx, "file1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := scheduler.MarkAsCompleted(ctx, "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		err := scheduler.MarkAsCompleted(ctx, "non-existent")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})
}

// TestMarkAsFailed tests marking a file as failed with retry logic.
func TestMarkAsFailed(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)
	defer repo.(*metadata.BadgerMetadataRepository).Close()

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
		file := createTestFileMetadata("file1", core.FileStatusProcessing)
		file.RetryCount = 0
		repo.AddOrUpdate(ctx, file)

		// Mark as failed
		err := scheduler.MarkAsFailed(ctx, "file1", "Test error")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file status
		updated, _ := repo.Get(ctx, "file1")
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
		file := createTestFileMetadata("file2", core.FileStatusProcessing)
		file.RetryCount = 3 // Already at max
		repo.AddOrUpdate(ctx, file)

		// Mark as failed (this should exceed max)
		err := scheduler.MarkAsFailed(ctx, "file2", "Final error")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify file is permanently failed
		updated, _ := repo.Get(ctx, "file2")
		if updated.Status != core.FileStatusPermanentlyFailed {
			t.Errorf("Expected status PermanentlyFailed, got %v", updated.Status)
		}

		if updated.RetryCount != 4 {
			t.Errorf("Expected RetryCount 4, got %d", updated.RetryCount)
		}

		if updated.AvailableForProcessingAt != nil {
			t.Error("Expected AvailableForProcessingAt to be nil for permanently failed")
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		err := scheduler.MarkAsFailed(ctx, "", "error")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})
}

// TestGetFileStatus tests getting file status.
func TestGetFileStatus(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)
	defer repo.(*metadata.BadgerMetadataRepository).Close()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Get status of existing file", func(t *testing.T) {
		file := createTestFileMetadata("file1", core.FileStatusPending)
		repo.AddOrUpdate(ctx, file)

		status, err := scheduler.GetFileStatus(ctx, "file1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", status)
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		_, err := scheduler.GetFileStatus(ctx, "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})

	t.Run("Non-existent file", func(t *testing.T) {
		_, err := scheduler.GetFileStatus(ctx, "non-existent")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})
}

// TestResetTimedOutFiles tests resetting timed-out processing files.
func TestResetTimedOutFiles(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)
	defer repo.(*metadata.BadgerMetadataRepository).Close()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)

	t.Run("Reset timed out files", func(t *testing.T) {
		// Add files with different processing start times
		longAgo := time.Now().Add(-2 * time.Hour)
		recent := time.Now().Add(-5 * time.Minute)

		file1 := createTestFileMetadata("file1", core.FileStatusProcessing)
		file1.ProcessingStartTime = &longAgo
		repo.AddOrUpdate(ctx, file1)

		file2 := createTestFileMetadata("file2", core.FileStatusProcessing)
		file2.ProcessingStartTime = &recent
		repo.AddOrUpdate(ctx, file2)

		// Reset files with timeout of 1 hour
		count, err := scheduler.ResetTimedOutFiles(ctx, 1*time.Hour)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 file to be reset, got %d", count)
		}

		// Verify file1 was reset to Pending
		updated1, _ := repo.Get(ctx, "file1")
		if updated1.Status != core.FileStatusPending {
			t.Errorf("Expected file1 status Pending, got %v", updated1.Status)
		}

		if updated1.ProcessingStartTime != nil {
			t.Error("Expected ProcessingStartTime to be cleared")
		}

		// Verify file2 is still Processing
		updated2, _ := repo.Get(ctx, "file2")
		if updated2.Status != core.FileStatusProcessing {
			t.Errorf("Expected file2 status Processing, got %v", updated2.Status)
		}
	})

	t.Run("No timed out files", func(t *testing.T) {
		count, err := scheduler.ResetTimedOutFiles(ctx, 1*time.Hour)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 0 {
			t.Errorf("Expected 0 files to be reset, got %d", count)
		}
	})
}

// Helper functions

func createTestRepository(t *testing.T) (core.MetadataRepository, string) {
	tmpDir, err := os.MkdirTemp("", "scheduler-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &metadata.BadgerRepositoryOptions{
		TenantID:       "test-tenant",
		DataPath:       tmpDir,
		CacheTTL:       5 * time.Minute,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := metadata.NewBadgerMetadataRepository(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create repository: %v", err)
	}

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
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create volume: %v", err)
	}

	return map[string]core.StorageVolume{
		"test-volume": vol,
	}
}

func cleanupVolumes(volumes map[string]core.StorageVolume) {
	for _, vol := range volumes {
		os.RemoveAll(vol.MountPath())
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
