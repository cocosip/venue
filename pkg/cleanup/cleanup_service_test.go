package cleanup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/volume"
)

// TestNewCleanupService tests creating a cleanup service.
func TestNewCleanupService(t *testing.T) {
	t.Run("Valid configuration", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer func() { _ = repo.Close() }()
		defer func() { _ = os.RemoveAll(tmpDir) }()

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

		opts := &CleanupServiceOptions{
			MetadataRepository: repo,
			FileScheduler:      sched,
			Volumes:            volumes,
		}

		service, err := NewCleanupService(opts)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if service == nil {
			t.Fatal("Expected service to be created")
		}
	})

	t.Run("Nil options", func(t *testing.T) {
		_, err := NewCleanupService(nil)
		if err == nil {
			t.Fatal("Expected error for nil options")
		}
	})

	t.Run("Nil metadata repository", func(t *testing.T) {
		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		opts := &CleanupServiceOptions{
			MetadataRepository: nil,
			FileScheduler:      nil,
			Volumes:            volumes,
		}

		_, err := NewCleanupService(opts)
		if err == nil {
			t.Fatal("Expected error for nil metadata repository")
		}
	})
}

// TestCleanupTimedOutProcessingFiles tests cleaning up timed out files.
func TestCleanupTimedOutProcessingFiles(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = repo.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &CleanupServiceOptions{
		MetadataRepository:       repo,
		FileScheduler:            sched,
		Volumes:                  volumes,
		DefaultProcessingTimeout: 30 * time.Minute,
	}

	service, _ := NewCleanupService(opts)

	t.Run("Reset timed out files", func(t *testing.T) {
		// Add a processing file with old start time
		longAgo := time.Now().Add(-2 * time.Hour)
		file := createTestFileMetadata("file1", core.FileStatusProcessing)
		file.ProcessingStartTime = &longAgo
		_ = repo.AddOrUpdate(ctx, file)

		// Add a recent processing file
		recent := time.Now().Add(-5 * time.Minute)
		file2 := createTestFileMetadata("file2", core.FileStatusProcessing)
		file2.ProcessingStartTime = &recent
		_ = repo.AddOrUpdate(ctx, file2)

		// Cleanup with 1 hour timeout
		stats, err := service.CleanupTimedOutProcessingFiles(ctx, 1*time.Hour)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.TimedOutFilesReset != 1 {
			t.Errorf("Expected 1 file to be reset, got %d", stats.TimedOutFilesReset)
		}

		// Verify file1 was reset to Pending
		updated, _ := repo.Get(ctx, "file1")
		if updated.Status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", updated.Status)
		}

		// Verify file2 is still Processing
		updated2, _ := repo.Get(ctx, "file2")
		if updated2.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", updated2.Status)
		}
	})
}

// TestCleanupPermanentlyFailedFiles tests cleaning up permanently failed files.
func TestCleanupPermanentlyFailedFiles(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = repo.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	tenantQuotaMgr := quota.NewTenantQuotaManager()
	dirQuotaRepo, tmpDir2 := createTestDirQuotaRepository(t)
	defer func() { _ = dirQuotaRepo.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir2) }()
	dirQuotaMgr, _ := quota.NewDirectoryQuotaManager(dirQuotaRepo)

	opts := &CleanupServiceOptions{
		MetadataRepository:    repo,
		FileScheduler:         sched,
		Volumes:               volumes,
		TenantQuotaManager:    tenantQuotaMgr,
		DirectoryQuotaManager: dirQuotaMgr,
	}

	service, _ := NewCleanupService(opts)

	t.Run("Delete permanently failed files", func(t *testing.T) {
		// Create a physical file
		vol := volumes["test-volume"]
		content := bytes.NewReader([]byte("test content"))
		relativePath := "tenant1/failed-file.txt"
		_, _ = vol.WriteFile(ctx, relativePath, content)

		// Add metadata for permanently failed file
		file := createTestFileMetadata("failed1", core.FileStatusPermanentlyFailed)
		file.PhysicalPath = relativePath
		file.FileSize = 12
		_ = repo.AddOrUpdate(ctx, file)

		// Set quotas
		_ = tenantQuotaMgr.SetQuota(ctx, "test-tenant", 100)
		_ = tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant")

		// Cleanup
		stats, err := service.CleanupPermanentlyFailedFiles(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.PermanentlyFailedFilesRemoved != 1 {
			t.Errorf("Expected 1 file to be removed, got %d", stats.PermanentlyFailedFilesRemoved)
		}

		if stats.SpaceFreed != 12 {
			t.Errorf("Expected 12 bytes freed, got %d", stats.SpaceFreed)
		}

		// Verify metadata was deleted
		_, err = repo.Get(ctx, "failed1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file metadata to be deleted, got error: %v", err)
		}

		// Verify physical file was deleted
		exists, _ := vol.FileExists(ctx, relativePath)
		if exists {
			t.Error("Expected physical file to be deleted")
		}
	})
}

// TestCleanupOrphanedMetadata tests cleaning up orphaned metadata.
func TestCleanupOrphanedMetadata(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = repo.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	tenantQuotaMgr := quota.NewTenantQuotaManager()

	opts := &CleanupServiceOptions{
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
		TenantQuotaManager: tenantQuotaMgr,
	}

	service, _ := NewCleanupService(opts)

	t.Run("Remove orphaned metadata", func(t *testing.T) {
		// Add metadata for non-existent file
		file := createTestFileMetadata("orphan1", core.FileStatusPending)
		file.PhysicalPath = "tenant1/nonexistent.txt"
		_ = repo.AddOrUpdate(ctx, file)

		// Set quota
		_ = tenantQuotaMgr.SetQuota(ctx, "test-tenant", 100)
		_ = tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant")

		// Cleanup
		stats, err := service.CleanupOrphanedMetadata(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.OrphanedMetadataRemoved == 0 {
			t.Error("Expected at least one orphaned metadata to be removed")
		}

		// Verify metadata was deleted
		_, err = repo.Get(ctx, "orphan1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected orphaned metadata to be deleted, got error: %v", err)
		}
	})
}

// TestCleanupEmptyDirectories tests cleaning up empty directories.
func TestCleanupEmptyDirectories(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = repo.Close() }()
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &CleanupServiceOptions{
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	service, _ := NewCleanupService(opts)

	t.Run("Remove empty directories", func(t *testing.T) {
		// Create some empty directories
		vol := volumes["test-volume"]
		emptyDir1 := filepath.Join(vol.MountPath(), "tenant1", "empty1")
		emptyDir2 := filepath.Join(vol.MountPath(), "tenant1", "empty2")

		_ = os.MkdirAll(emptyDir1, 0755)
		_ = os.MkdirAll(emptyDir2, 0755)

		// Create a non-empty directory
		nonEmptyDir := filepath.Join(vol.MountPath(), "tenant1", "nonempty")
		_ = os.MkdirAll(nonEmptyDir, 0755)
		_ = os.WriteFile(filepath.Join(nonEmptyDir, "file.txt"), []byte("content"), 0644)

		// Cleanup
		stats, err := service.CleanupEmptyDirectories(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.EmptyDirectoriesRemoved == 0 {
			t.Error("Expected at least one empty directory to be removed")
		}

		// Verify empty directories were removed
		_, err1 := os.Stat(emptyDir1)
		_, err2 := os.Stat(emptyDir2)

		if !os.IsNotExist(err1) {
			t.Error("Expected empty directory 1 to be removed")
		}

		if !os.IsNotExist(err2) {
			t.Error("Expected empty directory 2 to be removed")
		}

		// Verify non-empty directory still exists
		_, err3 := os.Stat(nonEmptyDir)
		if err3 != nil {
			t.Error("Expected non-empty directory to still exist")
		}
	})
}

// Helper functions

func createTestRepository(t *testing.T) (core.MetadataRepository, string) {
	tmpDir, err := os.MkdirTemp("", "cleanup-test-*")
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
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create repository: %v", err)
	}

	return repo, tmpDir
}

func createTestDirQuotaRepository(t *testing.T) (core.DirectoryQuotaRepository, string) {
	tmpDir, err := os.MkdirTemp("", "dirquota-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &quota.BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       tmpDir,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := quota.NewBadgerDirectoryQuotaRepository(opts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create directory quota repository: %v", err)
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
