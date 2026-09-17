package cleanup

import (
	"bytes"
	"context"
	"errors"
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
			TenantManager:      &stubTenantManager{},
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
		TenantManager:            &stubTenantManager{},
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
		updated, _ := repo.Get(ctx, "test-tenant", "file1")
		if updated.Status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", updated.Status)
		}

		// Verify file2 is still Processing
		updated2, _ := repo.Get(ctx, "test-tenant", "file2")
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
		TenantManager:         &stubTenantManager{},
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
		oldFailure := time.Now().Add(-2 * time.Hour)
		file.LastFailedAt = &oldFailure
		_ = repo.AddOrUpdate(ctx, file)

		recentPath := "tenant1/recent-failed-file.txt"
		_, _ = vol.WriteFile(ctx, recentPath, bytes.NewReader([]byte("recent")))
		recent := createTestFileMetadata("failed-recent", core.FileStatusPermanentlyFailed)
		recent.PhysicalPath = recentPath
		recentFailure := time.Now()
		recent.LastFailedAt = &recentFailure
		_ = repo.AddOrUpdate(ctx, recent)

		// Set quotas
		_ = tenantQuotaMgr.SetQuota(ctx, "test-tenant", 100)
		_ = tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant")

		// Cleanup
		stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
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
		_, err = repo.Get(ctx, "test-tenant", "failed1")
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file metadata to be deleted, got error: %v", err)
		}

		// Verify physical file was deleted
		exists, _ := vol.FileExists(ctx, relativePath)
		if exists {
			t.Error("Expected physical file to be deleted")
		}

		if _, err := repo.Get(ctx, "test-tenant", "failed-recent"); err != nil {
			t.Fatalf("recent metadata error = %v, want retained", err)
		}
		exists, err = vol.FileExists(ctx, recentPath)
		if err != nil || !exists {
			t.Fatalf("recent physical file exists = %v, error = %v", exists, err)
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
		TenantManager:      &stubTenantManager{},
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
		_, err = repo.Get(ctx, "test-tenant", "orphan1")
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
		TenantManager:      &stubTenantManager{},
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

	t.Run("Preserve system managed directories", func(t *testing.T) {
		vol := volumes["test-volume"]

		tenantRoot := filepath.Join(vol.MountPath(), "tenant-system")
		dateDir := filepath.Join(tenantRoot, "2026", "04", "04")
		hourDir := filepath.Join(dateDir, "15")
		shardDir := filepath.Join(vol.MountPath(), "ab", "cd", "ef")
		customEmptyDir := filepath.Join(tenantRoot, "custom-empty")

		_ = os.MkdirAll(hourDir, 0755)
		_ = os.MkdirAll(shardDir, 0755)
		_ = os.MkdirAll(customEmptyDir, 0755)

		stats, err := service.CleanupEmptyDirectories(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.EmptyDirectoriesRemoved == 0 {
			t.Error("Expected at least one non-system empty directory to be removed")
		}

		if _, err := os.Stat(vol.MountPath()); err != nil {
			t.Fatalf("Expected mount path to remain, got %v", err)
		}

		if _, err := os.Stat(tenantRoot); err != nil {
			t.Fatalf("Expected tenant root to remain, got %v", err)
		}

		if _, err := os.Stat(dateDir); err != nil {
			t.Fatalf("Expected date-based system directory to remain, got %v", err)
		}

		if _, err := os.Stat(hourDir); err != nil {
			t.Fatalf("Expected hour-based system directory to remain, got %v", err)
		}

		if _, err := os.Stat(shardDir); err != nil {
			t.Fatalf("Expected shard directory to remain, got %v", err)
		}

		if _, err := os.Stat(customEmptyDir); !os.IsNotExist(err) {
			t.Fatalf("Expected custom empty directory to be removed, got %v", err)
		}
	})
}

func TestOptimizeDatabases(t *testing.T) {
	ctx := context.Background()

	t.Run("Optimize metadata and quota repositories", func(t *testing.T) {
		metaRepo := &stubMetadataRepository{}
		quotaRepo := &stubDirectoryQuotaRepository{}

		service := &cleanupService{
			metadataRepo: metaRepo,
			dirQuotaRepo: quotaRepo,
		}

		stats, err := service.OptimizeDatabases(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if stats.MetadataDatabasesOptimized != 1 {
			t.Fatalf("Expected metadata optimization count 1, got %d", stats.MetadataDatabasesOptimized)
		}

		if stats.QuotaDatabasesOptimized != 1 {
			t.Fatalf("Expected quota optimization count 1, got %d", stats.QuotaDatabasesOptimized)
		}

		if metaRepo.optimizeCalls != 1 {
			t.Fatalf("Expected metadata optimize to be called once, got %d", metaRepo.optimizeCalls)
		}

		if quotaRepo.optimizeCalls != 1 {
			t.Fatalf("Expected quota optimize to be called once, got %d", quotaRepo.optimizeCalls)
		}
	})

	t.Run("Stop when metadata optimization fails", func(t *testing.T) {
		metaRepo := &stubMetadataRepository{optimizeErr: errors.New("metadata optimize failed")}
		quotaRepo := &stubDirectoryQuotaRepository{}

		service := &cleanupService{
			metadataRepo: metaRepo,
			dirQuotaRepo: quotaRepo,
		}

		_, err := service.OptimizeDatabases(ctx)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if quotaRepo.optimizeCalls != 0 {
			t.Fatalf("Expected quota optimize not to run after metadata failure, got %d", quotaRepo.optimizeCalls)
		}
	})
}

// Helper functions

type stubMetadataRepository struct {
	optimizeCalls int
	optimizeErr   error
}

func (r *stubMetadataRepository) AddOrUpdate(ctx context.Context, metadata *core.FileMetadata) error {
	return nil
}

func (r *stubMetadataRepository) AddOrUpdateBatch(ctx context.Context, metadata []*core.FileMetadata) error {
	return nil
}

func (r *stubMetadataRepository) Get(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	return nil, core.ErrFileNotFound
}

func (r *stubMetadataRepository) Delete(ctx context.Context, tenantID, fileKey string) error {
	return nil
}

func (r *stubMetadataRepository) DeleteBatch(ctx context.Context, tenantID string, fileKeys []string) error {
	return nil
}

func (r *stubMetadataRepository) GetByStatus(ctx context.Context, tenantID string, status core.FileProcessingStatus, limit int) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (r *stubMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (r *stubMetadataRepository) UpdateStatus(ctx context.Context, tenantID, fileKey string, newStatus core.FileProcessingStatus) error {
	return nil
}

func (r *stubMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	return nil, core.ErrFileNotFound
}

func (r *stubMetadataRepository) CompareAndUpdateProcessing(
	ctx context.Context,
	lease core.FileProcessingLease,
	update func(*core.FileMetadata) error,
) (*core.FileMetadata, error) {
	return nil, core.ErrProcessingLeaseMismatch
}

func (r *stubMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (r *stubMetadataRepository) Optimize(ctx context.Context) error {
	r.optimizeCalls++
	return r.optimizeErr
}

func (r *stubMetadataRepository) Close() error {
	return nil
}

type stubTenantManager struct{}

func (m *stubTenantManager) GetTenant(ctx context.Context, tenantID string) (core.TenantContext, error) {
	return core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled}, nil
}

func (m *stubTenantManager) IsTenantEnabled(ctx context.Context, tenantID string) (bool, error) {
	return true, nil
}

func (m *stubTenantManager) CreateTenant(ctx context.Context, tenantID string) error  { return nil }
func (m *stubTenantManager) EnableTenant(ctx context.Context, tenantID string) error  { return nil }
func (m *stubTenantManager) DisableTenant(ctx context.Context, tenantID string) error { return nil }

func (m *stubTenantManager) GetAllTenants(ctx context.Context) ([]core.TenantContext, error) {
	return []core.TenantContext{{ID: "test-tenant", Status: core.TenantStatusEnabled}}, nil
}

type stubDirectoryQuotaRepository struct {
	optimizeCalls int
	optimizeErr   error
}

func (r *stubDirectoryQuotaRepository) GetOrCreate(ctx context.Context, tenantID, directoryPath string) (*core.DirectoryQuota, error) {
	return &core.DirectoryQuota{DirectoryPath: directoryPath}, nil
}

func (r *stubDirectoryQuotaRepository) Update(ctx context.Context, tenantID string, quota *core.DirectoryQuota) error {
	return nil
}

func (r *stubDirectoryQuotaRepository) IncrementCount(ctx context.Context, tenantID, directoryPath string) error {
	return nil
}

func (r *stubDirectoryQuotaRepository) DecrementCount(ctx context.Context, tenantID, directoryPath string) error {
	return nil
}

func (r *stubDirectoryQuotaRepository) GetAll(ctx context.Context, tenantID string) ([]*core.DirectoryQuota, error) {
	return nil, nil
}

func (r *stubDirectoryQuotaRepository) Optimize(ctx context.Context) error {
	r.optimizeCalls++
	return r.optimizeErr
}

func (r *stubDirectoryQuotaRepository) Close() error {
	return nil
}

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
