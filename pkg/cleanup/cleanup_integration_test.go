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
	"github.com/cocosip/venue/pkg/pool"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/tenant"
	"github.com/cocosip/venue/pkg/volume"
)

// TestCleanupServiceIntegration tests the cleanup service with a full system setup.
func TestCleanupServiceIntegration(t *testing.T) {
	ctx := context.Background()

	// Setup full system
	system := setupFullSystem(t)
	defer cleanupSystem(system)

	t.Run("End-to-end timed out file cleanup", func(t *testing.T) {
		// Create a tenant
		tenantCtx, err := system.tenantManager.GetTenant(ctx, "test-tenant")
		if err != nil {
			t.Fatalf("Failed to get tenant: %v", err)
		}

		// Write a file
		content := bytes.NewReader([]byte("test content for timeout"))
		fileName := "timeout-test.txt"
		fileKey, err := system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Get the file for processing
		location, err := system.storagePool.GetNextFileForProcessing(ctx, tenantCtx)
		if err != nil {
			t.Fatalf("Failed to get file for processing: %v", err)
		}

		if location.FileKey != fileKey {
			t.Errorf("Expected file key %s, got %s", fileKey, location.FileKey)
		}

		// Verify file is in Processing status
		status, _ := system.storagePool.GetFileStatus(ctx, fileKey)
		if status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", status)
		}

		// Manually update the processing start time to simulate timeout
		meta, _ := system.metadataRepo.Get(ctx, fileKey)
		oldTime := time.Now().Add(-2 * time.Hour)
		meta.ProcessingStartTime = &oldTime
		system.metadataRepo.AddOrUpdate(ctx, meta)

		// Run cleanup with 1 hour timeout
		stats, err := system.cleanupService.CleanupTimedOutProcessingFiles(ctx, 1*time.Hour)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if stats.TimedOutFilesReset != 1 {
			t.Errorf("Expected 1 file to be reset, got %d", stats.TimedOutFilesReset)
		}

		// Verify file is back to Pending
		status, _ = system.storagePool.GetFileStatus(ctx, fileKey)
		if status != core.FileStatusPending {
			t.Errorf("Expected status Pending after cleanup, got %v", status)
		}

		// Verify file can be retrieved again for processing
		location2, err := system.storagePool.GetNextFileForProcessing(ctx, tenantCtx)
		if err != nil {
			t.Fatalf("Failed to get file for processing after reset: %v", err)
		}

		if location2.FileKey != fileKey {
			t.Errorf("Expected same file key after reset, got %s", location2.FileKey)
		}
	})

	t.Run("End-to-end permanently failed file cleanup", func(t *testing.T) {
		tenantCtx, _ := system.tenantManager.GetTenant(ctx, "test-tenant")

		// Write a file
		content := bytes.NewReader([]byte("test content for permanent failure"))
		fileName := "failed-test.txt"
		fileKey, err := system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Get initial quota counts
		initialTenantCount, _ := system.tenantQuotaMgr.GetFileCount(ctx, "test-tenant")

		// Mark file as failed multiple times to trigger permanent failure
		for i := 0; i < 6; i++ {
			err = system.storagePool.MarkAsFailed(ctx, fileKey, "Test error")
			if err != nil {
				// Ignore errors as it might be permanently failed already
				break
			}
		}

		// Verify file is permanently failed
		status, _ := system.storagePool.GetFileStatus(ctx, fileKey)
		if status != core.FileStatusPermanentlyFailed {
			t.Errorf("Expected status PermanentlyFailed, got %v", status)
		}

		// Run cleanup
		stats, err := system.cleanupService.CleanupPermanentlyFailedFiles(ctx)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if stats.PermanentlyFailedFilesRemoved != 1 {
			t.Errorf("Expected 1 file to be removed, got %d", stats.PermanentlyFailedFilesRemoved)
		}

		if stats.SpaceFreed == 0 {
			t.Error("Expected some space to be freed")
		}

		// Verify file metadata is deleted
		_, err = system.metadataRepo.Get(ctx, fileKey)
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file metadata to be deleted, got error: %v", err)
		}

		// Verify quota was decremented
		newTenantCount, _ := system.tenantQuotaMgr.GetFileCount(ctx, "test-tenant")
		if newTenantCount != initialTenantCount-1 {
			t.Errorf("Expected tenant count to be decremented from %d to %d, got %d",
				initialTenantCount, initialTenantCount-1, newTenantCount)
		}
	})

	t.Run("End-to-end orphaned metadata cleanup", func(t *testing.T) {
		tenantCtx, _ := system.tenantManager.GetTenant(ctx, "test-tenant")

		// Write a file
		content := bytes.NewReader([]byte("test content for orphan"))
		fileName := "orphan-test.txt"
		fileKey, err := system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Get file location
		location, err := system.storagePool.GetFileLocation(ctx, tenantCtx, fileKey)
		if err != nil {
			t.Fatalf("Failed to get file location: %v", err)
		}

		// Get initial quota count
		initialTenantCount, _ := system.tenantQuotaMgr.GetFileCount(ctx, "test-tenant")

		// Manually delete the physical file to create orphaned metadata
		volume := system.volumes[location.VolumeID]
		volume.DeleteFile(ctx, location.PhysicalPath)

		// Verify physical file is gone
		exists, _ := volume.FileExists(ctx, location.PhysicalPath)
		if exists {
			t.Fatal("Physical file should have been deleted")
		}

		// Run cleanup
		stats, err := system.cleanupService.CleanupOrphanedMetadata(ctx)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if stats.OrphanedMetadataRemoved == 0 {
			t.Error("Expected at least one orphaned metadata to be removed")
		}

		// Verify metadata is deleted
		_, err = system.metadataRepo.Get(ctx, fileKey)
		if err != core.ErrFileNotFound {
			t.Errorf("Expected orphaned metadata to be deleted, got error: %v", err)
		}

		// Verify quota was decremented
		newTenantCount, _ := system.tenantQuotaMgr.GetFileCount(ctx, "test-tenant")
		if newTenantCount >= initialTenantCount {
			t.Errorf("Expected tenant count to be decremented from %d, got %d",
				initialTenantCount, newTenantCount)
		}
	})

	t.Run("End-to-end empty directory cleanup", func(t *testing.T) {
		// Get a volume
		var vol core.StorageVolume
		for _, v := range system.volumes {
			vol = v
			break
		}

		// Create some empty directories
		emptyDir1 := filepath.Join(vol.MountPath(), "tenant1", "empty-dir-1")
		emptyDir2 := filepath.Join(vol.MountPath(), "tenant1", "empty-dir-2", "nested")

		err := os.MkdirAll(emptyDir1, 0755)
		if err != nil {
			t.Fatalf("Failed to create empty dir 1: %v", err)
		}

		err = os.MkdirAll(emptyDir2, 0755)
		if err != nil {
			t.Fatalf("Failed to create empty dir 2: %v", err)
		}

		// Create a non-empty directory
		nonEmptyDir := filepath.Join(vol.MountPath(), "tenant1", "non-empty-dir")
		os.MkdirAll(nonEmptyDir, 0755)
		os.WriteFile(filepath.Join(nonEmptyDir, "file.txt"), []byte("content"), 0644)

		// Verify directories exist
		_, err1 := os.Stat(emptyDir1)
		_, err2 := os.Stat(emptyDir2)
		_, err3 := os.Stat(nonEmptyDir)

		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatal("Failed to create test directories")
		}

		// Run cleanup
		stats, err := system.cleanupService.CleanupEmptyDirectories(ctx)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if stats.EmptyDirectoriesRemoved == 0 {
			t.Error("Expected at least one empty directory to be removed")
		}

		// Verify empty directories are gone
		_, err1 = os.Stat(emptyDir1)
		if !os.IsNotExist(err1) {
			t.Error("Expected empty directory 1 to be removed")
		}

		// Note: nested empty directories might still exist if parent wasn't cleaned yet
		// This is OK as multiple passes might be needed

		// Verify non-empty directory still exists
		_, err3 = os.Stat(nonEmptyDir)
		if err3 != nil {
			t.Error("Expected non-empty directory to still exist")
		}
	})

	t.Run("Complete workflow with quota management", func(t *testing.T) {
		// Create a new tenant with quota
		tenantID := "quota-test-tenant"
		system.tenantManager.CreateTenant(ctx, tenantID)
		system.tenantQuotaMgr.SetQuota(ctx, tenantID, 5) // Max 5 files

		tenantCtx, _ := system.tenantManager.GetTenant(ctx, tenantID)

		// Write 3 files
		var fileKeys []string
		for i := 0; i < 3; i++ {
			content := bytes.NewReader([]byte("test content"))
			fileName := "quota-test.txt"
			fileKey, err := system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
			if err != nil {
				t.Fatalf("Failed to write file %d: %v", i, err)
			}
			fileKeys = append(fileKeys, fileKey)
		}

		// Verify quota count
		count, _ := system.tenantQuotaMgr.GetFileCount(ctx, tenantID)
		if count != 3 {
			t.Errorf("Expected quota count 3, got %d", count)
		}

		// Mark one file as permanently failed and clean it up
		for i := 0; i < 6; i++ {
			system.storagePool.MarkAsFailed(ctx, fileKeys[0], "Test error")
		}

		stats, err := system.cleanupService.CleanupPermanentlyFailedFiles(ctx)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if stats.PermanentlyFailedFilesRemoved < 1 {
			t.Error("Expected at least one file to be cleaned up")
		}

		// Verify quota was decremented
		count, _ = system.tenantQuotaMgr.GetFileCount(ctx, tenantID)
		if count != 2 {
			t.Errorf("Expected quota count 2 after cleanup, got %d", count)
		}

		// Should be able to add more files now
		content := bytes.NewReader([]byte("new file after cleanup"))
		fileName := "new-file.txt"
		_, err = system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
		if err != nil {
			t.Errorf("Should be able to add file after cleanup: %v", err)
		}
	})
}

// TestCleanupServiceConcurrency tests cleanup service under concurrent load.
func TestCleanupServiceConcurrency(t *testing.T) {
	ctx := context.Background()

	// Setup full system
	system := setupFullSystem(t)
	defer cleanupSystem(system)

	tenantCtx, _ := system.tenantManager.GetTenant(ctx, "test-tenant")

	// Write multiple files concurrently (with retry for transaction conflicts)
	// Using 10 files to avoid excessive transaction conflicts in BadgerDB
	const numFiles = 10
	fileKeys := make([]string, numFiles)
	errChan := make(chan error, numFiles)

	for i := 0; i < numFiles; i++ {
		go func(idx int) {
			var fileKey string
			var err error
			// Retry up to 5 times for transaction conflicts
			for retry := 0; retry < 5; retry++ {
				content := bytes.NewReader([]byte("concurrent test content"))
				fileName := "concurrent-test.txt"
				fileKey, err = system.storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
				if err == nil {
					break
				}
				time.Sleep(time.Duration(retry+1) * 20 * time.Millisecond)
			}
			if err != nil {
				errChan <- err
				return
			}
			fileKeys[idx] = fileKey
			errChan <- nil
		}(i)
	}

	// Wait for all writes to complete
	for i := 0; i < numFiles; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}
	}

	// Mark half as permanently failed
	for i := 0; i < numFiles/2; i++ {
		for j := 0; j < 6; j++ {
			system.storagePool.MarkAsFailed(ctx, fileKeys[i], "Test error")
		}
	}

	// Get the other half for processing (to set them to Processing status)
	for i := numFiles / 2; i < numFiles; i++ {
		system.storagePool.GetNextFileForProcessing(ctx, tenantCtx)
	}

	// Simulate timeout by updating processing start times
	for i := numFiles / 2; i < numFiles; i++ {
		meta, _ := system.metadataRepo.Get(ctx, fileKeys[i])
		oldTime := time.Now().Add(-2 * time.Hour)
		meta.ProcessingStartTime = &oldTime
		system.metadataRepo.AddOrUpdate(ctx, meta)
	}

	// Run all cleanup operations concurrently
	done := make(chan bool, 3)

	go func() {
		system.cleanupService.CleanupPermanentlyFailedFiles(ctx)
		done <- true
	}()

	go func() {
		system.cleanupService.CleanupTimedOutProcessingFiles(ctx, 1*time.Hour)
		done <- true
	}()

	go func() {
		system.cleanupService.CleanupEmptyDirectories(ctx)
		done <- true
	}()

	// Wait for all cleanup operations
	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify system is in consistent state
	// All permanently failed files should be removed
	for i := 0; i < numFiles/2; i++ {
		_, err := system.metadataRepo.Get(ctx, fileKeys[i])
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file %s to be deleted, got error: %v", fileKeys[i], err)
		}
	}

	// All timed out files should be back to Pending
	for i := numFiles / 2; i < numFiles; i++ {
		status, _ := system.storagePool.GetFileStatus(ctx, fileKeys[i])
		if status != core.FileStatusPending {
			t.Errorf("Expected file %s to be Pending, got %v", fileKeys[i], status)
		}
	}
}

// System represents the full integrated system for testing.
type System struct {
	tenantManager  core.TenantManager
	metadataRepo   core.MetadataRepository
	dirQuotaRepo   core.DirectoryQuotaRepository
	fileScheduler  core.FileScheduler
	volumes        map[string]core.StorageVolume
	tenantQuotaMgr core.TenantQuotaManager
	dirQuotaMgr    core.DirectoryQuotaManager
	storagePool    core.StoragePool
	cleanupService core.CleanupService
	tempDirs       []string
}

// setupFullSystem creates a complete integrated system for testing.
func setupFullSystem(t *testing.T) *System {
	ctx := context.Background()

	// Create temporary directory for tenant manager
	tmpRootDir, err := os.MkdirTemp("", "integration-tenant-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create tenant manager
	tenantMgr, err := tenant.NewTenantManager(&tenant.TenantManagerOptions{
		RootPath:         tmpRootDir,
		CacheTTL:         5 * time.Minute,
		EnableAutoCreate: false,
	})
	if err != nil {
		os.RemoveAll(tmpRootDir)
		t.Fatalf("Failed to create tenant manager: %v", err)
	}

	tenantMgr.CreateTenant(ctx, "test-tenant")

	// Create metadata repository
	tmpDir1, err := os.MkdirTemp("", "integration-metadata-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	metaOpts := &metadata.BadgerRepositoryOptions{
		TenantID:       "test-tenant",
		DataPath:       tmpDir1,
		CacheTTL:       5 * time.Minute,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	metadataRepo, err := metadata.NewBadgerMetadataRepository(metaOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		t.Fatalf("Failed to create metadata repository: %v", err)
	}

	// Create storage volume
	tmpDir2, err := os.MkdirTemp("", "integration-volume-*")
	if err != nil {
		os.RemoveAll(tmpDir1)
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	volOpts := &volume.LocalFileSystemVolumeOptions{
		VolumeID:  "test-volume",
		MountPath: tmpDir2,
	}

	vol, err := volume.NewLocalFileSystemVolume(volOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		t.Fatalf("Failed to create volume: %v", err)
	}

	volumes := map[string]core.StorageVolume{
		"test-volume": vol,
	}

	// Create quota managers
	tenantQuotaMgr := quota.NewTenantQuotaManager()

	tmpDir3, err := os.MkdirTemp("", "integration-dirquota-*")
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dirQuotaOpts := &quota.BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       tmpDir3,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	dirQuotaRepo, err := quota.NewBadgerDirectoryQuotaRepository(dirQuotaOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		os.RemoveAll(tmpDir3)
		t.Fatalf("Failed to create directory quota repository: %v", err)
	}

	dirQuotaMgr, err := quota.NewDirectoryQuotaManager(dirQuotaRepo)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		os.RemoveAll(tmpDir3)
		t.Fatalf("Failed to create directory quota manager: %v", err)
	}

	// Create file scheduler
	schedOpts := &scheduler.FileSchedulerOptions{
		RetryPolicy:       core.DefaultFileRetryPolicy(),
		ProcessingTimeout: 30 * time.Minute,
	}

	fileScheduler, err := scheduler.NewFileScheduler(metadataRepo, volumes, schedOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		os.RemoveAll(tmpDir3)
		t.Fatalf("Failed to create file scheduler: %v", err)
	}

	// Create storage pool
	poolOpts := &pool.StoragePoolOptions{
		TenantManager:         tenantMgr,
		MetadataRepository:    metadataRepo,
		FileScheduler:         fileScheduler,
		Volumes:               volumes,
		TenantQuotaManager:    tenantQuotaMgr,
		DirectoryQuotaManager: dirQuotaMgr,
	}

	storagePool, err := pool.NewStoragePool(poolOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		os.RemoveAll(tmpDir3)
		t.Fatalf("Failed to create storage pool: %v", err)
	}

	// Create cleanup service
	cleanupOpts := &CleanupServiceOptions{
		MetadataRepository:       metadataRepo,
		FileScheduler:            fileScheduler,
		Volumes:                  volumes,
		TenantQuotaManager:       tenantQuotaMgr,
		DirectoryQuotaManager:    dirQuotaMgr,
		DefaultProcessingTimeout: 30 * time.Minute,
	}

	cleanupService, err := NewCleanupService(cleanupOpts)
	if err != nil {
		os.RemoveAll(tmpDir1)
		os.RemoveAll(tmpDir2)
		os.RemoveAll(tmpDir3)
		t.Fatalf("Failed to create cleanup service: %v", err)
	}

	return &System{
		tenantManager:  tenantMgr,
		metadataRepo:   metadataRepo,
		dirQuotaRepo:   dirQuotaRepo,
		fileScheduler:  fileScheduler,
		volumes:        volumes,
		tenantQuotaMgr: tenantQuotaMgr,
		dirQuotaMgr:    dirQuotaMgr,
		storagePool:    storagePool,
		cleanupService: cleanupService,
		tempDirs:       []string{tmpRootDir, tmpDir1, tmpDir2, tmpDir3},
	}
}

// cleanupSystem cleans up all resources used by the system.
func cleanupSystem(system *System) {
	// Close databases first (must be done before RemoveAll)
	if system.metadataRepo != nil {
		system.metadataRepo.Close()
	}
	if system.dirQuotaRepo != nil {
		system.dirQuotaRepo.Close()
	}

	// Then remove directories
	for _, dir := range system.tempDirs {
		os.RemoveAll(dir)
	}
	for _, vol := range system.volumes {
		os.RemoveAll(vol.MountPath())
	}
}
