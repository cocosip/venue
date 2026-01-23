package pool

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/scheduler"
)

// TestIntegration_CompleteWorkflow tests the complete file processing workflow.
func TestIntegration_CompleteWorkflow(t *testing.T) {
	ctx := context.Background()

	// Setup components
	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	t.Run("Complete file lifecycle", func(t *testing.T) {
		// 1. Write file
		testContent := "test content for processing"
		content := bytes.NewReader([]byte(testContent))
		fileName := "workflow-test.txt"

		fileKey, err := pool.WriteFile(ctx, tenant, content, &fileName)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// 2. Get file for processing
		location, err := pool.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("Failed to get file for processing: %v", err)
		}

		if location.FileKey != fileKey {
			t.Errorf("Expected file key %s, got %s", fileKey, location.FileKey)
		}

		if location.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", location.Status)
		}

		// 3. Read file content (simulate processing)
		reader, err := pool.ReadFile(ctx, tenant, fileKey)
		if err != nil {
			t.Fatalf("Failed to read file: %v", err)
		}

		readContent, _ := io.ReadAll(reader)
		reader.Close()

		if string(readContent) != testContent {
			t.Errorf("Expected content '%s', got '%s'", testContent, string(readContent))
		}

		// 4. Mark as completed
		err = pool.MarkAsCompleted(ctx, fileKey)
		if err != nil {
			t.Fatalf("Failed to mark as completed: %v", err)
		}

		// 5. Verify file is deleted
		_, err = pool.GetFileInfo(ctx, tenant, fileKey)
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file to be deleted, got error: %v", err)
		}
	})
}

// TestIntegration_FailedFileRetry tests file retry mechanism.
func TestIntegration_FailedFileRetry(t *testing.T) {
	ctx := context.Background()

	// Setup components with custom retry policy
	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	schedOpts := &scheduler.FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         2,
			InitialRetryDelay:     100 * time.Millisecond,
			UseExponentialBackoff: true,
			MaxRetryDelay:         1 * time.Second,
		},
		ProcessingTimeout: 30 * time.Minute,
	}

	sched, _ := scheduler.NewFileScheduler(repo, volumes, schedOpts)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	// Write a file
	content := bytes.NewReader([]byte("test content"))
	fileKey, _ := pool.WriteFile(ctx, tenant, content, nil)

	// First attempt
	location, _ := pool.GetNextFileForProcessing(ctx, tenant)

	// Fail it twice (within max retries)
	for i := 1; i <= 2; i++ {
		err := pool.MarkAsFailed(ctx, location.FileKey, "Test failure")
		if err != nil {
			t.Fatalf("Attempt %d: Failed to mark as failed: %v", i, err)
		}

		// Wait for retry delay
		time.Sleep(150 * time.Millisecond * time.Duration(i))

		// Get file again
		location, _ = pool.GetNextFileForProcessing(ctx, tenant)
	}

	// Third failure should make it permanently failed
	err := pool.MarkAsFailed(ctx, location.FileKey, "Final failure")
	if err != nil {
		t.Fatalf("Failed to mark as failed: %v", err)
	}

	// Verify status
	status, _ := pool.GetFileStatus(ctx, fileKey)
	if status != core.FileStatusPermanentlyFailed {
		t.Errorf("Expected status PermanentlyFailed, got %v", status)
	}

	// File should not be available for processing anymore
	_, err = pool.GetNextFileForProcessing(ctx, tenant)
	if err != core.ErrNoFilesAvailable {
		t.Errorf("Expected no files available, got error: %v", err)
	}
}

// TestIntegration_BatchProcessing tests batch file processing.
func TestIntegration_BatchProcessing(t *testing.T) {
	ctx := context.Background()

	// Setup components
	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	// Write 10 files
	for i := 0; i < 10; i++ {
		content := bytes.NewReader([]byte("test content"))
		pool.WriteFile(ctx, tenant, content, nil)
	}

	// Get batch of 5 files
	batch, err := pool.GetNextBatchForProcessing(ctx, tenant, 5)
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}

	if len(batch) != 5 {
		t.Errorf("Expected 5 files in batch, got %d", len(batch))
	}

	// Mark all as completed
	for _, location := range batch {
		pool.MarkAsCompleted(ctx, location.FileKey)
	}

	// Get another batch (remaining 5)
	batch2, err := pool.GetNextBatchForProcessing(ctx, tenant, 10)
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}

	if len(batch2) != 5 {
		t.Errorf("Expected 5 files in batch 2, got %d", len(batch2))
	}
}

// TestIntegration_ConcurrentProcessing tests concurrent file processing.
func TestIntegration_ConcurrentProcessing(t *testing.T) {
	ctx := context.Background()

	// Setup components
	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	// Write 30 files
	for i := 0; i < 30; i++ {
		content := bytes.NewReader([]byte("test content"))
		pool.WriteFile(ctx, tenant, content, nil)
	}

	// Simulate 5 concurrent workers
	var wg sync.WaitGroup
	processedCount := make(map[string]int)
	var mu sync.Mutex

	for workerID := 0; workerID < 5; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				location, err := pool.GetNextFileForProcessing(ctx, tenant)
				if err == core.ErrNoFilesAvailable {
					break
				}

				if err != nil {
					t.Errorf("Worker %d: Unexpected error: %v", id, err)
					break
				}

				// Check for duplicates
				mu.Lock()
				processedCount[location.FileKey]++
				if processedCount[location.FileKey] > 1 {
					t.Errorf("Worker %d: File %s processed multiple times!", id, location.FileKey)
				}
				mu.Unlock()

				// Simulate processing
				time.Sleep(5 * time.Millisecond)

				// Mark as completed
				pool.MarkAsCompleted(ctx, location.FileKey)
			}
		}(workerID)
	}

	wg.Wait()

	// Verify all files were processed
	if len(processedCount) != 30 {
		t.Errorf("Expected 30 files to be processed, got %d", len(processedCount))
	}

	// Verify no files are left
	_, err := pool.GetNextFileForProcessing(ctx, tenant)
	if err != core.ErrNoFilesAvailable {
		t.Errorf("Expected no files available, got error: %v", err)
	}
}

// TestIntegration_MultiTenantIsolation tests tenant isolation.
func TestIntegration_MultiTenantIsolation(t *testing.T) {
	ctx := context.Background()

	// Setup components
	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)

	tenant1 := core.TenantContext{
		ID:     "tenant-1",
		Status: core.TenantStatusEnabled,
	}

	tenant2 := core.TenantContext{
		ID:     "tenant-2",
		Status: core.TenantStatusEnabled,
	}

	// Write files for tenant1
	content1 := bytes.NewReader([]byte("tenant1 content"))
	fileKey1, _ := pool.WriteFile(ctx, tenant1, content1, nil)

	// Write files for tenant2
	content2 := bytes.NewReader([]byte("tenant2 content"))
	fileKey2, _ := pool.WriteFile(ctx, tenant2, content2, nil)

	// Tenant1 should only see their file
	_, err := pool.ReadFile(ctx, tenant1, fileKey1)
	if err != nil {
		t.Errorf("Tenant1 should be able to read their file, got error: %v", err)
	}

	_, err = pool.ReadFile(ctx, tenant1, fileKey2)
	if err != core.ErrFileNotFound {
		t.Errorf("Tenant1 should not be able to read tenant2's file, got error: %v", err)
	}

	// Tenant2 should only see their file
	_, err = pool.ReadFile(ctx, tenant2, fileKey2)
	if err != nil {
		t.Errorf("Tenant2 should be able to read their file, got error: %v", err)
	}

	_, err = pool.ReadFile(ctx, tenant2, fileKey1)
	if err != core.ErrFileNotFound {
		t.Errorf("Tenant2 should not be able to read tenant1's file, got error: %v", err)
	}
}

// TestIntegration_PathGeneration tests different path generators.
func TestIntegration_PathGeneration(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	t.Run("Date-based path generator", func(t *testing.T) {
		opts := &StoragePoolOptions{
			TenantManager:      tenantMgr,
			MetadataRepository: repo,
			FileScheduler:      sched,
			Volumes:            volumes,
			PathGenerator:      &DateBasedPathGenerator{},
		}

		pool, _ := NewStoragePool(opts)
		tenant := createTestTenant()

		content := bytes.NewReader([]byte("test"))
		fileName := "test.txt"
		fileKey, _ := pool.WriteFile(ctx, tenant, content, &fileName)

		location, _ := pool.GetFileLocation(ctx, tenant, fileKey)

		// Path should contain year/month/day
		now := time.Now()
		year := now.Format("2006")
		month := now.Format("01")
		day := now.Format("02")

		if !contains(location.PhysicalPath, year) {
			t.Errorf("Expected path to contain year %s, got %s", year, location.PhysicalPath)
		}

		if !contains(location.PhysicalPath, month) {
			t.Errorf("Expected path to contain month %s, got %s", month, location.PhysicalPath)
		}

		if !contains(location.PhysicalPath, day) {
			t.Errorf("Expected path to contain day %s, got %s", day, location.PhysicalPath)
		}
	})

	t.Run("Flat path generator", func(t *testing.T) {
		opts := &StoragePoolOptions{
			TenantManager:      tenantMgr,
			MetadataRepository: repo,
			FileScheduler:      sched,
			Volumes:            volumes,
			PathGenerator:      &FlatPathGenerator{},
		}

		pool, _ := NewStoragePool(opts)
		tenant := createTestTenant()

		content := bytes.NewReader([]byte("test"))
		fileName := "test.txt"
		fileKey, _ := pool.WriteFile(ctx, tenant, content, &fileName)

		location, _ := pool.GetFileLocation(ctx, tenant, fileKey)

		// Path should be flat: tenantID/fileKey.ext
		// Should NOT contain year/month/day
		now := time.Now()
		year := now.Format("2006")

		if contains(location.PhysicalPath, year) {
			t.Errorf("Flat path should not contain year, got %s", location.PhysicalPath)
		}
	})
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsInner(s, substr)))
}

func containsInner(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
