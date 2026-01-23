package scheduler

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestIntegration_FileLifecycle tests the complete lifecycle of a file.
func TestIntegration_FileLifecycle(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	t.Run("Successful processing lifecycle", func(t *testing.T) {
		// 1. Add a pending file
		file := createTestFileMetadata("lifecycle-file", core.FileStatusPending)
		repo.AddOrUpdate(ctx, file)

		// 2. Get file for processing
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if location.Status != core.FileStatusProcessing {
			t.Errorf("Expected status Processing, got %v", location.Status)
		}

		// 3. Mark as completed
		err = scheduler.MarkAsCompleted(ctx, location.FileKey)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// 4. Verify file is deleted
		_, err = repo.Get(ctx, location.FileKey)
		if err != core.ErrFileNotFound {
			t.Errorf("Expected file to be deleted, got error: %v", err)
		}
	})
}

// TestIntegration_RetryMechanism tests the retry mechanism.
func TestIntegration_RetryMechanism(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	opts := &FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         3,
			InitialRetryDelay:     100 * time.Millisecond,
			UseExponentialBackoff: true,
			MaxRetryDelay:         1 * time.Second,
		},
		ProcessingTimeout: 30 * time.Minute,
	}

	scheduler, _ := NewFileScheduler(repo, volumes, opts)
	tenant := createTestTenant()

	// Add a pending file
	file := createTestFileMetadata("retry-file", core.FileStatusPending)
	repo.AddOrUpdate(ctx, file)

	// First attempt
	location, _ := scheduler.GetNextFileForProcessing(ctx, tenant)

	// Fail 3 times (within max retries)
	for i := 1; i <= 3; i++ {
		err := scheduler.MarkAsFailed(ctx, location.FileKey, "Retry test error")
		if err != nil {
			t.Fatalf("Attempt %d: Expected no error, got %v", i, err)
		}

		// Verify retry count
		metadata, _ := repo.Get(ctx, location.FileKey)
		if metadata.RetryCount != i {
			t.Errorf("Attempt %d: Expected RetryCount %d, got %d", i, i, metadata.RetryCount)
		}

		if metadata.Status != core.FileStatusPending {
			t.Errorf("Attempt %d: Expected status Pending, got %v", i, metadata.Status)
		}

		// Calculate exponential backoff delay: 100ms * 2^(i-1)
		// i=1: 100ms, i=2: 200ms, i=3: 400ms
		delay := opts.RetryPolicy.CalculateRetryDelay(i)
		time.Sleep(delay + 50*time.Millisecond) // Add 50ms buffer

		// Get file again for next attempt
		location, err = scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("Attempt %d: Failed to get file for retry: %v", i, err)
		}
	}

	// 4th failure should permanently fail
	err := scheduler.MarkAsFailed(ctx, location.FileKey, "Final error")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify permanently failed
	metadata, _ := repo.Get(ctx, location.FileKey)
	if metadata.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("Expected status PermanentlyFailed, got %v", metadata.Status)
	}

	if metadata.RetryCount != 4 {
		t.Errorf("Expected RetryCount 4, got %d", metadata.RetryCount)
	}
}

// TestIntegration_ConcurrentProcessing tests concurrent file processing.
func TestIntegration_ConcurrentProcessing(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	// Add 20 pending files
	for i := 0; i < 20; i++ {
		file := createTestFileMetadata(string(rune('a'+i)), core.FileStatusPending)
		repo.AddOrUpdate(ctx, file)
	}

	// Simulate 5 concurrent workers
	var wg sync.WaitGroup
	processedFiles := make(map[string]bool)
	var mu sync.Mutex

	for workerID := 0; workerID < 5; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
				if err == core.ErrNoFilesAvailable {
					break
				}

				if err != nil {
					t.Errorf("Worker %d: Unexpected error: %v", id, err)
					break
				}

				// Ensure no duplicate processing
				mu.Lock()
				if processedFiles[location.FileKey] {
					t.Errorf("Worker %d: File %s was processed twice!", id, location.FileKey)
				}
				processedFiles[location.FileKey] = true
				mu.Unlock()

				// Simulate processing
				time.Sleep(10 * time.Millisecond)

				// Mark as completed
				scheduler.MarkAsCompleted(ctx, location.FileKey)
			}
		}(workerID)
	}

	wg.Wait()

	// Verify all files were processed
	if len(processedFiles) != 20 {
		t.Errorf("Expected 20 files to be processed, got %d", len(processedFiles))
	}

	// Verify no files left in pending status
	pendingFiles, _ := repo.GetPendingFiles(ctx, tenant.ID, 100)
	if len(pendingFiles) != 0 {
		t.Errorf("Expected 0 pending files, got %d", len(pendingFiles))
	}
}

// TestIntegration_TimeoutRecovery tests timeout recovery mechanism.
func TestIntegration_TimeoutRecovery(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	// Add a pending file
	file := createTestFileMetadata("timeout-file", core.FileStatusPending)
	repo.AddOrUpdate(ctx, file)

	// Get file for processing
	location, _ := scheduler.GetNextFileForProcessing(ctx, tenant)

	// Simulate timeout by manually setting old processing start time
	metadata, _ := repo.Get(ctx, location.FileKey)
	longAgo := time.Now().Add(-2 * time.Hour)
	metadata.ProcessingStartTime = &longAgo
	repo.AddOrUpdate(ctx, metadata)

	// Reset timed out files
	count, err := scheduler.ResetTimedOutFiles(ctx, 1*time.Hour)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 file to be reset, got %d", count)
	}

	// Verify file is back to Pending
	updated, _ := repo.Get(ctx, location.FileKey)
	if updated.Status != core.FileStatusPending {
		t.Errorf("Expected status Pending, got %v", updated.Status)
	}

	if updated.ProcessingStartTime != nil {
		t.Error("Expected ProcessingStartTime to be cleared")
	}

	// Verify file can be processed again
	location2, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("Expected file to be available again, got error: %v", err)
	}

	if location2.FileKey != location.FileKey {
		t.Errorf("Expected same file key, got different file")
	}
}

// TestIntegration_BatchProcessing tests batch processing.
func TestIntegration_BatchProcessing(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, _ := NewFileScheduler(repo, volumes, nil)
	tenant := createTestTenant()

	// Add 15 pending files
	for i := 0; i < 15; i++ {
		file := createTestFileMetadata(string(rune('a'+i)), core.FileStatusPending)
		repo.AddOrUpdate(ctx, file)
	}

	// Get first batch of 5
	batch1, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(batch1) != 5 {
		t.Errorf("Expected 5 files in batch 1, got %d", len(batch1))
	}

	// Get second batch of 5
	batch2, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(batch2) != 5 {
		t.Errorf("Expected 5 files in batch 2, got %d", len(batch2))
	}

	// Get third batch (only 5 remaining)
	batch3, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 10)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(batch3) != 5 {
		t.Errorf("Expected 5 files in batch 3, got %d", len(batch3))
	}

	// Ensure no duplicates across batches
	allFiles := make(map[string]bool)
	for _, loc := range append(append(batch1, batch2...), batch3...) {
		if allFiles[loc.FileKey] {
			t.Errorf("File %s appeared in multiple batches!", loc.FileKey)
		}
		allFiles[loc.FileKey] = true
	}

	// No more files should be available
	batch4, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(batch4) != 0 {
		t.Errorf("Expected 0 files in batch 4, got %d", len(batch4))
	}
}

// TestIntegration_ExponentialBackoff tests exponential backoff for retries.
func TestIntegration_ExponentialBackoff(t *testing.T) {
	ctx := context.Background()

	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	opts := &FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         5,
			InitialRetryDelay:     1 * time.Second,
			UseExponentialBackoff: true,
			MaxRetryDelay:         10 * time.Second,
		},
		ProcessingTimeout: 30 * time.Minute,
	}

	scheduler, _ := NewFileScheduler(repo, volumes, opts)
	tenant := createTestTenant()

	// Add a pending file
	file := createTestFileMetadata("backoff-file", core.FileStatusPending)
	repo.AddOrUpdate(ctx, file)

	// Get and fail the file
	location, _ := scheduler.GetNextFileForProcessing(ctx, tenant)
	scheduler.MarkAsFailed(ctx, location.FileKey, "Test error")

	// Check retry delay after first failure (should be 1 second)
	metadata, _ := repo.Get(ctx, location.FileKey)
	if metadata.AvailableForProcessingAt == nil {
		t.Fatal("Expected AvailableForProcessingAt to be set")
	}

	delay1 := metadata.AvailableForProcessingAt.Sub(time.Now())
	if delay1 < 900*time.Millisecond || delay1 > 1100*time.Millisecond {
		t.Errorf("Expected delay ~1s, got %v", delay1)
	}

	// Fail again
	time.Sleep(1100 * time.Millisecond)
	location, _ = scheduler.GetNextFileForProcessing(ctx, tenant)
	scheduler.MarkAsFailed(ctx, location.FileKey, "Test error 2")

	// Check retry delay after second failure (should be 2 seconds)
	metadata, _ = repo.Get(ctx, location.FileKey)
	delay2 := metadata.AvailableForProcessingAt.Sub(time.Now())
	if delay2 < 1900*time.Millisecond || delay2 > 2100*time.Millisecond {
		t.Errorf("Expected delay ~2s, got %v", delay2)
	}

	// Fail again
	time.Sleep(2100 * time.Millisecond)
	location, _ = scheduler.GetNextFileForProcessing(ctx, tenant)
	scheduler.MarkAsFailed(ctx, location.FileKey, "Test error 3")

	// Check retry delay after third failure (should be 4 seconds)
	metadata, _ = repo.Get(ctx, location.FileKey)
	delay3 := metadata.AvailableForProcessingAt.Sub(time.Now())
	if delay3 < 3900*time.Millisecond || delay3 > 4100*time.Millisecond {
		t.Errorf("Expected delay ~4s, got %v", delay3)
	}
}
