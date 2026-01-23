package quota

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestDirectoryQuotaRepository tests the directory quota repository.
func TestDirectoryQuotaRepository(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "quota-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	opts := &BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       tmpDir,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := NewBadgerDirectoryQuotaRepository(opts)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}

	t.Run("GetOrCreate creates default quota", func(t *testing.T) {
		quota, err := repo.GetOrCreate(ctx, "/path/to/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if quota.DirectoryPath != "/path/to/dir" {
			t.Errorf("Expected path '/path/to/dir', got %s", quota.DirectoryPath)
		}

		if quota.CurrentCount != 0 {
			t.Errorf("Expected count 0, got %d", quota.CurrentCount)
		}

		if quota.MaxCount != 0 {
			t.Errorf("Expected max count 0 (unlimited), got %d", quota.MaxCount)
		}

		if quota.Enabled {
			t.Error("Expected quota to be disabled by default")
		}
	})

	t.Run("GetOrCreate retrieves existing quota", func(t *testing.T) {
		// Get same quota again
		quota2, err := repo.GetOrCreate(ctx, "/path/to/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if quota2.DirectoryPath != "/path/to/dir" {
			t.Errorf("Expected same path, got %s", quota2.DirectoryPath)
		}
	})

	t.Run("Update quota", func(t *testing.T) {
		quota, _ := repo.GetOrCreate(ctx, "/path/to/update")
		quota.MaxCount = 100
		quota.Enabled = true

		err := repo.Update(ctx, quota)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify update
		updated, _ := repo.GetOrCreate(ctx, "/path/to/update")
		if updated.MaxCount != 100 {
			t.Errorf("Expected max count 100, got %d", updated.MaxCount)
		}

		if !updated.Enabled {
			t.Error("Expected quota to be enabled")
		}
	})

	t.Run("IncrementCount", func(t *testing.T) {
		// Get initial quota
		quota, _ := repo.GetOrCreate(ctx, "/path/to/increment")
		initialCount := quota.CurrentCount

		// Increment
		err := repo.IncrementCount(ctx, "/path/to/increment")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify increment
		updated, _ := repo.GetOrCreate(ctx, "/path/to/increment")
		if updated.CurrentCount != initialCount+1 {
			t.Errorf("Expected count %d, got %d", initialCount+1, updated.CurrentCount)
		}
	})

	t.Run("DecrementCount", func(t *testing.T) {
		// Setup: increment first
		_ = repo.IncrementCount(ctx, "/path/to/decrement")
		_ = repo.IncrementCount(ctx, "/path/to/decrement")

		quota, _ := repo.GetOrCreate(ctx, "/path/to/decrement")
		initialCount := quota.CurrentCount

		// Decrement
		err := repo.DecrementCount(ctx, "/path/to/decrement")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify decrement
		updated, _ := repo.GetOrCreate(ctx, "/path/to/decrement")
		if updated.CurrentCount != initialCount-1 {
			t.Errorf("Expected count %d, got %d", initialCount-1, updated.CurrentCount)
		}
	})
}

// TestDirectoryQuotaManager tests the directory quota manager.
func TestDirectoryQuotaManager(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "quota-mgr-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	opts := &BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       tmpDir,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, _ := NewBadgerDirectoryQuotaRepository(opts)
	manager, err := NewDirectoryQuotaManager(repo)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	t.Run("CanAddFile returns true for unlimited quota", func(t *testing.T) {
		canAdd, err := manager.CanAddFile(ctx, "tenant1", "/unlimited/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !canAdd {
			t.Error("Expected to be able to add file to unlimited quota")
		}
	})

	t.Run("SetQuota and check limit", func(t *testing.T) {
		// Set quota to 3 files
		err := manager.SetQuota(ctx, "tenant1", "/limited/dir", 3)
		if err != nil {
			t.Fatalf("Failed to set quota: %v", err)
		}

		// Should be able to add files
		for i := 0; i < 3; i++ {
			err := manager.IncrementFileCount(ctx, "tenant1", "/limited/dir")
			if err != nil {
				t.Errorf("Expected no error on increment %d, got %v", i+1, err)
			}
		}

		// 4th file should exceed quota
		err = manager.IncrementFileCount(ctx, "tenant1", "/limited/dir")
		if err != core.ErrDirectoryQuotaExceeded {
			t.Errorf("Expected ErrDirectoryQuotaExceeded, got %v", err)
		}
	})

	t.Run("GetFileCount", func(t *testing.T) {
		// Add some files
		_ = manager.IncrementFileCount(ctx, "tenant1", "/count/dir")
		_ = manager.IncrementFileCount(ctx, "tenant1", "/count/dir")
		_ = manager.IncrementFileCount(ctx, "tenant1", "/count/dir")

		count, err := manager.GetFileCount(ctx, "tenant1", "/count/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
	})

	t.Run("DecrementFileCount", func(t *testing.T) {
		// Add files first
		_ = manager.IncrementFileCount(ctx, "tenant1", "/decr/dir")
		_ = manager.IncrementFileCount(ctx, "tenant1", "/decr/dir")

		initialCount, _ := manager.GetFileCount(ctx, "tenant1", "/decr/dir")

		// Decrement
		err := manager.DecrementFileCount(ctx, "tenant1", "/decr/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		newCount, _ := manager.GetFileCount(ctx, "tenant1", "/decr/dir")
		if newCount != initialCount-1 {
			t.Errorf("Expected count %d, got %d", initialCount-1, newCount)
		}
	})

	t.Run("GetQuota", func(t *testing.T) {
		// Set quota
		_ = manager.SetQuota(ctx, "tenant1", "/get/dir", 50)

		quota, err := manager.GetQuota(ctx, "tenant1", "/get/dir")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if quota.MaxCount != 50 {
			t.Errorf("Expected max count 50, got %d", quota.MaxCount)
		}

		if !quota.Enabled {
			t.Error("Expected quota to be enabled")
		}
	})
}

// TestTenantQuotaManager tests the tenant quota manager.
func TestTenantQuotaManager(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager()

	t.Run("CanAddFile returns true for unlimited quota", func(t *testing.T) {
		canAdd, err := manager.CanAddFile(ctx, "tenant1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !canAdd {
			t.Error("Expected to be able to add file to unlimited quota")
		}
	})

	t.Run("SetQuota and check limit", func(t *testing.T) {
		// Set quota to 5 files
		err := manager.SetQuota(ctx, "tenant2", 5)
		if err != nil {
			t.Fatalf("Failed to set quota: %v", err)
		}

		// Add 5 files
		for i := 0; i < 5; i++ {
			err := manager.IncrementFileCount(ctx, "tenant2")
			if err != nil {
				t.Errorf("Expected no error on increment %d, got %v", i+1, err)
			}
		}

		// 6th file should exceed quota
		err = manager.IncrementFileCount(ctx, "tenant2")
		if err != core.ErrTenantQuotaExceeded {
			t.Errorf("Expected ErrTenantQuotaExceeded, got %v", err)
		}
	})

	t.Run("GetFileCount", func(t *testing.T) {
		// Clear tenant3 first by creating new manager or using a new tenant
		_ = manager.IncrementFileCount(ctx, "tenant3")
		_ = manager.IncrementFileCount(ctx, "tenant3")
		_ = manager.IncrementFileCount(ctx, "tenant3")

		count, err := manager.GetFileCount(ctx, "tenant3")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
	})

	t.Run("DecrementFileCount", func(t *testing.T) {
		_ = manager.IncrementFileCount(ctx, "tenant4")
		_ = manager.IncrementFileCount(ctx, "tenant4")

		initialCount, _ := manager.GetFileCount(ctx, "tenant4")

		err := manager.DecrementFileCount(ctx, "tenant4")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		newCount, _ := manager.GetFileCount(ctx, "tenant4")
		if newCount != initialCount-1 {
			t.Errorf("Expected count %d, got %d", initialCount-1, newCount)
		}
	})

	t.Run("Multiple tenants isolated", func(t *testing.T) {
		// Set different quotas for different tenants
		_ = manager.SetQuota(ctx, "tenant-a", 10)
		_ = manager.SetQuota(ctx, "tenant-b", 20)

		// Add files to each
		_ = manager.IncrementFileCount(ctx, "tenant-a")
		_ = manager.IncrementFileCount(ctx, "tenant-a")

		_ = manager.IncrementFileCount(ctx, "tenant-b")
		_ = manager.IncrementFileCount(ctx, "tenant-b")
		_ = manager.IncrementFileCount(ctx, "tenant-b")

		// Check counts are independent
		countA, _ := manager.GetFileCount(ctx, "tenant-a")
		countB, _ := manager.GetFileCount(ctx, "tenant-b")

		if countA != 2 {
			t.Errorf("Expected tenant-a count 2, got %d", countA)
		}

		if countB != 3 {
			t.Errorf("Expected tenant-b count 3, got %d", countB)
		}
	})
}
