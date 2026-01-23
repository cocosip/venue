package tenant

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

func setupTestManager(t *testing.T) (core.TenantManager, string) {
	t.Helper()

	tempDir := t.TempDir()
	opts := DefaultTenantManagerOptions(tempDir)

	manager, err := NewTenantManager(opts)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	return manager, tempDir
}

func TestNewTenantManager(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := DefaultTenantManagerOptions(tempDir)

		manager, err := NewTenantManager(opts)
		if err != nil {
			t.Errorf("NewTenantManager() error = %v", err)
		}
		if manager == nil {
			t.Error("NewTenantManager() returned nil")
		}
	})

	t.Run("nil options", func(t *testing.T) {
		_, err := NewTenantManager(nil)
		if err == nil {
			t.Error("Expected error for nil options")
		}
	})

	t.Run("empty root path", func(t *testing.T) {
		opts := &TenantManagerOptions{}
		_, err := NewTenantManager(opts)
		if err == nil {
			t.Error("Expected error for empty root path")
		}
	})
}

func TestTenantManager_CreateTenant(t *testing.T) {
	t.Run("create new tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		err := manager.CreateTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("CreateTenant() error = %v", err)
		}

		// Verify tenant exists
		tenant, err := manager.GetTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("GetTenant() error = %v", err)
		}
		if tenant.ID != "tenant-001" {
			t.Errorf("Expected tenant ID 'tenant-001', got '%s'", tenant.ID)
		}
		if tenant.Status != core.TenantStatusEnabled {
			t.Errorf("Expected status Enabled, got %v", tenant.Status)
		}
	})

	t.Run("duplicate tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		// Create first time
		manager.CreateTenant(ctx, "tenant-001")

		// Create second time should fail
		err := manager.CreateTenant(ctx, "tenant-001")
		if !errors.Is(err, core.ErrTenantAlreadyExists) {
			t.Errorf("Expected ErrTenantAlreadyExists, got %v", err)
		}
	})

	t.Run("empty tenant ID", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		err := manager.CreateTenant(ctx, "")
		if !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("Expected ErrInvalidArgument, got %v", err)
		}
	})
}

func TestTenantManager_GetTenant(t *testing.T) {
	t.Run("get existing tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		tenant, err := manager.GetTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("GetTenant() error = %v", err)
		}
		if tenant.ID != "tenant-001" {
			t.Errorf("Expected tenant ID 'tenant-001', got '%s'", tenant.ID)
		}
	})

	t.Run("get non-existent tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		_, err := manager.GetTenant(ctx, "non-existent")
		if !errors.Is(err, core.ErrTenantNotFound) {
			t.Errorf("Expected ErrTenantNotFound, got %v", err)
		}
	})

	t.Run("auto-create enabled", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := DefaultTenantManagerOptions(tempDir)
		opts.EnableAutoCreate = true

		manager, _ := NewTenantManager(opts)
		ctx := context.Background()

		// Should auto-create
		tenant, err := manager.GetTenant(ctx, "auto-tenant")
		if err != nil {
			t.Errorf("GetTenant() error = %v", err)
		}
		if tenant.ID != "auto-tenant" {
			t.Errorf("Expected tenant ID 'auto-tenant', got '%s'", tenant.ID)
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		// First call - loads from disk
		tenant1, _ := manager.GetTenant(ctx, "tenant-001")

		// Second call - should hit cache
		tenant2, _ := manager.GetTenant(ctx, "tenant-001")

		if tenant1.ID != tenant2.ID {
			t.Error("Cache returned different tenant")
		}
	})
}

func TestTenantManager_EnableDisableTenant(t *testing.T) {
	t.Run("disable tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		err := manager.DisableTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("DisableTenant() error = %v", err)
		}

		// Verify status
		tenant, _ := manager.GetTenant(ctx, "tenant-001")
		if tenant.Status != core.TenantStatusDisabled {
			t.Errorf("Expected status Disabled, got %v", tenant.Status)
		}
	})

	t.Run("enable tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")
		manager.DisableTenant(ctx, "tenant-001")

		err := manager.EnableTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("EnableTenant() error = %v", err)
		}

		// Verify status
		tenant, _ := manager.GetTenant(ctx, "tenant-001")
		if tenant.Status != core.TenantStatusEnabled {
			t.Errorf("Expected status Enabled, got %v", tenant.Status)
		}
	})

	t.Run("cache invalidation", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		// Get tenant to populate cache
		tenant1, _ := manager.GetTenant(ctx, "tenant-001")
		if tenant1.Status != core.TenantStatusEnabled {
			t.Error("Expected initial status Enabled")
		}

		// Disable tenant
		manager.DisableTenant(ctx, "tenant-001")

		// Get tenant again - should reload from disk
		tenant2, _ := manager.GetTenant(ctx, "tenant-001")
		if tenant2.Status != core.TenantStatusDisabled {
			t.Error("Cache was not invalidated after disable")
		}
	})
}

func TestTenantManager_IsTenantEnabled(t *testing.T) {
	t.Run("enabled tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		enabled, err := manager.IsTenantEnabled(ctx, "tenant-001")
		if err != nil {
			t.Errorf("IsTenantEnabled() error = %v", err)
		}
		if !enabled {
			t.Error("Expected tenant to be enabled")
		}
	})

	t.Run("disabled tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")
		manager.DisableTenant(ctx, "tenant-001")

		enabled, err := manager.IsTenantEnabled(ctx, "tenant-001")
		if err != nil {
			t.Errorf("IsTenantEnabled() error = %v", err)
		}
		if enabled {
			t.Error("Expected tenant to be disabled")
		}
	})

	t.Run("non-existent tenant", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		enabled, err := manager.IsTenantEnabled(ctx, "non-existent")
		if err != nil {
			t.Errorf("IsTenantEnabled() error = %v", err)
		}
		if enabled {
			t.Error("Expected non-existent tenant to be not enabled")
		}
	})
}

func TestTenantManager_GetAllTenants(t *testing.T) {
	t.Run("multiple tenants", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		// Create multiple tenants
		manager.CreateTenant(ctx, "tenant-001")
		manager.CreateTenant(ctx, "tenant-002")
		manager.CreateTenant(ctx, "tenant-003")

		tenants, err := manager.GetAllTenants(ctx)
		if err != nil {
			t.Errorf("GetAllTenants() error = %v", err)
		}

		if len(tenants) != 3 {
			t.Errorf("Expected 3 tenants, got %d", len(tenants))
		}
	})

	t.Run("no tenants", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		tenants, err := manager.GetAllTenants(ctx)
		if err != nil {
			t.Errorf("GetAllTenants() error = %v", err)
		}

		if len(tenants) != 0 {
			t.Errorf("Expected 0 tenants, got %d", len(tenants))
		}
	})
}

func TestTenantManager_CacheExpiration(t *testing.T) {
	t.Run("cache expires", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := DefaultTenantManagerOptions(tempDir)
		opts.CacheTTL = 100 * time.Millisecond // Very short TTL

		manager, _ := NewTenantManager(opts)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		// First call - loads from disk
		manager.GetTenant(ctx, "tenant-001")

		// Wait for cache to expire
		time.Sleep(150 * time.Millisecond)

		// Disable tenant on disk
		manager.DisableTenant(ctx, "tenant-001")

		// This should reload from disk (cache expired)
		tenant, _ := manager.GetTenant(ctx, "tenant-001")
		if tenant.Status != core.TenantStatusDisabled {
			t.Error("Cache did not expire, got old status")
		}
	})
}

func TestTenantManager_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent creates", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		// Try to create same tenant from multiple goroutines
		done := make(chan error, 10)
		for i := 0; i < 10; i++ {
			go func() {
				err := manager.CreateTenant(ctx, "tenant-001")
				done <- err
			}()
		}

		// Collect results
		successCount := 0
		alreadyExistsCount := 0
		for i := 0; i < 10; i++ {
			err := <-done
			if err == nil {
				successCount++
			} else if errors.Is(err, core.ErrTenantAlreadyExists) {
				alreadyExistsCount++
			}
		}

		// Should have exactly 1 success, rest should get ErrTenantAlreadyExists
		if successCount != 1 {
			t.Errorf("Expected 1 success, got %d", successCount)
		}
		if alreadyExistsCount != 9 {
			t.Errorf("Expected 9 already exists errors, got %d", alreadyExistsCount)
		}
	})

	t.Run("concurrent read/write", func(t *testing.T) {
		manager, _ := setupTestManager(t)
		ctx := context.Background()

		manager.CreateTenant(ctx, "tenant-001")

		// Concurrent reads and writes
		done := make(chan bool, 20)
		for i := 0; i < 10; i++ {
			go func() {
				manager.GetTenant(ctx, "tenant-001")
				done <- true
			}()
		}
		for i := 0; i < 10; i++ {
			go func(idx int) {
				if idx%2 == 0 {
					manager.EnableTenant(ctx, "tenant-001")
				} else {
					manager.DisableTenant(ctx, "tenant-001")
				}
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 20; i++ {
			<-done
		}

		// Should not panic or deadlock
	})
}

func TestTenantManager_Persistence(t *testing.T) {
	t.Run("reload after restart", func(t *testing.T) {
		tempDir := t.TempDir()
		opts := DefaultTenantManagerOptions(tempDir)

		// Create manager and tenant
		manager1, _ := NewTenantManager(opts)
		ctx := context.Background()
		manager1.CreateTenant(ctx, "tenant-001")
		manager1.DisableTenant(ctx, "tenant-001")

		// Create new manager with same root
		manager2, _ := NewTenantManager(opts)

		// Should load from disk
		tenant, err := manager2.GetTenant(ctx, "tenant-001")
		if err != nil {
			t.Errorf("GetTenant() error = %v", err)
		}
		if tenant.Status != core.TenantStatusDisabled {
			t.Error("Tenant status not persisted")
		}
	})
}

func TestMetadataStore(t *testing.T) {
	t.Run("save and load", func(t *testing.T) {
		tempDir := t.TempDir()
		metadataPath := filepath.Join(tempDir, ".locus", "tenants")

		store, err := NewMetadataStore(metadataPath)
		if err != nil {
			t.Fatalf("NewMetadataStore() error = %v", err)
		}

		// Create metadata
		metadata := createDefaultMetadata("tenant-001", "/storage/tenant-001")

		// Save
		err = store.Save(metadata)
		if err != nil {
			t.Errorf("Save() error = %v", err)
		}

		// Load
		loaded, err := store.Load("tenant-001")
		if err != nil {
			t.Errorf("Load() error = %v", err)
		}

		if loaded.TenantID != metadata.TenantID {
			t.Errorf("Expected TenantID %s, got %s", metadata.TenantID, loaded.TenantID)
		}
		if loaded.Status != metadata.Status {
			t.Errorf("Expected Status %v, got %v", metadata.Status, loaded.Status)
		}
	})

	t.Run("load non-existent", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := NewMetadataStore(tempDir)

		_, err := store.Load("non-existent")
		if !errors.Is(err, core.ErrTenantNotFound) {
			t.Errorf("Expected ErrTenantNotFound, got %v", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := NewMetadataStore(tempDir)

		metadata := createDefaultMetadata("tenant-001", "/storage/tenant-001")
		store.Save(metadata)

		err := store.Delete("tenant-001")
		if err != nil {
			t.Errorf("Delete() error = %v", err)
		}

		// Verify deleted
		exists, _ := store.Exists("tenant-001")
		if exists {
			t.Error("Tenant still exists after delete")
		}
	})

	t.Run("list all", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := NewMetadataStore(tempDir)

		// Create multiple tenants
		for i := 1; i <= 3; i++ {
			tenantID := fmt.Sprintf("tenant-%03d", i)
			metadata := createDefaultMetadata(tenantID, "/storage")
			store.Save(metadata)
		}

		// List all
		tenantIDs, err := store.ListAll()
		if err != nil {
			t.Errorf("ListAll() error = %v", err)
		}

		if len(tenantIDs) != 3 {
			t.Errorf("Expected 3 tenants, got %d", len(tenantIDs))
		}
	})

	t.Run("atomic write", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := NewMetadataStore(tempDir)

		metadata := createDefaultMetadata("tenant-001", "/storage/tenant-001")
		store.Save(metadata)

		// Save should be atomic via temp file + rename
		filePath := filepath.Join(tempDir, "tenant-001.json")
		tmpPath := filePath + ".tmp"

		// Temp file should not exist after save
		if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
			t.Error("Temp file still exists after save")
		}

		// Final file should exist
		if _, err := os.Stat(filePath); err != nil {
			t.Error("Final file does not exist after save")
		}
	})
}
