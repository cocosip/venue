package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

func setupBenchManager(b *testing.B) core.TenantManager {
	b.Helper()

	tempDir := b.TempDir()
	opts := DefaultTenantManagerOptions(tempDir)

	manager, err := NewTenantManager(opts)
	if err != nil {
		b.Fatalf("Failed to create manager: %v", err)
	}

	return manager
}

func BenchmarkTenantManager_CreateTenant(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.CreateTenant(ctx, tenantID)
	}
}

func BenchmarkTenantManager_GetTenant_CacheHit(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	// Create a tenant
	_ = manager.CreateTenant(ctx, "tenant-001")

	// Warm up cache
	_, _ = manager.GetTenant(ctx, "tenant-001")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.GetTenant(ctx, "tenant-001")
	}
}

func BenchmarkTenantManager_GetTenant_CacheMiss(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	// Create tenants
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.CreateTenant(ctx, tenantID)
	}

	// Invalidate all caches (type assertion for testing)
	if impl, ok := manager.(*TenantManager); ok {
		impl.cacheMu.Lock()
		impl.cache = make(map[string]*cacheEntry)
		impl.cacheMu.Unlock()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_, _ = manager.GetTenant(ctx, tenantID)
	}
}

func BenchmarkTenantManager_IsTenantEnabled_CacheHit(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	_ = manager.CreateTenant(ctx, "tenant-001")
	_, _ = manager.GetTenant(ctx, "tenant-001") // Warm cache

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.IsTenantEnabled(ctx, "tenant-001")
	}
}

func BenchmarkTenantManager_DisableTenant(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	// Create tenants
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.CreateTenant(ctx, tenantID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.DisableTenant(ctx, tenantID)
	}
}

func BenchmarkTenantManager_EnableTenant(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	// Create and disable tenants
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.CreateTenant(ctx, tenantID)
		_ = manager.DisableTenant(ctx, tenantID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_ = manager.EnableTenant(ctx, tenantID)
	}
}

func BenchmarkTenantManager_ConcurrentGetTenant(b *testing.B) {
	manager := setupBenchManager(b)
	ctx := context.Background()

	// Create a tenant
	_ = manager.CreateTenant(ctx, "tenant-001")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = manager.GetTenant(ctx, "tenant-001")
		}
	})
}

func BenchmarkMetadataStore_Save(b *testing.B) {
	tempDir := b.TempDir()
	store, _ := NewMetadataStore(tempDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		metadata := createDefaultMetadata(tenantID, "/storage")
		_ = store.Save(metadata)
	}
}

func BenchmarkMetadataStore_Load(b *testing.B) {
	tempDir := b.TempDir()
	store, _ := NewMetadataStore(tempDir)

	// Pre-create tenants
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		metadata := createDefaultMetadata(tenantID, "/storage")
		_ = store.Save(metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		_, _ = store.Load(tenantID)
	}
}

func BenchmarkMetadataStore_Exists(b *testing.B) {
	tempDir := b.TempDir()
	store, _ := NewMetadataStore(tempDir)

	// Pre-create some tenants
	for i := 0; i < 100; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		metadata := createDefaultMetadata(tenantID, "/storage")
		_ = store.Save(metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i%100)
		_, _ = store.Exists(tenantID)
	}
}
