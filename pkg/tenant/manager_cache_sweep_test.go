package tenant

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestCacheEvictsExpiredEntriesWhenSweepRuns verifies that expired cache
// entries are reclaimed by the periodic sweep in putCache: an ID stream that
// passes validation but names no real tenant must not grow the cache map
// without bound.
func TestCacheEvictsExpiredEntriesWhenSweepRuns(t *testing.T) {
	root := t.TempDir()
	manager, err := NewTenantManager(&TenantManagerOptions{
		RootPath:         filepath.Join(root, "tenants"),
		CacheTTL:         time.Millisecond,
		EnableAutoCreate: false,
	})
	if err != nil {
		t.Fatalf("NewTenantManager() error = %v", err)
	}
	concrete, ok := manager.(*TenantManager)
	if !ok {
		t.Fatalf("NewTenantManager() returned %T, want *TenantManager", manager)
	}

	// Exactly cacheSweepInterval writes: the final write crosses the sweep
	// threshold. Every entry except the last one has expired by then, and the
	// sweep must reclaim them instead of leaving the map at full size.
	for i := 0; i < cacheSweepInterval; i++ {
		concrete.putCache(fmt.Sprintf("tenant-%04d", i), core.TenantContext{})
		time.Sleep(2 * time.Millisecond)
	}

	concrete.cacheMu.RLock()
	size := len(concrete.cache)
	concrete.cacheMu.RUnlock()

	if size != 1 {
		t.Fatalf("cache holds %d entries after the sweep, want only the just-written entry", size)
	}
}
