package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestScanNow_AutoCreateTenantDirectoriesCacheTTL proves the tenant list is
// enumerated once per TTL instead of on every scan, and that a tenant created
// later becomes visible after at most one TTL.
func TestScanNow_AutoCreateTenantDirectoriesCacheTTL(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	manager := w.tenantMgr.(*fakeTenantManager)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MultiTenantMode = true
		config.TenantID = ""
		config.AutoCreateTenantDirectories = true
		config.AutoCreateTenantDirectoriesCacheTTL = time.Minute
	}))

	// Registration enumerates the store once to create the tenant directories.
	if got := manager.enumerationCount(); got != 1 {
		t.Fatalf("tenant enumerations after registration = %d, want 1", got)
	}

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if got := manager.enumerationCount(); got != 1 {
		t.Fatalf("tenant enumerations after a cached scan = %d, want 1", got)
	}

	// Still inside the TTL window.
	current = current.Add(59 * time.Second)
	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if got := manager.enumerationCount(); got != 1 {
		t.Fatalf("tenant enumerations before the TTL elapsed = %d, want 1", got)
	}

	// A tenant created after the cached enumeration must appear after one TTL.
	manager.addTenant("tenant-b")

	current = current.Add(2 * time.Second)
	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if got := manager.enumerationCount(); got != 2 {
		t.Fatalf("tenant enumerations after the TTL elapsed = %d, want 2 (cache invalidated)", got)
	}

	if info, err := os.Stat(filepath.Join(watchPath, "tenant-b")); err != nil || !info.IsDir() {
		t.Fatalf("tenant-b directory was not created after the cache expired: %v", err)
	}
}

// TestTenantList_ZeroTTLUsesOneMinuteDefault pins the documented default for an
// unset TTL.
func TestTenantList_ZeroTTLUsesOneMinuteDefault(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	manager := w.tenantMgr.(*fakeTenantManager)
	config := &core.FileWatcherConfiguration{
		WatcherID:                           "w1",
		MultiTenantMode:                     true,
		AutoCreateTenantDirectories:         true,
		AutoCreateTenantDirectoriesCacheTTL: 0,
	}

	for i := 0; i < 3; i++ {
		if _, err := w.tenantList(context.Background(), config); err != nil {
			t.Fatalf("tenantList() error = %v", err)
		}
	}
	if got := manager.enumerationCount(); got != 1 {
		t.Fatalf("tenant enumerations = %d, want 1 inside the default TTL window", got)
	}

	current = current.Add(time.Minute)
	if _, err := w.tenantList(context.Background(), config); err != nil {
		t.Fatalf("tenantList() error = %v", err)
	}
	if got := manager.enumerationCount(); got != 2 {
		t.Fatalf("tenant enumerations = %d, want 2 after the default 1m TTL elapsed", got)
	}
}

// TestTenantList_NegativeTTLDisablesCache covers the escape hatch: a negative
// TTL re-enumerates the store every time.
func TestTenantList_NegativeTTLDisablesCache(t *testing.T) {
	w, _ := newTestWatcher(t)

	manager := w.tenantMgr.(*fakeTenantManager)
	config := &core.FileWatcherConfiguration{
		WatcherID:                           "w1",
		MultiTenantMode:                     true,
		AutoCreateTenantDirectories:         true,
		AutoCreateTenantDirectoriesCacheTTL: -1,
	}

	for i := 0; i < 3; i++ {
		if _, err := w.tenantList(context.Background(), config); err != nil {
			t.Fatalf("tenantList() error = %v", err)
		}
	}

	if got := manager.enumerationCount(); got != 3 {
		t.Fatalf("tenant enumerations = %d, want 3 with the cache disabled", got)
	}
}

// TestTenantList_IsolatesWatchers proves one watcher's cached list never serves
// another watcher, so a TTL is per watcher as documented.
func TestTenantList_IsolatesWatchers(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	manager := w.tenantMgr.(*fakeTenantManager)

	first := &core.FileWatcherConfiguration{
		WatcherID: "w1", MultiTenantMode: true, AutoCreateTenantDirectories: true,
		AutoCreateTenantDirectoriesCacheTTL: time.Minute,
	}
	second := &core.FileWatcherConfiguration{
		WatcherID: "w2", MultiTenantMode: true, AutoCreateTenantDirectories: true,
		AutoCreateTenantDirectoriesCacheTTL: time.Minute,
	}

	if _, err := w.tenantList(context.Background(), first); err != nil {
		t.Fatalf("tenantList(w1) error = %v", err)
	}
	if _, err := w.tenantList(context.Background(), second); err != nil {
		t.Fatalf("tenantList(w2) error = %v", err)
	}

	if got := manager.enumerationCount(); got != 2 {
		t.Fatalf("tenant enumerations = %d, want 2 (one per watcher)", got)
	}
}

// TestTenantList_ConcurrentReadersAreRaceFree exercises the cache under -race:
// concurrent scans of the same watcher must not race on the entry.
func TestTenantList_ConcurrentReadersAreRaceFree(t *testing.T) {
	w, _ := newTestWatcher(t)

	config := &core.FileWatcherConfiguration{
		WatcherID: "w1", MultiTenantMode: true, AutoCreateTenantDirectories: true,
		AutoCreateTenantDirectoriesCacheTTL: time.Minute,
	}

	start := make(chan struct{})

	errs := make(chan error, 8)

	for i := 0; i < 8; i++ {
		go func() {
			<-start

			if _, err := w.tenantList(context.Background(), config); err != nil {
				errs <- err
			}

			errs <- nil
		}()
	}

	close(start)

	for i := 0; i < 8; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("tenantList() error = %v", err)
		}
	}
}
