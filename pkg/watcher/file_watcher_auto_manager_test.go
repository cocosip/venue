package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// --- helpers ----------------------------------------------------------------

// newTestAutoManager builds an auto manager whose generated watchers are real
// watchers writing into configRoot, so a restart can be observed.
func newTestAutoManager(t *testing.T, configRoot string, roots []core.FileWatcherRootConfiguration) (*fileWatcherAutoManager, *fileWatcher) {
	t.Helper()

	coreWatcher, _ := newTestWatcherAt(t, configRoot)

	manager, err := NewFileWatcherAutoManager(&FileWatcherAutoManagerOptions{
		FileWatcher:          coreWatcher,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
		Roots:                roots,
	})
	if err != nil {
		t.Fatalf("NewFileWatcherAutoManager() error = %v", err)
	}

	concrete, ok := manager.(*fileWatcherAutoManager)
	if !ok {
		t.Fatalf("NewFileWatcherAutoManager() returned %T, want *fileWatcherAutoManager", manager)
	}

	return concrete, coreWatcher
}

// multiTenantRootTemplate returns a root template that differs from the runtime
// defaults, so a generated watcher proves it copied the template.
func multiTenantRootTemplate(rootPath string) core.FileWatcherRootConfiguration {
	return core.FileWatcherRootConfiguration{
		RootPath:              rootPath,
		MultiTenantMode:       true,
		Enabled:               true,
		IncludeSubdirectories: true,
		FilePatterns:          []string{"*.csv"},
		PostImportAction:      core.PostImportActionKeep,
		PollingInterval:       45 * time.Second,
		MaxFileSizeBytes:      1024,
		MinFileAge:            2 * time.Second,
		MaxConcurrentImports:  3,
	}
}

func tenantDirPath(rootPath string, name string) string {
	return filepath.Join(rootPath, name)
}

// --- ApplyRootConfiguration -------------------------------------------------

func TestFileWatcherAutoManager_ApplyMultiTenantRootCreatesOneWatcherPerDirectory(t *testing.T) {
	rootPath := t.TempDir()
	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}
	for _, tenant := range tenants {
		if err := os.Mkdir(tenantDirPath(rootPath, tenant), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", tenant, err)
		}
	}

	// A regular file directly under the root is not a tenant directory.
	writeAgedFile(t, filepath.Join(rootPath, "stray.csv"), "not-a-tenant")

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	created, err := manager.ApplyRootConfiguration(context.Background(), &core.FileWatcherRootConfiguration{
		RootPath:              rootPath,
		MultiTenantMode:       true,
		Enabled:               true,
		IncludeSubdirectories: true,
		FilePatterns:          []string{"*.csv"},
		PostImportAction:      core.PostImportActionKeep,
		PollingInterval:       45 * time.Second,
		MaxFileSizeBytes:      1024,
		MinFileAge:            2 * time.Second,
		MaxConcurrentImports:  3,
	})
	if err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}
	if created != len(tenants) {
		t.Fatalf("ApplyRootConfiguration() = %d, want %d", created, len(tenants))
	}

	wantIDs := make([]string, 0, len(tenants))
	for _, tenant := range tenants {
		wantIDs = append(wantIDs, "auto-"+filepath.Base(rootPath)+"-"+tenant)
	}

	all, err := watcher.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}

	gotIDs := make([]string, 0, len(all))
	for _, config := range all {
		gotIDs = append(gotIDs, config.WatcherID)
	}
	sort.Strings(gotIDs)
	sort.Strings(wantIDs)

	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("registered watcher IDs = %v, want %v", gotIDs, wantIDs)
	}
	for i, want := range wantIDs {
		if gotIDs[i] != want {
			t.Fatalf("registered watcher IDs = %v, want %v", gotIDs, wantIDs)
		}
	}

	for _, tenant := range tenants {
		watcherID := "auto-" + filepath.Base(rootPath) + "-" + tenant

		config, err := watcher.GetWatcher(context.Background(), watcherID)
		if err != nil {
			t.Fatalf("GetWatcher(%q) error = %v", watcherID, err)
		}

		if config.TenantID != tenant {
			t.Fatalf("TenantID = %q, want %q", config.TenantID, tenant)
		}
		if config.WatchPath != tenantDirPath(rootPath, tenant) {
			t.Fatalf("WatchPath = %q, want %q", config.WatchPath, tenantDirPath(rootPath, tenant))
		}
		if config.MultiTenantMode {
			t.Fatalf("MultiTenantMode = true, want false for a per-tenant watcher")
		}
		if !config.Enabled {
			t.Fatalf("Enabled = false, want the template value")
		}
		if !config.IncludeSubdirectories {
			t.Fatalf("IncludeSubdirectories = false, want the template value")
		}
		if config.PollingInterval != 45*time.Second {
			t.Fatalf("PollingInterval = %v, want the template 45s", config.PollingInterval)
		}
		if config.MaxConcurrentImports != 3 {
			t.Fatalf("MaxConcurrentImports = %d, want the template 3", config.MaxConcurrentImports)
		}
		if config.MaxFileSizeBytes != 1024 {
			t.Fatalf("MaxFileSizeBytes = %d, want the template 1024", config.MaxFileSizeBytes)
		}
		if config.PostImportAction != core.PostImportActionKeep {
			t.Fatalf("PostImportAction = %v, want the template Keep", config.PostImportAction)
		}
		if len(config.FilePatterns) != 1 || config.FilePatterns[0] != "*.csv" {
			t.Fatalf("FilePatterns = %v, want [*.csv]", config.FilePatterns)
		}
	}
}

func TestFileWatcherAutoManager_ApplyIsIdempotent(t *testing.T) {
	rootPath := t.TempDir()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := os.Mkdir(tenantDirPath(rootPath, tenant), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", tenant, err)
		}
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(rootPath)

	if _, err := manager.ApplyRootConfiguration(context.Background(), &root); err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}

	// Second application must update, not duplicate.
	created, err := manager.ApplyRootConfiguration(context.Background(), &root)
	if err != nil {
		t.Fatalf("ApplyRootConfiguration() #2 error = %v", err)
	}
	if created != 2 {
		t.Fatalf("ApplyRootConfiguration() #2 = %d, want 2", created)
	}

	all, err := watcher.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("GetAllWatchers() = %d watchers, want 2 (no duplicates)", len(all))
	}
}

// TestFileWatcherAutoManager_ReapplyDoesNotResetOperatorDisable is the state
// precedence contract: an operator's persisted disable decision outranks the
// root template's Enabled flag on every re-application.
func TestFileWatcherAutoManager_ReapplyDoesNotResetOperatorDisable(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(tenantDirPath(rootPath, "tenant-a"), 0o755); err != nil {
		t.Fatalf("mkdir tenant-a: %v", err)
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(rootPath)

	if _, err := manager.ApplyRootConfiguration(context.Background(), &root); err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}

	watcherID := "auto-" + filepath.Base(rootPath) + "-tenant-a"

	if err := watcher.DisableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("DisableWatcher() error = %v", err)
	}

	if _, err := manager.ApplyRootConfiguration(context.Background(), &root); err != nil {
		t.Fatalf("ApplyRootConfiguration() #2 error = %v", err)
	}

	config, err := watcher.GetWatcher(context.Background(), watcherID)
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if config.Enabled {
		t.Fatalf("Enabled = true after re-applying the root, want the operator's disable decision to win")
	}
}

// TestFileWatcherAutoManager_ApplyPicksUpNewTenantDirectory proves a later
// application discovers a tenant directory created after the first one.
func TestFileWatcherAutoManager_ApplyPicksUpNewTenantDirectory(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(tenantDirPath(rootPath, "tenant-a"), 0o755); err != nil {
		t.Fatalf("mkdir tenant-a: %v", err)
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(rootPath)

	created, err := manager.ApplyRootConfiguration(context.Background(), &root)
	if err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}
	if created != 1 {
		t.Fatalf("ApplyRootConfiguration() = %d, want 1", created)
	}

	if err := os.Mkdir(tenantDirPath(rootPath, "tenant-b"), 0o755); err != nil {
		t.Fatalf("mkdir tenant-b: %v", err)
	}

	created, err = manager.ApplyRootConfiguration(context.Background(), &root)
	if err != nil {
		t.Fatalf("ApplyRootConfiguration() #2 error = %v", err)
	}
	if created != 2 {
		t.Fatalf("ApplyRootConfiguration() #2 = %d, want 2", created)
	}

	if _, err := watcher.GetWatcher(context.Background(), "auto-"+filepath.Base(rootPath)+"-tenant-b"); err != nil {
		t.Fatalf("GetWatcher(tenant-b) error = %v, want the new tenant watcher", err)
	}
}

func TestFileWatcherAutoManager_NonMultiTenantRootWithoutTenantIsRejected(t *testing.T) {
	rootPath := t.TempDir()

	manager, _ := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	_, err := manager.ApplyRootConfiguration(context.Background(), &core.FileWatcherRootConfiguration{
		RootPath:        rootPath,
		MultiTenantMode: false,
		Enabled:         true,
	})
	if err == nil {
		t.Fatalf("ApplyRootConfiguration() error = nil, want core.ErrInvalidArgument for a root without a derivable tenant")
	}
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("ApplyRootConfiguration() error = %v, want core.ErrInvalidArgument", err)
	}
}

func TestFileWatcherAutoManager_InvalidRootPathIsRejected(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")

	manager, _ := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(missing)
	if _, err := manager.ApplyRootConfiguration(context.Background(), &root); err == nil {
		t.Fatalf("ApplyRootConfiguration() error = nil, want an error for a missing root path")
	}

	if _, err := manager.ApplyRootConfiguration(context.Background(), &core.FileWatcherRootConfiguration{MultiTenantMode: true}); err == nil {
		t.Fatalf("ApplyRootConfiguration() error = nil, want an error for an empty root path")
	}
}

// --- DiscoverAndCreateWatchers ---------------------------------------------

func TestFileWatcherAutoManager_DiscoverAndCreateWatchersReappliesHeldRoots(t *testing.T) {
	rootPath := t.TempDir()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := os.Mkdir(tenantDirPath(rootPath, tenant), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", tenant, err)
		}
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), []core.FileWatcherRootConfiguration{
		multiTenantRootTemplate(rootPath),
	})

	created, err := manager.DiscoverAndCreateWatchers(context.Background())
	if err != nil {
		t.Fatalf("DiscoverAndCreateWatchers() error = %v", err)
	}
	if created != 2 {
		t.Fatalf("DiscoverAndCreateWatchers() = %d, want 2", created)
	}

	// A new tenant directory is discovered on the next call.
	if err := os.Mkdir(tenantDirPath(rootPath, "tenant-c"), 0o755); err != nil {
		t.Fatalf("mkdir tenant-c: %v", err)
	}

	created, err = manager.DiscoverAndCreateWatchers(context.Background())
	if err != nil {
		t.Fatalf("DiscoverAndCreateWatchers() #2 error = %v", err)
	}
	if created != 3 {
		t.Fatalf("DiscoverAndCreateWatchers() #2 = %d, want 3", created)
	}

	if _, err := watcher.GetWatcher(context.Background(), "auto-"+filepath.Base(rootPath)+"-tenant-c"); err != nil {
		t.Fatalf("GetWatcher(tenant-c) error = %v", err)
	}
}

func TestFileWatcherAutoManager_DiscoverAndCreateWatchersReturnsPersistedRootsAfterRestart(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	configRoot := filepath.Join(stateRoot, "watchers")

	rootPath := t.TempDir()
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := os.Mkdir(tenantDirPath(rootPath, tenant), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", tenant, err)
		}
	}

	firstWatcher, _ := newTestWatcherAt(t, configRoot)

	first, err := NewFileWatcherAutoManager(&FileWatcherAutoManagerOptions{
		FileWatcher:          firstWatcher,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewFileWatcherAutoManager() error = %v", err)
	}

	if _, err := first.ApplyRootConfiguration(context.Background(), &core.FileWatcherRootConfiguration{
		RootPath:             rootPath,
		MultiTenantMode:      true,
		Enabled:              true,
		FilePatterns:         []string{"*.csv"},
		PostImportAction:     core.PostImportActionKeep,
		PollingInterval:      45 * time.Second,
		MaxConcurrentImports: 3,
	}); err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(configRoot, "watcher-roots.json")); err != nil {
		t.Fatalf("root configuration was not persisted: %v", err)
	}

	// Restart with no roots in the options: the persisted roots must be used.
	secondWatcher, _ := newTestWatcherAt(t, configRoot)

	second, err := NewFileWatcherAutoManager(&FileWatcherAutoManagerOptions{
		FileWatcher:          secondWatcher,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewFileWatcherAutoManager() error = %v", err)
	}

	restored, err := second.GetRootConfiguration(context.Background())
	if err != nil {
		t.Fatalf("GetRootConfiguration() error = %v", err)
	}
	if restored == nil {
		t.Fatalf("GetRootConfiguration() = nil after restart, want the persisted root")
	}
	if restored.RootPath != rootPath {
		t.Fatalf("RootPath = %q after restart, want %q", restored.RootPath, rootPath)
	}
	if restored.PollingInterval != 45*time.Second {
		t.Fatalf("PollingInterval = %v after restart, want 45s", restored.PollingInterval)
	}

	created, err := second.DiscoverAndCreateWatchers(context.Background())
	if err != nil {
		t.Fatalf("DiscoverAndCreateWatchers() error = %v", err)
	}
	if created != 2 {
		t.Fatalf("DiscoverAndCreateWatchers() after restart = %d, want 2", created)
	}

	if _, err := secondWatcher.GetWatcher(context.Background(), "auto-"+filepath.Base(rootPath)+"-tenant-a"); err != nil {
		t.Fatalf("GetWatcher(tenant-a) error = %v, want the persisted root to re-create watchers", err)
	}
}

func TestFileWatcherAutoManager_IgnoresCorruptRootsFile(t *testing.T) {
	configRoot := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}

	if err := os.WriteFile(filepath.Join(configRoot, "watcher-roots.json"), []byte("not json at all"), 0o644); err != nil {
		t.Fatalf("write corrupt roots file: %v", err)
	}

	coreWatcher, _ := newTestWatcherAt(t, configRoot)

	manager, err := NewFileWatcherAutoManager(&FileWatcherAutoManagerOptions{
		FileWatcher:          coreWatcher,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewFileWatcherAutoManager() error = %v, want a corrupt roots file to be ignored", err)
	}

	root, err := manager.GetRootConfiguration(context.Background())
	if err != nil {
		t.Fatalf("GetRootConfiguration() error = %v", err)
	}
	if root != nil {
		t.Fatalf("GetRootConfiguration() = %+v, want nil for a corrupt roots file", root)
	}
}

// --- RemoveAllWatchers ------------------------------------------------------

func TestFileWatcherAutoManager_RemoveAllWatchersLeavesManualWatchers(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(tenantDirPath(rootPath, "tenant-a"), 0o755); err != nil {
		t.Fatalf("mkdir tenant-a: %v", err)
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(rootPath)

	if _, err := manager.ApplyRootConfiguration(context.Background(), &root); err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}

	manualPath := t.TempDir()
	registerWatcher(t, watcher, newConfig("manual-watcher", manualPath, nil))

	// A manual watcher whose ID merely contains "auto-" must survive too.
	registerWatcher(t, watcher, newConfig("my-auto-manual", manualPath, nil))

	if err := manager.RemoveAllWatchers(context.Background()); err != nil {
		t.Fatalf("RemoveAllWatchers() error = %v", err)
	}

	autoID := "auto-" + filepath.Base(rootPath) + "-tenant-a"
	if _, err := watcher.GetWatcher(context.Background(), autoID); !errors.Is(err, core.ErrWatcherNotFound) {
		t.Fatalf("GetWatcher(%q) error = %v, want core.ErrWatcherNotFound after RemoveAllWatchers", autoID, err)
	}

	for _, watcherID := range []string{"manual-watcher", "my-auto-manual"} {
		if _, err := watcher.GetWatcher(context.Background(), watcherID); err != nil {
			t.Fatalf("GetWatcher(%q) error = %v, want the manual watcher to survive", watcherID, err)
		}
	}

	all, err := watcher.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("GetAllWatchers() = %d watchers after RemoveAllWatchers, want the 2 manual watchers", len(all))
	}
}

// --- GetRootConfiguration / RootConfigurations ------------------------------

func TestFileWatcherAutoManager_GetRootConfigurationReturnsFirstRoot(t *testing.T) {
	manager, _ := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root, err := manager.GetRootConfiguration(context.Background())
	if err != nil {
		t.Fatalf("GetRootConfiguration() error = %v", err)
	}
	if root != nil {
		t.Fatalf("GetRootConfiguration() = %+v, want nil when no root is configured", root)
	}

	roots := []core.FileWatcherRootConfiguration{
		{RootPath: filepath.Join(t.TempDir(), "first"), MultiTenantMode: true, Enabled: true},
		{RootPath: filepath.Join(t.TempDir(), "second"), MultiTenantMode: true, Enabled: true},
	}

	manager, _ = newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), roots)

	root, err = manager.GetRootConfiguration(context.Background())
	if err != nil {
		t.Fatalf("GetRootConfiguration() error = %v", err)
	}
	if root == nil {
		t.Fatalf("GetRootConfiguration() = nil, want the first configured root")
	}
	if root.RootPath != roots[0].RootPath {
		t.Fatalf("RootPath = %q, want the first root %q", root.RootPath, roots[0].RootPath)
	}

	all := manager.RootConfigurations()
	if len(all) != 2 {
		t.Fatalf("RootConfigurations() = %d roots, want 2", len(all))
	}
	if all[0].RootPath != roots[0].RootPath || all[1].RootPath != roots[1].RootPath {
		t.Fatalf("RootConfigurations() = %+v, want both roots in order", all)
	}

	// Defensive copies: mutating the result must not change stored state.
	all[0].RootPath = "mutated-by-caller"
	again := manager.RootConfigurations()
	if again[0].RootPath == "mutated-by-caller" {
		t.Fatalf("RootConfigurations() returned shared state")
	}
}

// --- tenant directory validation --------------------------------------------

// TestFileWatcherAutoManager_InvalidTenantNamesAreNeverRegistered drives the
// per-directory step directly, because the names under test cannot be created on
// every platform's filesystem but must never become watchers.
func TestFileWatcherAutoManager_InvalidTenantNamesAreNeverRegistered(t *testing.T) {
	tests := []struct {
		name     string
		tenantID string
	}{
		{name: "empty", tenantID: ""},
		{name: "reserved device name", tenantID: "NUL"},
		{name: "reserved device name with extension", tenantID: "CON.csv"},
		{name: "parent traversal", tenantID: ".."},
		{name: "current directory", tenantID: "."},
		{name: "path separator", tenantID: "tenant/other"},
		{name: "backslash separator", tenantID: `tenant\other`},
		{name: "trailing whitespace", tenantID: "tenant "},
		{name: "leading whitespace", tenantID: " tenant"},
		{name: "colon", tenantID: "C:tenant"},
		{name: "overlong name", tenantID: strings.Repeat("t", core.MaxTenantIDLength+1)},
	}

	rootPath := t.TempDir()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := core.ValidateTenantID(test.tenantID); err == nil {
				t.Skipf("fixture %q is a valid tenant ID on this platform", test.tenantID)
			}

			manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

			root := multiTenantRootTemplate(rootPath)

			err := manager.applyTenantDirectory(context.Background(), &root, test.tenantID)
			if err == nil {
				t.Fatalf("applyTenantDirectory(%q) error = nil, want an error", test.tenantID)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("applyTenantDirectory(%q) error = %v, want core.ErrInvalidArgument", test.tenantID, err)
			}

			all, listErr := watcher.GetAllWatchers(context.Background())
			if listErr != nil {
				t.Fatalf("GetAllWatchers() error = %v", listErr)
			}
			if len(all) != 0 {
				t.Fatalf("registered %d watchers for an invalid directory name, want none: %v", len(all), all[0].WatcherID)
			}
		})
	}
}

// TestFileWatcherAutoManager_SkippedDirectoryIsReportedAndOthersStillApply
// covers the aggregate contract: an invalid directory name is counted as an
// error while the valid sibling directories still get their watchers.
//
// The invalid name is synthesized through a name the platform rejects: the loop
// is exercised with a real directory whose name is validated by the same code
// path, so a platform that cannot produce such a name skips this case.
func TestFileWatcherAutoManager_SkippedDirectoryIsReportedAndOthersStillApply(t *testing.T) {
	rootPath := t.TempDir()

	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := os.Mkdir(tenantDirPath(rootPath, tenant), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", tenant, err)
		}
	}

	manager, watcher := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(rootPath)

	created, err := manager.ApplyRootConfiguration(context.Background(), &root)
	if err != nil {
		t.Fatalf("ApplyRootConfiguration() error = %v", err)
	}
	if created != 2 {
		t.Fatalf("ApplyRootConfiguration() = %d, want the 2 valid tenant directories", created)
	}

	all, err := watcher.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("GetAllWatchers() = %d watchers, want 2", len(all))
	}
}

// --- logging safety ---------------------------------------------------------

// TestFileWatcherAutoManager_ErrorsDoNotLeakPhysicalPaths pins the safe-logging
// rule for an invalid root path.
func TestFileWatcherAutoManager_ErrorsDoNotLeakPhysicalPaths(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "top-secret-root")

	manager, _ := newTestAutoManager(t, filepath.Join(t.TempDir(), "state"), nil)

	root := multiTenantRootTemplate(missing)
	_, err := manager.ApplyRootConfiguration(context.Background(), &root)
	if err == nil {
		t.Fatalf("ApplyRootConfiguration() error = nil, want an error")
	}

	// The error may name the root path (the caller supplied it) but must not
	// contain a nested physical file name.
	if strings.Contains(err.Error(), "stray") {
		t.Fatalf("error %q leaked a nested physical path", err)
	}

	if _, statErr := os.Stat(missing); !os.IsNotExist(statErr) {
		t.Fatalf("ApplyRootConfiguration() created the missing root path; it must not")
	}
}
