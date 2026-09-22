package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// --- UpdateWatcher ----------------------------------------------------------

// TestUpdateWatcher_ReplacesConfigurationInPlace covers the core contract: the
// watcher ID is preserved, the new settings are visible immediately, and the
// watch path is created when it is missing.
func TestUpdateWatcher_ReplacesConfigurationInPlace(t *testing.T) {
	w, _ := newTestWatcher(t)

	oldPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", oldPath, nil))

	newPath := filepath.Join(t.TempDir(), "created-on-update")

	err := w.UpdateWatcher(context.Background(), newConfig("w1", newPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"*.pdf"}
		config.PollingInterval = 45 * time.Second
		config.MaxConcurrentImports = 7
	}))
	if err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	updated, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}

	if updated.WatcherID != "w1" {
		t.Fatalf("WatcherID = %q, want w1", updated.WatcherID)
	}
	if updated.WatchPath != newPath {
		t.Fatalf("WatchPath = %q, want %q", updated.WatchPath, newPath)
	}
	if updated.PollingInterval != 45*time.Second {
		t.Fatalf("PollingInterval = %v, want 45s", updated.PollingInterval)
	}
	if updated.MaxConcurrentImports != 7 {
		t.Fatalf("MaxConcurrentImports = %d, want 7", updated.MaxConcurrentImports)
	}
	if len(updated.FilePatterns) != 1 || updated.FilePatterns[0] != "*.pdf" {
		t.Fatalf("FilePatterns = %v, want [*.pdf]", updated.FilePatterns)
	}

	info, err := os.Stat(newPath)
	if err != nil {
		t.Fatalf("updated watch path was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("updated watch path is not a directory")
	}

	// The update must replace, not duplicate: one watcher with this ID.
	all, err := w.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("GetAllWatchers() returned %d watchers, want 1", len(all))
	}
}

func TestUpdateWatcher_UnknownIDReturnsErrWatcherNotFound(t *testing.T) {
	w, _ := newTestWatcher(t)

	err := w.UpdateWatcher(context.Background(), newConfig("missing", t.TempDir(), nil))
	if err == nil {
		t.Fatalf("UpdateWatcher() error = nil, want an error for an unknown watcher ID")
	}
	if !errors.Is(err, core.ErrWatcherNotFound) {
		t.Fatalf("UpdateWatcher() error = %v, want core.ErrWatcherNotFound", err)
	}
	if _, getErr := w.GetWatcher(context.Background(), "missing"); getErr == nil {
		t.Fatalf("GetWatcher() error = nil, want the watcher to stay unregistered")
	}
}

// TestUpdateWatcher_AppliesSameValidationAsRegister pins the validation matrix
// to RegisterWatcher's rules so the two entry points cannot drift.
func TestUpdateWatcher_AppliesSameValidationAsRegister(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.FileWatcherConfiguration)
	}{
		{
			name: "empty watch path",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.WatchPath = ""
			},
		},
		{
			name: "tenant required in single-tenant mode",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.TenantID = ""
			},
		},
		{
			name: "negative max concurrent imports",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.MaxConcurrentImports = -1
			},
		},
		{
			name: "invalid glob pattern",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.FilePatterns = []string{"["}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, _ := newTestWatcher(t)

			watchPath := t.TempDir()
			registerWatcher(t, w, newConfig("w1", watchPath, nil))

			replacement := newConfig("w1", watchPath, test.mutate)

			err := w.UpdateWatcher(context.Background(), replacement)
			if err == nil {
				t.Fatalf("UpdateWatcher() error = nil, want core.ErrInvalidArgument")
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("UpdateWatcher() error = %v, want core.ErrInvalidArgument", err)
			}

			// A rejected update must leave the previous configuration untouched.
			current, getErr := w.GetWatcher(context.Background(), "w1")
			if getErr != nil {
				t.Fatalf("GetWatcher() error = %v", getErr)
			}
			if current.WatchPath != watchPath {
				t.Fatalf("WatchPath = %q after a rejected update, want %q", current.WatchPath, watchPath)
			}
		})
	}
}

func TestUpdateWatcher_NormalizesPatterns(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	err := w.UpdateWatcher(context.Background(), newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"  *.txt  ", "*.txt", "", "*.pdf"}
	}))
	if err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	updated, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}

	want := []string{"*.txt", "*.pdf"}
	if len(updated.FilePatterns) != len(want) {
		t.Fatalf("FilePatterns = %v, want %v", updated.FilePatterns, want)
	}
	for i, pattern := range want {
		if updated.FilePatterns[i] != pattern {
			t.Fatalf("FilePatterns = %v, want %v", updated.FilePatterns, want)
		}
	}
}

// TestUpdateWatcher_KeepsOperatorDisableDecision proves a config update never
// resurrects a watcher an operator disabled, and that the decision survives a
// restart because it was persisted at disable time.
func TestUpdateWatcher_KeepsOperatorDisableDecision(t *testing.T) {
	configRoot := filepath.Join(t.TempDir(), "watchers")
	w, _ := newTestWatcherAt(t, configRoot)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	if err := w.DisableWatcher(context.Background(), "w1"); err != nil {
		t.Fatalf("DisableWatcher() error = %v", err)
	}

	err := w.UpdateWatcher(context.Background(), newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.Enabled = true
		config.PollingInterval = 20 * time.Second
	}))
	if err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	updated, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if updated.Enabled {
		t.Fatalf("Enabled = true after UpdateWatcher, want the persisted disable decision to win")
	}
	if updated.PollingInterval != 20*time.Second {
		t.Fatalf("PollingInterval = %v, want the new schedule to be applied", updated.PollingInterval)
	}

	// Restart against the same state directory: the decision must still win.
	restarted, _ := newTestWatcherAt(t, configRoot)

	if err := restarted.RegisterWatcher(context.Background(), newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.Enabled = true
	})); err != nil {
		t.Fatalf("RegisterWatcher() error = %v", err)
	}

	afterRestart, err := restarted.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if afterRestart.Enabled {
		t.Fatalf("Enabled = true after restart, want the persisted disable decision")
	}
}

// TestUpdateWatcher_PreservesImportHistory proves the de-duplication history
// outlives a configuration update: a file imported by the previous
// configuration is still not re-imported afterwards.
func TestUpdateWatcher_PreservesImportHistory(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if got := pool.writeCount(); got != 1 {
		t.Fatalf("WriteFile calls = %d, want 1 before the update", got)
	}

	// A new schedule only: the imported revision must still be remembered.
	err := w.UpdateWatcher(context.Background(), newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PollingInterval = 15 * time.Second
	}))
	if err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if got := pool.writeCount(); got != 1 {
		t.Fatalf("WriteFile calls = %d after UpdateWatcher, want the import history to survive", got)
	}
}

// TestUpdateWatcher_ReturnsDefensiveCopies makes sure the replacement is
// snapshotted: mutating the caller's value afterwards must not be visible.
func TestUpdateWatcher_ReturnsDefensiveCopies(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	replacement := newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"*.txt"}
	})

	if err := w.UpdateWatcher(context.Background(), replacement); err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	replacement.FilePatterns[0] = "*.mutated"
	replacement.WatchPath = "mutated-by-caller"

	updated, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if updated.FilePatterns[0] != "*.txt" {
		t.Fatalf("FilePatterns = %v after caller mutation, want [*.txt]", updated.FilePatterns)
	}
	if updated.WatchPath != watchPath {
		t.Fatalf("WatchPath = %q after caller mutation, want %q", updated.WatchPath, watchPath)
	}
}

// --- GetWatchersForTenant ---------------------------------------------------

func TestGetWatchersForTenant_ReturnsOnlySingleTenantWatchersOfThatTenant(t *testing.T) {
	tests := []struct {
		name    string
		configs []*core.FileWatcherConfiguration
		tenant  string
		want    []string
	}{
		{
			name: "filters by tenant and excludes multi-tenant",
			configs: []*core.FileWatcherConfiguration{
				{WatcherID: "a", TenantID: "tenant-a"},
				{WatcherID: "b", TenantID: "tenant-b"},
				{WatcherID: "c", TenantID: "tenant-a"},
				{WatcherID: "d", TenantID: "", MultiTenantMode: true},
				{WatcherID: "e", TenantID: "tenant-a", MultiTenantMode: true},
			},
			tenant: "tenant-a",
			want:   []string{"a", "c"},
		},
		{
			name: "unknown tenant yields nothing",
			configs: []*core.FileWatcherConfiguration{
				{WatcherID: "a", TenantID: "tenant-a"},
			},
			tenant: "tenant-z",
			want:   []string{},
		},
		{
			name: "multi-tenant watcher with a tenant ID is not attributed",
			configs: []*core.FileWatcherConfiguration{
				{WatcherID: "e", TenantID: "tenant-a", MultiTenantMode: true},
			},
			tenant: "tenant-a",
			want:   []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, _ := newTestWatcher(t)

			for _, config := range test.configs {
				config.WatchPath = filepath.Join(t.TempDir(), config.WatcherID)
				config.MinFileAge = time.Millisecond
				config.MaxConcurrentImports = 1

				registerWatcher(t, w, config)
			}

			matched, err := w.GetWatchersForTenant(context.Background(), test.tenant)
			if err != nil {
				t.Fatalf("GetWatchersForTenant() error = %v", err)
			}

			if len(matched) != len(test.want) {
				t.Fatalf("GetWatchersForTenant(%q) = %d watchers, want %d", test.tenant, len(matched), len(test.want))
			}

			for i, watcherID := range test.want {
				if matched[i].WatcherID != watcherID {
					t.Fatalf("GetWatchersForTenant(%q)[%d] = %q, want %q", test.tenant, i, matched[i].WatcherID, watcherID)
				}
			}
		})
	}
}

func TestGetWatchersForTenant_EmptyTenantIDReturnsErrInvalidArgument(t *testing.T) {
	w, _ := newTestWatcher(t)

	_, err := w.GetWatchersForTenant(context.Background(), "")
	if err == nil {
		t.Fatalf("GetWatchersForTenant(\"\") error = nil, want core.ErrInvalidArgument")
	}
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("GetWatchersForTenant(\"\") error = %v, want core.ErrInvalidArgument", err)
	}
}

func TestGetWatchersForTenant_ReturnsCopies(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	first, err := w.GetWatchersForTenant(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("GetWatchersForTenant() error = %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("GetWatchersForTenant() = %d watchers, want 1", len(first))
	}

	first[0].WatchPath = "mutated-by-caller"

	second, err := w.GetWatchersForTenant(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("GetWatchersForTenant() error = %v", err)
	}
	if second[0].WatchPath != watchPath {
		t.Fatalf("WatchPath = %q after caller mutation, want %q", second[0].WatchPath, watchPath)
	}
}
