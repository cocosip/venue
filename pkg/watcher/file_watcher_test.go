package watcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// --- test doubles -----------------------------------------------------------

type fakeTenantManager struct {
	mu      sync.Mutex
	tenants map[string]core.TenantContext

	// enumerateCalls counts GetAllTenants calls so the tenant-directory cache is
	// observable without touching production state.
	enumerateCalls int
}

func newFakeTenantManager() *fakeTenantManager {
	return &fakeTenantManager{tenants: make(map[string]core.TenantContext)}
}

func (f *fakeTenantManager) GetTenant(_ context.Context, tenantID string) (core.TenantContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if tenant, ok := f.tenants[tenantID]; ok {
		return tenant, nil
	}

	tenant := core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled, CreatedAt: time.Now()}
	f.tenants[tenantID] = tenant

	return tenant, nil
}

// TryGetTenant reports a tenant without materializing it, matching the
// contract's read-only probe semantics.
func (f *fakeTenantManager) TryGetTenant(_ context.Context, tenantID string) (core.TenantContext, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tenant, ok := f.tenants[tenantID]

	return tenant, ok, nil
}

func (f *fakeTenantManager) IsTenantEnabled(context.Context, string) (bool, error) { return true, nil }
func (f *fakeTenantManager) CreateTenant(context.Context, string) error            { return nil }
func (f *fakeTenantManager) EnableTenant(context.Context, string) error            { return nil }
func (f *fakeTenantManager) DisableTenant(context.Context, string) error           { return nil }

func (f *fakeTenantManager) GetAllTenants(context.Context) ([]core.TenantContext, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.enumerateCalls++

	out := make([]core.TenantContext, 0, len(f.tenants))
	for _, tenant := range f.tenants {
		out = append(out, tenant)
	}

	return out, nil
}

// enumerationCount returns how many times the tenant store was enumerated.
func (f *fakeTenantManager) enumerationCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.enumerateCalls
}

// addTenant materializes a tenant without going through GetTenant, so a test can
// simulate a tenant created in the store after a scan.
func (f *fakeTenantManager) addTenant(tenantID string) core.TenantContext {
	f.mu.Lock()
	defer f.mu.Unlock()

	tenant := core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled, CreatedAt: time.Now()}
	f.tenants[tenantID] = tenant

	return tenant
}

type writeCall struct {
	tenantID         string
	originalFileName string
}

type fakeStoragePool struct {
	mu       sync.Mutex
	writes   []writeCall
	contents []string
	onWrite  func(ctx context.Context, tenant core.TenantContext, name string) error
}

func (f *fakeStoragePool) WriteFile(ctx context.Context, tenant core.TenantContext, content io.Reader, originalFileName *string) (string, error) {
	return f.writeFile(ctx, tenant, content, originalFileName)
}

func (f *fakeStoragePool) WriteFileToDirectory(ctx context.Context, tenant core.TenantContext, content io.Reader, originalFileName *string, _ string) (string, error) {
	return f.writeFile(ctx, tenant, content, originalFileName)
}

func (f *fakeStoragePool) writeFile(ctx context.Context, tenant core.TenantContext, content io.Reader, originalFileName *string) (string, error) {
	data, err := io.ReadAll(content)
	if err != nil {
		return "", err
	}

	name := ""
	if originalFileName != nil {
		name = *originalFileName
	}

	if f.onWrite != nil {
		if err := f.onWrite(ctx, tenant, name); err != nil {
			return "", err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.writes = append(f.writes, writeCall{tenantID: tenant.ID, originalFileName: name})
	f.contents = append(f.contents, string(data))

	return fmt.Sprintf("filekey-%d", len(f.writes)), nil
}

func (f *fakeStoragePool) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.writes)
}

func (f *fakeStoragePool) ReadFile(context.Context, core.TenantContext, string) (io.ReadCloser, error) {
	return nil, core.ErrFileNotFound
}

func (f *fakeStoragePool) GetFileInfo(context.Context, core.TenantContext, string) (*core.FileInfo, error) {
	return nil, core.ErrFileNotFound
}

func (f *fakeStoragePool) GetFileLocation(context.Context, core.TenantContext, string) (*core.FileLocation, error) {
	return nil, core.ErrFileNotFound
}

func (f *fakeStoragePool) GetNextFileForProcessing(context.Context, core.TenantContext) (*core.FileLocation, error) {
	return nil, nil
}

func (f *fakeStoragePool) GetNextBatchForProcessing(context.Context, core.TenantContext, int) ([]*core.FileLocation, error) {
	return nil, nil
}

func (f *fakeStoragePool) MarkAsCompleted(context.Context, core.FileProcessingLease) error {
	return nil
}
func (f *fakeStoragePool) MarkAsFailed(context.Context, core.FileProcessingLease, string) error {
	return nil
}

func (f *fakeStoragePool) GetFileStatus(context.Context, core.TenantContext, string) (core.FileProcessingStatus, error) {
	return core.FileStatusPending, nil
}

func (f *fakeStoragePool) GetTotalCapacity(context.Context) (int64, error)  { return 0, nil }
func (f *fakeStoragePool) GetAvailableSpace(context.Context) (int64, error) { return 0, nil }

// --- helpers ----------------------------------------------------------------

func newTestWatcher(t *testing.T) (*fileWatcher, *fakeStoragePool) {
	t.Helper()

	return newTestWatcherAt(t, filepath.Join(t.TempDir(), "watchers"))
}

// newTestWatcherWithOptions builds a watcher whose construction options can be
// adjusted before NewFileWatcher runs (for example to inject a statistics
// recorder), with state isolated under a temporary configuration root.
func newTestWatcherWithOptions(t *testing.T, mutate func(*FileWatcherOptions)) (*fileWatcher, *fakeStoragePool) {
	t.Helper()

	return newTestWatcherAt(t, filepath.Join(t.TempDir(), "watchers"), mutate)
}

// newTestWatcherAt builds a watcher whose state lives under configRoot, so a
// test can restart against the same directory. Optional mutators adjust the
// construction options (for example a capturing logging runtime).
func newTestWatcherAt(t *testing.T, configRoot string, mutate ...func(*FileWatcherOptions)) (*fileWatcher, *fakeStoragePool) {
	t.Helper()

	pool := &fakeStoragePool{}

	opts := &FileWatcherOptions{
		TenantManager:        newFakeTenantManager(),
		StoragePool:          pool,
		ConfigurationRootDir: configRoot,
		Logging:              logging.Disabled(),
	}

	for _, apply := range mutate {
		apply(opts)
	}

	coreWatcher, err := NewFileWatcher(opts)
	if err != nil {
		t.Fatalf("NewFileWatcher() error = %v", err)
	}

	concrete, ok := coreWatcher.(*fileWatcher)
	if !ok {
		t.Fatalf("NewFileWatcher() returned %T, want *fileWatcher", coreWatcher)
	}

	t.Cleanup(func() {
		// Let any in-flight history write land before the temp dir is removed.
		concrete.awaitPendingHistoryFlush()

		if err := concrete.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return concrete, pool
}

// writeAgedFile writes content and backdates the modification time so the
// default MinFileAge gate does not skip it.
func writeAgedFile(t *testing.T, path string, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	old := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

func newConfig(watcherID string, watchPath string, mutate func(*core.FileWatcherConfiguration)) *core.FileWatcherConfiguration {
	config := &core.FileWatcherConfiguration{
		WatcherID:             watcherID,
		TenantID:              "tenant-a",
		WatchPath:             watchPath,
		IncludeSubdirectories: true,
		MinFileAge:            time.Millisecond,
		MaxConcurrentImports:  2,
		PostImportAction:      core.PostImportActionKeep,
		Enabled:               true,
	}

	if mutate != nil {
		mutate(config)
	}

	return config
}

func registerWatcher(t *testing.T, w *fileWatcher, config *core.FileWatcherConfiguration) {
	t.Helper()

	if err := w.RegisterWatcher(context.Background(), config); err != nil {
		t.Fatalf("RegisterWatcher() error = %v", err)
	}
}

func namesOf(files []string) []string {
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, filepath.Base(file))
	}

	return out
}

// --- defect 4: fingerprint de-duplication and history lifecycle -------------

func TestScanNow_ReimportsFileWhenFingerprintChanges(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "first")

	// Keep the source so the same revision is still on disk for the second scan.
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1", result.FilesImported)
	}

	// Same file, unchanged: must be skipped.
	result, err = w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesSkipped != 1 || result.FilesImported != 0 {
		t.Fatalf("second scan = {imported:%d skipped:%d}, want 0 imported / 1 skipped",
			result.FilesImported, result.FilesSkipped)
	}

	// Same path, rewritten with a different size and mtime: must be re-imported.
	writeAgedFile(t, target, strings.Repeat("second revision ", 64))

	result, err = w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported after rewrite = %d, want 1 (fingerprint changed)", result.FilesImported)
	}

	if got := pool.writeCount(); got != 2 {
		t.Fatalf("WriteFile calls = %d, want 2 (initial revision plus rewritten revision)", got)
	}
}

func TestScanNow_DeleteActionRemovesImportedRecord(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionDelete
	}))

	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "first")

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("source file still exists after Delete action: %v", err)
	}

	// A new file at the same path must be imported, not skipped forever.
	writeAgedFile(t, target, "second")

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1 after Delete action", result.FilesImported)
	}
}

func TestScanNow_MoveActionRemovesImportedRecord(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	moveTo := filepath.Join(t.TempDir(), "processed")
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionMove
		config.MoveToDirectory = moveTo
	}))

	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "first")

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(moveTo, "report.csv")); err != nil {
		t.Fatalf("moved file missing: %v", err)
	}

	// A watcher whose source refills must import the new file at the same path.
	writeAgedFile(t, target, "second")

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1 after Move action", result.FilesImported)
	}
}

func TestLoadImportedFilesHistory_PrunesStaleEntries(t *testing.T) {
	configRoot := filepath.Join(t.TempDir(), "watchers")
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	pool := &fakeStoragePool{}
	keepPath := filepath.Join(t.TempDir(), "keep.csv")
	if err := os.WriteFile(keepPath, []byte("data"), 0o644); err != nil {
		t.Fatalf("write keep file: %v", err)
	}
	stalePath := filepath.Join(filepath.Dir(keepPath), "gone.csv")

	history := fmt.Sprintf(`{%q: {"fingerprint":"11:22","time":1}, %q: {"fingerprint":"33:44","time":2}}`,
		keepPath, stalePath)
	if err := os.WriteFile(filepath.Join(configRoot, "imported-files.json"), []byte(history), 0o644); err != nil {
		t.Fatalf("write history: %v", err)
	}

	coreWatcher, err := NewFileWatcher(&FileWatcherOptions{
		TenantManager:        newFakeTenantManager(),
		StoragePool:          pool,
		ConfigurationRootDir: configRoot,
		Logging:              logging.Disabled(),
	})
	if err != nil {
		t.Fatalf("NewFileWatcher() error = %v", err)
	}

	w := coreWatcher.(*fileWatcher)
	if w.importedEntryCount() != 1 {
		t.Fatalf("imported entries = %d, want 1 after pruning the missing source file", w.importedEntryCount())
	}

	if _, ok := w.importedFiles.Load(stalePath); ok {
		t.Fatalf("stale entry %q was not pruned", stalePath)
	}
}

func TestSaveImportedFilesHistory_CapsHistorySize(t *testing.T) {
	w, _ := newTestWatcher(t)

	root := t.TempDir()
	for i := 0; i < maxImportedFilesHistory+25; i++ {
		path := filepath.Join(root, fmt.Sprintf("file-%05d.csv", i))
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}

		// Store without scheduling a background flush for every entry.
		w.storeImportedRecordSync(path, fmt.Sprintf("%d:%d", i, i), time.Unix(int64(i), 0))
	}

	if got := w.importedEntryCount(); got != maxImportedFilesHistory+25 {
		t.Fatalf("pre-save entries = %d, want %d", got, maxImportedFilesHistory+25)
	}

	if err := w.saveImportedFilesHistory(); err != nil {
		t.Fatalf("saveImportedFilesHistory() error = %v", err)
	}

	w.importedFiles = sync.Map{}
	w.importedCount = 0

	if err := w.loadImportedFilesHistory(); err != nil {
		t.Fatalf("loadImportedFilesHistory() error = %v", err)
	}

	if got := w.importedEntryCount(); got != maxImportedFilesHistory {
		t.Fatalf("persisted entries = %d, want %d", got, maxImportedFilesHistory)
	}

	// Oldest entries are dropped; the newest survive.
	if _, ok := w.importedFiles.Load(filepath.Join(root, "file-00000.csv")); ok {
		t.Fatalf("oldest entry survived the cap")
	}
	if _, ok := w.importedFiles.Load(filepath.Join(root, fmt.Sprintf("file-%05d.csv", maxImportedFilesHistory+24))); !ok {
		t.Fatalf("newest entry was dropped by the cap")
	}
}

// --- defect 5: failed post-import action is a failure -----------------------

func TestScanNow_FailedPostImportActionCountsAsFailure(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	// A destination that already exists as a file makes MkdirAll fail, so the
	// Move action fails deterministically.
	moveBlocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(moveBlocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionMove
		config.MoveToDirectory = moveBlocker
	}))

	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "payload")

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesFailed != 1 {
		t.Fatalf("FilesFailed = %d, want 1 when the post-import action fails", result.FilesFailed)
	}
	if result.FilesImported != 0 {
		t.Fatalf("FilesImported = %d, want 0 when the post-import action fails", result.FilesImported)
	}

	// The stuck source file must be retried on the next cycle.
	before := pool.writeCount()
	result, err = w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if pool.writeCount() <= before {
		t.Fatalf("source file was not retried after the post-import action failed")
	}
	if result.FilesFailed != 1 {
		t.Fatalf("FilesFailed on retry = %d, want 1", result.FilesFailed)
	}
}

// --- defect 6: Move must not overwrite an existing destination --------------

func TestMoveAction_AvoidsOverwritingExistingDestination(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	moveTo := filepath.Join(t.TempDir(), "processed")
	if err := os.MkdirAll(moveTo, 0o755); err != nil {
		t.Fatalf("mkdir move target: %v", err)
	}

	existing := filepath.Join(moveTo, "report.csv")
	if err := os.WriteFile(existing, []byte("original destination"), 0o644); err != nil {
		t.Fatalf("write existing destination: %v", err)
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionMove
		config.MoveToDirectory = moveTo
	}))

	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "new payload")

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	content, err := os.ReadFile(existing)
	if err != nil {
		t.Fatalf("read existing destination: %v", err)
	}
	if string(content) != "original destination" {
		t.Fatalf("existing destination was overwritten: %q", content)
	}

	suffixed := filepath.Join(moveTo, "report_1.csv")
	content, err = os.ReadFile(suffixed)
	if err != nil {
		t.Fatalf("expected suffixed destination: %v", err)
	}
	if string(content) != "new payload" {
		t.Fatalf("suffixed destination content = %q, want %q", content, "new payload")
	}
}

func TestMoveAction_UsesIncrementingCollisionSuffix(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	moveTo := filepath.Join(t.TempDir(), "processed")
	if err := os.MkdirAll(moveTo, 0o755); err != nil {
		t.Fatalf("mkdir move target: %v", err)
	}

	for _, name := range []string{"report.csv", "report_1.csv", "report_2.csv"} {
		if err := os.WriteFile(filepath.Join(moveTo, name), []byte("occupant"), 0o644); err != nil {
			t.Fatalf("write occupant %s: %v", name, err)
		}
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionMove
		config.MoveToDirectory = moveTo
	}))

	target := filepath.Join(watchPath, "report.csv")
	writeAgedFile(t, target, "payload")

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(moveTo, "report_3.csv")); err != nil {
		t.Fatalf("expected report_3.csv: %v", err)
	}
}

// --- defect 7: IncludeSubdirectories ---------------------------------------

func TestScanNow_IncludeSubdirectories(t *testing.T) {
	tests := []struct {
		name           string
		include        bool
		wantDiscovered int
		wantImported   int
	}{
		{name: "enabled walks nested directories", include: true, wantDiscovered: 2, wantImported: 2},
		{name: "disabled stays top-level", include: false, wantDiscovered: 1, wantImported: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, _ := newTestWatcher(t)

			watchPath := t.TempDir()
			writeAgedFile(t, filepath.Join(watchPath, "root.csv"), "root")
			writeAgedFile(t, filepath.Join(watchPath, "nested", "child.csv"), "child")

			registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
				config.IncludeSubdirectories = test.include
			}))

			result, err := w.ScanNow(context.Background(), "w1")
			if err != nil {
				t.Fatalf("ScanNow() error = %v", err)
			}

			if result.FilesDiscovered != test.wantDiscovered {
				t.Fatalf("FilesDiscovered = %d, want %d", result.FilesDiscovered, test.wantDiscovered)
			}
			if result.FilesImported != test.wantImported {
				t.Fatalf("FilesImported = %d, want %d", result.FilesImported, test.wantImported)
			}
		})
	}
}

func TestDiscoverFiles_IncludeSubdirectories(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "root.csv"), "root")
	writeAgedFile(t, filepath.Join(watchPath, "nested", "deep", "child.csv"), "child")

	recursive, err := w.discoverFiles(context.Background(), watchPath, &core.FileWatcherConfiguration{
		IncludeSubdirectories: true,
		MinFileAge:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("discoverFiles(recursive) error = %v", err)
	}
	if len(recursive) != 2 {
		t.Fatalf("recursive discovery = %v, want 2 files", namesOf(recursive))
	}

	topLevel, err := w.discoverFiles(context.Background(), watchPath, &core.FileWatcherConfiguration{
		IncludeSubdirectories: false,
		MinFileAge:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("discoverFiles(top-level) error = %v", err)
	}
	if len(topLevel) != 1 || filepath.Base(topLevel[0]) != "root.csv" {
		t.Fatalf("top-level discovery = %v, want [root.csv]", namesOf(topLevel))
	}
}

// --- defect 8: negative MaxConcurrentImports --------------------------------

func TestRegisterWatcher_RejectsNegativeMaxConcurrentImports(t *testing.T) {
	w, _ := newTestWatcher(t)

	config := newConfig("w1", t.TempDir(), func(config *core.FileWatcherConfiguration) {
		config.MaxConcurrentImports = -5
	})

	if err := w.RegisterWatcher(context.Background(), config); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("RegisterWatcher() error = %v, want ErrInvalidArgument", err)
	}
}

func TestScanWatcher_ClampsNonPositiveMaxConcurrentImports(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	writeAgedFile(t, filepath.Join(watchPath, "a.csv"), "a")
	writeAgedFile(t, filepath.Join(watchPath, "b.csv"), "b")

	// Reach the scan path with a non-positive limit, which the defensive clamp
	// inside the scan must absorb instead of panicking make(chan, n).
	entry, err := w.loadEntry("w1")
	if err != nil {
		t.Fatalf("loadEntry() error = %v", err)
	}
	entry.config.MaxConcurrentImports = -5

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 2 {
		t.Fatalf("FilesImported = %d, want 2", result.FilesImported)
	}
}

func TestRegisterWatcher_RejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.FileWatcherConfiguration)
	}{
		{
			name: "nil configuration",
		},
		{
			name: "empty watcher ID",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.WatcherID = ""
			},
		},
		{
			name: "empty watch path",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.WatchPath = ""
			},
		},
		{
			name: "missing tenant in single tenant mode",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.TenantID = ""
			},
		},
		{
			name: "invalid glob pattern",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.FilePatterns = []string{"[unclosed"}
			},
		},
		{
			name: "negative max concurrent imports",
			mutate: func(config *core.FileWatcherConfiguration) {
				config.MaxConcurrentImports = -1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, _ := newTestWatcher(t)

			if test.name == "nil configuration" {
				if err := w.RegisterWatcher(context.Background(), nil); !errors.Is(err, core.ErrInvalidArgument) {
					t.Fatalf("RegisterWatcher(nil) error = %v, want ErrInvalidArgument", err)
				}

				return
			}

			config := newConfig("w1", t.TempDir(), test.mutate)
			err := w.RegisterWatcher(context.Background(), config)
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("RegisterWatcher() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

// --- defect 11: watch directories are created at registration ---------------

func TestRegisterWatcher_CreatesMissingWatchPath(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := filepath.Join(t.TempDir(), "incoming", "nested")

	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	info, err := os.Stat(watchPath)
	if err != nil {
		t.Fatalf("watch path was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("watch path is not a directory")
	}
}

func TestRegisterWatcher_ReportsWatchPathCreationFailure(t *testing.T) {
	w, _ := newTestWatcher(t)

	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("file"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	config := newConfig("w1", filepath.Join(blocker, "child"), nil)

	err := w.RegisterWatcher(context.Background(), config)
	if err == nil {
		t.Fatalf("RegisterWatcher() error = nil, want a creation failure")
	}
}

// --- defect 12: pattern normalization and case-insensitive matching ---------

func TestRegisterWatcher_NormalizesPatterns(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"  ", "", "*.csv", "*.csv", " *.txt "}
	}))

	stored, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}

	want := []string{"*.csv", "*.txt"}
	if len(stored.FilePatterns) != len(want) {
		t.Fatalf("FilePatterns = %v, want %v", stored.FilePatterns, want)
	}
	for i, pattern := range want {
		if stored.FilePatterns[i] != pattern {
			t.Fatalf("FilePatterns[%d] = %q, want %q", i, stored.FilePatterns[i], pattern)
		}
	}

	// Blank-only patterns must fall back to "*" instead of disabling the watcher.
	w2, _ := newTestWatcher(t)
	watchPath2 := t.TempDir()
	registerWatcher(t, w2, newConfig("w1", watchPath2, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"", "   "}
	}))

	stored2, err := w2.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if len(stored2.FilePatterns) != 1 || stored2.FilePatterns[0] != "*" {
		t.Fatalf("FilePatterns = %v, want [*]", stored2.FilePatterns)
	}

	writeAgedFile(t, filepath.Join(watchPath2, "anything.dat"), "x")

	result, err := w2.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1 with the fallback pattern", result.FilesImported)
	}
}

func TestScanNow_PatternMatching(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "REPORT.CSV"), "upper")
	writeAgedFile(t, filepath.Join(watchPath, "notes.txt"), "text")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"*.csv"}
	}))

	files, err := w.discoverFiles(context.Background(), watchPath, &core.FileWatcherConfiguration{
		FilePatterns: []string{"*.csv"},
		MinFileAge:   time.Millisecond,
	})
	if err != nil {
		t.Fatalf("discoverFiles() error = %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("discoverFiles() = %v, want one match", namesOf(files))
	}
	if filepath.Base(files[0]) != "REPORT.CSV" {
		t.Fatalf("matched %q, want REPORT.CSV", filepath.Base(files[0]))
	}
}

// --- defect 13: concurrent scans must not double-import --------------------

func TestScanNow_ConcurrentScansImportOnce(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	const fileCount = 25
	for i := 0; i < fileCount; i++ {
		writeAgedFile(t, filepath.Join(watchPath, fmt.Sprintf("file-%02d.csv", i)), fmt.Sprintf("payload-%d", i))
	}

	// Widen the race window so a non-atomic check/record would double-import.
	pool.onWrite = func(context.Context, core.TenantContext, string) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
				t.Errorf("ScanNow() error = %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := pool.writeCount(); got != fileCount {
		pool.mu.Lock()
		names := make([]string, 0, len(pool.writes))
		for _, call := range pool.writes {
			names = append(names, call.originalFileName)
		}
		pool.mu.Unlock()

		t.Fatalf("WriteFile calls = %d, want %d (no duplicate imports): %v", got, fileCount, names)
	}
}

// --- defect 14: immutable snapshots, disabled scans, error cap -------------

func TestGetWatcher_ReturnsCopy(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	first, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	first.WatchPath = "mutated-by-caller"

	second, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if second.WatchPath != watchPath {
		t.Fatalf("WatchPath = %q, want %q (callers must not mutate stored configuration)", second.WatchPath, watchPath)
	}
}

func TestGetAllWatchers_ReturnsCopies(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	configs, err := w.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("GetAllWatchers() = %d entries, want 1", len(configs))
	}

	configs[0].WatchPath = "mutated-by-caller"

	second, err := w.GetAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if second[0].WatchPath != watchPath {
		t.Fatalf("WatchPath = %q, want %q", second[0].WatchPath, watchPath)
	}
}

func TestScanNow_DisabledWatcherReturnsError(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.Enabled = false
	}))

	if _, err := w.ScanNow(context.Background(), "w1"); err == nil {
		t.Fatalf("ScanNow() on a disabled watcher error = nil, want a clear error")
	}
}

func TestScanResult_CapsRecordedErrors(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	const fileCount = 150
	for i := 0; i < fileCount; i++ {
		writeAgedFile(t, filepath.Join(watchPath, fmt.Sprintf("file-%03d.csv", i)), "payload")
	}

	pool.onWrite = func(context.Context, core.TenantContext, string) error {
		return errors.New("simulated failure")
	}

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesFailed != fileCount {
		t.Fatalf("FilesFailed = %d, want %d", result.FilesFailed, fileCount)
	}
	if len(result.Errors) != maxRecordedErrorsPerScanResult {
		t.Fatalf("len(Errors) = %d, want %d", len(result.Errors), maxRecordedErrorsPerScanResult)
	}
}

func TestErrorMessagesDoNotContainPhysicalPaths(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))
	writeAgedFile(t, filepath.Join(watchPath, "secret-name.csv"), "payload")

	pool.onWrite = func(context.Context, core.TenantContext, string) error {
		return errors.New("simulated failure")
	}

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	for _, message := range result.Errors {
		if strings.Contains(message, watchPath) {
			t.Fatalf("error message leaked the physical path: %q", message)
		}
		if strings.Contains(message, "secret-name.csv") {
			t.Fatalf("error message leaked the original file name: %q", message)
		}
	}
}

func TestScanNow_DoesNotImportFilesYoungerThanMinFileAge(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(watchPath, "fresh.csv"), []byte("fresh"), 0o644); err != nil {
		t.Fatalf("write fresh file: %v", err)
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MinFileAge = time.Hour
	}))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesDiscovered != 0 || result.FilesImported != 0 {
		t.Fatalf("result = {discovered:%d imported:%d}, want none for a fresh file",
			result.FilesDiscovered, result.FilesImported)
	}
}

func TestScanNow_MaxFileSizeSkipsLargeFiles(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "big.csv"), strings.Repeat("x", 4096))
	writeAgedFile(t, filepath.Join(watchPath, "small.csv"), "small")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MaxFileSizeBytes = 128
	}))

	files, err := w.discoverFiles(context.Background(), watchPath, &core.FileWatcherConfiguration{
		MaxFileSizeBytes: 128,
		MinFileAge:       time.Millisecond,
	})
	if err != nil {
		t.Fatalf("discoverFiles() error = %v", err)
	}

	if len(files) != 1 || filepath.Base(files[0]) != "small.csv" {
		t.Fatalf("discoverFiles() = %v, want [small.csv]", namesOf(files))
	}
}

func TestScanNow_MultiTenantModeRoutesFilesToTenantDirectories(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "tenant-a", "a.csv"), "a")
	writeAgedFile(t, filepath.Join(watchPath, "tenant-b", "b.csv"), "b")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MultiTenantMode = true
		config.TenantID = ""
	}))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesImported != 2 {
		t.Fatalf("FilesImported = %d, want 2 (%v)", result.FilesImported, result.Errors)
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	seen := make(map[string]bool)
	for _, call := range pool.writes {
		seen[call.tenantID] = true
	}
	if !seen["tenant-a"] || !seen["tenant-b"] {
		t.Fatalf("writes routed to %v, want tenant-a and tenant-b", seen)
	}
}
