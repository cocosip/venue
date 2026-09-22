package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// --- helpers ----------------------------------------------------------------

// flushSpy replaces the debounced flush scheduler so a test decides exactly when
// a scheduled write runs instead of waiting for a wall-clock window.
type flushSpy struct {
	mu     sync.Mutex
	delays []time.Duration
	fns    []func()
}

func (s *flushSpy) schedule(delay time.Duration, fn func()) *time.Timer {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.delays = append(s.delays, delay)
	s.fns = append(s.fns, fn)

	// Return a real, long-lived timer so the watcher observes "one write is
	// scheduled" exactly as it would with time.AfterFunc; the test decides when
	// the captured callback runs.
	return time.NewTimer(time.Hour)
}

func (s *flushSpy) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.fns)
}

func (s *flushSpy) delayAt(t *testing.T, index int) time.Duration {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if index >= len(s.delays) {
		t.Fatalf("scheduled history writes = %d, want at least %d", len(s.delays), index+1)
	}

	return s.delays[index]
}

func (s *flushSpy) run(t *testing.T, index int) {
	t.Helper()

	s.mu.Lock()

	if index >= len(s.fns) {
		s.mu.Unlock()

		t.Fatalf("scheduled history writes = %d, want at least %d", len(s.fns), index+1)
	}

	fn := s.fns[index]
	s.mu.Unlock()

	fn()
}

// readPersistedHistory decodes the persisted import history.
func readPersistedHistory(t *testing.T, configRoot string) map[string]importedFileRecord {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(configRoot, importedFilesHistoryFileName))
	if err != nil {
		t.Fatalf("read imported files history: %v", err)
	}

	var history map[string]importedFileRecord
	if err := json.Unmarshal(data, &history); err != nil {
		t.Fatalf("parse imported files history: %v", err)
	}

	return history
}

// missingPath returns a source path that does not exist, so its history record
// counts as stale.
func missingPath(t *testing.T, name string) string {
	t.Helper()

	return filepath.Join(t.TempDir(), name)
}

func scanWatcherNow(t *testing.T, w *fileWatcher, watcherID string) *core.FileWatcherScanResult {
	t.Helper()

	result, err := w.ScanNow(context.Background(), watcherID)
	if err != nil {
		t.Fatalf("ScanNow(%s) error = %v", watcherID, err)
	}

	return result
}

// --- import-history prune throttle -----------------------------------------

func TestScanNow_PruneThrottleRunsOncePerInterval(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesPruneThrottle = true
		config.ImportedFilesPruneInterval = time.Minute
	}))

	// The first scan has no earlier prune to throttle against, so it prunes.
	firstStale := missingPath(t, "gone-1.csv")
	w.storeImportedRecordSync(firstStale, "1:2", current)

	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(firstStale); ok {
		t.Fatalf("stale entry survived the first prune")
	}

	secondStale := missingPath(t, "gone-2.csv")
	w.storeImportedRecordSync(secondStale, "3:4", current)

	// Inside the interval the prune is throttled.
	current = current.Add(30 * time.Second)
	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(secondStale); !ok {
		t.Fatalf("prune ran inside the throttle interval")
	}

	// The throttle must never skip a prune forever: the first scan after the
	// interval elapsed prunes again.
	current = current.Add(31 * time.Second)
	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(secondStale); ok {
		t.Fatalf("prune did not run after the throttle interval elapsed")
	}
}

// TestScanNow_PruneThrottleUsesDefaultInterval pins the documented five-minute
// default for an unset interval.
func TestScanNow_PruneThrottleUsesDefaultInterval(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesPruneThrottle = true
		config.ImportedFilesPruneInterval = 0
	}))

	scanWatcherNow(t, w, "w1")

	stale := missingPath(t, "gone.csv")
	w.storeImportedRecordSync(stale, "1:2", current)

	current = current.Add(4 * time.Minute)
	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(stale); !ok {
		t.Fatalf("prune ran before the default 5m interval elapsed")
	}

	current = current.Add(time.Minute)
	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(stale); ok {
		t.Fatalf("prune did not run after the default 5m interval elapsed")
	}
}

func TestScanNow_PruneThrottleDisabledPrunesEveryScan(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		// Throttle off: pruning keeps the un-throttled baseline of running on
		// every scan.
		config.EnableImportedFilesPruneThrottle = false
		config.ImportedFilesPruneInterval = time.Hour
	}))

	scanWatcherNow(t, w, "w1")

	stale := missingPath(t, "gone.csv")
	w.storeImportedRecordSync(stale, "1:2", current)

	current = current.Add(time.Second)
	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(stale); ok {
		t.Fatalf("prune did not run on the next scan with the throttle disabled")
	}
}

func TestPruneStaleImportedRecords_KeepsLiveRecords(t *testing.T) {
	w, _ := newTestWatcher(t)

	live := filepath.Join(t.TempDir(), "alive.csv")
	writeYoungFile(t, live, "payload")

	stale := missingPath(t, "gone.csv")

	w.storeImportedRecordSync(live, "1:2", time.Now())
	w.storeImportedRecordSync(stale, "3:4", time.Now())

	pruned := w.pruneStaleImportedRecords(context.Background(), &core.FileWatcherConfiguration{})
	if pruned != 1 {
		t.Fatalf("pruned = %d, want 1", pruned)
	}
	if _, ok := w.importedFiles.Load(live); !ok {
		t.Fatalf("live entry was pruned")
	}
	if _, ok := w.importedFiles.Load(stale); ok {
		t.Fatalf("stale entry survived the prune")
	}
	if got := w.importedEntryCount(); got != 1 {
		t.Fatalf("importedEntryCount() = %d, want 1 after the prune", got)
	}
}

// TestPruneStaleImportedRecords_KeepsInFlightClaims proves a prune can never
// evict an import that is still in flight, which would let a concurrent scan
// import the same revision twice.
func TestPruneStaleImportedRecords_KeepsInFlightClaims(t *testing.T) {
	w, _ := newTestWatcher(t)

	claimed := missingPath(t, "claimed.csv")

	if !w.reserveImportSlot(claimed, "token-1", "1:2") {
		t.Fatalf("reserveImportSlot() = false, want the claim")
	}

	if pruned := w.pruneStaleImportedRecords(context.Background(), &core.FileWatcherConfiguration{}); pruned != 0 {
		t.Fatalf("pruned = %d, want 0 while a claim is in flight", pruned)
	}

	value, ok := w.importedFiles.Load(claimed)
	if !ok {
		t.Fatalf("in-flight claim was evicted by the prune")
	}
	if record, ok := value.(importedFileRecord); !ok || record.InFlightToken != "token-1" {
		t.Fatalf("in-flight claim = %+v, want the token to survive", value)
	}
}

// --- import-history flush debounce -----------------------------------------

func TestScanNow_DebounceCoalescesHistoryWrites(t *testing.T) {
	w, _ := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	watchPath := t.TempDir()
	for i := 0; i < 3; i++ {
		writeAgedFile(t, filepath.Join(watchPath, fmt.Sprintf("file-%d.csv", i)), "payload")
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesHistoryFlushDebounce = true
		// A generous window keeps the spacing assertion below independent of how
		// long the scan itself takes.
		config.ImportedFilesHistoryFlushInterval = 30 * time.Second
	}))

	result := scanWatcherNow(t, w, "w1")
	if result.FilesImported != 3 {
		t.Fatalf("FilesImported = %d, want 3", result.FilesImported)
	}

	if got := spy.count(); got != 1 {
		t.Fatalf("scheduled history writes = %d, want 1 for a burst of imports", got)
	}
	if got := spy.delayAt(t, 0); got != 0 {
		t.Fatalf("first delay = %v, want the first pending change written immediately", got)
	}

	spy.run(t, 0)
	w.awaitPendingHistoryFlush()

	if history := readPersistedHistory(t, w.configRoot); len(history) != 3 {
		t.Fatalf("persisted history entries = %d, want 3 from the single coalesced write", len(history))
	}

	// A later change is spaced a full interval after the write that just ran.
	writeAgedFile(t, filepath.Join(watchPath, "late.csv"), "late")
	scanWatcherNow(t, w, "w1")

	if got := spy.count(); got != 2 {
		t.Fatalf("scheduled history writes = %d, want 2", got)
	}

	delay := spy.delayAt(t, 1)
	if delay <= 0 || delay > 30*time.Second {
		t.Fatalf("second delay = %v, want it inside (0, 30s] (at most one write per interval)", delay)
	}
}

// TestPersistHistory_DebounceUsesDefaultInterval pins the documented two-second
// default for an unset interval with an injected clock.
func TestPersistHistory_DebounceUsesDefaultInterval(t *testing.T) {
	w, _ := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	config := &core.FileWatcherConfiguration{
		EnableImportedFilesHistoryFlushDebounce: true,
		ImportedFilesHistoryFlushInterval:       0,
	}

	w.persistHistory(config)

	if got := spy.delayAt(t, 0); got != 0 {
		t.Fatalf("first delay = %v, want 0 before any write happened", got)
	}

	spy.run(t, 0)
	w.awaitPendingHistoryFlush()

	w.persistHistory(config)

	if got := spy.delayAt(t, 1); got != 2*time.Second {
		t.Fatalf("second delay = %v, want the default 2s interval", got)
	}
}

// TestPersistHistory_WithoutDebounceWritesImmediately proves the switch keeps
// today's write-per-change behaviour when it is off.
func TestPersistHistory_WithoutDebounceWritesImmediately(t *testing.T) {
	w, _ := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	path := filepath.Join(t.TempDir(), "report.csv")
	writeYoungFile(t, path, "payload")

	w.storeImportedRecordSync(path, "1:2", time.Now())
	w.persistHistory(&core.FileWatcherConfiguration{EnableImportedFilesHistoryFlushDebounce: false})
	w.awaitPendingHistoryFlush()

	if got := spy.count(); got != 0 {
		t.Fatalf("scheduled debounced writes = %d, want 0 with the debounce disabled", got)
	}
	if history := readPersistedHistory(t, w.configRoot); len(history) != 1 {
		t.Fatalf("persisted history entries = %d, want 1 written immediately", len(history))
	}
}

func TestClose_FlushesPendingDebouncedHistory(t *testing.T) {
	w, _ := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesHistoryFlushDebounce = true
		config.ImportedFilesHistoryFlushInterval = time.Hour
	}))

	result := scanWatcherNow(t, w, "w1")
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1", result.FilesImported)
	}
	if got := spy.count(); got != 1 {
		t.Fatalf("scheduled history writes = %d, want 1", got)
	}

	// The write is still pending: nothing may be on disk yet.
	historyPath := filepath.Join(w.configRoot, importedFilesHistoryFileName)
	if _, err := os.Stat(historyPath); !os.IsNotExist(err) {
		t.Fatalf("history was written before the debounce window elapsed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if history := readPersistedHistory(t, w.configRoot); len(history) != 1 {
		t.Fatalf("persisted history entries after Close = %d, want the pending change flushed", len(history))
	}
}

// TestScanNow_DebounceKeepsInFlightDeduplication proves a deferred history write
// never widens the de-duplication window: the same path is still imported once.
// It is the debounce counterpart of the parallel-scan regression test and is
// intended to run under -race.
func TestScanNow_DebounceKeepsInFlightDeduplication(t *testing.T) {
	w, pool := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesHistoryFlushDebounce = true
		config.ImportedFilesHistoryFlushInterval = time.Hour
	}))

	// Widen the race window so a non-atomic reservation would double-import.
	pool.onWrite = func(context.Context, core.TenantContext, string) error {
		time.Sleep(time.Millisecond)

		return nil
	}

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
				t.Errorf("ScanNow() error = %v", err)
			}
		}()
	}

	wg.Wait()

	if got := pool.writeCount(); got != 1 {
		t.Fatalf("WriteFile calls = %d, want 1 (a deferred history write must not allow a duplicate import)", got)
	}
	if got := spy.count(); got != 1 {
		t.Fatalf("scheduled history writes = %d, want 1", got)
	}
}
