package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// Regression tests for the file watcher invariants that are easy to break
// silently: history capping, error redaction, per-watcher bookkeeping lifetimes,
// and the cancellation contracts of the background loops. Each test pins one
// documented invariant and states the failure it prevents.

// hasEvent reports whether any captured record carries the given event name.
func hasEvent(records []capturedLog, event string) bool {
	for _, record := range records {
		if record.event == event {
			return true
		}
	}

	return false
}

// eventNames returns the captured event names in emission order.
func eventNames(records []capturedLog) []string {
	names := make([]string, 0, len(records))
	for _, record := range records {
		names = append(names, record.event)
	}

	return names
}

// The pending-action history store honours the history cap

func TestStoreImportedStateSyncAppliesHistoryCap(t *testing.T) {
	w, _ := newTestWatcher(t)

	for i := 0; i < maxImportedFilesHistory; i++ {
		w.importedFiles.Store(fmt.Sprintf("source-%05d.csv", i), importedFileRecord{
			Fingerprint:    "fp",
			ImportedAtUnix: int64(i),
		})
		w.importedCount++
	}

	const pendingPath = "pending-action.csv"

	record := importedFileRecord{
		Fingerprint:             "fp",
		ImportedAtUnix:          int64(maxImportedFilesHistory) + 1,
		PendingPostImportAction: true,
		WatcherID:               "w1",
		TenantID:                "tenant-a",
		PostImportAction:        core.PostImportActionDelete,
	}
	config := &core.FileWatcherConfiguration{
		WatcherID:                               "w1",
		EnableImportedFilesHistoryFlushDebounce: true,
		ImportedFilesHistoryFlushInterval:       time.Hour,
	}

	if err := w.storeImportedStateSync(pendingPath, record, config); err != nil {
		t.Fatalf("storeImportedStateSync() error = %v", err)
	}

	if got := w.importedEntryCount(); got > maxImportedFilesHistory {
		t.Fatalf("history entries after storeImportedStateSync = %d, want at most the cap %d: the pending-action path must apply the same cap as storeImportedRecord", got, maxImportedFilesHistory)
	}
	if _, ok := w.importedFiles.Load(pendingPath); !ok {
		t.Fatalf("the just-stored pending-action record was evicted")
	}
	if _, ok := w.importedFiles.Load("source-00000.csv"); ok {
		t.Fatalf("the oldest record survived the cap applied by storeImportedStateSync")
	}
}

// ScanAllWatchers must not leak physical paths

func TestScanAllWatchersRedactsScanError(t *testing.T) {
	handler := &recordingHandler{}

	logRuntime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	w, _ := newTestWatcherWithOptions(t, func(opts *FileWatcherOptions) {
		opts.Logging = logRuntime
	})

	watchPath := filepath.Join(t.TempDir(), "gone-dir")
	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	// Registration creates the watch directory; removing it makes the scan fail
	// with a *fs.PathError that carries the physical path.
	if err := os.RemoveAll(watchPath); err != nil {
		t.Fatalf("remove watch path: %v", err)
	}

	results, err := w.ScanAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("ScanAllWatchers() error = %v", err)
	}

	result := results["w1"]
	if result == nil || len(result.Errors) == 0 {
		t.Fatalf("ScanAllWatchers() = %#v, want one reported scan error", result)
	}
	if !strings.Contains(result.Errors[0], "w1") {
		t.Fatalf("scan error %q does not name the watcher", result.Errors[0])
	}
	for _, message := range result.Errors {
		if strings.Contains(message, watchPath) {
			t.Fatalf("scan error %q leaked the physical watch path", message)
		}
	}
	if !hasEvent(handler.snapshot(), "watcher_scan_failed") {
		t.Fatalf("scan failure events = %v, want the same watcher_scan_failed name the background service uses", eventNames(handler.snapshot()))
	}
}

// The prune throttle is per watcher

func TestPruneThrottleIsKeyedPerWatcher(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	firstPath := t.TempDir()
	secondPath := t.TempDir()

	throttled := func(config *core.FileWatcherConfiguration) {
		config.EnableImportedFilesPruneThrottle = true
		config.ImportedFilesPruneInterval = time.Minute
	}
	registerWatcher(t, w, newConfig("w1", firstPath, throttled))
	registerWatcher(t, w, newConfig("w2", secondPath, throttled))

	seed := func(path, watcherID string) {
		w.storeImportedRecordSync(path, "fp", current)

		value, ok := w.importedFiles.Load(path)
		if !ok {
			t.Fatalf("seeded record for %s is missing", path)
		}

		record := value.(importedFileRecord)
		record.WatcherID = watcherID
		w.importedFiles.Store(path, record)
	}

	firstStale := filepath.Join(firstPath, "gone.csv")
	secondStale := filepath.Join(secondPath, "gone.csv")
	seed(firstStale, "w1")
	seed(secondStale, "w2")

	scanWatcherNow(t, w, "w1")

	if _, ok := w.importedFiles.Load(firstStale); ok {
		t.Fatalf("the first watcher's stale record survived its own prune")
	}
	if _, ok := w.importedFiles.Load(secondStale); !ok {
		t.Fatalf("another watcher's record was pruned by w1")
	}

	// The second watcher has its own throttle window: w1's scan must not consume
	// it, or a throttle-enabled watcher would never prune.
	scanWatcherNow(t, w, "w2")

	if _, ok := w.importedFiles.Load(secondStale); ok {
		t.Fatalf("the second watcher's prune was throttled by the first watcher's scan")
	}
}

// A non-"gone" stat failure is not evidence of absence

func TestPruneStaleImportedRecordsKeepsRecordsOnTransientStatFailure(t *testing.T) {
	w, _ := newTestWatcher(t)

	// A NUL byte makes os.Stat fail with a non-"not exist" error on every
	// supported platform, which is the portable stand-in for a transient
	// permission or sharing violation.
	badPath := filepath.Join(t.TempDir(), "bad\x00name.csv")
	if _, err := os.Stat(badPath); err == nil || os.IsNotExist(err) {
		t.Skipf("this platform cannot produce a non-ErrNotExist stat failure: %v", err)
	}

	w.storeImportedRecordSync(badPath, "fp", time.Now())

	if pruned := w.pruneStaleImportedRecords(context.Background(), &core.FileWatcherConfiguration{}); pruned != 0 {
		t.Fatalf("pruned = %d, want 0 when the source could not be inspected", pruned)
	}
	if _, ok := w.importedFiles.Load(badPath); !ok {
		t.Fatalf("a record whose source stat failed for another reason was pruned; the file becomes importable again")
	}
}

func TestLoadImportedFilesHistoryKeepsRecordsOnTransientStatFailure(t *testing.T) {
	badPath := filepath.Join(t.TempDir(), "bad\x00name.csv")
	if _, err := os.Stat(badPath); err == nil || os.IsNotExist(err) {
		t.Skipf("this platform cannot produce a non-ErrNotExist stat failure: %v", err)
	}

	configRoot := filepath.Join(t.TempDir(), "watchers")
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		t.Fatalf("mkdir config root: %v", err)
	}

	history := map[string]importedFileRecord{
		badPath: {Fingerprint: "fp", ImportedAtUnix: 1},
	}
	payload, err := json.Marshal(history)
	if err != nil {
		t.Fatalf("marshal history: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configRoot, importedFilesHistoryFileName), payload, 0o644); err != nil {
		t.Fatalf("write history: %v", err)
	}

	w, _ := newTestWatcherAt(t, configRoot)

	if _, ok := w.importedFiles.Load(badPath); !ok {
		t.Fatalf("a persisted record was dropped on load because its source could not be inspected; the file becomes importable again")
	}
}

// Unregister drops every per-watcher entry

func TestUnregisterWatcherDropsPerWatcherCaches(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MultiTenantMode = true
		config.TenantID = ""
		config.AutoCreateTenantDirectories = true
	}))

	// A scan populates the tenant enumeration and tenant-directory caches.
	scanWatcherNow(t, w, "w1")
	w.warnedIdempotentFallback.Store("w1", struct{}{})

	w.tenantCacheMu.Lock()
	_, tenantCached := w.tenantCache["w1"]
	_, directoryCached := w.tenantDirCache["w1"]
	w.tenantCacheMu.Unlock()

	if !tenantCached || !directoryCached {
		t.Fatalf("precondition failed: the per-watcher caches were not populated (tenant=%v directory=%v)", tenantCached, directoryCached)
	}

	if err := w.UnregisterWatcher(context.Background(), "w1"); err != nil {
		t.Fatalf("UnregisterWatcher() error = %v", err)
	}

	w.tenantCacheMu.Lock()
	_, tenantCached = w.tenantCache["w1"]
	_, directoryCached = w.tenantDirCache["w1"]
	w.tenantCacheMu.Unlock()

	if tenantCached || directoryCached {
		t.Fatalf("UnregisterWatcher left per-watcher cache entries behind (tenant=%v directory=%v)", tenantCached, directoryCached)
	}
	if _, ok := w.warnedIdempotentFallback.Load("w1"); ok {
		t.Fatalf("UnregisterWatcher left the one-time idempotence warning marker behind")
	}
}

// A cancelled pass still joins its spawned actions

// claimOnceStore returns a fixed job batch on its first ClaimDue and nothing
// afterwards, so a test can control exactly when a pass has actions in flight.
type claimOnceStore struct {
	SourceCleanupStore

	mu      sync.Mutex
	jobs    []SourceCleanupJob
	claimed bool
}

func (c *claimOnceStore) ClaimDue(context.Context, time.Time, time.Duration, int) ([]SourceCleanupJob, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.claimed {
		return nil, nil
	}
	c.claimed = true

	return append([]SourceCleanupJob(nil), c.jobs...), nil
}

func openTestSourceCleanupStore(t *testing.T) SourceCleanupStore {
	t.Helper()

	store, err := OpenSourceCleanupStore(
		context.Background(),
		filepath.Join(t.TempDir(), "cleanup.db"),
		SourceCleanupStoreOptions{MaxActiveJobs: 10},
	)
	if err != nil {
		t.Fatalf("OpenSourceCleanupStore() error = %v", err)
	}

	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return store
}

func TestProcessPassWaitsForSpawnedActionsWhenCancelled(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	started := make(chan struct{}, 1)

	claims := &claimOnceStore{
		SourceCleanupStore: openTestSourceCleanupStore(t),
		jobs: []SourceCleanupJob{
			{JobID: 1, WatcherID: "w", TenantID: "t", SourcePath: "a.csv", Fingerprint: "fp", Action: SourceCleanupActionDelete},
			{JobID: 2, WatcherID: "w", TenantID: "t", SourcePath: "b.csv", Fingerprint: "fp", Action: SourceCleanupActionDelete},
		},
	}

	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: claims, PollingInterval: time.Hour, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10,
		Execute: func(context.Context, SourceCleanupJob) error {
			started <- struct{}{}
			<-release

			return errors.New("cancelled")
		},
	})
	if err != nil {
		t.Fatalf("NewSourceCleanupWorker() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	passDone := make(chan struct{})
	go func() {
		worker.processPass(ctx)
		close(passDone)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no cleanup action started")
	}

	// The action budget is full, so the loop is waiting to dispatch the second
	// job when the cancellation lands.
	cancel()

	select {
	case <-passDone:
		t.Fatalf("processPass() returned while a spawned cleanup action was still running")
	case <-time.After(100 * time.Millisecond):
	}

	unblock()

	select {
	case <-passDone:
	case <-time.After(time.Second):
		t.Fatal("processPass() did not return after the action finished")
	}
}

// Start cannot Add while Stop is joining

func TestWorkerStartWaitsForStopJoin(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	started := make(chan struct{})
	cancelSeen := make(chan struct{})

	claims := &claimOnceStore{
		SourceCleanupStore: openTestSourceCleanupStore(t),
		jobs: []SourceCleanupJob{
			{JobID: 1, WatcherID: "w", TenantID: "t", SourcePath: "a.csv", Fingerprint: "fp", Action: SourceCleanupActionDelete},
		},
	}

	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: claims, PollingInterval: time.Hour, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10,
		Execute: func(ctx context.Context, _ SourceCleanupJob) error {
			close(started)
			<-ctx.Done()
			close(cancelSeen)
			<-release

			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("NewSourceCleanupWorker() error = %v", err)
	}

	t.Cleanup(worker.Stop)

	worker.Start(context.Background())

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup action never started")
	}

	stopDone := make(chan struct{})
	go func() {
		worker.Stop()
		close(stopDone)
	}()

	select {
	case <-cancelSeen:
	case <-time.After(time.Second):
		t.Fatal("Stop() never cancelled the run loop")
	}

	// Stop now holds the worker and is joining the blocked action.
	time.Sleep(20 * time.Millisecond)

	startReturned := make(chan struct{})
	go func() {
		worker.Start(context.Background())
		close(startReturned)
	}()

	select {
	case <-startReturned:
		unblock()

		// Let the Stop that is already in flight finish, and cancel whatever
		// loop the racing Start launched so the test cannot hang.
		go worker.Stop()

		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatalf("Stop() did not finish after the racing Start")
		}

		t.Fatalf("Start() returned while Stop() was still joining the run loop")
	case <-time.After(100 * time.Millisecond):
	}

	unblock()

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop() did not return after the action finished")
	}

	select {
	case <-startReturned:
	case <-time.After(time.Second):
		t.Fatal("Start() did not return after Stop() completed")
	}
}

// The invalid-options fallback keeps the resolved enabled state

func TestServiceInvalidOptionsFallbackPreservesInitialEnabled(t *testing.T) {
	configRoot := t.TempDir()

	// A persisted document that cannot describe a runnable schedule: a negative
	// default interval is rejected by applyServiceOptionsDefaults.
	document := `{"version":1,"enabled":true,"defaultPollingInterval":-1,` +
		`"minimumPollingInterval":0,"maximumPollingInterval":0,` +
		`"disabledCheckInterval":0,"maxParallelWatcherScans":4}`
	if err := os.WriteFile(filepath.Join(configRoot, fileWatcherOptionsFileName), []byte(document), 0o644); err != nil {
		t.Fatalf("write invalid options document: %v", err)
	}

	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          newStubFileWatcher(),
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
		InitialEnabled:       boolPointer(false),
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if service.IsEnabled() {
		t.Fatalf("IsEnabled() = true, want the construction-time disable preserved by the invalid-options fallback")
	}
	if got := service.Options().DefaultPollingInterval; got != 30*time.Second {
		t.Fatalf("DefaultPollingInterval = %v, want the documented default after the fallback", got)
	}

	// The default-safe direction is unchanged: without InitialEnabled the
	// fallback still starts enabled.
	enabled, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          newStubFileWatcher(),
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}
	if !enabled.IsEnabled() {
		t.Fatalf("IsEnabled() = false, want the default-safe enabled state after the fallback")
	}
}

// A job whose lease was not taken is not dispatched

func TestClaimDueSkipsJobWhoseLeaseWasNotTaken(t *testing.T) {
	ctx := context.Background()

	store := openTestSourceCleanupStore(t)

	job := SourceCleanupJob{
		WatcherID: "w", TenantID: "t", SourcePath: "claim-race.csv",
		Fingerprint: "fp", Action: SourceCleanupActionDelete, MaxAttempts: 3,
	}
	if reserved, err := store.TryReserve(ctx, &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(ctx, job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatalf("MarkImported() error = %v", err)
	}

	sqliteStore, ok := store.(*sqliteSourceCleanupStore)
	if !ok {
		t.Fatalf("store is %T, want *sqliteSourceCleanupStore", store)
	}

	// Simulate the lease race deterministically: the row is visible to the
	// SELECT but the lease UPDATE changes nothing, exactly as when another
	// worker took the lease in between. RAISE(IGNORE) abandons the row change
	// without an error, so RowsAffected is 0.
	if _, err := sqliteStore.db.ExecContext(ctx,
		`CREATE TRIGGER claim_race BEFORE UPDATE ON source_cleanup_jobs BEGIN SELECT RAISE(IGNORE); END`,
	); err != nil {
		t.Skipf("this SQLite build cannot raise IGNORE from a trigger: %v", err)
	}

	claimed, err := store.ClaimDue(ctx, time.Now(), time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimDue() returned %d jobs whose lease was not taken; the action would run and then fail with ErrSourceCleanupLeaseLost", len(claimed))
	}

	if _, err := sqliteStore.db.ExecContext(ctx, `DROP TRIGGER claim_race`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}

	// The job is still claimable once the lease can be taken again.
	claimed, err = store.ClaimDue(ctx, time.Now(), time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDue() #2 error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue() #2 = %d jobs, want the still-unleased job", len(claimed))
	}
}

// Clamp notices are per watcher and direction

func TestWarnClampOncePerWatcherAndDirection(t *testing.T) {
	handler := &recordingHandler{}

	logRuntime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	service := newBackgroundService(t, newStubFileWatcher(), func(opts *BackgroundFileWatcherServiceOptions) {
		opts.Logging = logRuntime
		opts.ServiceOptions = core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  30 * time.Second,
			MinimumPollingInterval:  time.Second,
			MaximumPollingInterval:  time.Minute,
			DisabledCheckInterval:   time.Minute,
			MaxParallelWatcherScans: 1,
		}
	})

	config := &core.FileWatcherConfiguration{WatcherID: "w1", PollingInterval: time.Millisecond}

	if got := service.pollingIntervalFor(config); got != time.Second {
		t.Fatalf("pollingIntervalFor(low) = %v, want the minimum", got)
	}

	config.PollingInterval = time.Hour
	if got := service.pollingIntervalFor(config); got != time.Minute {
		t.Fatalf("pollingIntervalFor(high) = %v, want the maximum", got)
	}

	// Repeating the same direction stays quiet, because it is the same notice.
	config.PollingInterval = time.Millisecond
	service.pollingIntervalFor(config)

	count := 0
	for _, record := range handler.snapshot() {
		if record.event == "polling_interval_clamped" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("clamp notices = %d, want one per watcher and direction (2): %v", count, eventNames(handler.snapshot()))
	}
}

// Each failure condition owns a stable event name

func TestImportHistorySaveFailureEventName(t *testing.T) {
	handler := &recordingHandler{}

	logRuntime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	configRoot := filepath.Join(t.TempDir(), "watchers")
	w, _ := newTestWatcherAt(t, configRoot, func(opts *FileWatcherOptions) {
		opts.Logging = logRuntime
	})

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	// Block every history write: replace the state directory with a regular
	// file after construction, then restore it before the watcher cleanup runs.
	if err := os.RemoveAll(configRoot); err != nil {
		t.Fatalf("remove config root: %v", err)
	}
	if err := os.WriteFile(configRoot, []byte("blocker"), 0o644); err != nil {
		t.Fatalf("create config root blocker: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(configRoot)
		_ = os.MkdirAll(configRoot, 0o755)
	})

	result := scanWatcherNow(t, w, "w1")
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1", result.FilesImported)
	}
	w.awaitPendingHistoryFlush()

	if !hasEvent(handler.snapshot(), "history_save_failed") {
		t.Fatalf("events = %v, want history_save_failed for the failed history write", eventNames(handler.snapshot()))
	}
}

// Background-service events carry the subject prefix

func TestBackgroundServiceEventNamesArePrefixed(t *testing.T) {
	handler := &recordingHandler{}

	logRuntime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true})

	service := newBackgroundService(t, watcher, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.Logging = logRuntime
		opts.InitialDelay = time.Millisecond
	})

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	if err := service.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	names := eventNames(handler.snapshot())

	for _, want := range []string{"watcher_service_started", "watcher_service_stopping", "watcher_service_stopped"} {
		if !hasEvent(handler.snapshot(), want) {
			t.Fatalf("event %q missing; events = %v", want, names)
		}
	}
	for _, legacy := range []string{"started", "stopping", "stopped"} {
		if hasEvent(handler.snapshot(), legacy) {
			t.Fatalf("legacy bare event %q is still emitted; events = %v", legacy, names)
		}
	}
}

// Documented construction-time enabled resolution

func TestServiceEnabledResolutionAtConstruction(t *testing.T) {
	// ServiceOptions.Enabled cannot express an explicit disable, so false here
	// must not silently disable scanning.
	service := newServiceWithState(t, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.ServiceOptions.Enabled = false
	})
	if !service.IsEnabled() {
		t.Fatalf("ServiceOptions.Enabled=false at construction disabled the service")
	}

	// A true value confirms the default-safe state.
	service = newServiceWithState(t, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.ServiceOptions.Enabled = true
	})
	if !service.IsEnabled() {
		t.Fatalf("ServiceOptions.Enabled=true at construction did not keep the service enabled")
	}

	// InitialEnabled is the documented explicit construction-time control.
	service = newServiceWithState(t, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.ServiceOptions.Enabled = true
		opts.InitialEnabled = boolPointer(false)
	})
	if service.IsEnabled() {
		t.Fatalf("InitialEnabled=false was overridden; it is the explicit construction-time control")
	}
}

// Prepared patterns are lower-cased and matched once

func TestRegisteredFilePatternsAreLowercased(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FilePatterns = []string{"*.CSV", "REPORT?.TXT"}
	}))

	stored, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}

	want := []string{"*.csv", "report?.txt"}
	if len(stored.FilePatterns) != len(want) {
		t.Fatalf("FilePatterns = %v, want %v", stored.FilePatterns, want)
	}
	for i, pattern := range want {
		if stored.FilePatterns[i] != pattern {
			t.Fatalf("FilePatterns[%d] = %q, want the prepared lower-case form %q", i, stored.FilePatterns[i], pattern)
		}
	}

	// The prepared form still matches case-insensitively.
	writeAgedFile(t, filepath.Join(watchPath, "report1.txt"), "payload")
	writeAgedFile(t, filepath.Join(watchPath, "notes.md"), "nope")

	result := scanWatcherNow(t, w, "w1")
	if result.FilesDiscovered != 1 || result.FilesImported != 1 {
		t.Fatalf("discovered/imported = %d/%d, want 1/1 for the lower-case prepared pattern", result.FilesDiscovered, result.FilesImported)
	}
}

// One dimension map per scan

// dimensionPointerRecorder records the identity of each dimension map it is
// handed, so a test can prove the scan builds it once instead of once per
// measurement.
type dimensionPointerRecorder struct {
	mu       sync.Mutex
	pointers []uintptr
}

func (r *dimensionPointerRecorder) Record(_ string, value int64, _ time.Time, dimensions map[string]string) {
	if value == 0 || len(dimensions) == 0 {
		return
	}

	r.mu.Lock()
	r.pointers = append(r.pointers, uintptr(reflect.ValueOf(dimensions).UnsafePointer()))
	r.mu.Unlock()
}

func (r *dimensionPointerRecorder) snapshotPointers() []uintptr {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]uintptr(nil), r.pointers...)
}

func TestScanStatisticsReuseOneDimensionMap(t *testing.T) {
	recorder := &dimensionPointerRecorder{}

	w, _ := newTestWatcherWithOptions(t, func(opts *FileWatcherOptions) {
		opts.StatisticsRecorder = recorder
	})

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, nil))
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	result := scanWatcherNow(t, w, "w1")
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1", result.FilesImported)
	}

	pointers := recorder.snapshotPointers()
	if len(pointers) < 2 {
		t.Fatalf("recorded measurements = %d, want at least two non-zero measurements", len(pointers))
	}
	for i, pointer := range pointers[1:] {
		if pointer != pointers[0] {
			t.Fatalf("measurement %d used a different dimension map; want one map reused per scan", i+1)
		}
	}
}

// Registration uses the core watcher-ID validation

func TestRegisterWatcherRejectsIDsThatCoreRejects(t *testing.T) {
	w, _ := newTestWatcher(t)

	for _, watcherID := range []string{
		"CON",
		"a:b",
		"bad*name",
		strings.Repeat("a", core.MaxWatcherIDLength+1),
	} {
		err := w.RegisterWatcher(context.Background(), newConfig(watcherID, t.TempDir(), nil))
		if err == nil {
			t.Errorf("RegisterWatcher(%q) error = nil, want an error", watcherID)

			continue
		}
		if !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("RegisterWatcher(%q) error = %v, want core.ErrInvalidArgument", watcherID, err)
		}
	}

	if err := w.RegisterWatcher(context.Background(), newConfig("watcher-1", t.TempDir(), nil)); err != nil {
		t.Fatalf("RegisterWatcher(watcher-1) error = %v, want nil", err)
	}
}
