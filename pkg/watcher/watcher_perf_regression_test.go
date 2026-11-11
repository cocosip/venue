package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// --- W2: post-import history writes are coalesced ---------------------------

// TestScanNow_DeleteActionDefersHistoryWritesWhenDebounceEnabled proves a
// delete/move import no longer performs one full history rewrite before and one
// after the post-import action when the debounce is enabled: both changes are
// coalesced into the existing debounced write.
func TestScanNow_DeleteActionDefersHistoryWritesWhenDebounceEnabled(t *testing.T) {
	w, _ := newTestWatcher(t)

	spy := &flushSpy{}
	w.scheduleHistoryFlush = spy.schedule

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionDelete
		config.EnableImportedFilesHistoryFlushDebounce = true
		config.ImportedFilesHistoryFlushInterval = time.Hour
	}))

	result := scanWatcherNow(t, w, "w1")
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1 (%v)", result.FilesImported, result.Errors)
	}

	historyPath := filepath.Join(w.configRoot, importedFilesHistoryFileName)
	if _, err := os.Stat(historyPath); !os.IsNotExist(err) {
		t.Fatalf("history was written synchronously despite the enabled debounce: %v", err)
	}
	if got := spy.count(); got != 1 {
		t.Fatalf("scheduled history writes = %d, want 1 coalesced write for the store and remove pair", got)
	}

	spy.run(t, 0)
	w.awaitPendingHistoryFlush()

	if history := readPersistedHistory(t, w.configRoot); len(history) != 0 {
		t.Fatalf("persisted history entries = %d, want 0: the completed post-import state was removed", len(history))
	}
}

// --- W3: terminal pruning stops re-statting kept tombstones ----------------

// TestSourceCleanupStorePruneTerminalStampsConfirmedKeptRows proves a Kept row
// whose source still exists records the verification time, so the repeated prune
// cycle does not stat the same tombstone every pass.
func TestSourceCleanupStorePruneTerminalStampsConfirmedKeptRows(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSourceCleanupStore(ctx, filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	source := filepath.Join(t.TempDir(), "present.dcm")
	if err := os.WriteFile(source, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	sqliteStore := store.(*sqliteSourceCleanupStore)
	statCalls := 0
	sqliteStore.statSource = func(path string) (os.FileInfo, error) {
		statCalls++

		return os.Stat(path)
	}

	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: source, Fingerprint: "fp", Action: SourceCleanupActionKeep}
	if reserved, err := store.TryReserve(ctx, &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(ctx, job.JobID, "file-1", "op-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(ctx, time.Now(), time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue() = %d, %v; want one job", len(claimed), err)
	}
	if err := store.MarkSucceeded(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}

	// Let the wall clock advance past the row's creation time: Windows reports
	// time.Now in coarse ticks, so a same-tick cutoff would not select the row.
	time.Sleep(20 * time.Millisecond)
	before := time.Now()
	if pruned, err := store.PruneTerminal(ctx, before, 100); err != nil || pruned != 0 {
		t.Fatalf("first PruneTerminal() = %d, %v; want 0 pruned", pruned, err)
	}
	if statCalls != 1 {
		t.Fatalf("source stats after the first prune = %d, want 1", statCalls)
	}

	// The confirmed tombstone was stamped with the verification time, so the
	// next pass inside the retention window does not select it again.
	if pruned, err := store.PruneTerminal(ctx, before, 100); err != nil || pruned != 0 {
		t.Fatalf("second PruneTerminal() = %d, %v; want 0 pruned", pruned, err)
	}
	if statCalls != 1 {
		t.Fatalf("source stats after the second prune = %d, want 1: a confirmed Kept row must not be re-statted before retention elapses", statCalls)
	}

	row, err := store.GetBySource(ctx, job.WatcherID, job.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.State != SourceCleanupStateKept {
		t.Fatalf("GetBySource() = %#v, want the Kept tombstone to survive", row)
	}
}

// TestSourceCleanupStorePruneTerminalDeletesAbsentSourcesInBatch protects the
// batched DELETE: several tombstones whose sources are gone retire in one pass.
func TestSourceCleanupStorePruneTerminalDeletesAbsentSourcesInBatch(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSourceCleanupStore(ctx, filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	absentDir := t.TempDir()
	paths := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		source := filepath.Join(absentDir, fmt.Sprintf("gone-%d.dcm", i))
		paths = append(paths, source)

		job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: source, Fingerprint: "fp", Action: SourceCleanupActionKeep}
		if reserved, err := store.TryReserve(ctx, &job); err != nil || !reserved {
			t.Fatalf("TryReserve() = %v, %v", reserved, err)
		}
		if err := store.MarkImported(ctx, job.JobID, "file", "op", time.Now()); err != nil {
			t.Fatal(err)
		}
		claimed, err := store.ClaimDue(ctx, time.Now(), time.Minute, 10)
		if err != nil || len(claimed) == 0 {
			t.Fatalf("ClaimDue() = %d, %v", len(claimed), err)
		}
		if err := store.MarkSucceeded(ctx, claimed[0]); err != nil {
			t.Fatal(err)
		}
	}

	pruned, err := store.PruneTerminal(ctx, time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("PruneTerminal() error = %v", err)
	}
	if pruned != len(paths) {
		t.Fatalf("PruneTerminal() = %d, want %d", pruned, len(paths))
	}
	for _, source := range paths {
		row, err := store.GetBySource(ctx, "w", source)
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatalf("GetBySource(%s) = %#v, want the tombstone retired", filepath.Base(source), row)
		}
	}
}

// --- W4: eviction trims in one pass -----------------------------------------

// TestEvictOldestImportedRecordsLocked_TrimsInOnePass proves hitting the history
// cap trims a whole batch in one pass instead of dropping one record per insert,
// while the in-flight and pending-action rules are preserved.
func TestEvictOldestImportedRecordsLocked_TrimsInOnePass(t *testing.T) {
	w, _ := newTestWatcher(t)

	const extra = 50
	total := maxImportedFilesHistory + extra
	for i := 0; i < total; i++ {
		path := fmt.Sprintf("source-%05d.csv", i)
		w.importedFiles.Store(path, importedFileRecord{
			Fingerprint:    "fp",
			ImportedAtUnix: int64(i),
		})
		w.importedCount++
	}

	pendingPath := "pending-action.csv"
	w.importedFiles.Store(pendingPath, importedFileRecord{
		Fingerprint:             "fp",
		ImportedAtUnix:          0,
		PendingPostImportAction: true,
	})
	w.importedCount++

	claimedPath := "claimed.csv"
	w.importedFiles.Store(claimedPath, importedFileRecord{
		InFlightToken:  "token",
		ImportedAtUnix: 0,
	})
	w.importedCount++

	w.evictOldestImportedRecordsLocked()

	want := maxImportedFilesHistory * 9 / 10
	if got := w.importedEntryCount(); got != want {
		t.Fatalf("history entries after one eviction pass = %d, want %d", got, want)
	}

	removed := total + 2 - want
	for i := 0; i < removed; i++ {
		if _, ok := w.importedFiles.Load(fmt.Sprintf("source-%05d.csv", i)); ok {
			t.Fatalf("source-%05d.csv survived eviction, want the oldest records removed", i)
		}
	}
	if _, ok := w.importedFiles.Load(fmt.Sprintf("source-%05d.csv", removed)); !ok {
		t.Fatalf("source-%05d.csv was evicted, want the newest records kept", removed)
	}
	if _, ok := w.importedFiles.Load(pendingPath); !ok {
		t.Fatalf("pending-action record was evicted while plain records remained")
	}
	if _, ok := w.importedFiles.Load(claimedPath); !ok {
		t.Fatalf("in-flight claim was evicted")
	}
}

// --- W5: pruning is scoped to the scanning watcher --------------------------

// TestPruneStaleImportedRecords_ScopesToWatcherAndWatchPath proves one watcher's
// prune does not stat every shared history entry: another watcher's records and
// paths outside the watch directory are left to their owner.
func TestPruneStaleImportedRecords_ScopesToWatcherAndWatchPath(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.csv")

	ownMissing := filepath.Join(watchPath, "own-gone.csv")
	otherWatcherMissing := filepath.Join(watchPath, "other-gone.csv")
	outsideMissing := outsidePath
	legacyMissing := filepath.Join(watchPath, "legacy-gone.csv")

	seed := func(path, watcherID string) {
		w.storeImportedRecordSync(path, "fp", time.Now())
		value, ok := w.importedFiles.Load(path)
		if !ok {
			t.Fatalf("history record for %s is missing", path)
		}
		record := value.(importedFileRecord)
		record.WatcherID = watcherID
		w.importedFiles.Store(path, record)
	}

	seed(ownMissing, "w1")
	seed(otherWatcherMissing, "w2")
	seed(outsideMissing, "w1")
	seed(legacyMissing, "")

	pruned := w.pruneStaleImportedRecords(context.Background(), &core.FileWatcherConfiguration{
		WatcherID: "w1",
		WatchPath: watchPath,
	})
	if pruned != 2 {
		t.Fatalf("pruned = %d, want 2 (the watcher's own missing source and the legacy record)", pruned)
	}
	if _, ok := w.importedFiles.Load(ownMissing); ok {
		t.Fatalf("the scanning watcher's stale record survived")
	}
	if _, ok := w.importedFiles.Load(legacyMissing); ok {
		t.Fatalf("a legacy record without an owner inside the watch path survived")
	}
	if _, ok := w.importedFiles.Load(otherWatcherMissing); !ok {
		t.Fatalf("another watcher's record was pruned by this watcher")
	}
	if _, ok := w.importedFiles.Load(outsideMissing); !ok {
		t.Fatalf("a record outside the watch path was pruned by this watcher")
	}
}

// --- W7: confirmed tenant directories are cached ---------------------------

// TestCreateTenantDirectories_CachesConfirmedDirectoriesWithinTTL proves a
// steady-state scan does not stat every tenant directory again: a directory
// confirmed present is trusted for the configured TTL and re-checked afterwards.
func TestCreateTenantDirectories_CachesConfirmedDirectoriesWithinTTL(t *testing.T) {
	w, _ := newTestWatcher(t)

	current := time.Unix(1_700_000_000, 0)
	w.now = func() time.Time { return current }

	manager := w.tenantMgr.(*fakeTenantManager)
	manager.addTenant("tenant-a")
	manager.addTenant("tenant-b")

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MultiTenantMode = true
		config.TenantID = ""
		config.AutoCreateTenantDirectories = true
		config.AutoCreateTenantDirectoriesCacheTTL = time.Minute
	}))

	tenantBPath := filepath.Join(watchPath, "tenant-b")
	if info, err := os.Stat(tenantBPath); err != nil || !info.IsDir() {
		t.Fatalf("tenant-b directory was not created at registration: %v", err)
	}

	// A directory removed outside the watcher is not noticed until the TTL
	// elapses, because a confirmed directory is trusted for the whole window.
	if err := os.Remove(tenantBPath); err != nil {
		t.Fatalf("remove tenant-b directory: %v", err)
	}

	config, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}

	current = current.Add(30 * time.Second)
	if err := w.createTenantDirectories(context.Background(), config); err != nil {
		t.Fatalf("createTenantDirectories() error = %v", err)
	}
	if _, err := os.Stat(tenantBPath); !os.IsNotExist(err) {
		t.Fatalf("tenant-b directory was recreated inside the TTL: %v", err)
	}

	current = current.Add(31 * time.Second)
	if err := w.createTenantDirectories(context.Background(), config); err != nil {
		t.Fatalf("createTenantDirectories() error = %v", err)
	}
	if info, err := os.Stat(tenantBPath); err != nil || !info.IsDir() {
		t.Fatalf("tenant-b directory was not recreated after the TTL elapsed: %v", err)
	}
}

// --- W8: the persisted fingerprint format is unchanged ----------------------

// TestFileFingerprint_KeepsPersistedFormat pins the persisted de-duplication key
// format (a fingerprint may be reloaded after an upgrade) and the small-file
// sampling rule used by the allocation-reduced implementation.
func TestFileFingerprint_KeepsPersistedFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.bin")
	payload := []byte("hello world")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open sample: %v", err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat sample: %v", err)
	}

	got, err := fileFingerprint(file, info)
	if err != nil {
		t.Fatalf("fileFingerprint() error = %v", err)
	}

	sum := sha256.Sum256(payload)
	modified := info.ModTime().UTC().UnixNano()
	want := fmt.Sprintf("fp:v3:%d:%d:%d:%s", len(payload), modified, modified, base64.StdEncoding.EncodeToString(sum[:]))
	if got != want {
		t.Fatalf("fileFingerprint() = %q, want %q", got, want)
	}
}

// referenceContentSampleHash replicates the pre-optimization sampling rule
// (explicit visited-position map) so the allocation-reduced implementation can
// be checked for an identical hash across the rounding edge cases.
func referenceContentSampleHash(file *os.File, size int64) (string, error) {
	positions := []int64{
		0,
		max(0, (size-fingerprintSampleSize)/2),
		max(0, size-fingerprintSampleSize),
	}

	hasher := sha256.New()
	visited := make(map[int64]struct{}, len(positions))
	buffer := make([]byte, fingerprintSampleSize)

	for _, position := range positions {
		if _, ok := visited[position]; ok {
			continue
		}
		visited[position] = struct{}{}

		remaining := min(int64(fingerprintSampleSize), max(0, size-position))
		for remaining > 0 {
			readSize := min(int64(len(buffer)), remaining)
			n, err := file.ReadAt(buffer[:readSize], position)
			if n > 0 {
				_, _ = hasher.Write(buffer[:n])
				position += int64(n)
				remaining -= int64(n)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return "", err
			}
			if n == 0 {
				break
			}
		}
	}

	return base64.StdEncoding.EncodeToString(hasher.Sum(nil)), nil
}

// TestContentSampleHash_MatchesReferenceSampling protects the persisted
// fingerprint payload: the sample positions and their rounding must produce the
// same hash as the original map-based implementation, including the size where
// the middle and end samples round to the same position.
func TestContentSampleHash_MatchesReferenceSampling(t *testing.T) {
	sizes := []int{0, 1, 11, fingerprintSampleSize - 1, fingerprintSampleSize, fingerprintSampleSize + 1, fingerprintSampleSize + 2, 2 * fingerprintSampleSize, 3 * fingerprintSampleSize, 20000}

	for _, size := range sizes {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i*31 + 7)
		}

		path := filepath.Join(t.TempDir(), fmt.Sprintf("sample-%d.bin", size))
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatalf("write sample %d: %v", size, err)
		}

		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open sample %d: %v", size, err)
		}

		got, gotErr := contentSampleHash(file, int64(size))
		want, wantErr := referenceContentSampleHash(file, int64(size))
		closeErr := file.Close()
		if gotErr != nil || wantErr != nil || closeErr != nil {
			t.Fatalf("size %d: hash errors = %v, %v, %v", size, gotErr, wantErr, closeErr)
		}
		if got != want {
			t.Fatalf("size %d: contentSampleHash() = %q, want %q", size, got, want)
		}
	}
}

// --- W9: the cycle does not spin while the parallel bound blocks ------------

// TestDispatchScanCycle_DoesNotSpinWhileParallelBoundBlocksDueWatcher proves a
// due watcher deferred by the parallel-scan bound no longer makes the run loop
// poll every 200ms: the cycle waits for the real future due time and a released
// scan slot wakes it.
func TestDispatchScanCycle_DoesNotSpinWhileParallelBoundBlocksDueWatcher(t *testing.T) {
	watcher := newStubFileWatcher(
		&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true, PollingInterval: 30 * time.Second},
		&core.FileWatcherConfiguration{WatcherID: "w2", Enabled: true, PollingInterval: 30 * time.Second},
	)

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }

	started := make(chan struct{}, 1)
	watcher.scanHook = func(ctx context.Context, _ string) (*core.FileWatcherScanResult, error) {
		select {
		case started <- struct{}{}:
		default:
		}

		select {
		case <-release:
		case <-ctx.Done():
		}

		return &core.FileWatcherScanResult{}, nil
	}

	service := newBackgroundService(t, watcher, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.ServiceOptions = core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  30 * time.Second,
			MinimumPollingInterval:  time.Second,
			MaximumPollingInterval:  time.Hour,
			DisabledCheckInterval:   time.Second,
			MaxParallelWatcherScans: 1,
		}
	})
	t.Cleanup(func() {
		unblock()
		service.wg.Wait()
	})

	base := time.Unix(1_700_000_000, 0)

	delay := service.dispatchScanCycleAt(context.Background(), base)
	if delay <= 500*time.Millisecond {
		t.Fatalf("cycle delay = %v, want the future due time instead of a 200ms poll", delay)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the dispatched scan never started")
	}

	// The same clock again: one watcher is in flight, the other is due but the
	// parallel bound defers it. The cycle must still wait for the future due.
	if got := service.dispatchScanCycleAt(context.Background(), base); got <= 500*time.Millisecond {
		t.Fatalf("blocked cycle delay = %v, want the future due time", got)
	}

	unblock()
	service.wg.Wait()

	select {
	case <-service.scanWake:
	case <-time.After(time.Second):
		t.Fatal("releasing a scan slot did not wake the run loop")
	}
}
