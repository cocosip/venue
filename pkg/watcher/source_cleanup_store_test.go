package watcher

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSourceCleanupStoreConcurrentReservationsRespectCapacity(t *testing.T) {
	store, err := OpenSourceCleanupStore(context.Background(), filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	var wg sync.WaitGroup
	results := make(chan bool, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: string(rune('a' + i)), Fingerprint: "fp", Action: SourceCleanupActionDelete}
			reserved, reserveErr := store.TryReserve(context.Background(), &job)
			if reserveErr != nil && !errors.Is(reserveErr, ErrSourceCleanupCapacityExceeded) {
				t.Errorf("TryReserve() error = %v", reserveErr)
			}
			results <- reserved
		}(i)
	}
	wg.Wait()
	close(results)
	reservedCount := 0
	for reserved := range results {
		if reserved {
			reservedCount++
		}
	}
	if reservedCount != 1 {
		t.Fatalf("concurrent reservations = %d, want exactly 1", reservedCount)
	}
}

func TestSourceCleanupStoreReservesCapacityAndPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cleanup.db")
	store, err := OpenSourceCleanupStore(context.Background(), path, SourceCleanupStoreOptions{MaxActiveJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	job := SourceCleanupJob{
		WatcherID: "watcher-1", TenantID: "tenant-1", SourcePath: `C:\incoming\one.dcm`,
		Fingerprint: "fp-1", Action: SourceCleanupActionDelete,
		RetryInitialDelay: 3 * time.Second, RetryMaxDelay: 2 * time.Minute,
	}
	reserved, err := store.TryReserve(context.Background(), &job)
	if err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v; want true, nil", reserved, err)
	}
	second := job
	second.SourcePath = `C:\incoming\two.dcm`
	reserved, err = store.TryReserve(context.Background(), &second)
	if !errors.Is(err, ErrSourceCleanupCapacityExceeded) || reserved {
		t.Fatalf("second TryReserve() = %v, %v; want false, ErrSourceCleanupCapacityExceeded", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file-1", "op-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSourceCleanupStore(context.Background(), path, SourceCleanupStoreOptions{MaxActiveJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	loaded, err := store.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.FileKey != "file-1" || loaded.OperationID != "op-1" ||
		loaded.RetryInitialDelay != 3*time.Second || loaded.RetryMaxDelay != 2*time.Minute {
		t.Fatalf("loaded job = %#v, want persisted import details", loaded)
	}
}

func TestSourceCleanupStoreLeaseAllowsOnlyCurrentTokenToUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cleanup.db")
	store, err := OpenSourceCleanupStore(context.Background(), path, SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	job := SourceCleanupJob{
		WatcherID: "watcher-1", TenantID: "tenant-1", SourcePath: "source.dcm",
		Fingerprint: "fp-1", Action: SourceCleanupActionDelete,
	}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file-1", "op-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(context.Background(), time.Now().Add(time.Second), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue() returned %d jobs, want 1", len(claimed))
	}
	stale := claimed[0]
	stale.LeaseToken = "stale-token"
	if err := store.MarkSucceeded(context.Background(), stale); err == nil {
		t.Fatal("MarkSucceeded() with stale lease = nil, want error")
	}
	if err := store.MarkSucceeded(context.Background(), claimed[0]); err != nil {
		t.Fatal(err)
	}
}

func TestSourceCleanupStoreRecoversStaleImportReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cleanup.db")
	store, err := OpenSourceCleanupStore(context.Background(), path, SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "fp", Action: SourceCleanupActionDelete}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if _, err := store.RecoverStaleImports(context.Background(), time.Now().Add(time.Minute), time.Second, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != nil {
		t.Fatalf("stale reservation = %#v, want removed", loaded)
	}
}

func TestSourceCleanupStoreReplacesPendingJobWhenSourceRevisionChanges(t *testing.T) {
	store, err := OpenSourceCleanupStore(context.Background(), filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	old := SourceCleanupJob{
		WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "old",
		Action: SourceCleanupActionDelete,
	}
	if reserved, err := store.TryReserve(context.Background(), &old); err != nil || !reserved {
		t.Fatalf("old TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), old.JobID, "old-file", "old-op", time.Now()); err != nil {
		t.Fatal(err)
	}

	newJob := SourceCleanupJob{
		WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "new",
		Action: SourceCleanupActionDelete,
	}
	if reserved, err := store.TryReserve(context.Background(), &newJob); err != nil || !reserved {
		t.Fatalf("new TryReserve() = %v, %v; want replacement despite full capacity", reserved, err)
	}
	loaded, err := store.GetBySource(context.Background(), "w", "source")
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Fingerprint != "new" || loaded.State != SourceCleanupStateImporting {
		t.Fatalf("loaded replacement = %#v, want new importing job", loaded)
	}
}

// TestSourceCleanupStorePruneTerminalKeepsRowWhenStatFails proves a Kept row
// survives a stat that failed for a reason other than "the source is gone".
//
// The Kept row is the durable dedup tombstone of a source file that still
// exists. Treating any stat error as absence (a transient Windows sharing
// violation is the realistic case) retires the tombstone and the next scan
// re-imports the same file.
func TestSourceCleanupStorePruneTerminalKeepsRowWhenStatFails(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSourceCleanupStore(ctx, filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	sqliteStore := store.(*sqliteSourceCleanupStore)
	sqliteStore.statSource = func(string) (os.FileInfo, error) {
		return nil, &fs.PathError{Op: "stat", Path: "source", Err: fs.ErrPermission}
	}

	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source.dcm", Fingerprint: "fp", Action: SourceCleanupActionKeep}
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

	pruned, err := store.PruneTerminal(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("PruneTerminal() error = %v", err)
	}
	if pruned != 0 {
		t.Fatalf("PruneTerminal() removed %d rows, want 0 when the stat failed for a reason other than absence", pruned)
	}

	row, err := store.GetBySource(ctx, job.WatcherID, job.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.State != SourceCleanupStateKept {
		t.Fatalf("GetBySource() = %#v, want the Kept tombstone to survive a failed stat", row)
	}

	// A stat that reports absence is still a legitimate reason to retire it.
	sqliteStore.statSource = func(string) (os.FileInfo, error) {
		return nil, &fs.PathError{Op: "stat", Path: "source", Err: fs.ErrNotExist}
	}
	pruned, err = store.PruneTerminal(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("PruneTerminal() error = %v", err)
	}
	if pruned != 1 {
		t.Fatalf("PruneTerminal() removed %d rows, want 1 once the source is confirmed gone", pruned)
	}
}

// TestSourceCleanupStoreStoresErrorTypeOnly proves last_error carries the error
// type, never the raw message.
//
// SourceCleanupJob.LastError is returned to callers, and the project rule forbids
// leaking raw errors that embed full physical paths or original file names; a
// filesystem error does exactly that.
func TestSourceCleanupStoreStoresErrorTypeOnly(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSourceCleanupStore(ctx, filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source.dcm", Fingerprint: "fp", Action: SourceCleanupActionDelete}
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

	const secretPath = `C:\storage\tenant-a\reports\quarterly.dcm`
	actionErr := &fs.PathError{Op: "remove", Path: secretPath, Err: fs.ErrPermission}
	if err := store.MarkFailed(ctx, claimed[0], actionErr); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}

	loaded, err := store.GetBySource(ctx, job.WatcherID, job.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("GetBySource() = nil, want the failed job")
	}
	if strings.Contains(loaded.LastError, secretPath) {
		t.Fatalf("LastError = %q, want no physical path", loaded.LastError)
	}
	if loaded.LastError != fmt.Sprintf("%T", actionErr) {
		t.Fatalf("LastError = %q, want the error type %q", loaded.LastError, fmt.Sprintf("%T", actionErr))
	}
}

func TestSourceCleanupStoreCapacityIgnoresTerminalRecords(t *testing.T) {
	store, err := OpenSourceCleanupStore(context.Background(), filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "fp", Action: SourceCleanupActionKeep}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(context.Background(), time.Now(), time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue() = %d, %v; want one job", len(claimed), err)
	}
	if err := store.MarkSucceeded(context.Background(), claimed[0]); err != nil {
		t.Fatal(err)
	}
	second := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "other", Fingerprint: "fp2", Action: SourceCleanupActionDelete}
	if reserved, err := store.TryReserve(context.Background(), &second); err != nil || !reserved {
		t.Fatalf("TryReserve() after terminal record = %v, %v; want a fresh reservation: terminal rows must not consume capacity", reserved, err)
	}
}
