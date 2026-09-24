package watcher

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSourceCleanupStoreConcurrentReservationsRespectCapacity(t *testing.T) {
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
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
			if reserveErr != nil {
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
	store, err := OpenSourceCleanupStore(path, SourceCleanupStoreOptions{MaxActiveJobs: 1})
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
	if err != nil || reserved {
		t.Fatalf("second TryReserve() = %v, %v; want false, nil", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file-1", "op-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenSourceCleanupStore(path, SourceCleanupStoreOptions{MaxActiveJobs: 1})
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
	store, err := OpenSourceCleanupStore(path, SourceCleanupStoreOptions{MaxActiveJobs: 10})
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
	store, err := OpenSourceCleanupStore(path, SourceCleanupStoreOptions{MaxActiveJobs: 10})
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
	if _, err := store.RecoverStaleImports(context.Background(), time.Now().Add(time.Minute), time.Second); err != nil {
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
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
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

func TestSourceCleanupStoreCapacityIncludesTerminalSuppressionRecords(t *testing.T) {
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 1})
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
	if reserved, err := store.TryReserve(context.Background(), &second); err != nil || reserved {
		t.Fatalf("TryReserve() after terminal record = %v, %v; want capacity full", reserved, err)
	}
}
