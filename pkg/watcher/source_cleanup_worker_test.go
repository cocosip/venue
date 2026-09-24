package watcher

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceCleanupWorkerRetriesDueJobAndCompletesIt(t *testing.T) {
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
	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "fp", Action: SourceCleanupActionDelete, MaxAttempts: 3}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: store, PollingInterval: 5 * time.Millisecond, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10, Execute: func(context.Context, SourceCleanupJob) error {
			if attempts.Add(1) == 1 {
				return errors.New("temporary")
			}
			return nil
		},
		RetryInitialDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if attempts.Load() != 2 {
		worker.Stop()
		t.Fatalf("execute attempts = %d, want 2", attempts.Load())
	}
	for time.Now().Before(deadline) {
		loaded, loadErr := store.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
		if loadErr != nil {
			worker.Stop()
			t.Fatal(loadErr)
		}
		if loaded == nil {
			worker.Stop()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	worker.Stop()
	t.Fatal("completed Delete job was not removed")
}

func TestSourceCleanupWorkerPausesWhenRuntimeIsDisabled(t *testing.T) {
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
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
	if err := store.MarkImported(context.Background(), job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: store, PollingInterval: 5 * time.Millisecond, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10, RuntimeEnabled: func(context.Context) bool { return false },
		Execute: func(context.Context, SourceCleanupJob) error {
			attempts.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	worker.Stop()
	if attempts.Load() != 0 {
		t.Fatalf("execute attempts = %d, want 0 while runtime disabled", attempts.Load())
	}
}

func TestSourceCleanupWorkerDiscardsObsoleteFingerprintJob(t *testing.T) {
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	job := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "old", Action: SourceCleanupActionDelete, MaxAttempts: 5}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatal(err)
	}
	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: store, PollingInterval: 5 * time.Millisecond, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10,
		Execute:                func(context.Context, SourceCleanupJob) error { return ErrSourceCleanupFingerprintChanged },
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		loaded, loadErr := store.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if loaded == nil {
			worker.Stop()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	worker.Stop()
	t.Fatal("obsolete fingerprint job was not discarded")
}

func TestSourceCleanupWorkerRetriesFailureQuarantineAfterCleanupExhaustion(t *testing.T) {
	store, err := OpenSourceCleanupStore(filepath.Join(t.TempDir(), "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	job := SourceCleanupJob{
		WatcherID: "w", TenantID: "t", SourcePath: "source", Fingerprint: "fp",
		Action: SourceCleanupActionDelete, FailureDirectory: "failed", MaxAttempts: 1,
		RetryInitialDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
	}
	if reserved, err := store.TryReserve(context.Background(), &job); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(context.Background(), job.JobID, "file", "op", time.Now()); err != nil {
		t.Fatal(err)
	}
	var quarantineAttempts atomic.Int32
	worker, err := NewSourceCleanupWorker(SourceCleanupWorkerOptions{
		Store: store, PollingInterval: 5 * time.Millisecond, MaxConcurrentActions: 1,
		ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
		TerminalPruneBatchSize: 10, RetryInitialDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
		Execute: func(context.Context, SourceCleanupJob) error { return errors.New("delete failed") },
		Quarantine: func(context.Context, SourceCleanupJob) error {
			if quarantineAttempts.Add(1) == 1 {
				return errors.New("quarantine temporarily unavailable")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.Start(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		loaded, loadErr := store.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if loaded == nil {
			worker.Stop()
			if quarantineAttempts.Load() != 2 {
				t.Fatalf("quarantine attempts = %d, want 2", quarantineAttempts.Load())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	worker.Stop()
	t.Fatal("failure quarantine was not retried to completion")
}
