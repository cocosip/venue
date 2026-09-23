package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

func TestFileWatcherDurableSourceCleanupRunsAfterImport(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "cleanup.db")
	store, err := OpenSourceCleanupStore(storePath, SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	watchPath := t.TempDir()
	w, pool := newTestWatcherWithOptions(t, func(options *FileWatcherOptions) {
		options.SourceCleanupStore = store
		options.SourceCleanupWorkerOptions = &SourceCleanupWorkerOptions{
			PollingInterval: 5 * time.Millisecond, MaxConcurrentActions: 1,
			ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
			TerminalPruneBatchSize: 10, RetryInitialDelay: time.Millisecond,
			RetryMaxDelay: time.Millisecond,
		}
	})
	config := newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PostImportAction = core.PostImportActionDelete
		config.SourceCleanupFailureDirectory = "failed"
	})
	registerWatcher(t, w, config)
	sourcePath := filepath.Join(watchPath, "source.csv")
	writeAgedFile(t, sourcePath, "payload")
	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatal(err)
	}
	if result.FilesImported != 1 || pool.writeCount() != 1 {
		t.Fatalf("scan = %#v, writes = %d, want one import", result, pool.writeCount())
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("source should remain until worker runs: %v", err)
	}
	w.StartSourceCleanup(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("durable source cleanup did not delete the source")
}

func TestSourceCleanupDoesNotDeleteReplacedSourceRevision(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.csv")
	writeAgedFile(t, sourcePath, "old")
	file, err := os.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := fileFingerprint(file, info)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourcePath, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, _ := newTestWatcher(t)
	err = w.executeSourceCleanupJob(context.Background(), SourceCleanupJob{
		SourcePath: sourcePath, Fingerprint: fingerprint, Action: SourceCleanupActionDelete,
	})
	if err == nil {
		t.Fatal("executeSourceCleanupJob() error = nil, want fingerprint mismatch")
	}
	if _, statErr := os.Stat(sourcePath); statErr != nil {
		t.Fatalf("replaced source was removed: %v", statErr)
	}
}

func TestFileWatcherRejectsEscapingSourceCleanupFailureDirectory(t *testing.T) {
	w, _ := newTestWatcher(t)
	config := newConfig("w1", t.TempDir(), func(config *core.FileWatcherConfiguration) {
		config.SourceCleanupFailureDirectory = `..\outside`
	})
	if err := w.RegisterWatcher(context.Background(), config); err == nil {
		t.Fatal("RegisterWatcher() error = nil, want source cleanup path validation error")
	}
}
