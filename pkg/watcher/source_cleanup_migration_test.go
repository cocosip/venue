package watcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewFileWatcherMigratesLegacyPendingSourceCleanup(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.dcm")
	if err := os.WriteFile(sourcePath, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	history := map[string]importedFileRecord{
		sourcePath: {
			Fingerprint: "fp-legacy", ImportedAtUnix: time.Now().Unix(),
			PendingPostImportAction: true, FileKey: "file-legacy", WatcherID: "w1",
			TenantID: "tenant-1", PostImportAction: 0,
		},
	}
	historyBytes, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, importedFilesHistoryFileName), historyBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSourceCleanupStore(filepath.Join(root, "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	watcher, err := NewFileWatcher(&FileWatcherOptions{
		TenantManager: newFakeTenantManager(), StoragePool: &fakeStoragePool{},
		ConfigurationRootDir: root, SourceCleanupStore: store,
		SourceCleanupWorkerOptions: &SourceCleanupWorkerOptions{
			PollingInterval: time.Hour, MaxConcurrentActions: 1,
			ImportReservationTimeout: time.Minute, TerminalRetentionPeriod: time.Hour,
			TerminalPruneBatchSize: 10,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := watcher.(interface{ Close() error }).Close(); err != nil {
			t.Errorf("watcher Close() error = %v", err)
		}
	}()
	job, err := store.GetBySource(context.Background(), "w1", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.FileKey != "file-legacy" || job.State != SourceCleanupStatePending {
		t.Fatalf("migrated job = %#v, want pending durable job", job)
	}
	var remaining map[string]importedFileRecord
	data, err := os.ReadFile(filepath.Join(root, importedFilesHistoryFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("legacy history after migration = %#v, want empty", remaining)
	}
}
