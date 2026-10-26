package tenant

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// newStoreWithSyncRecorder builds a store whose directory-sync seam is recorded
// instead of executed, so the durability contract of Save is observable without
// depending on the platform's willingness to fsync a directory handle.
func newStoreWithSyncRecorder(t *testing.T) (*MetadataStore, *syncRecorder) {
	t.Helper()

	root := t.TempDir()
	store, err := NewMetadataStore(root)
	if err != nil {
		t.Fatalf("NewMetadataStore() error = %v", err)
	}

	recorder := &syncRecorder{}
	store.syncDirectory = recorder.record
	return store, recorder
}

// syncRecorder records syncDirectory calls and can inject a failure.
type syncRecorder struct {
	mu     sync.Mutex
	paths  []string
	rawErr error
}

func (r *syncRecorder) record(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = append(r.paths, path)
	return r.rawErr
}

func (r *syncRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}

// TestSaveSyncsDirectoryAfterRename pins the durability contract: the file
// contents are fsynced before the rename, so the directory entry that names the
// fresh tenant record must be synced too, or a crash can lose the name while
// the bytes survived.
func TestSaveSyncsDirectoryAfterRename(t *testing.T) {
	store, recorder := newStoreWithSyncRecorder(t)

	if err := store.Save(createDefaultMetadata("tenant-sync", "")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	calls := recorder.recorded()
	if len(calls) != 1 {
		t.Fatalf("syncDirectory calls = %v, want exactly one", calls)
	}
	// The root is compared cleaned: a temp directory can carry mixed
	// separators on Windows, while the saved file's directory is native.
	if want := filepath.Clean(store.rootPath); filepath.Clean(calls[0]) != want {
		t.Errorf("syncDirectory path = %q, want the metadata root %q", calls[0], want)
	}
}

// TestSaveToleratesDirectorySyncFailure keeps the sync best-effort: opening a
// directory for fsync is not portable (Windows rejects the fsync), and the
// tenant record itself is already durable, so a sync failure must not fail the
// Save that the caller already observes as committed.
func TestSaveToleratesDirectorySyncFailure(t *testing.T) {
	store, recorder := newStoreWithSyncRecorder(t)
	recorder.rawErr = errors.New("directory sync is not supported here")

	if err := store.Save(createDefaultMetadata("tenant-unsynced", "")); err != nil {
		t.Fatalf("Save() error = %v, want nil for a best-effort directory sync", err)
	}

	if _, err := store.Load("tenant-unsynced"); err != nil {
		t.Errorf("Load() after save error = %v, want the saved record", err)
	}
}
