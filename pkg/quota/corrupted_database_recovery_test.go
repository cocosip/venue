package quota

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// quotaRecoveryTenant is the tenant the recovery tests store quotas for.
const quotaRecoveryTenant = "tenant1"

// quotaDatabasePath mirrors the layout NewBadgerDirectoryQuotaRepository builds.
func quotaDatabasePath(dataPath string) string {
	return filepath.Join(dataPath, "quota")
}

// plantCorruptQuotaDatabase creates a directory that BadgerDB cannot open.
func plantCorruptQuotaDatabase(t *testing.T, dbPath string) {
	t.Helper()

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", dbPath, err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "MANIFEST"), []byte("this is not a badger manifest\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(MANIFEST) error = %v", err)
	}
}

// openQuotaRepository opens a repository and registers shutdown before the
// temporary directory that holds it is removed.
func openQuotaRepository(t *testing.T, opts *BadgerDirectoryQuotaRepositoryOptions) (core.DirectoryQuotaRepository, error) {
	t.Helper()

	repo, err := NewBadgerDirectoryQuotaRepository(opts)
	if repo != nil {
		t.Cleanup(func() { _ = repo.Close() })
	}
	return repo, err
}

// quarantinedQuotaDirectories lists the quarantine siblings of the quota path.
func quarantinedQuotaDirectories(t *testing.T, dbPath string) []string {
	t.Helper()

	matches, err := filepath.Glob(dbPath + ".corrupted.*")
	if err != nil {
		t.Fatalf("Glob(%s.corrupted.*) error = %v", dbPath, err)
	}
	return matches
}

// TestNewBadgerDirectoryQuotaRepositoryRejectsCorruptedDatabaseByDefault keeps
// the quota repository fail-fast unless recovery is explicitly enabled.
func TestNewBadgerDirectoryQuotaRepositoryRejectsCorruptedDatabaseByDefault(t *testing.T) {
	dataPath := t.TempDir()
	dbPath := quotaDatabasePath(dataPath)
	plantCorruptQuotaDatabase(t, dbPath)

	repo, err := openQuotaRepository(t, &BadgerDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	if err == nil {
		t.Fatalf("NewBadgerDirectoryQuotaRepository() error = nil, want a failure for a corrupted database")
	}
	if repo != nil {
		t.Fatalf("NewBadgerDirectoryQuotaRepository() returned repository %T with error %v", repo, err)
	}
	if quarantined := quarantinedQuotaDirectories(t, dbPath); len(quarantined) != 0 {
		t.Fatalf("quarantined = %v, want none while RecoverCorruptedDatabase is false", quarantined)
	}
	if _, statErr := os.Lstat(dbPath); statErr != nil {
		t.Fatalf("the database path was removed instead of being left alone: %v", statErr)
	}
}

// TestNewBadgerDirectoryQuotaRepositoryRecoversCorruptedDatabase is the
// regression test for a quota database that cannot be opened blocking startup:
// the directory is quarantined, the callback reports it, and the rebuilt store
// is usable.
func TestNewBadgerDirectoryQuotaRepositoryRecoversCorruptedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	dbPath := quotaDatabasePath(dataPath)
	plantCorruptQuotaDatabase(t, dbPath)

	var notified []string
	repo, err := openQuotaRepository(t, &BadgerDirectoryQuotaRepositoryOptions{
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		OnCorruptedDatabase:        func(path string) { notified = append(notified, path) },
	})
	if err != nil {
		t.Fatalf("NewBadgerDirectoryQuotaRepository(RecoverCorruptedDatabase: true) error = %v", err)
	}
	if repo == nil {
		t.Fatal("NewBadgerDirectoryQuotaRepository() returned a nil repository without an error")
	}

	quarantined := quarantinedQuotaDirectories(t, dbPath)
	if len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
	}
	if len(notified) != 1 || notified[0] != quarantined[0] {
		t.Fatalf("OnCorruptedDatabase calls = %v, want exactly [%s]", notified, quarantined[0])
	}
	if _, statErr := os.Lstat(filepath.Join(quarantined[0], "MANIFEST")); statErr != nil {
		t.Fatalf("quarantined data was not preserved: %v", statErr)
	}

	if _, err := repo.GetOrCreate(ctx, quotaRecoveryTenant, "/path/recovered"); err != nil {
		t.Fatalf("GetOrCreate() on the rebuilt database error = %v", err)
	}
	if err := repo.IncrementCount(ctx, quotaRecoveryTenant, "/path/recovered"); err != nil {
		t.Fatalf("IncrementCount() on the rebuilt database error = %v", err)
	}
	quotas, err := repo.GetAll(ctx, quotaRecoveryTenant)
	if err != nil {
		t.Fatalf("GetAll() on the rebuilt database error = %v", err)
	}
	if len(quotas) != 1 || quotas[0].CurrentCount != 1 {
		t.Fatalf("GetAll() = %+v, want one quota with count 1", quotas)
	}
}

// TestNewBadgerDirectoryQuotaRepositoryDoesNotQuarantineALockedDatabase is the
// safety guard for the quota store: a healthy database held by another owner
// must never be moved, even when recovery is enabled.
func TestNewBadgerDirectoryQuotaRepositoryDoesNotQuarantineALockedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	dbPath := quotaDatabasePath(dataPath)

	first, err := openQuotaRepository(t, &BadgerDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	if err != nil {
		t.Fatalf("first NewBadgerDirectoryQuotaRepository() error = %v", err)
	}
	if _, err := first.GetOrCreate(ctx, quotaRecoveryTenant, "/path/held"); err != nil {
		t.Fatalf("GetOrCreate() error = %v", err)
	}

	var notified []string
	second, err := NewBadgerDirectoryQuotaRepository(&BadgerDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		OnCorruptedDatabase:      func(path string) { notified = append(notified, path) },
	})
	if second != nil {
		_ = second.Close()
	}
	if err == nil {
		t.Fatal("opening a quota database another repository holds succeeded, want a lock failure")
	}
	if len(notified) != 0 {
		t.Fatalf("OnCorruptedDatabase calls = %v, want none for a held lock", notified)
	}
	if quarantined := quarantinedQuotaDirectories(t, dbPath); len(quarantined) != 0 {
		t.Fatalf("quarantined = %v, want none for a held lock", quarantined)
	}

	if _, err := first.GetOrCreate(ctx, quotaRecoveryTenant, "/path/still-here"); err != nil {
		t.Fatalf("GetOrCreate() after the rejected second open error = %v", err)
	}
}

// TestNewBadgerDirectoryQuotaRepositoryPrunesExpiredQuarantineDirectories
// covers the bounded retention of quarantined quota data.
func TestNewBadgerDirectoryQuotaRepositoryPrunesExpiredQuarantineDirectories(t *testing.T) {
	dataPath := t.TempDir()
	dbPath := quotaDatabasePath(dataPath)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(dbPath), err)
	}

	stale := dbPath + ".corrupted.20200101T000000Z"
	fresh := dbPath + ".corrupted.29990101T000000Z"
	for _, dir := range []string{stale, fresh} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", dir, err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", stale, err)
	}

	repo, err := openQuotaRepository(t, &BadgerDirectoryQuotaRepositoryOptions{
		DataPath:                   dataPath,
		CorruptedDatabaseRetention: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewBadgerDirectoryQuotaRepository() error = %v", err)
	}
	if repo == nil {
		t.Fatal("NewBadgerDirectoryQuotaRepository() returned a nil repository without an error")
	}

	if _, staleErr := os.Lstat(stale); !errors.Is(staleErr, os.ErrNotExist) {
		t.Fatalf("stale quarantine directory still present: %v", staleErr)
	}
	if _, freshErr := os.Lstat(fresh); freshErr != nil {
		t.Fatalf("fresh quarantine directory was pruned: %v", freshErr)
	}
}
