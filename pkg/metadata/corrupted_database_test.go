package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// recoveryTestTenant is the tenant whose database directory the recovery tests
// exercise.
const recoveryTestTenant = "test-tenant"

// metadataDatabasePath mirrors the layout NewBadgerMetadataRepository builds.
func metadataDatabasePath(dataPath string) string {
	return filepath.Join(dataPath, recoveryTestTenant, "metadata")
}

// corruptionFixture plants a database path that BadgerDB cannot open.
type corruptionFixture struct {
	name  string
	plant func(t *testing.T, dbPath string)
}

// corruptDatabaseFixtures are the shapes a database directory that cannot be
// opened can take: a directory that looks like a store but is not one, and a
// plain file occupying the path.
func corruptDatabaseFixtures() []corruptionFixture {
	return []corruptionFixture{
		{
			name: "directory with a garbage manifest",
			plant: func(t *testing.T, dbPath string) {
				t.Helper()
				if err := os.MkdirAll(dbPath, 0o755); err != nil {
					t.Fatalf("MkdirAll(%s) error = %v", dbPath, err)
				}
				if err := os.WriteFile(filepath.Join(dbPath, "MANIFEST"), []byte("this is not a badger manifest\n"), 0o644); err != nil {
					t.Fatalf("WriteFile(MANIFEST) error = %v", err)
				}
			},
		},
		{
			name: "file occupying the database path",
			plant: func(t *testing.T, dbPath string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
					t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(dbPath), err)
				}
				if err := os.WriteFile(dbPath, []byte("not a database"), 0o644); err != nil {
					t.Fatalf("WriteFile(%s) error = %v", dbPath, err)
				}
			},
		},
	}
}

// openMetadataRepository opens a repository and registers shutdown before the
// temporary directory that holds it is removed.
func openMetadataRepository(t *testing.T, opts *BadgerRepositoryOptions) (core.MetadataRepository, error) {
	t.Helper()

	repo, err := NewBadgerMetadataRepository(opts)
	if repo != nil {
		t.Cleanup(func() { _ = repo.Close() })
	}
	return repo, err
}

// quarantinedDirectories lists the quarantine siblings of a database path.
func quarantinedDirectories(t *testing.T, dbPath string) []string {
	t.Helper()

	matches, err := filepath.Glob(dbPath + ".corrupted.*")
	if err != nil {
		t.Fatalf("Glob(%s.corrupted.*) error = %v", dbPath, err)
	}
	return matches
}

// TestNewBadgerMetadataRepositoryRejectsCorruptedDatabaseByDefault keeps the
// current fail-fast behaviour: recovery is opt-in.
func TestNewBadgerMetadataRepositoryRejectsCorruptedDatabaseByDefault(t *testing.T) {
	for _, fixture := range corruptDatabaseFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			dataPath := t.TempDir()
			dbPath := metadataDatabasePath(dataPath)
			fixture.plant(t, dbPath)

			repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
				TenantID: recoveryTestTenant,
				DataPath: dataPath,
			})
			if err == nil {
				t.Fatalf("NewBadgerMetadataRepository() error = nil, want a failure for %s", fixture.name)
			}
			if repo != nil {
				t.Fatalf("NewBadgerMetadataRepository() returned repository %T with error %v", repo, err)
			}
			if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 0 {
				t.Fatalf("quarantined = %v, want none while RecoverCorruptedDatabase is false", quarantined)
			}
			if _, statErr := os.Lstat(dbPath); statErr != nil {
				t.Fatalf("the database path was removed instead of being left alone: %v", statErr)
			}
		})
	}
}

// TestNewBadgerMetadataRepositoryQuarantinesAndRebuildsCorruptedDatabase is the
// regression test for a database that cannot be opened blocking startup: with
// recovery enabled the directory is quarantined, the callback reports it, and
// the fresh database is usable.
func TestNewBadgerMetadataRepositoryQuarantinesAndRebuildsCorruptedDatabase(t *testing.T) {
	ctx := context.Background()

	for _, fixture := range corruptDatabaseFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			dataPath := t.TempDir()
			dbPath := metadataDatabasePath(dataPath)
			fixture.plant(t, dbPath)

			var notified []string
			repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
				TenantID:                   recoveryTestTenant,
				DataPath:                   dataPath,
				RecoverCorruptedDatabase:   true,
				CorruptedDatabaseRetention: time.Hour,
				OnCorruptedDatabase:        func(path string) { notified = append(notified, path) },
			})
			if err != nil {
				t.Fatalf("NewBadgerMetadataRepository(RecoverCorruptedDatabase: true) error = %v", err)
			}
			if repo == nil {
				t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
			}

			quarantined := quarantinedDirectories(t, dbPath)
			if len(quarantined) != 1 {
				t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
			}
			if len(notified) != 1 {
				t.Fatalf("OnCorruptedDatabase calls = %d (%v), want 1", len(notified), notified)
			}
			if notified[0] != quarantined[0] {
				t.Fatalf("OnCorruptedDatabase path = %q, want %q", notified[0], quarantined[0])
			}
			if base := filepath.Base(quarantined[0]); base == "" {
				t.Fatalf("quarantine path %q has no base name", quarantined[0])
			}

			// The quarantined directory must still hold the unusable data: the
			// quarantine is a rescue, not a delete.
			if _, statErr := os.Lstat(quarantined[0]); statErr != nil {
				t.Fatalf("quarantined path %q is gone: %v", quarantined[0], statErr)
			}

			// The rebuilt repository must be fully usable.
			record := createTestMetadata("recovered-file", core.FileStatusPending)
			if err := repo.AddOrUpdate(ctx, record); err != nil {
				t.Fatalf("AddOrUpdate() on the rebuilt database error = %v", err)
			}
			got, err := repo.Get(ctx, record.TenantID, record.FileKey)
			if err != nil {
				t.Fatalf("Get() on the rebuilt database error = %v", err)
			}
			if got.FileKey != record.FileKey {
				t.Fatalf("Get() file key = %s, want %s", got.FileKey, record.FileKey)
			}
		})
	}
}

// TestNewBadgerMetadataRepositoryQuarantinePathIsWindowsSafe keeps the
// quarantine name free of characters Windows rejects in a path segment.
func TestNewBadgerMetadataRepositoryQuarantinePathIsWindowsSafe(t *testing.T) {
	dataPath := t.TempDir()
	dbPath := metadataDatabasePath(dataPath)
	corruptDatabaseFixtures()[0].plant(t, dbPath)

	repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                 recoveryTestTenant,
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if repo == nil {
		t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
	}

	quarantined := quarantinedDirectories(t, dbPath)
	if len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
	}

	const timestampLayout = "20060102T150405Z"
	suffix := strings.TrimPrefix(filepath.Base(quarantined[0]), filepath.Base(dbPath)+".corrupted.")
	if len(suffix) < len(timestampLayout) {
		t.Fatalf("quarantine suffix of %q is too short to hold a timestamp", quarantined[0])
	}
	if _, err := time.Parse(timestampLayout, suffix[:len(timestampLayout)]); err != nil {
		t.Fatalf("quarantine suffix %q does not start with a UTC timestamp: %v", suffix, err)
	}
	if strings.ContainsAny(suffix, `:*?"<>|`) {
		t.Fatalf("quarantine suffix %q contains a character Windows rejects in a path", suffix)
	}
}

// TestNewBadgerMetadataRepositoryDoesNotQuarantineALockedDatabase is the
// safety guard: a healthy database held by another owner must never be moved,
// even when recovery is enabled.
func TestNewBadgerMetadataRepositoryDoesNotQuarantineALockedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	dbPath := metadataDatabasePath(dataPath)

	first, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID: recoveryTestTenant,
		DataPath: dataPath,
	})
	if err != nil {
		t.Fatalf("first NewBadgerMetadataRepository() error = %v", err)
	}
	seed := createTestMetadata("locked-file", core.FileStatusPending)
	if err := first.AddOrUpdate(ctx, seed); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	var notified []string
	second, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID:                 recoveryTestTenant,
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		OnCorruptedDatabase:      func(path string) { notified = append(notified, path) },
	})
	if second != nil {
		_ = second.Close()
	}
	if err == nil {
		t.Fatal("opening a database another repository holds succeeded, want a lock failure")
	}
	if !isBadgerLockError(err) {
		t.Fatalf("error = %v, want a lock/ownership failure that recovery must not quarantine", err)
	}
	if len(notified) != 0 {
		t.Fatalf("OnCorruptedDatabase calls = %v, want none for a held lock", notified)
	}
	if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 0 {
		t.Fatalf("quarantined = %v, want none for a held lock", quarantined)
	}

	// The healthy database is untouched and still usable.
	if err := first.AddOrUpdate(ctx, createTestMetadata("still-here", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() after the rejected second open error = %v", err)
	}
	if _, err := first.Get(ctx, seed.TenantID, seed.FileKey); err != nil {
		t.Fatalf("Get() after the rejected second open error = %v", err)
	}
}

// TestIsBadgerLockErrorIsConservative pins the classifier that protects a
// healthy database from being quarantined.
func TestIsBadgerLockErrorIsConservative(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "manifest read failure", err: errors.New("While opening manifest file: unexpected EOF"), want: false},
		{name: "corruption", err: errors.New("Value log truncate required to run DB"), want: false},
		{
			name: "windows lock failure",
			err:  errors.New(`Cannot create lock file "C:\data\metadata\LOCK".  Another process is using this Badger database`),
			want: true,
		},
		{
			name: "unix lock failure",
			err:  errors.New(`Cannot acquire directory lock on "/data/metadata".  Another process is using this Badger database.`),
			want: true,
		},
		{
			name: "wrapped lock failure",
			err:  fmt.Errorf("failed to open BadgerDB: %w", errors.New("Cannot create lock file \"LOCK\"")),
			want: true,
		},
		{name: "badger conflict", err: badger.ErrConflict, want: true},
		{name: "wrapped badger conflict", err: fmt.Errorf("update failed: %w", badger.ErrConflict), want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBadgerLockError(tc.err); got != tc.want {
				t.Fatalf("isBadgerLockError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestNewBadgerMetadataRepositoryPrunesExpiredQuarantineDirectories covers the
// bounded retention of quarantined data.
func TestNewBadgerMetadataRepositoryPrunesExpiredQuarantineDirectories(t *testing.T) {
	testCases := []struct {
		name          string
		retention     time.Duration
		wantStaleGone bool
	}{
		{name: "expired quarantine is pruned", retention: time.Hour, wantStaleGone: true},
		{name: "negative retention disables pruning", retention: -time.Hour, wantStaleGone: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			dbPath := metadataDatabasePath(dataPath)
			tenantDir := filepath.Dir(dbPath)
			if err := os.MkdirAll(tenantDir, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s) error = %v", tenantDir, err)
			}

			stale := filepath.Join(tenantDir, filepath.Base(dbPath)+".corrupted.20200101T000000Z")
			fresh := filepath.Join(tenantDir, filepath.Base(dbPath)+".corrupted.29990101T000000Z")
			for _, dir := range []string{stale, fresh} {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("MkdirAll(%s) error = %v", dir, err)
				}
			}
			old := time.Now().Add(-48 * time.Hour)
			if err := os.Chtimes(stale, old, old); err != nil {
				t.Fatalf("Chtimes(%s) error = %v", stale, err)
			}

			repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
				TenantID:                   recoveryTestTenant,
				DataPath:                   dataPath,
				CorruptedDatabaseRetention: tc.retention,
			})
			if err != nil {
				t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
			}
			if repo == nil {
				t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
			}

			_, staleErr := os.Lstat(stale)
			if tc.wantStaleGone && !errors.Is(staleErr, os.ErrNotExist) {
				t.Fatalf("stale quarantine directory still present: %v", staleErr)
			}
			if !tc.wantStaleGone && staleErr != nil {
				t.Fatalf("stale quarantine directory was pruned with pruning disabled: %v", staleErr)
			}
			if _, freshErr := os.Lstat(fresh); freshErr != nil {
				t.Fatalf("fresh quarantine directory was pruned: %v", freshErr)
			}
		})
	}
}

// TestOpenBadgerWithRecoveryRejectsNilOpen keeps the shared helper honest for
// the quota package caller.
func TestOpenBadgerWithRecoveryRejectsNilOpen(t *testing.T) {
	_, err := OpenBadgerWithRecovery(t.TempDir(), CorruptedDatabaseRecoveryOptions{}, nil)
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("OpenBadgerWithRecovery(nil open) error = %v, want %v", err, core.ErrInvalidArgument)
	}
}
