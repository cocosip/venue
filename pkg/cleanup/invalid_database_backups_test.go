package cleanup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// writeAgedFile creates a file of the given size below a directory and forces
// its modification time, so retention can be evaluated deterministically.
func writeAgedFile(t *testing.T, path string, size int, modified time.Time) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), size), 0644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", path, err)
	}
}

// writeAgedQuarantine creates a quarantined database file of the given size and
// forces its modification time. The SQLite quarantine object is the per-tenant
// database file moved aside as "<dbFile>.corrupted.<stamp>", so the fixture
// mirrors the file the sweep has to recognize rather than the Badger directory
// it used to be.
func writeAgedQuarantine(t *testing.T, path string, size int, modified time.Time) {
	t.Helper()

	writeAgedFile(t, path, size, modified)
}

// newQuarantineSweepService builds a cleanup service whose quarantine sweep
// roots are isolated temporary directories.
func newQuarantineSweepService(t *testing.T, retention time.Duration) (core.CleanupService, string, string) {
	t.Helper()

	metadataRoot := t.TempDir()
	quotaRoot := t.TempDir()

	service, _, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.MetadataDirectory = metadataRoot
		opts.QuotaDirectory = quotaRoot
		opts.CorruptedDatabaseRetention = retention
	})

	return service, metadataRoot, quotaRoot
}

// TestCleanupInvalidDatabaseBackups_RemovesOnlyExpiredQuarantineDirectories is
// the regression test for the Locus corruption-backup sweep: entries named
// "<dbFile>.corrupted.<stamp>" older than the retention are removed from both
// database trees, while fresh quarantines and live databases are untouched.
func TestCleanupInvalidDatabaseBackups_RemovesOnlyExpiredQuarantineDirectories(t *testing.T) {
	ctx := context.Background()

	service, metadataRoot, quotaRoot := newQuarantineSweepService(t, 24*time.Hour)

	stale := time.Now().Add(-100 * time.Hour)
	fresh := time.Now().Add(-1 * time.Hour)

	staleMetadata := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200101T000000Z")
	staleQuota := filepath.Join(quotaRoot, "shared", "quotas.db.corrupted.20200101T000000Z")
	freshMetadata := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.29990101T000000Z")
	liveDatabase := filepath.Join(metadataRoot, "shared", "metadata.db")

	writeAgedQuarantine(t, staleMetadata, 1000, stale)
	writeAgedQuarantine(t, staleQuota, 500, stale)
	writeAgedQuarantine(t, freshMetadata, 2000, fresh)
	writeAgedQuarantine(t, liveDatabase, 4096, stale)

	stats, err := service.CleanupInvalidDatabaseBackups(ctx)
	if err != nil {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v", err)
	}

	if stats.InvalidDatabaseBackupsRemoved != 2 {
		t.Errorf("InvalidDatabaseBackupsRemoved = %d, want 2", stats.InvalidDatabaseBackupsRemoved)
	}
	if wantFreed := int64(1000 + 500); stats.SpaceFreed != wantFreed {
		t.Errorf("SpaceFreed = %d, want %d", stats.SpaceFreed, wantFreed)
	}

	for _, removed := range []string{staleMetadata, staleQuota} {
		if _, statErr := os.Stat(removed); !os.IsNotExist(statErr) {
			t.Errorf("expired quarantine %s still exists, stat error = %v", removed, statErr)
		}
	}
	for _, retained := range []string{freshMetadata, liveDatabase} {
		if _, statErr := os.Stat(retained); statErr != nil {
			t.Errorf("expected %s to be retained, stat error = %v", retained, statErr)
		}
	}
}

// TestCleanupInvalidDatabaseBackups_ZeroRetentionSelectsDefaultRetention pins the
// documented 72h runtime default for a zero retention.
func TestCleanupInvalidDatabaseBackups_ZeroRetentionSelectsDefaultRetention(t *testing.T) {
	ctx := context.Background()

	service, metadataRoot, _ := newQuarantineSweepService(t, 0)

	olderThanDefault := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200101T000000Z")
	newerThanDefault := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200102T000000Z")
	writeAgedQuarantine(t, olderThanDefault, 10, time.Now().Add(-100*time.Hour))
	writeAgedQuarantine(t, newerThanDefault, 10, time.Now().Add(-1*time.Hour))

	stats, err := service.CleanupInvalidDatabaseBackups(ctx)
	if err != nil {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v", err)
	}
	if stats.InvalidDatabaseBackupsRemoved != 1 {
		t.Errorf("InvalidDatabaseBackupsRemoved = %d, want 1", stats.InvalidDatabaseBackupsRemoved)
	}
	if _, statErr := os.Stat(olderThanDefault); !os.IsNotExist(statErr) {
		t.Errorf("quarantine older than 72h should be removed, stat error = %v", statErr)
	}
	if _, statErr := os.Stat(newerThanDefault); statErr != nil {
		t.Errorf("quarantine newer than 72h should be retained, stat error = %v", statErr)
	}
}

// TestCleanupInvalidDatabaseBackups_NegativeRetentionDisablesSweep verifies that
// a negative retention selects "never sweep".
func TestCleanupInvalidDatabaseBackups_NegativeRetentionDisablesSweep(t *testing.T) {
	ctx := context.Background()

	service, metadataRoot, _ := newQuarantineSweepService(t, -time.Hour)

	stale := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200101T000000Z")
	writeAgedQuarantine(t, stale, 10, time.Now().Add(-100*time.Hour))

	stats, err := service.CleanupInvalidDatabaseBackups(ctx)
	if err != nil {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v", err)
	}
	if stats.InvalidDatabaseBackupsRemoved != 0 || stats.SpaceFreed != 0 {
		t.Errorf("disabled sweep statistics = %+v, want an empty statistic", stats)
	}
	if _, statErr := os.Stat(stale); statErr != nil {
		t.Errorf("disabled sweep must not remove quarantines, stat error = %v", statErr)
	}
}

// TestCleanupInvalidDatabaseBackups_MissingRootIsBestEffort verifies that the
// sweep never fails as a whole because one root is unavailable.
func TestCleanupInvalidDatabaseBackups_MissingRootIsBestEffort(t *testing.T) {
	ctx := context.Background()

	metadataRoot := t.TempDir()
	missingQuotaRoot := filepath.Join(t.TempDir(), "does-not-exist")

	service, _, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.MetadataDirectory = metadataRoot
		opts.QuotaDirectory = missingQuotaRoot
		opts.CorruptedDatabaseRetention = time.Hour
	})

	stale := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200101T000000Z")
	writeAgedQuarantine(t, stale, 10, time.Now().Add(-100*time.Hour))

	stats, err := service.CleanupInvalidDatabaseBackups(ctx)
	if err != nil {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v", err)
	}
	if stats.InvalidDatabaseBackupsRemoved != 1 {
		t.Errorf("InvalidDatabaseBackupsRemoved = %d, want 1", stats.InvalidDatabaseBackupsRemoved)
	}
}

// TestCleanupInvalidDatabaseBackups_HonoursContextCancellation verifies the sweep
// is cancellable.
func TestCleanupInvalidDatabaseBackups_HonoursContextCancellation(t *testing.T) {
	service, metadataRoot, _ := newQuarantineSweepService(t, time.Hour)

	stale := filepath.Join(metadataRoot, "shared", "metadata.db.corrupted.20200101T000000Z")
	writeAgedQuarantine(t, stale, 10, time.Now().Add(-100*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.CleanupInvalidDatabaseBackups(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(stale); statErr != nil {
		t.Errorf("cancelled sweep must not remove quarantines, stat error = %v", statErr)
	}
}
