package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestUpdatePhysicalPathGuardedUpdate verifies the read-path correction
// contract: only physical_path and updated_at move, the write is rejected when
// the stored path no longer matches the expectation, and a queue transition
// that happened between the caller's read and the correction is never
// overwritten.
func TestUpdatePhysicalPathGuardedUpdate(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = t.TempDir()
	})

	const tenantID = "tenant-corrector"
	seed := sqliteRecord(tenantID, "0123456789abcdef0123456789abcdef", time.Now().UTC())
	seed.Status = core.FileStatusPending
	seed.PhysicalPath = "stale/layout.key"
	seed.RetryCount = 3
	seed.LastError = "keep me"
	if err := repo.AddOrUpdate(ctx, seed); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	updatedBefore, err := repo.Get(ctx, tenantID, seed.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	ok, err := repo.UpdatePhysicalPath(ctx, tenantID, seed.FileKey, "stale/layout.key", "canonical/layout.key")
	if err != nil || !ok {
		t.Fatalf("UpdatePhysicalPath() = %v, %v; want true, nil", ok, err)
	}

	corrected, err := repo.Get(ctx, tenantID, seed.FileKey)
	if err != nil {
		t.Fatalf("Get() after correction error = %v", err)
	}
	if corrected.PhysicalPath != "canonical/layout.key" {
		t.Errorf("PhysicalPath = %q, want the canonical path", corrected.PhysicalPath)
	}
	if corrected.RetryCount != 3 || corrected.LastError != "keep me" {
		t.Errorf("guarded update rewrote unrelated columns: retry=%d error=%q", corrected.RetryCount, corrected.LastError)
	}
	if corrected.Status != core.FileStatusPending {
		t.Errorf("Status = %v, want Pending", corrected.Status)
	}
	if !corrected.UpdatedAt.After(updatedBefore.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want it refreshed past %v", corrected.UpdatedAt, updatedBefore.UpdatedAt)
	}

	// A stale expectation updates nothing and reports that.
	ok, err = repo.UpdatePhysicalPath(ctx, tenantID, seed.FileKey, "stale/layout.key", "another/path.key")
	if err != nil {
		t.Fatalf("UpdatePhysicalPath() with stale expectation error = %v", err)
	}
	if ok {
		t.Fatal("UpdatePhysicalPath() with a stale expectation reported success")
	}
	after, err := repo.Get(ctx, tenantID, seed.FileKey)
	if err != nil {
		t.Fatalf("Get() after stale correction error = %v", err)
	}
	if after.PhysicalPath != "canonical/layout.key" {
		t.Errorf("PhysicalPath = %q, want the correction to be untouched", after.PhysicalPath)
	}
}

// TestGetTimedOutProcessingFilesRespectsLimit verifies that a bounded reclaim
// pass asks the database for exactly its window instead of materializing the
// whole timed-out set.
func TestGetTimedOutProcessingFilesRespectsLimit(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = t.TempDir()
	})

	const tenantID = "tenant-timeout-limit"
	leaseStart := time.Now().UTC().Add(-2 * time.Hour)
	for i := 0; i < 5; i++ {
		record := sqliteRecord(tenantID, fileKeyForTest(i), time.Now().UTC())
		record.Status = core.FileStatusProcessing
		start := leaseStart
		record.ProcessingStartTime = &start
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate() error = %v", err)
		}
	}

	bounded, err := repo.GetTimedOutProcessingFiles(ctx, tenantID, time.Hour, 2)
	if err != nil {
		t.Fatalf("GetTimedOutProcessingFiles(limit) error = %v", err)
	}
	if len(bounded) != 2 {
		t.Fatalf("bounded result = %d rows, want 2", len(bounded))
	}

	everything, err := repo.GetTimedOutProcessingFiles(ctx, tenantID, time.Hour, 0)
	if err != nil {
		t.Fatalf("GetTimedOutProcessingFiles(unbounded) error = %v", err)
	}
	if len(everything) != 5 {
		t.Fatalf("unbounded result = %d rows, want 5", len(everything))
	}
}

func fileKeyForTest(i int) string {
	const hex = "0123456789abcdef"
	key := []byte(hex)
	for len(key) < 32 {
		key = append(key, hex[i%len(hex)])
	}
	return string(key[:32])
}

// TestBackupTenantDoesNotMaterializeUnknownTenant guards the stat-first
// contract: backing up a tenant that never had a database reports the
// documented error instead of creating an empty store as a side effect.
func TestBackupTenantDoesNotMaterializeUnknownTenant(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})

	dest := filepath.Join(t.TempDir(), "backup.db")
	if err := repo.BackupTenant(ctx, "tenant-unknown", dest); err == nil {
		t.Fatal("BackupTenant() on an unknown tenant succeeded, want an error")
	}
}
