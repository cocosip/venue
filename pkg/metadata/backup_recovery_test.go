package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// backupTestTenant is the tenant whose database the automatic-recovery tests
// quarantine and rebuild.
const backupTestTenant = "auto-restore-tenant"

// unsupportedMetadataRepository is a MetadataRepository without the optional
// MetadataBackupService capability.
type unsupportedMetadataRepository struct {
	core.MetadataRepository
}

// failingWriter fails every write with a fixed error so a test can prove the
// error surfaces unchanged.
type failingWriter struct {
	err error
}

// Write always fails with the configured error.
func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

// failingReader fails every read with a fixed error.
type failingReader struct {
	err error
}

// Read always fails with the configured error.
func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

// recordedStatistic is one captured recorder call.
type recordedStatistic struct {
	name       string
	value      int64
	timestamp  time.Time
	dimensions map[string]string
}

// recordingStatisticsRecorder captures every statistics delta for assertions.
type recordingStatisticsRecorder struct {
	mu      sync.Mutex
	records []recordedStatistic
}

// Record captures one statistics delta.
func (r *recordingStatisticsRecorder) Record(name string, value int64, timestamp time.Time, dimensions map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, recordedStatistic{name: name, value: value, timestamp: timestamp, dimensions: dimensions})
}

// totals returns the accumulated value of every captured measurement.
func (r *recordingStatisticsRecorder) totals() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	totals := map[string]int64{}
	for _, record := range r.records {
		totals[record.name] += record.value
	}
	return totals
}

// dimensionKeys reports whether every captured measurement carried dimensions
// and returns the distinct key sets it saw.
func (r *recordingStatisticsRecorder) dimensionCounts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make([]int, 0, len(r.records))
	for _, record := range r.records {
		counts = append(counts, len(record.dimensions))
	}
	return counts
}

// TestRepositoryRecordsMetadataPersistenceStatistics pins the statistics
// contract: every committed metadata batch records exactly one batch and one
// operation per operation it persisted.
func TestRepositoryRecordsMetadataPersistenceStatistics(t *testing.T) {
	ctx := context.Background()

	t.Run("committed batches", func(t *testing.T) {
		recorder := &recordingStatisticsRecorder{}
		repo := newBackupTestRepository(t, t.TempDir(), &BadgerRepositoryOptions{StatisticsRecorder: recorder})

		if err := repo.AddOrUpdate(ctx, createTestMetadata("stats-single", core.FileStatusPending)); err != nil {
			t.Fatalf("AddOrUpdate() error = %v", err)
		}
		batch := []*core.FileMetadata{
			createTestMetadata("stats-batch-1", core.FileStatusPending),
			createTestMetadata("stats-batch-2", core.FileStatusCompleted),
			createTestMetadata("stats-batch-3", core.FileStatusPending),
		}
		if err := repo.AddOrUpdateBatch(ctx, batch); err != nil {
			t.Fatalf("AddOrUpdateBatch() error = %v", err)
		}
		if err := repo.DeleteBatch(ctx, "test-tenant", []string{"stats-batch-1", "stats-batch-2"}); err != nil {
			t.Fatalf("DeleteBatch() error = %v", err)
		}
		if err := repo.Delete(ctx, "test-tenant", "stats-single"); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := repo.UpdateStatus(ctx, "test-tenant", "stats-batch-3", core.FileStatusCompleted); err != nil {
			t.Fatalf("UpdateStatus() error = %v", err)
		}

		totals := recorder.totals()
		const wantBatches = 5
		if got := totals[core.StatisticMetadataPersistedBatchCount]; got != wantBatches {
			t.Fatalf("%s = %d, want %d", core.StatisticMetadataPersistedBatchCount, got, wantBatches)
		}
		// 1 + 3 + 2 + 1 + 1 operations.
		const wantOperations = 8
		if got := totals[core.StatisticMetadataPersistedOperationCount]; got != wantOperations {
			t.Fatalf("%s = %d, want %d", core.StatisticMetadataPersistedOperationCount, got, wantOperations)
		}
		for _, count := range recorder.dimensionCounts() {
			if count != 0 {
				t.Fatalf("a metadata statistic carried %d dimensions, want none", count)
			}
		}
	})

	t.Run("rejected and empty calls record nothing", func(t *testing.T) {
		recorder := &recordingStatisticsRecorder{}
		repo := newBackupTestRepository(t, t.TempDir(), &BadgerRepositoryOptions{StatisticsRecorder: recorder})

		if err := repo.AddOrUpdateBatch(ctx, nil); err != nil {
			t.Fatalf("AddOrUpdateBatch(nil) error = %v", err)
		}
		if err := repo.DeleteBatch(ctx, "test-tenant", nil); err != nil {
			t.Fatalf("DeleteBatch(nil) error = %v", err)
		}
		if err := repo.AddOrUpdate(ctx, nil); !errors.Is(err, core.ErrInvalidArgument) {
			t.Fatalf("AddOrUpdate(nil) error = %v, want %v", err, core.ErrInvalidArgument)
		}
		if err := repo.Delete(ctx, "test-tenant", ""); !errors.Is(err, core.ErrInvalidArgument) {
			t.Fatalf("Delete(empty key) error = %v, want %v", err, core.ErrInvalidArgument)
		}

		if totals := recorder.totals(); len(totals) != 0 {
			t.Fatalf("statistics recorded for calls that persisted nothing: %v", totals)
		}
	})

	t.Run("nil recorder is safe", func(t *testing.T) {
		repo := newBackupTestRepository(t, t.TempDir(), nil)
		if err := repo.AddOrUpdate(ctx, createTestMetadata("stats-nil", core.FileStatusPending)); err != nil {
			t.Fatalf("AddOrUpdate() with a nil recorder error = %v", err)
		}
	})
}

// TestRepositoryRecordsStatisticsOncePerCommittedBatch keeps a conflict retry
// from double-counting: the batch is recorded once per commit, not per attempt.
func TestRepositoryRecordsStatisticsOncePerCommittedBatch(t *testing.T) {
	recorder := &recordingStatisticsRecorder{}
	repo := newBackupTestRepository(t, t.TempDir(), &BadgerRepositoryOptions{StatisticsRecorder: recorder})

	// A conflicting writer forces the retry path, which must still record
	// exactly one batch for the commit that succeeds.
	conflicting := make(chan struct{})
	done := make(chan error, 1)
	record := createTestMetadata("stats-conflict", core.FileStatusPending)
	record.RetryCount = 1
	go func() {
		close(conflicting)
		done <- repo.AddOrUpdate(context.Background(), record)
	}()
	<-conflicting
	if err := repo.AddOrUpdate(context.Background(), record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("concurrent AddOrUpdate() error = %v", err)
	}

	totals := recorder.totals()
	if got := totals[core.StatisticMetadataPersistedBatchCount]; got != 2 {
		t.Fatalf("%s = %d, want 2 (one per committed transaction)", core.StatisticMetadataPersistedBatchCount, got)
	}
	if got := totals[core.StatisticMetadataPersistedOperationCount]; got != 2 {
		t.Fatalf("%s = %d, want 2", core.StatisticMetadataPersistedOperationCount, got)
	}
}

// corruptRepositoryDatabase leaves a database that BadgerDB cannot open at the
// repository's database path, so the next open must quarantine it.
//
// Only the directory's contents are removed: the directory itself, and any
// rescue copy a previous quarantine left beside it, must survive or the test
// could not observe the quarantine.
func corruptRepositoryDatabase(t *testing.T, dataPath string) string {
	t.Helper()

	dbPath := backupTenantDatabasePath(t, dataPath, backupTestTenant)
	entries, err := os.ReadDir(dbPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadDir(%s) error = %v", dbPath, err)
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dbPath, entry.Name())); err != nil {
			t.Fatalf("RemoveAll(%s) error = %v", entry.Name(), err)
		}
	}
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", dbPath, err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "MANIFEST"), []byte("this is not a badger manifest\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(MANIFEST) error = %v", err)
	}
	return dbPath
}

// TestAutoRestoreFromBackupRecoversQuarantinedDatabase covers the automatic
// recovery path: a database that had to be quarantined comes back with the
// records from the newest backup instead of starting empty.
func TestAutoRestoreFromBackupRecoversQuarantinedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID: backupTestTenant,
		DataPath: dataPath,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "auto-restored-file", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "auto-restored-done", core.FileStatusCompleted)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	backupPath := filepath.Join(backupDirectory, "metadata.20240101T000000Z.bak")
	file, err := os.Create(backupPath)
	if err != nil {
		t.Fatalf("Create(%s) error = %v", backupPath, err)
	}
	if _, err := BackupRepository(ctx, repo, file); err != nil {
		_ = file.Close()
		t.Fatalf("Backup() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbPath := corruptRepositoryDatabase(t, dataPath)

	restored, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                   backupTestTenant,
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		BackupDirectory:            backupDirectory,
		AutoRestoreFromBackup:      true,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository(AutoRestoreFromBackup) error = %v", err)
	}
	if restored == nil {
		t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
	}
	if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
	}

	pending, err := restored.Get(ctx, backupTestTenant, "auto-restored-file")
	if err != nil {
		t.Fatalf("Get(auto-restored-file) after the automatic restore error = %v", err)
	}
	if pending.Status != core.FileStatusPending {
		t.Fatalf("restored status = %s, want Pending", pending.Status)
	}
	completed, err := restored.Get(ctx, backupTestTenant, "auto-restored-done")
	if err != nil {
		t.Fatalf("Get(auto-restored-done) after the automatic restore error = %v", err)
	}
	if completed.Status != core.FileStatusCompleted {
		t.Fatalf("restored status = %s, want Completed", completed.Status)
	}
	// The restored database must be a working repository, not a read-only copy.
	if err := restored.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "after-restore", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() after the automatic restore error = %v", err)
	}
}

// TestAutoRestoreFromBackupSkipsUnreadableBackups keeps a corrupt backup stream
// from blocking startup: the repository opens empty and degraded instead.
func TestAutoRestoreFromBackupSkipsUnreadableBackups(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	// The newest backup is unreadable garbage.
	corruptBackup := filepath.Join(backupDirectory, "metadata.20240102T000000Z.bak")
	if err := os.WriteFile(corruptBackup, []byte("this is not a badger backup stream"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	dbPath := corruptRepositoryDatabase(t, dataPath)

	repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                   backupTestTenant,
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		BackupDirectory:            backupDirectory,
		AutoRestoreFromBackup:      true,
	})
	if err != nil {
		t.Fatalf("startup failed because of a corrupt backup: %v", err)
	}
	if repo == nil {
		t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
	}
	if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
	}
	if _, err := repo.Get(ctx, backupTestTenant, "never-existed"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() on the degraded repository error = %v, want %v", err, core.ErrFileNotFound)
	}
	// The degraded repository must still be usable.
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "degraded", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() on the degraded repository error = %v", err)
	}

	// The unusable backup is left in place for an operator.
	if _, err := os.Lstat(corruptBackup); err != nil {
		t.Fatalf("the corrupt backup was removed: %v", err)
	}
}

// TestAutoRestoreFromBackupUsesTheNewestReadableBackup covers the fallback: an
// unusable newest backup must not stop an older valid backup from being used.
func TestAutoRestoreFromBackupUsesTheNewestReadableBackup(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID: backupTestTenant,
		DataPath: dataPath,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "older-backup-file", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	validBackup := filepath.Join(backupDirectory, "metadata.20240101T000000Z.bak")
	file, err := os.Create(validBackup)
	if err != nil {
		t.Fatalf("Create(%s) error = %v", validBackup, err)
	}
	if _, err := BackupRepository(ctx, repo, file); err != nil {
		_ = file.Close()
		t.Fatalf("Backup() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A newer, unusable backup file exists.
	corruptBackup := filepath.Join(backupDirectory, "metadata.20240105T000000Z.bak")
	if err := os.WriteFile(corruptBackup, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	newest := time.Now()
	if err := os.Chtimes(corruptBackup, newest, newest); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}
	if err := os.Chtimes(validBackup, newest.Add(-time.Hour), newest.Add(-time.Hour)); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	_ = corruptRepositoryDatabase(t, dataPath)

	restored, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                   backupTestTenant,
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		BackupDirectory:            backupDirectory,
		AutoRestoreFromBackup:      true,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if restored == nil {
		t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
	}
	if _, err := restored.Get(ctx, backupTestTenant, "older-backup-file"); err != nil {
		t.Fatalf("Get(older-backup-file) error = %v, want the older valid backup to be restored", err)
	}
}

// TestAutoRestoreFromBackupWithoutAnyBackupContinuesEmpty keeps startup alive
// when there is nothing to restore from.
func TestAutoRestoreFromBackupWithoutAnyBackupContinuesEmpty(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	dbPath := corruptRepositoryDatabase(t, dataPath)

	repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                   backupTestTenant,
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		BackupDirectory:            backupDirectory,
		AutoRestoreFromBackup:      true,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() without any backup error = %v", err)
	}
	if repo == nil {
		t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
	}
	if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one", quarantined)
	}
	if _, err := repo.Get(ctx, backupTestTenant, "anything"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() error = %v, want %v", err, core.ErrFileNotFound)
	}
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "empty-start", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
}

// TestAutoRestoreDoesNotRunWithoutQuarantine keeps the repair aid from touching
// a healthy database.
func TestAutoRestoreDoesNotRunWithoutQuarantine(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	seeded, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: dataPath})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if err := seeded.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "healthy-file", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	backupPath := filepath.Join(backupDirectory, "metadata.20240101T000000Z.bak")
	file, err := os.Create(backupPath)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := BackupRepository(ctx, seeded, file); err != nil {
		_ = file.Close()
		t.Fatalf("Backup() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// The backup only holds healthy-file; anything else proves a restore ran.
	backupInfo, err := LatestBackup(backupDirectory)
	if err != nil {
		t.Fatalf("LatestBackup() error = %v", err)
	}
	if backupInfo.BackupCount != 1 {
		t.Fatalf("LatestBackup().BackupCount = %d, want 1", backupInfo.BackupCount)
	}

	reopened, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:              backupTestTenant,
		DataPath:              dataPath,
		BackupDirectory:       backupDirectory,
		AutoRestoreFromBackup: true,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if _, err := reopened.Get(ctx, backupTestTenant, "healthy-file"); err != nil {
		t.Fatalf("Get(healthy-file) error = %v", err)
	}
	if quarantined := quarantinedDirectories(t, backupTenantDatabasePath(t, dataPath, backupTestTenant)); len(quarantined) != 0 {
		t.Fatalf("a healthy database was quarantined: %v", quarantined)
	}
}

// TestOpenBadgerOptionsAreSharedWithRestore pins the invariant that an offline
// restore opens the database with the same options the repository will use, so
// a restored database stays readable after the restore.
func TestOpenBadgerOptionsAreSharedWithRestore(t *testing.T) {
	opts := &BadgerRepositoryOptions{
		TenantID:         backupTestTenant,
		DataPath:         t.TempDir(),
		MemTableSize:     1 << 20,
		ValueLogFileSize: 1 << 20,
		BlockCacheSize:   1 << 20,
		SyncWrites:       true,
	}

	dbPath, dbOpts, err := newBadgerOptions(opts)
	if err != nil {
		t.Fatalf("newBadgerOptions() error = %v", err)
	}
	if want := backupTenantDatabasePath(t, opts.DataPath, backupTestTenant); dbPath != want {
		t.Fatalf("newBadgerOptions() path = %q, want %q", dbPath, want)
	}
	if dbOpts.Dir != dbPath {
		t.Fatalf("badger options Dir = %q, want %q", dbOpts.Dir, dbPath)
	}
	if dbOpts.MemTableSize != opts.MemTableSize {
		t.Fatalf("MemTableSize = %d, want %d", dbOpts.MemTableSize, opts.MemTableSize)
	}
	if dbOpts.SyncWrites != opts.SyncWrites {
		t.Fatalf("SyncWrites = %v, want %v", dbOpts.SyncWrites, opts.SyncWrites)
	}
	if dbOpts.Logger != nil {
		t.Fatal("badger options Logger must stay nil")
	}

	if _, _, err := newBadgerOptions(nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("newBadgerOptions(nil) error = %v, want %v", err, core.ErrInvalidArgument)
	}
}

// TestAutoRestoreKeepsTheQuarantineAndCleansRestoreSiblings keeps the repair aid
// from leaving a growing pile of staging directories behind.
func TestAutoRestoreKeepsTheQuarantineAndCleansRestoreSiblings(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: dataPath})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "sibling-file", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	backupPath := filepath.Join(backupDirectory, "metadata.20240101T000000Z.bak")
	file, err := os.Create(backupPath)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := BackupRepository(ctx, repo, file); err != nil {
		_ = file.Close()
		t.Fatalf("Backup() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	dbPath := corruptRepositoryDatabase(t, dataPath)
	if _, err := openMetadataRepository(t, &BadgerRepositoryOptions{
		TenantID:                   backupTestTenant,
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		BackupDirectory:            backupDirectory,
		AutoRestoreFromBackup:      true,
	}); err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}

	siblings, err := filepath.Glob(dbPath + ".restore.*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(siblings) != 0 {
		t.Fatalf("restore staging directories were left behind: %v", siblings)
	}
	if quarantined := quarantinedDirectories(t, dbPath); len(quarantined) != 1 {
		t.Fatalf("quarantined directories = %v, want exactly one rescue copy", quarantined)
	}
}

// TestAutoRestoreWarnsWithoutLeakingPaths covers the operator-visible diagnostic
// of automatic recovery: an unusable backup is reported as one structured
// warning, and a successful restore is silent.
func TestAutoRestoreWarnsWithoutLeakingPaths(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name        string
		backupBytes []byte
		wantWarn    bool
	}{
		{name: "corrupt backup is reported", backupBytes: []byte("this is not a badger backup stream"), wantWarn: true},
		{name: "successful restore is silent", wantWarn: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			backupDirectory := t.TempDir()

			seeded, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: dataPath})
			if err != nil {
				t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
			}
			if err := seeded.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "warned-file", core.FileStatusPending)); err != nil {
				t.Fatalf("AddOrUpdate() error = %v", err)
			}
			backupPath := filepath.Join(backupDirectory, "metadata.20240101T000000Z.bak")
			file, err := os.Create(backupPath)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if _, err := BackupRepository(ctx, seeded, file); err != nil {
				_ = file.Close()
				t.Fatalf("BackupRepository() error = %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			if err := seeded.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			if tc.backupBytes != nil {
				// Replace the unusable backup instead of overwriting it: Windows
				// can briefly keep a handle on a just truncated file, which would
				// make the quarantine rename below fail for an unrelated reason.
				if err := os.Remove(backupPath); err != nil {
					t.Fatalf("Remove() error = %v", err)
				}
				if err := os.WriteFile(backupPath, tc.backupBytes, 0o644); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
			}

			_ = corruptRepositoryDatabase(t, dataPath)

			handler := &recordingLogHandler{}
			runtime, err := logging.New(logging.Config{Handler: handler})
			if err != nil {
				t.Fatalf("logging.New() error = %v", err)
			}

			repo, err := openMetadataRepository(t, &BadgerRepositoryOptions{
				TenantID:                   backupTestTenant,
				DataPath:                   dataPath,
				RecoverCorruptedDatabase:   true,
				CorruptedDatabaseRetention: time.Hour,
				BackupDirectory:            backupDirectory,
				AutoRestoreFromBackup:      true,
				Logging:                    runtime,
			})
			if err != nil {
				t.Fatalf("startup failed: %v", err)
			}
			if repo == nil {
				t.Fatal("NewBadgerMetadataRepository() returned a nil repository without an error")
			}

			warning := handler.warning()
			if !tc.wantWarn {
				if warning != nil {
					t.Fatalf("automatic recovery warned on a successful restore: %+v", warning)
				}
				if _, err := repo.Get(ctx, backupTestTenant, "warned-file"); err != nil {
					t.Fatalf("Get(warned-file) error = %v", err)
				}
				return
			}

			if warning == nil {
				t.Fatal("automatic recovery did not warn about the unusable backup")
			}
			// One warning describes the failed backup; a second may report that
			// no readable backup was available at all. Both must stay structured
			// and path-free, and neither may be a raw error dump.
			for _, record := range handler.snapshot() {
				if record.Level != slog.LevelWarn {
					t.Fatalf("automatic recovery emitted a %v record: %+v", record.Level, record)
				}
				if record.Message == "" {
					t.Error("a warning has no message")
				}
				if strings.Contains(record.Message, backupDirectory) || strings.Contains(record.Message, dataPath) {
					t.Errorf("warning message %q leaks a physical path", record.Message)
				}
				if attributes := handler.attributes(record); attributes["reason"] == "" || attributes["error_kind"] == "" {
					t.Errorf("warning attributes = %v, want a stable reason and error kind", attributes)
				}
			}
		})
	}
}

// TestBackupDirectoryInventoryIsSortedByNewest keeps the fallback order stable.
func TestBackupDirectoryInventoryIsSortedByNewest(t *testing.T) {
	directory := t.TempDir()
	names := []string{
		"metadata.20240101T000000Z.bak",
		"metadata.20240103T000000Z.bak",
		"metadata.20240102T000000Z.bak",
	}
	base := time.Now()
	for index, name := range names {
		full := filepath.Join(directory, name)
		if err := os.WriteFile(full, []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		stamp := base.Add(time.Duration(index) * time.Hour)
		if err := os.Chtimes(full, stamp, stamp); err != nil {
			t.Fatalf("Chtimes() error = %v", err)
		}
	}

	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		t.Fatalf("listBackupsNewestFirst() error = %v", err)
	}
	got := make([]string, 0, len(backups))
	for _, backup := range backups {
		got = append(got, filepath.Base(backup.path))
	}
	want := []string{names[2], names[1], names[0]}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("listBackupsNewestFirst() = %v, want %v", got, want)
	}

	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	if len(backups) != len(sorted) {
		t.Fatalf("listBackupsNewestFirst() returned %d backups, want %d", len(backups), len(sorted))
	}
}

// TestRestoreDatabaseHonorsDeletionsAndUpdates keeps a restored snapshot
// consistent with the instant it was taken: a later full backup carries the
// engine's deletion markers and rewritten values, so restoring it must not
// resurrect a deleted record or return a stale one.
func TestRestoreDatabaseHonorsDeletionsAndUpdates(t *testing.T) {
	ctx := context.Background()
	source := newBackupTestRepository(t, t.TempDir(), nil)
	if err := source.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "kept-record", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate(kept-record) error = %v", err)
	}
	if err := source.AddOrUpdate(ctx, tenantRecord(backupTestTenant, "deleted-record", core.FileStatusPending)); err != nil {
		t.Fatalf("AddOrUpdate(deleted-record) error = %v", err)
	}

	var first bytes.Buffer
	if _, err := BackupRepository(ctx, source, &first); err != nil {
		t.Fatalf("first BackupRepository() error = %v", err)
	}

	firstTarget := t.TempDir()
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: firstTarget}, bytes.NewReader(first.Bytes())); err != nil {
		t.Fatalf("RestoreDatabase(first) error = %v", err)
	}
	firstRestore := newBackupTestRepository(t, firstTarget, nil)
	for _, fileKey := range []string{"kept-record", "deleted-record"} {
		if _, err := firstRestore.Get(ctx, backupTestTenant, fileKey); err != nil {
			t.Fatalf("Get(%s) from the first snapshot error = %v", fileKey, err)
		}
	}

	// Delete one record and rewrite the other, then take a second snapshot.
	if err := source.Delete(ctx, backupTestTenant, "deleted-record"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := source.UpdateStatus(ctx, backupTestTenant, "kept-record", core.FileStatusCompleted); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	var second bytes.Buffer
	if _, err := BackupRepository(ctx, source, &second); err != nil {
		t.Fatalf("second BackupRepository() error = %v", err)
	}

	secondTarget := t.TempDir()
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: secondTarget}, bytes.NewReader(second.Bytes())); err != nil {
		t.Fatalf("RestoreDatabase(second) error = %v", err)
	}
	secondRestore := newBackupTestRepository(t, secondTarget, nil)
	if _, err := secondRestore.Get(ctx, backupTestTenant, "deleted-record"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get(deleted-record) from the second snapshot error = %v, want %v", err, core.ErrFileNotFound)
	}
	kept, err := secondRestore.Get(ctx, backupTestTenant, "kept-record")
	if err != nil {
		t.Fatalf("Get(kept-record) from the second snapshot error = %v", err)
	}
	if kept.Status != core.FileStatusCompleted {
		t.Fatalf("Get(kept-record) status = %s, want the updated status Completed", kept.Status)
	}
	if _, err := secondRestore.GetByStatus(ctx, backupTestTenant, core.FileStatusCompleted, 0); err != nil {
		t.Fatalf("GetByStatus(Completed) on the second snapshot error = %v", err)
	}
}
