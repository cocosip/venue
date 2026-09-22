package metadata

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/sqlite"
)

// sqliteTenantDatabasePath is the database file a tenant owns below dataPath.
func sqliteTenantDatabasePath(dataPath string, tenantID string) string {
	return filepath.Join(dataPath, tenantID, metadataDatabaseFileName)
}

// corruptSQLiteDatabase replaces the tenant's database file with bytes that are
// not a SQLite database, which is exactly the SQLITE_NOTADB corruption verdict.
//
// The file is replaced rather than rewritten: replacing guarantees the reader
// sees new content on every platform, while truncating a file that a previous
// reader still had mapped can leave the old pages visible.
func corruptSQLiteDatabase(t *testing.T, dataPath string, tenantID string) string {
	t.Helper()

	path := sqliteTenantDatabasePath(dataPath, tenantID)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stat(%s) error = %v", path, err)
	}
	garbage := bytes.Repeat([]byte("this is not a sqlite database\n"), 64)
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(%s) error = %v", path, err)
	}
	if err := os.WriteFile(path, garbage, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	return path
}

// quarantinedSQLiteDatabases lists the quarantine siblings of a tenant database.
func quarantinedSQLiteDatabases(t *testing.T, dataPath string, tenantID string) []string {
	t.Helper()

	pattern := sqliteTenantDatabasePath(dataPath, tenantID) + corruptedDatabaseSuffix + "*"
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("Glob(%s) error = %v", pattern, err)
	}
	return matches
}

// seedSQLiteTenant writes one pending record and returns the repository that
// owns it.
func seedSQLiteTenant(t *testing.T, options *SQLiteRepositoryOptions, tenantID string, fileKeys ...string) *SQLiteMetadataRepository {
	t.Helper()

	repo := newSQLiteFixtureRepository(t, func(target *SQLiteRepositoryOptions) {
		*target = *options
	})
	ctx := context.Background()
	for _, fileKey := range fileKeys {
		if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, fileKey, time.Now().UTC())); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", fileKey, err)
		}
	}
	return repo
}

// writeSQLiteBackupFile produces one good tenant backup at
// {directory}/{tenantID}/metadata.<stamp>.bak.
func writeSQLiteBackupFile(t *testing.T, directory string, tenantID string, source *SQLiteMetadataRepository, stamp string) string {
	t.Helper()

	tenantDirectory := filepath.Join(directory, tenantID)
	if err := os.MkdirAll(tenantDirectory, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", tenantDirectory, err)
	}
	target := filepath.Join(tenantDirectory, backupFilePrefix+stamp+backupFileSuffix)
	if err := source.BackupTenant(context.Background(), tenantID, target); err != nil {
		t.Fatalf("BackupTenant() error = %v", err)
	}
	return target
}

// ---------------------------------------------------------------------------
// Corruption quarantine
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryQuarantinesCorruptedDatabaseAndRecreatesIt(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-quarantine"

	// A repository opens no database until the tenant is touched, so the record
	// write is what creates the file the next open has to quarantine.
	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-lost", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	corruptSQLiteDatabase(t, dataPath, tenantID)

	var (
		quarantined []string
		incomplete  []string
	)
	recovered := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = true
		options.CorruptedDatabaseRetention = time.Hour
		options.OnCorruptedDatabase = func(path string) { quarantined = append(quarantined, path) }
		options.OnRecoveryIncomplete = func(path string) { incomplete = append(incomplete, path) }
	})

	// Nothing happened yet: the handle, and therefore the quarantine, is created
	// on the tenant's first access.
	if len(quarantined) != 0 {
		t.Fatalf("OnCorruptedDatabase calls = %d before the first access, want 0", len(quarantined))
	}
	if _, err := recovered.Get(ctx, tenantID, "file-lost"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() on the recovered store error = %v, want core.ErrFileNotFound", err)
	}

	if len(quarantined) != 1 {
		t.Fatalf("OnCorruptedDatabase calls = %d, want 1", len(quarantined))
	}
	if !strings.HasPrefix(quarantined[0], sqliteTenantDatabasePath(dataPath, tenantID)+corruptedDatabaseSuffix) {
		t.Fatalf("quarantine path = %q, want the database file plus the quarantine suffix", quarantined[0])
	}
	if len(incomplete) != 1 || incomplete[0] != quarantined[0] {
		t.Fatalf("OnRecoveryIncomplete calls = %v, want one call with the quarantine path", incomplete)
	}
	if files := quarantinedSQLiteDatabases(t, dataPath, tenantID); len(files) != 1 {
		t.Fatalf("quarantine files = %v, want exactly one rescue copy", files)
	}

	// The quarantined copy keeps the damaged bytes; the recreated database is a
	// new, empty, usable store.
	rescue, err := os.ReadFile(quarantined[0])
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", quarantined[0], err)
	}
	if !bytes.Contains(rescue, []byte("not a sqlite database")) {
		t.Fatalf("the quarantined copy does not hold the damaged bytes")
	}
	if err := recovered.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-fresh", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() on the recovered store error = %v", err)
	}
}

func TestSQLiteMetadataRepositoryLeavesCorruptionAloneWithoutRecovery(t *testing.T) {
	dataPath := t.TempDir()
	const tenantID = "tenant-no-recovery"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(context.Background(), sqliteRecord(tenantID, "file-x", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	path := corruptSQLiteDatabase(t, dataPath, tenantID)

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = false
	})

	// The first tenant access must fail rather than silently serving an empty
	// database; nothing may be quarantined, because recovery was not requested.
	if _, err := repo.Get(context.Background(), tenantID, "file-x"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Get() on a corrupted database error = %v, want core.ErrDatabaseError", err)
	}
	if files := quarantinedSQLiteDatabases(t, dataPath, tenantID); len(files) != 0 {
		t.Fatalf("quarantine files = %v, want none when recovery is disabled", files)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the corrupted database was moved without recovery enabled: %v", err)
	}
}

func TestSQLiteMetadataRepositoryDoesNotQuarantinePermissionFailures(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-permission"

	options := &SQLiteRepositoryOptions{
		DataPath:                   dataPath,
		Sqlite:                     sqlite.DefaultOptions(),
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
	}
	seeded := seedSQLiteTenant(t, options, tenantID, "file-permission")
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A directory where the database file belongs is a path defect, not
	// corruption: the quarantine path must stay untouched.
	databasePath := sqliteTenantDatabasePath(dataPath, tenantID)
	if err := os.Remove(databasePath); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if err := os.Mkdir(databasePath, 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(target *SQLiteRepositoryOptions) {
		*target = *options
	})
	if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-permission-2", time.Now().UTC())); err == nil {
		t.Fatal("AddOrUpdate() on an unusable database path succeeded, want a failure")
	}
	if files := quarantinedSQLiteDatabases(t, dataPath, tenantID); len(files) != 0 {
		t.Fatalf("quarantine files = %v, want none for a path failure", files)
	}
	if info, err := os.Stat(databasePath); err != nil || !info.IsDir() {
		t.Fatalf("the unusable database path was moved: info=%v err=%v", info, err)
	}
}

func TestSQLiteMetadataRepositoryPrunesQuarantinedFilesByRetention(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-retention"

	// Two quarantine siblings: one beyond the retention, one inside it.
	directory := filepath.Join(dataPath, tenantID)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	oldFile := filepath.Join(directory, metadataDatabaseFileName+corruptedDatabaseSuffix+"20200101T000000Z")
	freshFile := filepath.Join(directory, metadataDatabaseFileName+corruptedDatabaseSuffix+"20250101T000000Z")
	for _, path := range []string{oldFile, freshFile} {
		if err := os.WriteFile(path, []byte("rescue copy"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.CorruptedDatabaseRetention = 24 * time.Hour
	})
	if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-retention", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	if _, err := os.Stat(oldFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired quarantine file still exists: %v", err)
	}
	if _, err := os.Stat(freshFile); err != nil {
		t.Fatalf("unexpired quarantine file was removed: %v", err)
	}
}

func TestSQLiteMetadataRepositoryNegativeRetentionKeepsQuarantineFiles(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-negative-retention"

	directory := filepath.Join(dataPath, tenantID)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	oldFile := filepath.Join(directory, metadataDatabaseFileName+corruptedDatabaseSuffix+"20200101T000000Z")
	if err := os.WriteFile(oldFile, []byte("rescue copy"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		// A negative retention has to survive constructor validation, so it is
		// applied to the opened repository directly.
	})
	repo.corruptRetention = -1

	if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-negative", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if _, err := os.Stat(oldFile); err != nil {
		t.Fatalf("a negative retention pruned a quarantine file: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Automatic restore
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryAutoRestoresFromBackup(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()
	const tenantID = "tenant-auto-restore"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-restored", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	writeSQLiteBackupFile(t, backupDirectory, tenantID, seeded, "20240101T000000Z")
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	corruptSQLiteDatabase(t, dataPath, tenantID)

	var (
		quarantined []string
		incomplete  []string
	)
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = true
		options.CorruptedDatabaseRetention = time.Hour
		options.BackupDirectory = backupDirectory
		options.AutoRestoreFromBackup = true
		options.OnCorruptedDatabase = func(path string) { quarantined = append(quarantined, path) }
		options.OnRecoveryIncomplete = func(path string) { incomplete = append(incomplete, path) }
	})

	restored, err := repo.Get(ctx, tenantID, "file-restored")
	if err != nil {
		t.Fatalf("Get(file-restored) error = %v, want the backup to be restored", err)
	}
	if len(quarantined) != 1 {
		t.Fatalf("OnCorruptedDatabase calls = %d, want 1", len(quarantined))
	}
	if len(incomplete) != 0 {
		t.Fatalf("OnRecoveryIncomplete calls = %v, want none after a successful restore", incomplete)
	}
	if restored.Status != core.FileStatusPending {
		t.Fatalf("restored status = %s, want Pending", restored.Status)
	}
	// The restored database must be writable, not a read-only artifact.
	if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-after-restore", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() after the restore error = %v", err)
	}
}

func TestSQLiteMetadataRepositoryAutoRestoreSkipsUnreadableBackups(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()
	const tenantID = "tenant-bad-backup"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-good", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	goodBackup := writeSQLiteBackupFile(t, backupDirectory, tenantID, seeded, "20240101T000000Z")
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A newer, unusable backup must be skipped in favor of the older valid one.
	corruptBackup := filepath.Join(backupDirectory, tenantID, backupFilePrefix+"20240105T000000Z"+backupFileSuffix)
	if err := os.WriteFile(corruptBackup, bytes.Repeat([]byte("this is not a sqlite backup\n"), 4096), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	newest := time.Now()
	if err := os.Chtimes(corruptBackup, newest, newest); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}
	older := newest.Add(-time.Hour)
	if err := os.Chtimes(goodBackup, older, older); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	corruptSQLiteDatabase(t, dataPath, tenantID)

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = true
		options.CorruptedDatabaseRetention = time.Hour
		options.BackupDirectory = backupDirectory
		options.AutoRestoreFromBackup = true
	})

	if _, err := repo.Get(ctx, tenantID, "file-good"); err != nil {
		t.Fatalf("Get(file-good) error = %v, want the older readable backup to be restored", err)
	}
	if files := quarantinedSQLiteDatabases(t, dataPath, tenantID); len(files) != 1 {
		t.Fatalf("quarantine files = %v, want exactly one rescue copy", files)
	}
	// The unusable backup is left in place for an operator.
	if _, err := os.Stat(corruptBackup); err != nil {
		t.Fatalf("the unusable backup was removed: %v", err)
	}
}

func TestSQLiteMetadataRepositoryAutoRestoreWithoutBackupContinuesEmpty(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()
	const tenantID = "tenant-no-backup"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-gone", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	corruptSQLiteDatabase(t, dataPath, tenantID)

	var incomplete []string
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = true
		options.CorruptedDatabaseRetention = time.Hour
		options.BackupDirectory = backupDirectory
		options.AutoRestoreFromBackup = true
		options.OnRecoveryIncomplete = func(path string) { incomplete = append(incomplete, path) }
	})

	if len(incomplete) != 0 {
		t.Fatalf("OnRecoveryIncomplete calls = %v before the first access, want none", incomplete)
	}
	// The quarantine and the failed restore happen on the tenant's first access.
	if _, err := repo.Get(ctx, tenantID, "file-gone"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() error = %v, want an empty degraded store", err)
	}
	if len(incomplete) != 1 {
		t.Fatalf("OnRecoveryIncomplete calls = %d, want 1 when no backup could be restored", len(incomplete))
	}
	if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-degraded", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() on the degraded store error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// Backup capability
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryTenantBackupRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, nil)
	const tenantID = "tenant-backup"

	records := []*core.FileMetadata{
		sqliteRecord(tenantID, "file-a", time.Now().UTC()),
		sqliteRecord(tenantID, "file-b", time.Now().UTC()),
	}
	records[0].Status = core.FileStatusCompleted
	records[1].RetryCount = 4
	sqliteStoreRecords(t, repo, tenantID, records)

	target := filepath.Join(t.TempDir(), "tenant-backup.db")
	if err := repo.BackupTenant(ctx, tenantID, target); err != nil {
		t.Fatalf("BackupTenant() error = %v", err)
	}

	// The artifact is a complete, openable SQLite database.
	recovered := restoreBackupIntoRepository(t, target)
	for _, record := range records {
		stored, err := recovered.Get(ctx, tenantID, record.FileKey)
		if err != nil {
			t.Fatalf("Get(%q) from the restored backup error = %v", record.FileKey, err)
		}
		if stored.Status != record.Status || stored.RetryCount != record.RetryCount {
			t.Fatalf("restored record %q = %+v, want %+v", record.FileKey, stored, record)
		}
	}

	// A second backup onto the same path is refused: VACUUM INTO never
	// overwrites an existing file.
	if err := repo.BackupTenant(ctx, tenantID, target); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("BackupTenant() onto an existing target error = %v, want core.ErrInvalidArgument", err)
	}
	if err := repo.BackupTenant(ctx, "", filepath.Join(t.TempDir(), "x.db")); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("BackupTenant(empty tenant) error = %v, want core.ErrInvalidArgument", err)
	}
	if err := repo.BackupTenant(ctx, "tenant", ""); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("BackupTenant(empty target) error = %v, want core.ErrInvalidArgument", err)
	}
}

// restoreBackupIntoRepository copies one backup artifact into a fresh data
// directory as if an operator had restored it offline, and returns a repository
// that serves it.
func restoreBackupIntoRepository(t *testing.T, backupPath string) *SQLiteMetadataRepository {
	t.Helper()

	dataPath := t.TempDir()
	tenantID := "tenant-backup"
	directory := filepath.Join(dataPath, tenantID)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	source, err := os.Open(backupPath)
	if err != nil {
		t.Fatalf("Open(%s) error = %v", backupPath, err)
	}
	defer func() { _ = source.Close() }()

	target, err := os.Create(sqliteTenantDatabasePath(dataPath, tenantID))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		t.Fatalf("Copy() error = %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
}

func TestSQLiteMetadataRepositoryBackupIsAZipOfCompletePerTenantDatabases(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, nil)

	tenants := []string{"tenant-a", "tenant-b"}
	for _, tenantID := range tenants {
		sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{
			sqliteRecord(tenantID, "file-1", time.Now().UTC()),
			sqliteRecord(tenantID, "file-2", time.Now().UTC()),
		})
	}

	var stream bytes.Buffer
	since, err := repo.Backup(ctx, &stream)
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if since != 0 {
		t.Fatalf("Backup() since = %d, want 0 for the SQLite engine", since)
	}

	archive, err := zip.NewReader(bytes.NewReader(stream.Bytes()), int64(stream.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader() error = %v; the stream is not a zip container", err)
	}

	names := make([]string, 0, len(archive.File))
	entryData := make(map[string][]byte, len(archive.File))
	for _, entry := range archive.File {
		names = append(names, entry.Name)
		reader, openErr := entry.Open()
		if openErr != nil {
			t.Fatalf("Open(%s) error = %v", entry.Name, openErr)
		}
		data, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr != nil {
			t.Fatalf("ReadAll(%s) error = %v", entry.Name, readErr)
		}
		entryData[entry.Name] = data
	}

	want := []string{"tenant-a/metadata.db", "tenant-b/metadata.db"}
	if !equalStrings(names, want) {
		t.Fatalf("zip entries = %v, want %v", names, want)
	}

	// Each entry is a complete SQLite database: it opens and the records are
	// readable through the same repository the runtime uses.
	for _, tenantID := range tenants {
		data := entryData[tenantID+"/"+metadataDatabaseFileName]
		if len(data) == 0 {
			t.Fatalf("entry for %q is empty", tenantID)
		}
		dataPath := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dataPath, tenantID), 0o755); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		if err := os.WriteFile(sqliteTenantDatabasePath(dataPath, tenantID), data, 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		restored := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
			options.DataPath = dataPath
		})
		pending, getErr := restored.GetByStatus(ctx, tenantID, core.FileStatusPending, 0)
		if getErr != nil {
			t.Fatalf("GetByStatus() from the archived database error = %v", getErr)
		}
		if len(pending) != 2 {
			t.Fatalf("records in the archived database for %q = %d, want 2", tenantID, len(pending))
		}
	}
}

func TestSQLiteMetadataRepositoryBackupCoversTenantDirectoriesWithoutHandles(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})

	// A tenant directory on disk that this process never opened, plus an entry
	// that is not a valid tenant identifier and must be ignored.
	for _, tenantID := range []string{"tenant-ondisk-a", "tenant-ondisk-b"} {
		if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-1", time.Now().UTC())); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", tenantID, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dataPath, ".locus"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.locus) error = %v", err)
	}
	// Close the handles so the enumeration must read the directory: on Windows an
	// open database holds a file lock that would also break t.TempDir cleanup.
	repo.mu.Lock()
	handles := make([]*sqliteTenantDatabase, 0, len(repo.databases))
	for _, handle := range repo.databases {
		handles = append(handles, handle)
	}
	repo.databases = map[string]*sqliteTenantDatabase{}
	repo.mu.Unlock()
	for _, handle := range handles {
		if err := repo.closeHandle(handle); err != nil {
			t.Fatalf("closeHandle() error = %v", err)
		}
	}

	known, err := repo.KnownTenantIDs(ctx)
	if err != nil {
		t.Fatalf("KnownTenantIDs() error = %v", err)
	}
	if !equalStrings(known, []string{"tenant-ondisk-a", "tenant-ondisk-b"}) {
		t.Fatalf("KnownTenantIDs() = %v, want both on-disk tenants in sorted order", known)
	}

	var stream bytes.Buffer
	if _, err := repo.Backup(ctx, &stream); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	archive, err := zip.NewReader(bytes.NewReader(stream.Bytes()), int64(stream.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader() error = %v", err)
	}
	if len(archive.File) != 2 {
		t.Fatalf("zip entries = %d, want 2", len(archive.File))
	}
	if archive.File[0].Name != "tenant-ondisk-a/"+metadataDatabaseFileName {
		t.Fatalf("first entry = %q, want a stable sorted order", archive.File[0].Name)
	}
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

func TestSQLiteMetadataRepositoryBackupRejectsNilWriterAndReportsWriterFailure(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, nil)

	if _, err := repo.Backup(ctx, nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("Backup(nil) error = %v, want core.ErrInvalidArgument", err)
	}

	sqliteStoreRecords(t, repo, "tenant-writer", []*core.FileMetadata{
		sqliteRecord("tenant-writer", "file-1", time.Now().UTC()),
	})

	writeFailure := errors.New("disk full")
	_, err := repo.Backup(ctx, failingWriter{err: writeFailure})
	if !errors.Is(err, writeFailure) {
		t.Fatalf("Backup(failing writer) error = %v, want the writer's own error", err)
	}
}

func TestSQLiteMetadataRepositoryBackupSkipsVerificationWhenConfigured(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.SkipBackupVerification = true
	})
	const tenantID = "tenant-skip-verify"

	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{
		sqliteRecord(tenantID, "file-1", time.Now().UTC()),
	})

	target := filepath.Join(t.TempDir(), "unverified.db")
	if err := repo.BackupTenant(ctx, tenantID, target); err != nil {
		t.Fatalf("BackupTenant() with verification skipped error = %v", err)
	}
	if info, err := os.Stat(target); err != nil || info.Size() == 0 {
		t.Fatalf("unverified backup target = %v, err = %v", info, err)
	}
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryOptimizeCompactsOpenTenants(t *testing.T) {
	ctx := context.Background()
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.OptimizeIdleTenantDatabases = true
	})

	tenants := []string{"tenant-optimize-a", "tenant-optimize-b"}
	for _, tenantID := range tenants {
		sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{
			sqliteRecord(tenantID, "file-1", time.Now().UTC()),
			sqliteRecord(tenantID, "file-2", time.Now().UTC()),
		})
		if err := repo.Delete(ctx, tenantID, "file-2"); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
	}

	if err := repo.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}

	// Compaction must not lose or resurrect a record.
	for _, tenantID := range tenants {
		if _, err := repo.Get(ctx, tenantID, "file-1"); err != nil {
			t.Fatalf("Get(file-1) after Optimize error = %v", err)
		}
		if _, err := repo.Get(ctx, tenantID, "file-2"); !errors.Is(err, core.ErrFileNotFound) {
			t.Fatalf("Get(file-2) after Optimize error = %v, want core.ErrFileNotFound", err)
		}
	}
}

func TestSQLiteMetadataRepositoryRejectsNewerSchemaVersion(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-schema"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-schema", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	databasePath := seeded.databases[tenantID].path
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err := sqlite.Open(databasePath, sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"UPDATE schema_meta SET value = '99' WHERE key = 'schema_version'"); err != nil {
		_ = db.Close()
		t.Fatalf("UPDATE schema_meta error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if _, err := repo.Get(ctx, tenantID, "file-schema"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Get() on a newer schema error = %v, want core.ErrDatabaseError", err)
	}
}

// ---------------------------------------------------------------------------
// Logging hygiene
// ---------------------------------------------------------------------------

// recordingLogHandler captures every emitted record so a test can assert what a
// repository diagnostic does and does not carry.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []logging.Record
}

// Enabled accepts every level; the runtime still applies its own filtering.
func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle captures one record, including the component and event attributes the
// logging runtime adds to every emission.
func (h *recordingLogHandler) Handle(_ context.Context, record slog.Record) error {
	var attrs []slog.Attr
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})

	captured := logging.Record{
		Level:   record.Level,
		Message: record.Message,
		Attrs:   attrs,
	}
	if component, ok := attributeValue(attrs, "component"); ok {
		captured.Component = component
	}
	if event, ok := attributeValue(attrs, "event"); ok {
		captured.Event = event
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, captured)
	return nil
}

// WithAttrs returns the handler unchanged; these tests never add group state.
func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

// WithGroup returns the handler unchanged; these tests never add group state.
func (h *recordingLogHandler) WithGroup(string) slog.Handler { return h }

// snapshot returns the captured records.
func (h *recordingLogHandler) snapshot() []logging.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logging.Record(nil), h.records...)
}

// attributeValue reads a flattened attribute by key.
func attributeValue(attrs []slog.Attr, key string) (string, bool) {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.String(), true
		}
	}
	return "", false
}

func TestSQLiteMetadataRepositoryLogsCarryNoPaths(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	backupDirectory := t.TempDir()
	const tenantID = "tenant-logging"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	if err := seeded.AddOrUpdate(ctx, sqliteRecord(tenantID, "file-logging", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	corruptSQLiteDatabase(t, dataPath, tenantID)

	handler := &recordingLogHandler{}
	runtime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
		options.RecoverCorruptedDatabase = true
		options.CorruptedDatabaseRetention = time.Hour
		options.BackupDirectory = backupDirectory
		options.AutoRestoreFromBackup = true
		options.Logging = runtime
	})
	if _, err := repo.Get(ctx, tenantID, "file-logging"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() error = %v, want the degraded store", err)
	}

	records := handler.snapshot()
	if len(records) == 0 {
		t.Fatal("quarantine emitted no structured record")
	}
	for _, record := range records {
		if strings.Contains(record.Message, dataPath) || strings.Contains(record.Message, backupDirectory) {
			t.Fatalf("log message %q leaks a physical path", record.Message)
		}
		for _, attr := range record.Attrs {
			value := attr.Value.String()
			if strings.Contains(value, dataPath) || strings.Contains(value, backupDirectory) {
				t.Fatalf("log attribute %q = %q leaks a physical path", attr.Key, value)
			}
		}
	}
}
