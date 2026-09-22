package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// testDatabase opens a database inside the test's own temporary directory and
// closes it before that directory is removed. t.TempDir registers its cleanup
// first, so the LIFO order of t.Cleanup closes the handle before the directory
// disappears (on Windows an open file cannot be removed).
func testDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "metadata.db")
	db, err := Open(path, DefaultOptions())
	if err != nil {
		t.Fatalf("Open(%q) = %v, want nil", path, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close() = %v, want nil", err)
		}
	})
	return db, path
}

// createTestTable creates the minimal table the behaviour tests use.
func createTestTable(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (
		file_key TEXT PRIMARY KEY NOT NULL,
		tenant_id TEXT NOT NULL,
		file_size INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// TestOpenCreatesConfiguredDatabase covers the connection policy: the file is
// created, the DSN pragmas are the effective ones, a table can be used, and a
// second handle on the same file works.
func TestOpenCreatesConfiguredDatabase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.db")

	opts := DefaultOptions()
	opts.BusyTimeoutMs = 750
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open() = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close() = %v, want nil", err)
		}
	})

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database file was not created: %v", err)
	}
	if stats := db.Stats(); stats.MaxOpenConnections != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", stats.MaxOpenConnections)
	}

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Errorf("PRAGMA journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != 750 {
		t.Errorf("PRAGMA busy_timeout = %d, want 750", busyTimeout)
	}

	createTestTable(t, db)
	if _, err := db.Exec("INSERT INTO files (file_key, tenant_id, file_size) VALUES (?, ?, ?)", "k1", "t1", 42); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var (
		tenantID string
		fileSize int
	)
	if err := db.QueryRow("SELECT tenant_id, file_size FROM files WHERE file_key = ?", "k1").Scan(&tenantID, &fileSize); err != nil {
		t.Fatalf("select: %v", err)
	}
	if tenantID != "t1" || fileSize != 42 {
		t.Errorf("row = (%q, %d), want (t1, 42)", tenantID, fileSize)
	}

	// A second handle on the same file must work: one long-lived connection per
	// handle is the policy, not a single-process limit.
	second, err := Open(path, DefaultOptions())
	if err != nil {
		t.Fatalf("second Open() = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("second.Close() = %v, want nil", err)
		}
	})

	var count int
	if err := second.QueryRow("SELECT count(*) FROM files").Scan(&count); err != nil {
		t.Fatalf("count from second handle: %v", err)
	}
	if count != 1 {
		t.Errorf("count from second handle = %d, want 1", count)
	}
}

// TestOpenDoesNotCreateParentDirectory pins the layout ownership rule: the
// caller owns directories.
func TestOpenDoesNotCreateParentDirectory(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "tenant-1", "metadata.db")
	db, err := Open(missing, DefaultOptions())
	if db != nil {
		_ = db.Close()
		t.Fatal("Open() returned a handle for a missing parent directory")
	}
	if err == nil {
		t.Fatal("Open() = nil, want an error for a missing parent directory")
	}
	if !errors.Is(err, core.ErrDatabaseError) {
		t.Errorf("Open() = %v, want a wrapped core.ErrDatabaseError", err)
	}
	if _, statErr := os.Stat(filepath.Dir(missing)); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("parent directory %q was created (stat error %v), want it to stay absent", filepath.Dir(missing), statErr)
	}
}

// TestOpenRejectsUnusablePath covers a path whose parent exists but is not a
// directory (a database failure) and an empty path (an argument failure).
func TestOpenRejectsUnusablePath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	tests := []struct {
		name   string
		path   string
		target error
	}{
		{name: "parent is a file", path: filepath.Join(filePath, "metadata.db"), target: core.ErrDatabaseError},
		{name: "empty path", path: "", target: core.ErrInvalidArgument},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := Open(tc.path, DefaultOptions())
			if db != nil {
				_ = db.Close()
				t.Fatalf("Open(%q) returned a handle, want nil", tc.path)
			}
			if err == nil {
				t.Fatalf("Open(%q) = nil, want an error", tc.path)
			}
			if !errors.Is(err, tc.target) {
				t.Fatalf("Open(%q) = %v, want a wrapped %v", tc.path, err, tc.target)
			}
			if IsCorruptionError(err) {
				t.Errorf("IsCorruptionError(%v) = true, want false: a path defect is not corruption", err)
			}
		})
	}
}

// TestOpenClassifiesCorruptFile pins the open-path corruption verdict: the
// caller must be able to tell "damaged" from "locked" before deciding whether
// to quarantine, because quarantine is destructive.
func TestOpenClassifiesCorruptFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.db")
	if err := os.WriteFile(path, []byte("this file is definitely not a SQLite database, just text"), 0o600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	db, err := Open(path, DefaultOptions())
	if db != nil {
		_ = db.Close()
		t.Fatal("Open() returned a handle for a corrupt file, want nil")
	}
	if err == nil {
		t.Fatal("Open() = nil, want an error for a corrupt file")
	}
	if !errors.Is(err, core.ErrDatabaseError) {
		t.Errorf("Open() = %v, want a wrapped core.ErrDatabaseError", err)
	}
	if !IsCorruptionError(err) {
		t.Errorf("IsCorruptionError(%v) = false, want true", err)
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Errorf("Open() = %v, want the message to name corruption", err)
	}
}

// TestOpenReopensDatabaseFile verifies reopening the same file after a clean
// close preserves the data.
func TestOpenReopensDatabaseFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metadata.db")

	first, err := Open(path, DefaultOptions())
	if err != nil {
		t.Fatalf("first Open() = %v, want nil", err)
	}
	createTestTable(t, first)
	if _, err := first.Exec("INSERT INTO files (file_key, tenant_id) VALUES ('k1', 't1')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := TruncateCheckpoint(context.Background(), first); err != nil {
		t.Fatalf("TruncateCheckpoint() = %v, want nil", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first.Close() = %v, want nil", err)
	}

	second, err := Open(path, DefaultOptions())
	if err != nil {
		t.Fatalf("second Open() = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("second.Close() = %v, want nil", err)
		}
	})

	var fileKey string
	if err := second.QueryRow("SELECT file_key FROM files").Scan(&fileKey); err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if fileKey != "k1" {
		t.Errorf("file_key = %q, want k1", fileKey)
	}
}

// TestIntegrityCheckHealthy verifies a healthy database passes verification.
func TestIntegrityCheckHealthy(t *testing.T) {
	t.Parallel()

	db, _ := testDatabase(t)
	createTestTable(t, db)
	if _, err := db.Exec("INSERT INTO files (file_key, tenant_id) VALUES ('k1', 't1')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := IntegrityCheck(context.Background(), db); err != nil {
		t.Fatalf("IntegrityCheck() = %v, want nil", err)
	}
}

// TestIntegrityCheckDetectsDamagedDatabase damages a copy of a real database
// and verifies that both integrity_check and a plain read report corruption.
func TestIntegrityCheckDetectsDamagedDatabase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.db")
	db, err := Open(path, DefaultOptions())
	if err != nil {
		t.Fatalf("Open() = %v, want nil", err)
	}
	if _, err := db.Exec("CREATE TABLE files (file_key TEXT PRIMARY KEY NOT NULL, pad BLOB)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	pad := make([]byte, 8000)
	for i := range pad {
		pad[i] = byte(i)
	}
	for i := range 20 {
		if _, err := db.Exec("INSERT INTO files (file_key, pad) VALUES (?, ?)", fmt.Sprintf("k%02d", i), pad); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if err := TruncateCheckpoint(context.Background(), db); err != nil {
		t.Fatalf("TruncateCheckpoint() = %v, want nil", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	// Overwrite pages 2 and 3 while leaving the header and page 1 untouched, so
	// the file still parses but its table content is unusable.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read database: %v", err)
	}
	if len(raw) < 4*4096 {
		t.Fatalf("database is %d bytes, want at least 4 pages", len(raw))
	}
	for i := 4096; i < 3*4096; i++ {
		raw[i] = 0xff
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write damaged database: %v", err)
	}

	damaged, err := Open(path, DefaultOptions())
	if err != nil {
		// Some damage is caught while connecting.
		if !errors.Is(err, core.ErrDatabaseError) {
			t.Fatalf("Open() = %v, want a wrapped core.ErrDatabaseError", err)
		}
		if !IsCorruptionError(err) {
			t.Fatalf("IsCorruptionError(%v) = false, want true", err)
		}
		return
	}
	t.Cleanup(func() {
		if err := damaged.Close(); err != nil {
			t.Errorf("damaged.Close() = %v, want nil", err)
		}
	})

	checkErr := IntegrityCheck(context.Background(), damaged)
	if checkErr == nil {
		t.Fatal("IntegrityCheck() = nil on a damaged database, want an error")
	}
	if !errors.Is(checkErr, core.ErrDatabaseError) {
		t.Errorf("IntegrityCheck() = %v, want a wrapped core.ErrDatabaseError", checkErr)
	}
	if !IsCorruptionError(checkErr) {
		t.Errorf("IsCorruptionError(%v) = false, want true", checkErr)
	}

	// A corrupted file whose header is unreadable is the other shape of the
	// same verdict.
	notADatabase := filepath.Join(dir, "not-a-database.db")
	if err := os.WriteFile(notADatabase, []byte("SQLite format 3\x00 but nothing else that makes sense"), 0o600); err != nil {
		t.Fatalf("write not-a-database: %v", err)
	}
	other, err := Open(notADatabase, DefaultOptions())
	if other != nil {
		_ = other.Close()
		t.Fatal("Open() returned a handle for a file that is not a database")
	}
	if !IsCorruptionError(err) {
		t.Errorf("IsCorruptionError(%v) = false, want true", err)
	}
}

// TestIsCorruptionErrorClassification locks the destructive-action boundary:
// only a damaged file may be quarantined, so lock, constraint and not-found
// failures must not be classified as corruption.
func TestIsCorruptionErrorClassification(t *testing.T) {
	t.Parallel()

	db, _ := testDatabase(t)
	createTestTable(t, db)
	if _, err := db.Exec("INSERT INTO files (file_key, tenant_id) VALUES ('k1', 't1')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, constraintErr := db.Exec("INSERT INTO files (file_key, tenant_id) VALUES ('k1', 't1')")
	if constraintErr == nil {
		t.Fatal("duplicate insert succeeded, want a constraint violation")
	}

	var fileKey string
	notFoundErr := db.QueryRow("SELECT file_key FROM files WHERE file_key = 'missing'").Scan(&fileKey)
	if notFoundErr == nil {
		t.Fatal("missing row query succeeded, want sql.ErrNoRows")
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "constraint violation", err: constraintErr, want: false},
		{name: "no rows", err: notFoundErr, want: false},
		{name: "not found sentinel", err: core.ErrFileNotFound, want: false},
		{name: "database error sentinel", err: core.ErrDatabaseError, want: false},
		{name: "invalid argument sentinel", err: core.ErrInvalidArgument, want: false},
		{name: "ordinary error", err: errors.New("connection reset by peer"), want: false},
		{name: "busy message", err: errors.New("database is locked (5) (SQLITE_BUSY)"), want: false},
		{name: "read-only message", err: errors.New("attempt to write a readonly database (8) (SQLITE_READONLY)"), want: false},
		{name: "cannot open message", err: errors.New("unable to open database file (14) (SQLITE_CANTOPEN)"), want: false},
		{name: "malformed message", err: errors.New("database disk image is malformed (11)"), want: true},
		{name: "not a database message", err: errors.New("file is not a database (26)"), want: true},
		{name: "corrupt message", err: errors.New("database corrupt"), want: true},
		{name: "integrity verdict", err: errors.New("*** in database main ***\nTree 2 page 2: btreeInitPage() returns error code 11"), want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := IsCorruptionError(tc.err); got != tc.want {
				t.Fatalf("IsCorruptionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestVacuumIntoProducesConsistentCopy verifies the online backup primitive: the
// copy opens, holds the same rows, and the source keeps serving readers.
func TestVacuumIntoProducesConsistentCopy(t *testing.T) {
	t.Parallel()

	source, sourcePath := testDatabase(t)
	createTestTable(t, source)
	const rows = 25
	for i := range rows {
		if _, err := source.Exec("INSERT INTO files (file_key, tenant_id, file_size) VALUES (?, ?, ?)",
			fmt.Sprintf("k%02d", i), "t1", i); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	target := filepath.Join(t.TempDir(), "metadata.backup")
	if err := VacuumInto(context.Background(), source, target); err != nil {
		t.Fatalf("VacuumInto() = %v, want nil", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("backup file was not created: %v", err)
	}

	copyDB, err := Open(target, DefaultOptions())
	if err != nil {
		t.Fatalf("Open(backup) = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := copyDB.Close(); err != nil {
			t.Errorf("copyDB.Close() = %v, want nil", err)
		}
	})
	if err := IntegrityCheck(context.Background(), copyDB); err != nil {
		t.Fatalf("IntegrityCheck(backup) = %v, want nil", err)
	}

	var copyCount int
	if err := copyDB.QueryRow("SELECT count(*) FROM files").Scan(&copyCount); err != nil {
		t.Fatalf("count in backup: %v", err)
	}
	if copyCount != rows {
		t.Errorf("backup row count = %d, want %d", copyCount, rows)
	}
	var fileSize int
	if err := copyDB.QueryRow("SELECT file_size FROM files WHERE file_key = ?", "k07").Scan(&fileSize); err != nil {
		t.Fatalf("select from backup: %v", err)
	}
	if fileSize != 7 {
		t.Errorf("backup file_size = %d, want 7", fileSize)
	}

	// The source is untouched and still readable.
	if err := IntegrityCheck(context.Background(), source); err != nil {
		t.Fatalf("IntegrityCheck(source) = %v, want nil", err)
	}
	var sourceCount int
	if err := source.QueryRow("SELECT count(*) FROM files").Scan(&sourceCount); err != nil {
		t.Fatalf("count in source: %v", err)
	}
	if sourceCount != rows {
		t.Errorf("source row count = %d, want %d", sourceCount, rows)
	}
	if _, err := os.Stat(sourcePath); err != nil {
		t.Fatalf("source database disappeared: %v", err)
	}
}

// TestVacuumIntoRefusesExistingTarget pins the VACUUM INTO contract: the target
// must not pre-exist, and the refusal is an argument error rather than a
// database failure.
func TestVacuumIntoRefusesExistingTarget(t *testing.T) {
	t.Parallel()

	source, _ := testDatabase(t)
	createTestTable(t, source)

	dir := t.TempDir()
	target := filepath.Join(dir, "metadata.backup")
	if err := os.WriteFile(target, []byte("already here"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}

	err := VacuumInto(context.Background(), source, target)
	if err == nil {
		t.Fatal("VacuumInto() = nil for an existing target, want an error")
	}
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("VacuumInto() = %v, want a wrapped core.ErrInvalidArgument", err)
	}

	content, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("read target: %v", readErr)
	}
	if string(content) != "already here" {
		t.Errorf("existing target was modified: %q", content)
	}
}

// TestVacuumIntoRejectsUnusableTarget covers the empty target and a missing
// target directory.
func TestVacuumIntoRejectsUnusableTarget(t *testing.T) {
	t.Parallel()

	source, _ := testDatabase(t)
	createTestTable(t, source)

	if err := VacuumInto(context.Background(), source, ""); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("VacuumInto(empty) = %v, want a wrapped core.ErrInvalidArgument", err)
	}

	missing := filepath.Join(t.TempDir(), "absent", "metadata.backup")
	if err := VacuumInto(context.Background(), source, missing); err == nil {
		t.Error("VacuumInto() = nil for a missing target directory, want an error")
	}
	if _, statErr := os.Stat(missing); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("target %q exists after a failed vacuum (stat error %v), want it absent", missing, statErr)
	}
	// A failed copy must leave the source usable.
	if err := IntegrityCheck(context.Background(), source); err != nil {
		t.Errorf("IntegrityCheck(source) = %v, want nil after a failed vacuum", err)
	}
}

// TestVacuumIntoWhileReading verifies the copy is consistent while the source
// keeps serving readers.
func TestVacuumIntoWhileReading(t *testing.T) {
	t.Parallel()

	source, _ := testDatabase(t)
	createTestTable(t, source)
	const rows = 50
	for i := range rows {
		if _, err := source.Exec("INSERT INTO files (file_key, tenant_id) VALUES (?, ?)",
			fmt.Sprintf("k%02d", i), "t1"); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	readErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var count int
			if err := source.QueryRow("SELECT count(*) FROM files").Scan(&count); err != nil {
				select {
				case readErr <- err:
				default:
				}
				return
			}
			if count > rows {
				select {
				case readErr <- fmt.Errorf("reader observed %d rows, want at most %d", count, rows):
				default:
				}
				return
			}
		}
	}()

	target := filepath.Join(t.TempDir(), "metadata.backup")
	vacuumErr := VacuumInto(context.Background(), source, target)
	close(stop)
	wg.Wait()
	select {
	case err := <-readErr:
		t.Fatalf("concurrent reader failed: %v", err)
	default:
	}
	if vacuumErr != nil {
		t.Fatalf("VacuumInto() = %v, want nil", vacuumErr)
	}

	copyDB, err := Open(target, DefaultOptions())
	if err != nil {
		t.Fatalf("Open(backup) = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := copyDB.Close(); err != nil {
			t.Errorf("copyDB.Close() = %v, want nil", err)
		}
	})
	var copyCount int
	if err := copyDB.QueryRow("SELECT count(*) FROM files").Scan(&copyCount); err != nil {
		t.Fatalf("count in backup: %v", err)
	}
	if copyCount != rows {
		t.Errorf("backup row count = %d, want %d", copyCount, rows)
	}
}

// TestCheckpointModes verifies both checkpoint statements run outside a
// transaction and on a healthy database.
func TestCheckpointModes(t *testing.T) {
	t.Parallel()

	db, path := testDatabase(t)
	createTestTable(t, db)
	if _, err := db.Exec("INSERT INTO files (file_key, tenant_id) VALUES ('k1', 't1')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := Checkpoint(context.Background(), db); err != nil {
		t.Fatalf("Checkpoint() = %v, want nil", err)
	}
	if err := TruncateCheckpoint(context.Background(), db); err != nil {
		t.Fatalf("TruncateCheckpoint() = %v, want nil", err)
	}

	// TRUNCATE leaves the write-ahead log empty.
	walInfo, err := os.Stat(path + "-wal")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat -wal: %v", err)
	}
	if err == nil && walInfo.Size() != 0 {
		t.Errorf("-wal size = %d after TruncateCheckpoint, want 0", walInfo.Size())
	}

	// The data survives the checkpoint.
	var count int
	if err := db.QueryRow("SELECT count(*) FROM files").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

// TestCheckpointAfterBatchIsCallerOwned verifies Open does not run a checkpoint
// behind the caller's back: with the option enabled the committed pages stay in
// the write-ahead log until the caller asks for a checkpoint.
func TestCheckpointAfterBatchIsCallerOwned(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.db")

	opts := DefaultOptions()
	opts.CheckpointAfterBatch = true
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open() = %v, want nil", err)
	}
	createTestTable(t, db)

	pad := make([]byte, 4000)
	for i := range 20 {
		if _, err := db.Exec("INSERT INTO files (file_key, tenant_id, file_size) VALUES (?, 't1', ?)",
			fmt.Sprintf("k%02d", i), len(pad)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	walInfo, statErr := os.Stat(path + "-wal")
	if statErr != nil {
		t.Fatalf("stat -wal: %v", statErr)
	}
	if walInfo.Size() == 0 {
		t.Error("-wal is empty after commits with CheckpointAfterBatch=true, want uncheckpointed pages: Open must not checkpoint on its own")
	}

	if err := Checkpoint(context.Background(), db); err != nil {
		t.Fatalf("Checkpoint() = %v, want nil", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}
