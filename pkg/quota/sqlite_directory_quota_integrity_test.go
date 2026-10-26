package quota

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/cocosip/venue/pkg/sqlite"
)

// damageQuotaDatabasePage appends one all-zero page to a healthy quota database
// and counts it in the header, so the file still opens and still serves queries
// while PRAGMA integrity_check(1) reports "Page N: never used".
//
// That is the state the cold-open integrity verdict is aimed at, and the only
// state in which the cost of the verdict (a full scan of every page) can be
// observed independently of a database that cannot be opened at all.
func damageQuotaDatabasePage(t *testing.T, path string) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	if len(raw) < 100 {
		t.Fatalf("database %s is %d bytes, want at least a full SQLite header", path, len(raw))
	}

	pageSize := int(binary.BigEndian.Uint16(raw[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	pageCount := int(binary.BigEndian.Uint32(raw[28:32]))
	if pageCount == 0 {
		pageCount = len(raw) / pageSize
	}

	raw = append(raw, make([]byte, pageSize)...)
	binary.BigEndian.PutUint32(raw[28:32], uint32(pageCount+1))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

// integrityVerdict returns the verdict of the bounded integrity check for path.
func integrityVerdict(t *testing.T, path string) string {
	t.Helper()

	ctx := context.Background()
	db, err := sqlite.Open(ctx, path, sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("sqlite.Open(%s) error = %v", path, err)
	}
	defer func() { _ = db.Close() }()

	var verdict string
	if err := db.QueryRowContext(ctx, quotaIntegrityCheckSQL).Scan(&verdict); err != nil {
		t.Fatalf("%s on %s error = %v", quotaIntegrityCheckSQL, path, err)
	}
	return verdict
}

// TestSQLiteDirectoryQuotaRepositorySkipsIntegrityCheckWithoutRecovery is the
// regression test for the integrity verdict running on every cold open: the scan
// reads every page, so it belongs to the corruption-recovery path only. With
// recovery disabled a database that opens and serves reads must be served, even
// when the verdict would be "not ok".
func TestSQLiteDirectoryQuotaRepositorySkipsIntegrityCheckWithoutRecovery(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant)

	seeded := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	mustGetOrCreate(t, seeded, sqliteQuotaTenant, "/kept")
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	damageQuotaDatabasePage(t, path)

	// The damage is real: the verdict is not "ok", yet the file is readable.
	if verdict := integrityVerdict(t, path); verdict == "ok" {
		t.Fatalf("integrity verdict = %q, want a damaged verdict for the fixture", verdict)
	}

	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	quota, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/kept")
	if err != nil {
		t.Fatalf("GetOrCreate() with recovery disabled error = %v, want a usable open", err)
	}
	if quota.DirectoryPath != "/kept" {
		t.Errorf("DirectoryPath = %q, want %q", quota.DirectoryPath, "/kept")
	}
	if quarantined := quarantinedSQLiteQuotaFiles(t, path); len(quarantined) != 0 {
		t.Errorf("quarantined = %v, want none while RecoverCorruptedDatabase is false", quarantined)
	}
}

// TestSQLiteDirectoryQuotaRepositoryVerifiesIntegrityWithRecovery keeps the other
// half of the contract: when the caller asked for corruption recovery, the same
// damaged database is detected, quarantined and recreated.
func TestSQLiteDirectoryQuotaRepositoryVerifiesIntegrityWithRecovery(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant)

	seeded := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	mustGetOrCreate(t, seeded, sqliteQuotaTenant, "/damaged")
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	damageQuotaDatabasePage(t, path)

	var notified []string
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		OnCorruptedDatabase:      func(quarantinedPath string) { notified = append(notified, quarantinedPath) },
	})

	quota, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/recreated")
	if err != nil {
		t.Fatalf("GetOrCreate() with recovery enabled error = %v", err)
	}
	if quota.CurrentCount != 0 || quota.Enabled {
		t.Fatalf("recovered quota = %+v, want the domain defaults", quota)
	}
	if len(notified) != 1 {
		t.Fatalf("OnCorruptedDatabase calls = %v, want exactly one", notified)
	}
	if _, statErr := os.Lstat(notified[0]); statErr != nil {
		t.Errorf("quarantine file %s error = %v, want the rescue copy to exist", notified[0], statErr)
	}
}
