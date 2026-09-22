package quota

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/sqlite"
)

// sqliteQuotaCorruptPayload is not a SQLite file: the header magic does not
// match, so opening it is a positive corruption verdict (SQLITE_NOTADB).
const sqliteQuotaCorruptPayload = "this is not a sqlite database file, it is a test fixture\n"

// fastBusyTimeout returns the default options with a short lock wait, so a test
// that deliberately holds a lock does not wait out the production timeout.
func fastBusyTimeout() sqlite.Options {
	options := sqlite.DefaultOptions()
	options.BusyTimeoutMs = 50
	return options
}

// plantCorruptSQLiteQuotaDatabase writes unreadable content into the tenant's
// quota database path, creating the directory when needed.
func plantCorruptSQLiteQuotaDatabase(t *testing.T, dataPath string, tenantID string) string {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(dataPath, tenantID), quotaDatabaseDirPermissions); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	path := sqliteQuotaDatabasePath(dataPath, tenantID)
	if err := os.WriteFile(path, []byte(sqliteQuotaCorruptPayload), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	return path
}

// quarantinedSQLiteQuotaFiles lists the quarantine siblings of a quota database.
func quarantinedSQLiteQuotaFiles(t *testing.T, path string) []string {
	t.Helper()

	matches, err := filepath.Glob(path + quotaCorruptedDatabaseSuffix + "*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	return matches
}

// TestSQLiteDirectoryQuotaRepositoryRejectsCorruptedDatabaseByDefault keeps the
// quota repository fail-fast unless recovery is explicitly enabled, and proves a
// rejected open leaves the file exactly where it was.
func TestSQLiteDirectoryQuotaRepositoryRejectsCorruptedDatabaseByDefault(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)

	var notified []string
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:            dataPath,
		OnCorruptedDatabase: func(quarantinedPath string) { notified = append(notified, quarantinedPath) },
	})

	_, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/rejected")
	if err == nil {
		t.Fatal("GetOrCreate() on a corrupted database succeeded, want a failure")
	}
	if !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("GetOrCreate() error = %v, want core.ErrDatabaseError", err)
	}
	if quarantined := quarantinedSQLiteQuotaFiles(t, path); len(quarantined) != 0 {
		t.Fatalf("quarantined = %v, want none while RecoverCorruptedDatabase is false", quarantined)
	}
	if len(notified) != 0 {
		t.Fatalf("OnCorruptedDatabase calls = %v, want none", notified)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("the corrupted database was removed instead of being left alone: %v", readErr)
	}
	if string(content) != sqliteQuotaCorruptPayload {
		t.Fatalf("the corrupted database content changed: %q", content)
	}
}

// TestSQLiteDirectoryQuotaRepositoryRecoversCorruptedDatabase is the regression
// test for a damaged quota database blocking the caller: the file is quarantined,
// the callback reports it, the sidecars are cleared, and the recreated store is
// usable.
func TestSQLiteDirectoryQuotaRepositoryRecoversCorruptedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)

	// Sidecars of the damaged database must not survive the quarantine: leaving
	// them behind would let SQLite replay a foreign write-ahead log.
	for _, suffix := range []string{quotaWalSuffix, quotaShmSuffix} {
		if err := os.WriteFile(path+suffix, []byte("stale sidecar\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path+suffix, err)
		}
	}

	var notified []string
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                   dataPath,
		RecoverCorruptedDatabase:   true,
		CorruptedDatabaseRetention: time.Hour,
		OnCorruptedDatabase:        func(quarantinedPath string) { notified = append(notified, quarantinedPath) },
	})

	quota, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/recovered")
	if err != nil {
		t.Fatalf("GetOrCreate() on a recovered database error = %v", err)
	}
	if quota.CurrentCount != 0 || quota.MaxCount != 0 || quota.Enabled {
		t.Fatalf("recovered quota = %+v, want the domain defaults", quota)
	}

	quarantined := quarantinedSQLiteQuotaFiles(t, path)
	if len(quarantined) != 1 {
		t.Fatalf("quarantine files = %v, want exactly one", quarantined)
	}
	if len(notified) != 1 || notified[0] != quarantined[0] {
		t.Fatalf("OnCorruptedDatabase calls = %v, want exactly [%s]", notified, quarantined[0])
	}
	content, err := os.ReadFile(quarantined[0])
	if err != nil {
		t.Fatalf("reading the quarantine file error = %v", err)
	}
	if string(content) != sqliteQuotaCorruptPayload {
		t.Fatalf("quarantined content = %q, want the damaged payload", content)
	}

	// The recreated database owns fresh sidecars; what must not survive is the
	// stale content of the damaged one.
	for _, suffix := range []string{quotaWalSuffix, quotaShmSuffix} {
		content, err := os.ReadFile(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("reading sidecar %s error = %v", suffix, err)
		}
		if strings.Contains(string(content), "stale sidecar") {
			t.Fatalf("sidecar %s still holds the content of the quarantined database", suffix)
		}
	}

	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/recovered"); err != nil {
		t.Fatalf("IncrementCount() on the recreated database error = %v", err)
	}
	quotas, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() on the recreated database error = %v", err)
	}
	if len(quotas) != 1 || quotas[0].CurrentCount != 1 {
		t.Fatalf("GetAll() = %+v, want one quota with count 1", quotas)
	}
}

// TestSQLiteDirectoryQuotaRepositoryQuarantineCollisionSuffix proves two
// quarantines in the same second cannot overwrite each other.
func TestSQLiteDirectoryQuotaRepositoryQuarantineCollisionSuffix(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	frozen := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	path := plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)
	first := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		now:                      func() time.Time { return frozen },
	})
	if _, err := first.GetOrCreate(ctx, sqliteQuotaTenant, "/first"); err != nil {
		t.Fatalf("first GetOrCreate() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	// Damage the recreated database and recover it again within the same second.
	if err := os.WriteFile(path, []byte(sqliteQuotaCorruptPayload), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	second := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		now:                      func() time.Time { return frozen },
	})
	if _, err := second.GetOrCreate(ctx, sqliteQuotaTenant, "/second"); err != nil {
		t.Fatalf("second GetOrCreate() error = %v", err)
	}

	stamp := frozen.Format(quotaCorruptedDatabaseTimestampLayout)
	want := []string{path + quotaCorruptedDatabaseSuffix + stamp, path + quotaCorruptedDatabaseSuffix + stamp + ".1"}
	got := quarantinedSQLiteQuotaFiles(t, path)
	if len(got) != len(want) {
		t.Fatalf("quarantine files = %v, want %v", got, want)
	}
	for index, quarantinedPath := range want {
		if got[index] != quarantinedPath {
			t.Fatalf("quarantine files = %v, want %v", got, want)
		}
	}
}

// TestSQLiteDirectoryQuotaRepositoryQuarantineCallbackCanReenterRepository
// proves the OnCorruptedDatabase hook runs outside every repository lock: a hook
// that reads another tenant would otherwise deadlock the repository.
func TestSQLiteDirectoryQuotaRepositoryQuarantineCallbackCanReenterRepository(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)

	var (
		repository  *sqliteDirectoryQuotaRepository
		callbackErr error
		callbacks   int
	)
	options := &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		OnCorruptedDatabase: func(string) {
			callbacks++
			_, callbackErr = repository.GetAll(ctx, "tenant-other")
		},
	}
	repository = newSQLiteQuotaRepository(t, options)

	done := make(chan error, 1)
	go func() {
		_, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/reentrant")
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("GetOrCreate() error = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("GetOrCreate() did not return: the quarantine hook ran under the repository lock")
	}

	if callbacks != 1 {
		t.Fatalf("OnCorruptedDatabase calls = %d, want 1", callbacks)
	}
	if callbackErr != nil {
		t.Fatalf("the hook's re-entrant GetAll() error = %v", callbackErr)
	}
}

// TestSQLiteDirectoryQuotaRepositoryLoggingOmitsPathsAndTenants proves the
// structured diagnostics go through the injected runtime and never carry a
// physical path, a tenant file name or a raw error.
func TestSQLiteDirectoryQuotaRepositoryLoggingOmitsPathsAndTenants(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)

	var buffer bytes.Buffer
	runtime, err := logging.New(logging.Config{
		Handler: slog.NewJSONHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug}),
	})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		RecoverCorruptedDatabase: true,
		Logging:                  runtime,
	})
	if _, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/logged"); err != nil {
		t.Fatalf("GetOrCreate() error = %v", err)
	}

	logged := buffer.String()
	if !strings.Contains(logged, "quota_database_quarantined") {
		t.Fatalf("log output = %q, want the quarantine event", logged)
	}
	for _, forbidden := range []string{dataPath, sqliteQuotaTenant, path, quotaDatabaseFileName, sqliteQuotaCorruptPayload} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("log output leaked %q: %q", forbidden, logged)
		}
	}
}

// TestSQLiteDirectoryQuotaRepositoryPrunesExpiredQuarantineFiles covers the
// bounded retention of quarantined quota data.
func TestSQLiteDirectoryQuotaRepositoryPrunesExpiredQuarantineFiles(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	path := plantCorruptSQLiteQuotaDatabase(t, dataPath, sqliteQuotaTenant)
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove(%s) error = %v", path, err)
	}

	stale := path + quotaCorruptedDatabaseSuffix + "20200101T000000Z"
	fresh := path + quotaCorruptedDatabaseSuffix + "29990101T000000Z"
	for _, quarantinedPath := range []string{stale, fresh} {
		if err := os.WriteFile(quarantinedPath, []byte("rescue copy\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", quarantinedPath, err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", stale, err)
	}

	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                   dataPath,
		CorruptedDatabaseRetention: time.Hour,
	})
	if _, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/pruned"); err != nil {
		t.Fatalf("GetOrCreate() error = %v", err)
	}

	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale quarantine file still present: %v", err)
	}
	if _, err := os.Lstat(fresh); err != nil {
		t.Fatalf("fresh quarantine file was pruned: %v", err)
	}
}

// TestSQLiteDirectoryQuotaRepositoryDoesNotQuarantineLockedDatabase is the safety
// guard for the quota store: a healthy database held by another writer is
// temporarily unavailable, never corrupt, so it must not be moved aside even when
// recovery is enabled.
func TestSQLiteDirectoryQuotaRepositoryDoesNotQuarantineLockedDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataPath, sqliteQuotaTenant), quotaDatabaseDirPermissions); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	path := sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant)

	options := sqlite.DefaultOptions()
	options.JournalMode = "DELETE"
	options.BusyTimeoutMs = 1
	holder, err := sqlite.Open(path, options)
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	defer func() { _ = holder.Close() }()

	connection, err := holder.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn() error = %v", err)
	}
	defer func() { _ = connection.Close() }()

	if _, err := connection.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		t.Fatalf("BEGIN EXCLUSIVE error = %v", err)
	}
	defer func() { _, _ = connection.ExecContext(context.Background(), "ROLLBACK") }()

	var notified []string
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                 dataPath,
		Sqlite:                   fastBusyTimeout(),
		RecoverCorruptedDatabase: true,
		OnCorruptedDatabase:      func(quarantinedPath string) { notified = append(notified, quarantinedPath) },
	})

	if _, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/locked"); err == nil {
		t.Fatal("GetOrCreate() against an exclusively locked database succeeded, want a lock failure")
	}
	if len(notified) != 0 {
		t.Fatalf("OnCorruptedDatabase calls = %v, want none for a held lock", notified)
	}
	if quarantined := quarantinedSQLiteQuotaFiles(t, path); len(quarantined) != 0 {
		t.Fatalf("quarantined = %v, want none for a held lock", quarantined)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("the database path was removed instead of being left alone: %v", err)
	}
}
