package quota

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
)

// sqliteQuotaTenant is the tenant most SQLite quota tests store quotas for.
const sqliteQuotaTenant = "tenant-one"

// newSQLiteQuotaRepository opens a SQLite quota repository and registers its
// shutdown before the temporary directory that holds it is removed.
//
// t.Cleanup runs in LIFO order, so a test that calls t.TempDir() first gets its
// directory removed only after the repository has released every file handle.
func newSQLiteQuotaRepository(t *testing.T, opts *SQLiteDirectoryQuotaRepositoryOptions) *sqliteDirectoryQuotaRepository {
	t.Helper()

	repository, err := NewSQLiteDirectoryQuotaRepository(opts)
	if err != nil {
		t.Fatalf("NewSQLiteDirectoryQuotaRepository() error = %v", err)
	}
	t.Cleanup(func() { _ = repository.Close() })

	concrete, ok := repository.(*sqliteDirectoryQuotaRepository)
	if !ok {
		t.Fatalf("NewSQLiteDirectoryQuotaRepository() returned %T, want *sqliteDirectoryQuotaRepository", repository)
	}
	return concrete
}

// mustGetOrCreate fails the test when a quota cannot be read or created.
func mustGetOrCreate(t *testing.T, repository core.DirectoryQuotaRepository, tenantID string, directoryPath string) *core.DirectoryQuota {
	t.Helper()

	quota, err := repository.GetOrCreate(context.Background(), tenantID, directoryPath)
	if err != nil {
		t.Fatalf("GetOrCreate(%q, %q) error = %v", tenantID, directoryPath, err)
	}
	if quota == nil {
		t.Fatalf("GetOrCreate(%q, %q) returned a nil quota without an error", tenantID, directoryPath)
	}
	return quota
}

// sqliteQuotaDatabasePath is the documented per-tenant database location.
func sqliteQuotaDatabasePath(dataPath string, tenantID string) string {
	return filepath.Join(dataPath, tenantID, quotaDatabaseFileName)
}

// TestNewSQLiteDirectoryQuotaRepositoryDefaultsSqliteOptions proves the zero
// sqlite.Options value selects the Locus-compatible defaults instead of failing
// validation, and that the repository opens no database until a tenant is used.
func TestNewSQLiteDirectoryQuotaRepositoryDefaultsSqliteOptions(t *testing.T) {
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})

	if repository.sqlite != sqlite.DefaultOptions() {
		t.Fatalf("sqlite options = %+v, want %+v", repository.sqlite, sqlite.DefaultOptions())
	}
	if got := repository.openHandleCount(); got != 0 {
		t.Fatalf("open handle count = %d, want 0 before the first tenant access", got)
	}

	entries, err := os.ReadDir(dataPath)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", dataPath, err)
	}
	if len(entries) != 0 {
		t.Fatalf("data path entries = %v, want none before the first tenant access", entries)
	}
}

// TestNewSQLiteDirectoryQuotaRepositoryRejectsInvalidOptions is the
// configuration matrix of the constructor.
func TestNewSQLiteDirectoryQuotaRepositoryRejectsInvalidOptions(t *testing.T) {
	valid := func() *SQLiteDirectoryQuotaRepositoryOptions {
		return &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()}
	}

	tests := []struct {
		name    string
		mutate  func(*SQLiteDirectoryQuotaRepositoryOptions)
		wantErr error
	}{
		{
			name:    "empty data path",
			mutate:  func(opts *SQLiteDirectoryQuotaRepositoryOptions) { opts.DataPath = "  " },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "negative max open databases",
			mutate:  func(opts *SQLiteDirectoryQuotaRepositoryOptions) { opts.MaxOpenDatabases = -1 },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "negative idle timeout",
			mutate:  func(opts *SQLiteDirectoryQuotaRepositoryOptions) { opts.OpenDatabaseIdleTimeout = -time.Second },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "negative corrupted database retention",
			mutate:  func(opts *SQLiteDirectoryQuotaRepositoryOptions) { opts.CorruptedDatabaseRetention = -time.Hour },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name: "unknown journal mode",
			mutate: func(opts *SQLiteDirectoryQuotaRepositoryOptions) {
				opts.Sqlite = sqlite.DefaultOptions()
				opts.Sqlite.JournalMode = "SIDEWAYS"
			},
			wantErr: core.ErrInvalidArgument,
		},
		{
			name: "zero cache size",
			mutate: func(opts *SQLiteDirectoryQuotaRepositoryOptions) {
				opts.Sqlite = sqlite.DefaultOptions()
				opts.Sqlite.CacheSizeKb = 0
			},
			wantErr: core.ErrInvalidArgument,
		},
	}

	if repository, err := NewSQLiteDirectoryQuotaRepository(nil); err == nil || repository != nil {
		t.Fatalf("NewSQLiteDirectoryQuotaRepository(nil) = (%v, %v), want a nil repository and an error", repository, err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := valid()
			test.mutate(opts)

			repository, err := NewSQLiteDirectoryQuotaRepository(opts)
			if err == nil {
				t.Fatalf("NewSQLiteDirectoryQuotaRepository() error = nil, want an error")
			}
			if repository != nil {
				t.Fatalf("NewSQLiteDirectoryQuotaRepository() returned %T with error %v", repository, err)
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewSQLiteDirectoryQuotaRepository() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

// TestSQLiteDirectoryQuotaRepositoryCreatesDefaults pins the Venue domain
// defaults of a new row: unlimited, disabled, count zero, and a single stored row
// no matter how often it is read.
func TestSQLiteDirectoryQuotaRepositoryCreatesDefaults(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	created := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/path/to/dir")
	if created.DirectoryPath != "/path/to/dir" {
		t.Errorf("DirectoryPath = %q, want %q", created.DirectoryPath, "/path/to/dir")
	}
	if created.CurrentCount != 0 {
		t.Errorf("CurrentCount = %d, want 0", created.CurrentCount)
	}
	if created.MaxCount != 0 {
		t.Errorf("MaxCount = %d, want 0 (unlimited)", created.MaxCount)
	}
	if created.Enabled {
		t.Error("Enabled = true, want false for a new quota")
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Errorf("CreatedAt = %v, UpdatedAt = %v, want both set", created.CreatedAt, created.UpdatedAt)
	}
	if created.CreatedAt.Location() != time.UTC || created.UpdatedAt.Location() != time.UTC {
		t.Errorf("timestamps = %v / %v, want UTC", created.CreatedAt, created.UpdatedAt)
	}

	existing := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/path/to/dir")
	if !existing.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("CreatedAt changed across GetOrCreate: %v then %v", created.CreatedAt, existing.CreatedAt)
	}
	if !existing.UpdatedAt.Equal(created.UpdatedAt) {
		t.Errorf("UpdatedAt changed across GetOrCreate: %v then %v", created.UpdatedAt, existing.UpdatedAt)
	}

	quotas, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(quotas) != 1 {
		t.Fatalf("GetAll() returned %d quotas, want 1: %+v", len(quotas), quotas)
	}
}

// TestSQLiteDirectoryQuotaRepositoryValidation covers the argument contract of
// every method, including the tenant-ID path-segment rule.
func TestSQLiteDirectoryQuotaRepositoryValidation(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})
	validQuota := &core.DirectoryQuota{DirectoryPath: "/valid", MaxCount: 1}

	tests := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{
			name:    "GetOrCreate rejects an empty directory path",
			call:    func() error { _, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, ""); return err },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "GetOrCreate rejects an empty tenant",
			call:    func() error { _, err := repository.GetOrCreate(ctx, "", "/dir"); return err },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "GetOrCreate rejects a tenant path escape",
			call:    func() error { _, err := repository.GetOrCreate(ctx, "../escape", "/dir"); return err },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "GetOrCreate rejects a reserved tenant name",
			call:    func() error { _, err := repository.GetOrCreate(ctx, "NUL", "/dir"); return err },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "Update rejects a nil quota",
			call:    func() error { return repository.Update(ctx, sqliteQuotaTenant, nil) },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "Update rejects an empty directory path",
			call:    func() error { return repository.Update(ctx, sqliteQuotaTenant, &core.DirectoryQuota{}) },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "Update rejects an empty tenant",
			call:    func() error { return repository.Update(ctx, "", validQuota) },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "IncrementCount rejects an empty directory path",
			call:    func() error { return repository.IncrementCount(ctx, sqliteQuotaTenant, "") },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "IncrementCount rejects an empty tenant",
			call:    func() error { return repository.IncrementCount(ctx, "", "/dir") },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "DecrementCount rejects an empty directory path",
			call:    func() error { return repository.DecrementCount(ctx, sqliteQuotaTenant, "") },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "DecrementCount rejects an empty tenant",
			call:    func() error { return repository.DecrementCount(ctx, "", "/dir") },
			wantErr: core.ErrInvalidArgument,
		},
		{
			name:    "GetAll rejects an empty tenant",
			call:    func() error { _, err := repository.GetAll(ctx, ""); return err },
			wantErr: core.ErrInvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if err == nil {
				t.Fatal("error = nil, want a rejection")
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}

	// A rejected tenant must never have created a directory below the root.
	entries, err := os.ReadDir(repository.dataPath)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", repository.dataPath, err)
	}
	if len(entries) != 0 {
		t.Fatalf("data path entries = %v, want none after rejected calls", entries)
	}
}

// TestSQLiteDirectoryQuotaRepositoryUpdateRoundTrip covers the administrative
// absolute write, including the immutable creation instant.
func TestSQLiteDirectoryQuotaRepositoryUpdateRoundTrip(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath: t.TempDir(),
		now:      func() time.Time { return current },
	})

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/data/docs")
	createdAt := quota.CreatedAt

	current = current.Add(90 * time.Minute)
	quota.CurrentCount = 7
	quota.MaxCount = 100
	quota.Enabled = true
	if err := repository.Update(ctx, sqliteQuotaTenant, quota); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if !quota.UpdatedAt.Equal(current) {
		t.Errorf("Update() left UpdatedAt = %v, want the repository clock %v", quota.UpdatedAt, current)
	}

	stored := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/data/docs")
	if stored.CurrentCount != 7 || stored.MaxCount != 100 || !stored.Enabled {
		t.Fatalf("stored quota = %+v, want count 7, max 100, enabled", stored)
	}
	if !stored.CreatedAt.Equal(createdAt) {
		t.Errorf("CreatedAt = %v, want the original %v", stored.CreatedAt, createdAt)
	}
	if !stored.UpdatedAt.Equal(current) {
		t.Errorf("UpdatedAt = %v, want %v", stored.UpdatedAt, current)
	}

	all, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(all) != 1 || all[0].CurrentCount != 7 || !all[0].Enabled || all[0].MaxCount != 100 {
		t.Fatalf("GetAll() = %+v, want one enabled quota with count 7 and max 100", all)
	}
}

// TestSQLiteDirectoryQuotaRepositoryUpdateCreatesMissingRow proves Update is not
// a silent no-op for a directory that has no row yet.
func TestSQLiteDirectoryQuotaRepositoryUpdateCreatesMissingRow(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	missing := &core.DirectoryQuota{DirectoryPath: "/never/created", CurrentCount: 3, MaxCount: 9, Enabled: true}
	if err := repository.Update(ctx, sqliteQuotaTenant, missing); err != nil {
		t.Fatalf("Update() on a missing row error = %v", err)
	}

	stored := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/never/created")
	if stored.CurrentCount != 3 || stored.MaxCount != 9 || !stored.Enabled {
		t.Fatalf("stored quota = %+v, want count 3, max 9, enabled", stored)
	}
	if stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatalf("stored timestamps = %v / %v, want both set", stored.CreatedAt, stored.UpdatedAt)
	}
}

// TestSQLiteDirectoryQuotaRepositoryTenantIsolation stores the same logical
// directory under two tenants and proves the counts and the files are separate.
func TestSQLiteDirectoryQuotaRepositoryTenantIsolation(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})

	const (
		firstTenant  = "tenant-alpha"
		secondTenant = "tenant-beta"
		sharedPath   = "/shared/directory"
	)

	mustGetOrCreate(t, repository, firstTenant, sharedPath)
	for i := 0; i < 3; i++ {
		if err := repository.IncrementCount(ctx, firstTenant, sharedPath); err != nil {
			t.Fatalf("IncrementCount(%s) error = %v", firstTenant, err)
		}
	}
	mustGetOrCreate(t, repository, secondTenant, sharedPath)

	first := mustGetOrCreate(t, repository, firstTenant, sharedPath)
	second := mustGetOrCreate(t, repository, secondTenant, sharedPath)
	if first.CurrentCount != 3 {
		t.Errorf("%s count = %d, want 3", firstTenant, first.CurrentCount)
	}
	if second.CurrentCount != 0 {
		t.Errorf("%s count = %d, want 0: a tenant must not see another tenant's count", secondTenant, second.CurrentCount)
	}

	secondQuotas, err := repository.GetAll(ctx, secondTenant)
	if err != nil {
		t.Fatalf("GetAll(%s) error = %v", secondTenant, err)
	}
	if len(secondQuotas) != 1 || secondQuotas[0].CurrentCount != 0 {
		t.Fatalf("GetAll(%s) = %+v, want one quota with count 0", secondTenant, secondQuotas)
	}

	for _, tenantID := range []string{firstTenant, secondTenant} {
		path := sqliteQuotaDatabasePath(dataPath, tenantID)
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("tenant database %s is missing: %v", path, err)
		}
	}

	// A third tenant must not see any row and must not be affected either.
	if quotas, err := repository.GetAll(ctx, "tenant-gamma"); err != nil {
		t.Fatalf("GetAll(tenant-gamma) error = %v", err)
	} else if len(quotas) != 0 {
		t.Fatalf("GetAll(tenant-gamma) = %+v, want no quotas", quotas)
	}
}

// TestSQLiteDirectoryQuotaRepositoryGetAllOrdering pins the deterministic order
// (byte order of the directory path, like the Badger key prefix).
func TestSQLiteDirectoryQuotaRepositoryGetAllOrdering(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	// Insertion order differs from the expected order on purpose; the uppercase
	// entry pins the BINARY collation.
	for _, directoryPath := range []string{"/zulu", "/alpha/beta", "/Alpha", "/alpha", "/alpha/beta/gamma"} {
		mustGetOrCreate(t, repository, sqliteQuotaTenant, directoryPath)
	}

	quotas, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	want := []string{"/Alpha", "/alpha", "/alpha/beta", "/alpha/beta/gamma", "/zulu"}
	if len(quotas) != len(want) {
		t.Fatalf("GetAll() returned %d quotas, want %d", len(quotas), len(want))
	}
	for i, quota := range quotas {
		if quota.DirectoryPath != want[i] {
			t.Fatalf("GetAll()[%d].DirectoryPath = %q, want %q (order %v)",
				i, quota.DirectoryPath, want[i], want)
		}
	}
}

// TestSQLiteDirectoryQuotaRepositoryEmptyGetAllIsNotNil matches the Badger
// implementation, which returns an empty slice rather than nil.
func TestSQLiteDirectoryQuotaRepositoryEmptyGetAllIsNotNil(t *testing.T) {
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	quotas, err := repository.GetAll(context.Background(), sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if quotas == nil {
		t.Fatal("GetAll() = nil, want an empty slice")
	}
	if len(quotas) != 0 {
		t.Fatalf("GetAll() = %+v, want no quotas", quotas)
	}
}

// TestSQLiteDirectoryQuotaRepositoryPersistenceAcrossReopen is the restart test:
// counts and settings survive a close of the repository and a fresh open of the
// same data path.
func TestSQLiteDirectoryQuotaRepositoryPersistenceAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()

	first := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	quota := mustGetOrCreate(t, first, sqliteQuotaTenant, "/persistent")
	quota.MaxCount = 12
	quota.Enabled = true
	if err := first.Update(ctx, sqliteQuotaTenant, quota); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := first.IncrementCount(ctx, sqliteQuotaTenant, "/persistent"); err != nil {
			t.Fatalf("IncrementCount() error = %v", err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	second := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	stored := mustGetOrCreate(t, second, sqliteQuotaTenant, "/persistent")
	if stored.CurrentCount != 5 || stored.MaxCount != 12 || !stored.Enabled {
		t.Fatalf("reopened quota = %+v, want count 5, max 12, enabled", stored)
	}
}

// TestSQLiteDirectoryQuotaRepositoryOptimizeOnIdleRepository covers Optimize on
// an empty repository and on one that only has an open handle.
func TestSQLiteDirectoryQuotaRepositoryOptimizeOnIdleRepository(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	if err := repository.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() on an empty repository error = %v", err)
	}

	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/optimized")
	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/optimized"); err != nil {
		t.Fatalf("IncrementCount() error = %v", err)
	}
	if err := repository.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/optimized")
	if quota.CurrentCount != 1 {
		t.Fatalf("count after Optimize() = %d, want 1", quota.CurrentCount)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := repository.Optimize(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Optimize(canceled) error = %v, want context.Canceled", err)
	}
}

// TestSQLiteDirectoryQuotaRepositoryCloseIsIdempotent covers repeated and
// concurrent Close calls, and the closed-repository contract.
func TestSQLiteDirectoryQuotaRepositoryCloseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})
	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/closed")

	const callers = 8

	var group sync.WaitGroup
	errs := make([]error, callers)
	group.Add(callers)
	for i := 0; i < callers; i++ {
		go func(index int) {
			defer group.Done()
			errs[index] = repository.Close()
		}(i)
	}
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close()[%d] error = %v", index, err)
		}
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := repository.openHandleCount(); got != 0 {
		t.Fatalf("open handle count after Close = %d, want 0", got)
	}

	if _, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, "/closed"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("GetOrCreate() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/closed"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("IncrementCount() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
	if err := repository.DecrementCount(ctx, sqliteQuotaTenant, "/closed"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("DecrementCount() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
	if _, err := repository.GetAll(ctx, sqliteQuotaTenant); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("GetAll() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
	if err := repository.Update(ctx, sqliteQuotaTenant, &core.DirectoryQuota{DirectoryPath: "/closed"}); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Update() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
	if err := repository.Optimize(ctx); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Optimize() on a closed repository error = %v, want core.ErrDatabaseError", err)
	}
}

// TestSQLiteDirectoryQuotaRepositorySchemaMatchesDesign reads the physical schema
// back through an independent connection: the table, the index, the timestamp
// representation and the `enabled DEFAULT 0` decision must all be observable.
func TestSQLiteDirectoryQuotaRepositorySchemaMatchesDesign(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/schema")
	if err := repository.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err := sqlite.Open(sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant), sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	type column struct {
		name         string
		columnType   string
		notNull      int
		defaultValue sql.NullString
		primaryKey   int
	}
	want := []column{
		{name: "directory_path", columnType: "TEXT", notNull: 1, primaryKey: 1},
		{name: "current_count", columnType: "INTEGER", notNull: 1, defaultValue: sql.NullString{String: "0", Valid: true}},
		{name: "max_count", columnType: "INTEGER", notNull: 1, defaultValue: sql.NullString{String: "0", Valid: true}},
		{name: "enabled", columnType: "INTEGER", notNull: 1, defaultValue: sql.NullString{String: "0", Valid: true}},
		{name: "last_updated", columnType: "INTEGER", notNull: 1},
		{name: "created_at", columnType: "INTEGER", notNull: 1},
	}

	rows, err := db.QueryContext(ctx, "PRAGMA table_info(quotas)")
	if err != nil {
		t.Fatalf("PRAGMA table_info(quotas) error = %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []column
	for rows.Next() {
		var (
			ordinal int
			current column
		)
		if err := rows.Scan(&ordinal, &current.name, &current.columnType, &current.notNull,
			&current.defaultValue, &current.primaryKey); err != nil {
			t.Fatalf("scan table_info error = %v", err)
		}
		if ordinal != len(got) {
			t.Fatalf("table_info column %q has ordinal %d, want %d", current.name, ordinal, len(got))
		}
		got = append(got, current)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info rows error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("quotas has %d columns, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("quotas column %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	var indexCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_quotas_enabled'`).Scan(&indexCount); err != nil {
		t.Fatalf("index lookup error = %v", err)
	}
	if indexCount != 1 {
		t.Fatalf("idx_quotas_enabled count = %d, want 1", indexCount)
	}

	var schemaVersion string
	if err := db.QueryRowContext(ctx,
		`SELECT value FROM schema_meta WHERE key = 'schema_version'`).Scan(&schemaVersion); err != nil {
		t.Fatalf("schema version lookup error = %v", err)
	}
	if schemaVersion != quotaSchemaVersion {
		t.Fatalf("schema_version = %q, want %q", schemaVersion, quotaSchemaVersion)
	}
}

// TestSQLiteDirectoryQuotaRepositoryOperatorInsertedRowStaysDisabled is the §7.4
// regression test: a row inserted outside the repository without binding enabled
// must read back as a disabled quota, because the DDL default is 0.
func TestSQLiteDirectoryQuotaRepositoryOperatorInsertedRowStaysDisabled(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()

	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/seed")
	if err := repository.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	path := sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant)
	db, err := sqlite.Open(path, sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("sqlite.Open(%s) error = %v", path, err)
	}
	now := timeToNanos(time.Now())
	if _, err := db.ExecContext(ctx,
		`INSERT INTO quotas (directory_path, last_updated, created_at) VALUES (?, ?, ?)`,
		"/operator/inserted", now, now); err != nil {
		_ = db.Close()
		t.Fatalf("operator INSERT error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	reopened := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})
	quota := mustGetOrCreate(t, reopened, sqliteQuotaTenant, "/operator/inserted")
	if quota.Enabled {
		t.Fatal("Enabled = true for an operator-inserted row, want false (enabled DEFAULT 0)")
	}
	if quota.MaxCount != 0 || quota.CurrentCount != 0 {
		t.Fatalf("quota = %+v, want the DDL defaults 0/0", quota)
	}
}

// TestSQLiteDirectoryQuotaRepositoryDirectoryLayout pins the on-disk layout: one
// database per tenant below {DataPath}/{tenantId}/quotas.db, created lazily.
func TestSQLiteDirectoryQuotaRepositoryDirectoryLayout(t *testing.T) {
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})

	if _, err := os.Lstat(filepath.Join(dataPath, sqliteQuotaTenant)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tenant directory exists before first access: %v", err)
	}

	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/layout")
	if _, err := os.Stat(sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant)); err != nil {
		t.Fatalf("tenant database missing after first access: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(dataPath, sqliteQuotaTenant))
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	joined := strings.Join(names, ",")
	if !strings.Contains(joined, quotaDatabaseFileName) {
		t.Fatalf("tenant directory entries = %v, want %s", names, quotaDatabaseFileName)
	}
}
