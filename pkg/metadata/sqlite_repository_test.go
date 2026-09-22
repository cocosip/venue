package metadata

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// traceClock is a deterministic, concurrency-safe clock. It is the seam the
// idle-eviction and timeout tests use instead of sleeping on the wall clock.
type traceClock struct {
	mu  sync.Mutex
	now time.Time
}

// newTraceClock starts a trace clock at instant.
func newTraceClock(instant time.Time) *traceClock {
	return &traceClock{now: instant.UTC()}
}

// Now reports the clock's current instant.
func (c *traceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by delta.
func (c *traceClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

// Set moves the clock to instant.
func (c *traceClock) Set(instant time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = instant.UTC()
}

// newSQLiteFixtureRepository builds a repository in a temporary directory and
// registers shutdown before the directory is removed: t.TempDir cleanup runs
// last, so Close always happens while the directory still exists.
func newSQLiteFixtureRepository(t *testing.T, mutate func(*SQLiteRepositoryOptions)) *SQLiteMetadataRepository {
	t.Helper()

	options := &SQLiteRepositoryOptions{
		DataPath:        t.TempDir(),
		CacheTTL:        time.Minute,
		MaxCacheEntries: 64,
		Sqlite:          sqlite.DefaultOptions(),
	}
	if mutate != nil {
		mutate(options)
	}
	if options.now == nil {
		options.now = time.Now
	}

	repo, err := NewSQLiteMetadataRepository(options)
	if err != nil {
		t.Fatalf("NewSQLiteMetadataRepository() error = %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	concrete, ok := repo.(*SQLiteMetadataRepository)
	if !ok {
		t.Fatalf("repository type = %T, want *SQLiteMetadataRepository", repo)
	}
	return concrete
}

// newSQLiteRepositoryResult builds a repository and returns the constructor's
// own result so a test can assert on a rejected configuration.
func newSQLiteRepositoryResult(t *testing.T, options *SQLiteRepositoryOptions) (core.MetadataRepository, error) {
	t.Helper()
	return NewSQLiteMetadataRepository(options)
}

// sqliteRecord builds a complete record so a create/read round trip compares
// every column rather than only the key.
func sqliteRecord(tenantID string, fileKey string, createdAt time.Time) *core.FileMetadata {
	return &core.FileMetadata{
		FileKey:          fileKey,
		TenantID:         tenantID,
		VolumeID:         "volume-1",
		PhysicalPath:     "files/" + fileKey,
		DirectoryPath:    "/docs",
		FileSize:         int64(len(fileKey)) * 100,
		FileExtension:    ".pdf",
		OriginalFileName: fileKey + ".pdf",
		Status:           core.FileStatusPending,
		RetryCount:       3,
		LastError:        "previous attempt failed",
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
	}
}

// sqliteStoreRecords writes records in order and fails the test on the first
// error.
func sqliteStoreRecords(t *testing.T, repo core.MetadataRepository, tenantID string, records []*core.FileMetadata) {
	t.Helper()
	for _, record := range records {
		if err := repo.AddOrUpdate(context.Background(), record); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", record.FileKey, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Construction and validation
// ---------------------------------------------------------------------------

func TestNewSQLiteMetadataRepositoryRejectsInvalidOptions(t *testing.T) {
	base := func() *SQLiteRepositoryOptions {
		return &SQLiteRepositoryOptions{
			DataPath:                   t.TempDir(),
			Sqlite:                     sqlite.DefaultOptions(),
			CorruptedDatabaseRetention: time.Hour,
		}
	}

	cases := []struct {
		name   string
		mutate func(*SQLiteRepositoryOptions)
	}{
		{name: "empty data path", mutate: func(o *SQLiteRepositoryOptions) { o.DataPath = "  " }},
		{name: "negative cache ttl", mutate: func(o *SQLiteRepositoryOptions) { o.CacheTTL = -time.Second }},
		{name: "negative cache entries", mutate: func(o *SQLiteRepositoryOptions) { o.MaxCacheEntries = -1 }},
		{name: "negative open databases", mutate: func(o *SQLiteRepositoryOptions) { o.MaxOpenDatabases = -1 }},
		{name: "negative idle timeout", mutate: func(o *SQLiteRepositoryOptions) { o.OpenDatabaseIdleTimeout = -time.Second }},
		{name: "negative corruption retention", mutate: func(o *SQLiteRepositoryOptions) { o.CorruptedDatabaseRetention = -1 }},
		{name: "negative backup retention", mutate: func(o *SQLiteRepositoryOptions) { o.BackupRetention = -1 }},
		{name: "auto restore without directory", mutate: func(o *SQLiteRepositoryOptions) { o.AutoRestoreFromBackup = true }},
		{name: "invalid journal mode", mutate: func(o *SQLiteRepositoryOptions) { o.Sqlite.JournalMode = "SIDEWAYS" }},
		{name: "invalid synchronous mode", mutate: func(o *SQLiteRepositoryOptions) { o.Sqlite.SynchronousMode = "SOMETIMES" }},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			options := base()
			testCase.mutate(options)

			repo, err := newSQLiteRepositoryResult(t, options)
			if err == nil {
				t.Fatalf("NewSQLiteMetadataRepository() error = nil, want a rejection")
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("error = %v, want it to wrap core.ErrInvalidArgument", err)
			}
			if repo != nil {
				t.Fatalf("repository = %T, want nil on a rejected configuration", repo)
			}
		})
	}

	if _, err := newSQLiteRepositoryResult(t, nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("nil options error = %v, want core.ErrInvalidArgument", err)
	}
}

func TestSQLiteMetadataRepositoryCreatesTenantDatabaseLazily(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)

	// No tenant has been touched yet, so no database file exists.
	if count := repo.openHandleCount(); count != 0 {
		t.Fatalf("open handles = %d, want 0 before any tenant is touched", count)
	}

	record := sqliteRecord("tenant-a", "file-1", time.Now().UTC())
	if err := repo.AddOrUpdate(context.Background(), record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if count := repo.openHandleCount(); count != 1 {
		t.Fatalf("open handles = %d, want 1 after the first tenant access", count)
	}

	databasePath := repo.databases["tenant-a"].path
	if _, err := os.Stat(databasePath); err != nil {
		t.Fatalf("tenant database %s was not created: %v", databasePath, err)
	}
}

// ---------------------------------------------------------------------------
// CRUD round trips
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryCreateReadUpdateDeleteRoundTrip(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-round-trip"

	createdAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Nanosecond)
	record := sqliteRecord(tenantID, "file-round-trip", createdAt)
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	stored, err := repo.Get(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.FileKey != record.FileKey ||
		stored.TenantID != record.TenantID ||
		stored.VolumeID != record.VolumeID ||
		stored.PhysicalPath != record.PhysicalPath ||
		stored.DirectoryPath != record.DirectoryPath ||
		stored.FileSize != record.FileSize ||
		stored.FileExtension != record.FileExtension ||
		stored.OriginalFileName != record.OriginalFileName ||
		stored.Status != record.Status ||
		stored.RetryCount != record.RetryCount ||
		stored.LastError != record.LastError {
		t.Fatalf("stored record = %+v, want it to match %+v", stored, record)
	}
	if !stored.CreatedAt.Equal(record.CreatedAt) || !stored.UpdatedAt.Equal(record.UpdatedAt) {
		t.Fatalf("stored timestamps = (%s, %s), want (%s, %s)",
			stored.CreatedAt, stored.UpdatedAt, record.CreatedAt, record.UpdatedAt)
	}
	if stored.AvailableForProcessingAt != nil {
		t.Fatalf("AvailableForProcessingAt = %v, want nil for the sentinel 0", stored.AvailableForProcessingAt)
	}
	if stored.ProcessingStartTime != nil || stored.CompletedAt != nil ||
		stored.LastFailedAt != nil || stored.DeadLetteredAt != nil ||
		stored.ReleasedProcessingStartTimeUTC != nil {
		t.Fatalf("nullable timestamps = %+v, want every one nil", stored)
	}

	// An update replaces every column, including the status.
	available := createdAt.Add(30 * time.Minute)
	updated := sqliteRecord(tenantID, record.FileKey, createdAt)
	updated.Status = core.FileStatusPermanentlyFailed
	updated.RetryCount = 9
	updated.LastFailedAt = &available
	updated.LastError = "gave up"
	updated.UpdatedAt = createdAt.Add(time.Minute)
	if err := repo.AddOrUpdate(ctx, updated); err != nil {
		t.Fatalf("AddOrUpdate(update) error = %v", err)
	}

	stored, err = repo.Get(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if stored.Status != core.FileStatusPermanentlyFailed || stored.RetryCount != 9 || stored.LastError != "gave up" {
		t.Fatalf("updated record = %+v, want the second revision", stored)
	}
	if stored.LastFailedAt == nil || !stored.LastFailedAt.Equal(available) {
		t.Fatalf("LastFailedAt = %v, want %s", stored.LastFailedAt, available)
	}

	if err := repo.Delete(ctx, tenantID, record.FileKey); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.Get(ctx, tenantID, record.FileKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() after delete error = %v, want core.ErrFileNotFound", err)
	}

	// Deleting a record that does not exist is not an error.
	if err := repo.Delete(ctx, tenantID, record.FileKey); err != nil {
		t.Fatalf("Delete() of a missing record error = %v, want nil", err)
	}
}

func TestSQLiteMetadataRepositoryImportOperationIDLifecycle(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	record := sqliteRecord("tenant-operation", "file-operation", time.Now().UTC())
	record.ImportOperationID = "watcher-operation-1"

	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	stored, err := repo.GetByImportOperationID(ctx, record.TenantID, record.ImportOperationID)
	if err != nil {
		t.Fatalf("GetByImportOperationID() error = %v", err)
	}
	if stored.FileKey != record.FileKey || stored.ImportOperationID != record.ImportOperationID {
		t.Fatalf("stored operation mapping = (%q, %q), want (%q, %q)",
			stored.FileKey, stored.ImportOperationID, record.FileKey, record.ImportOperationID)
	}

	if err := repo.Delete(ctx, record.TenantID, record.FileKey); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.GetByImportOperationID(ctx, record.TenantID, record.ImportOperationID); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("GetByImportOperationID() after delete error = %v, want ErrFileNotFound", err)
	}
}

func TestSQLiteMetadataRepositoryImportOperationIDSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	options := &SQLiteRepositoryOptions{
		DataPath:        root,
		CacheTTL:        time.Minute,
		MaxCacheEntries: 64,
		Sqlite:          sqlite.DefaultOptions(),
	}

	firstRepo, err := NewSQLiteMetadataRepository(options)
	if err != nil {
		t.Fatalf("NewSQLiteMetadataRepository(first) error = %v", err)
	}
	record := sqliteRecord("tenant-operation-restart", "file-operation-restart", time.Now().UTC())
	record.ImportOperationID = "watcher-operation-restart"
	if err := firstRepo.AddOrUpdate(ctx, record); err != nil {
		_ = firstRepo.Close()
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := firstRepo.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	secondRepo, err := NewSQLiteMetadataRepository(options)
	if err != nil {
		t.Fatalf("NewSQLiteMetadataRepository(second) error = %v", err)
	}
	t.Cleanup(func() { _ = secondRepo.Close() })
	operationRepo, ok := secondRepo.(core.ImportOperationRepository)
	if !ok {
		t.Fatalf("repository type %T does not implement core.ImportOperationRepository", secondRepo)
	}
	stored, err := operationRepo.GetByImportOperationID(ctx, record.TenantID, record.ImportOperationID)
	if err != nil {
		t.Fatalf("GetByImportOperationID() after restart error = %v", err)
	}
	if stored.FileKey != record.FileKey {
		t.Fatalf("file key after restart = %q, want %q", stored.FileKey, record.FileKey)
	}
}

func TestSQLiteMetadataRepositoryMigratesVersionOneForImportOperations(t *testing.T) {
	root := t.TempDir()
	tenantID := "tenant-schema-v1"
	tenantDir := filepath.Join(root, tenantID)
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	db, err := sqlite.Open(filepath.Join(tenantDir, metadataDatabaseFileName), sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("open version-one fixture: %v", err)
	}
	oldFilesDDL := strings.Replace(sqliteFilesTableDDL,
		",\n    import_operation_id             TEXT", "", 1)
	for _, statement := range []string{
		oldFilesDDL,
		sqliteSchemaMetaTableDDL,
		`INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			t.Fatalf("create version-one fixture: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close version-one fixture: %v", err)
	}

	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = root
	})
	record := sqliteRecord(tenantID, "migrated-file", time.Now().UTC())
	record.ImportOperationID = "migrated-operation"
	if err := repo.AddOrUpdate(context.Background(), record); err != nil {
		t.Fatalf("AddOrUpdate() after migration error = %v", err)
	}
	stored, err := repo.GetByImportOperationID(context.Background(), tenantID, record.ImportOperationID)
	if err != nil {
		t.Fatalf("GetByImportOperationID() after migration error = %v", err)
	}
	if stored.FileKey != record.FileKey {
		t.Fatalf("migrated operation file key = %q, want %q", stored.FileKey, record.FileKey)
	}
}

func TestSQLiteMetadataRepositoryUpdateStatusIsUnconditional(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-update-status"

	record := sqliteRecord(tenantID, "file-status", time.Now().UTC())
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	if err := repo.UpdateStatus(ctx, tenantID, record.FileKey, core.FileStatusDeadLettered); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	stored, err := repo.Get(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusDeadLettered {
		t.Fatalf("Status = %s, want DeadLettered", stored.Status)
	}

	if err := repo.UpdateStatus(ctx, tenantID, "missing", core.FileStatusCompleted); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("UpdateStatus(missing) error = %v, want core.ErrFileNotFound", err)
	}
}

func TestSQLiteMetadataRepositoryPerTenantIsolation(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()

	sharedKey := "file-shared-key"
	first := sqliteRecord("tenant-one", sharedKey, time.Now().UTC())
	second := sqliteRecord("tenant-two", sharedKey, time.Now().UTC())
	first.FileSize = 111
	second.FileSize = 222
	sqliteStoreRecords(t, repo, "tenant-one", []*core.FileMetadata{first})
	sqliteStoreRecords(t, repo, "tenant-two", []*core.FileMetadata{second})

	for _, testCase := range []struct {
		tenantID string
		wantSize int64
	}{
		{tenantID: "tenant-one", wantSize: 111},
		{tenantID: "tenant-two", wantSize: 222},
	} {
		stored, err := repo.Get(ctx, testCase.tenantID, sharedKey)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", testCase.tenantID, err)
		}
		if stored.FileSize != testCase.wantSize {
			t.Fatalf("Get(%q).FileSize = %d, want %d", testCase.tenantID, stored.FileSize, testCase.wantSize)
		}
		if stored.TenantID != testCase.tenantID {
			t.Fatalf("Get(%q).TenantID = %q, want %q", testCase.tenantID, stored.TenantID, testCase.tenantID)
		}
	}

	// Deleting in one tenant must not touch the other.
	if err := repo.Delete(ctx, "tenant-one", sharedKey); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.Get(ctx, "tenant-two", sharedKey); err != nil {
		t.Fatalf("Get(tenant-two) after deleting tenant-one error = %v, want the record to survive", err)
	}

	// A batch that mixes tenants is rejected and writes nothing.
	mixed := []*core.FileMetadata{
		sqliteRecord("tenant-one", "file-mixed-a", time.Now().UTC()),
		sqliteRecord("tenant-two", "file-mixed-b", time.Now().UTC()),
	}
	if err := repo.AddOrUpdateBatch(ctx, mixed); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("AddOrUpdateBatch(mixed tenants) error = %v, want core.ErrInvalidArgument", err)
	}
	if _, err := repo.Get(ctx, "tenant-one", "file-mixed-a"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get(file-mixed-a) error = %v, want the rejected batch to write nothing", err)
	}
}

// ---------------------------------------------------------------------------
// Claims, leases, and timeout recovery
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryClaimOrderingAndContention(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-claims"

	base := clock.Now().Add(-time.Hour)
	older := sqliteRecord(tenantID, "file-older", base)
	newer := sqliteRecord(tenantID, "file-newer", base.Add(time.Minute))
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{newer, older})

	claimed, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, older.FileKey)
	if err != nil {
		t.Fatalf("CompareAndTransitionToProcessing() error = %v", err)
	}
	if claimed.Status != core.FileStatusProcessing {
		t.Fatalf("claimed status = %s, want Processing", claimed.Status)
	}
	if claimed.ProcessingStartTime == nil || !claimed.ProcessingStartTime.Equal(clock.Now()) {
		t.Fatalf("ProcessingStartTime = %v, want %s", claimed.ProcessingStartTime, clock.Now())
	}

	// A second claim of the same record is contention, not an infrastructure
	// failure, and the message names the blocking status.
	_, err = repo.CompareAndTransitionToProcessing(ctx, tenantID, older.FileKey)
	if !errors.Is(err, core.ErrFileNotClaimable) {
		t.Fatalf("second claim error = %v, want core.ErrFileNotClaimable", err)
	}
	if errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("second claim error = %v, want it not to be classified as an infrastructure failure", err)
	}
	if !containsSubstring(err.Error(), "Processing") {
		t.Fatalf("second claim error = %q, want it to name the current status", err)
	}

	// The next candidate is the other record, in creation order.
	next, err := repo.GetPendingFiles(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("GetPendingFiles() error = %v", err)
	}
	if len(next) != 1 || next[0].FileKey != newer.FileKey {
		t.Fatalf("GetPendingFiles() = %v, want only %q", fileKeys(next), newer.FileKey)
	}
}

func TestSQLiteMetadataRepositoryClaimRejectsUnavailableAndMissingRecords(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-availability"

	readyAt := clock.Now().Add(time.Hour)
	delayed := sqliteRecord(tenantID, "file-delayed", clock.Now())
	delayed.AvailableForProcessingAt = &readyAt
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{delayed})

	if _, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, delayed.FileKey); !errors.Is(err, core.ErrFileNotClaimable) {
		t.Fatalf("claim of a future-available record error = %v, want core.ErrFileNotClaimable", err)
	}
	if pending, err := repo.GetPendingFiles(ctx, tenantID, 10); err != nil || len(pending) != 0 {
		t.Fatalf("GetPendingFiles() = (%v, %v), want no candidates before the availability instant", fileKeys(pending), err)
	}

	// The comparison is inclusive: exactly at the availability instant the file
	// is claimable.
	clock.Set(readyAt)
	if pending, err := repo.GetPendingFiles(ctx, tenantID, 10); err != nil || len(pending) != 1 {
		t.Fatalf("GetPendingFiles() at the availability instant = (%v, %v), want exactly one candidate", fileKeys(pending), err)
	}
	if _, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, delayed.FileKey); err != nil {
		t.Fatalf("claim at the availability instant error = %v, want success", err)
	}

	if _, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, "file-missing"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("claim of a missing record error = %v, want core.ErrFileNotFound", err)
	}
}

func TestSQLiteMetadataRepositoryConcurrentClaimsHaveExactlyOneWinner(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-concurrent-claims"

	const workers = 16
	record := sqliteRecord(tenantID, "file-contended", time.Now().UTC())
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{record})

	var (
		winners atomic.Int64
		losers  atomic.Int64
		others  atomic.Int64
		start   = make(chan struct{})
		wg      sync.WaitGroup
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, record.FileKey)
			switch {
			case err == nil:
				winners.Add(1)
			case errors.Is(err, core.ErrFileNotClaimable):
				losers.Add(1)
			default:
				others.Add(1)
				t.Errorf("claim error = %v, want nil or core.ErrFileNotClaimable", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners.Load() != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners.Load())
	}
	if losers.Load() != workers-1 {
		t.Fatalf("losers = %d, want %d", losers.Load(), workers-1)
	}
	if others.Load() != 0 {
		t.Fatalf("misclassified failures = %d, want 0", others.Load())
	}
}

func TestSQLiteMetadataRepositoryLeaseMismatchAndDuplicateRelease(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-leases"

	if _, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, ""); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("claim with an empty key error = %v, want core.ErrInvalidArgument", err)
	}

	record := sqliteRecord(tenantID, "file-lease", clock.Now().Add(-time.Minute))
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{record})

	claimed, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("claim error = %v", err)
	}
	lease := core.FileProcessingLease{
		TenantID:               tenantID,
		FileKey:                record.FileKey,
		ProcessingStartTimeUTC: *claimed.ProcessingStartTime,
	}

	// An unknown lease is rejected and reports the active state.
	unknown := lease
	unknown.ProcessingStartTimeUTC = lease.ProcessingStartTimeUTC.Add(time.Second)
	_, err = repo.CompareAndUpdateProcessing(ctx, unknown, func(*core.FileMetadata) error { return nil })
	var mismatch *core.FileProcessingLeaseMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("unknown lease error = %v, want *core.FileProcessingLeaseMismatchError", err)
	}
	if mismatch.ActualStatus == nil || *mismatch.ActualStatus != core.FileStatusProcessing {
		t.Fatalf("mismatch.ActualStatus = %v, want Processing", mismatch.ActualStatus)
	}
	if mismatch.ActualProcessingStartTimeUTC == nil || !mismatch.ActualProcessingStartTimeUTC.Equal(lease.ProcessingStartTimeUTC) {
		t.Fatalf("mismatch.ActualProcessingStartTimeUTC = %v, want %s", mismatch.ActualProcessingStartTimeUTC, lease.ProcessingStartTimeUTC)
	}

	// A missing record is a lease mismatch with no actual state.
	missingLease := core.FileProcessingLease{
		TenantID:               tenantID,
		FileKey:                "file-missing",
		ProcessingStartTimeUTC: lease.ProcessingStartTimeUTC,
	}
	_, err = repo.CompareAndUpdateProcessing(ctx, missingLease, func(*core.FileMetadata) error { return nil })
	mismatch = nil
	if !errors.As(err, &mismatch) {
		t.Fatalf("missing record error = %v, want *core.FileProcessingLeaseMismatchError", err)
	}
	if mismatch.ActualStatus != nil {
		t.Fatalf("mismatch.ActualStatus = %v, want nil for a missing record", mismatch.ActualStatus)
	}

	// A completion carrying the active lease succeeds and records the release.
	completed, err := repo.CompareAndUpdateProcessing(ctx, lease, func(metadata *core.FileMetadata) error {
		finished := clock.Now()
		metadata.Status = core.FileStatusCompleted
		metadata.CompletedAt = &finished
		metadata.ProcessingStartTime = nil
		return nil
	})
	if err != nil {
		t.Fatalf("completion error = %v", err)
	}
	if completed.Status != core.FileStatusCompleted {
		t.Fatalf("completed status = %s, want Completed", completed.Status)
	}
	if completed.ReleasedProcessingStartTimeUTC == nil || !completed.ReleasedProcessingStartTimeUTC.Equal(lease.ProcessingStartTimeUTC) {
		t.Fatalf("ReleasedProcessingStartTimeUTC = %v, want %s",
			completed.ReleasedProcessingStartTimeUTC, lease.ProcessingStartTimeUTC)
	}

	// Releasing the same lease again is an idempotent success with no write.
	released, err := repo.CompareAndUpdateProcessing(ctx, lease, func(metadata *core.FileMetadata) error {
		metadata.Status = core.FileStatusPermanentlyFailed
		return nil
	})
	if err != nil {
		t.Fatalf("repeated release error = %v, want an idempotent success", err)
	}
	if released.Status != core.FileStatusCompleted {
		t.Fatalf("repeated release status = %s, want the stored Completed record untouched", released.Status)
	}

	stored, err := repo.Get(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusCompleted {
		t.Fatalf("stored status = %s, want the repeated release to write nothing", stored.Status)
	}

	// A callback that changes identity is rejected.
	reclaimSource := sqliteRecord(tenantID, "file-identity", clock.Now().Add(-time.Minute))
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{reclaimSource})
	reclaimed, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, reclaimSource.FileKey)
	if err != nil {
		t.Fatalf("reclaim error = %v", err)
	}
	reclaimLease := core.FileProcessingLease{
		TenantID:               tenantID,
		FileKey:                reclaimSource.FileKey,
		ProcessingStartTimeUTC: *reclaimed.ProcessingStartTime,
	}
	_, err = repo.CompareAndUpdateProcessing(ctx, reclaimLease, func(metadata *core.FileMetadata) error {
		metadata.FileKey = "somewhere-else"
		return nil
	})
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("identity-changing update error = %v, want core.ErrInvalidArgument", err)
	}
}

func TestSQLiteMetadataRepositoryTimeoutRecoveryDoesNotClobberNewerClaim(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-timeout"
	timeout := 5 * time.Minute

	record := sqliteRecord(tenantID, "file-timeout", clock.Now().Add(-time.Hour))
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{record})

	claimed, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("claim error = %v", err)
	}
	workerLease := core.FileProcessingLease{
		TenantID:               tenantID,
		FileKey:                record.FileKey,
		ProcessingStartTimeUTC: *claimed.ProcessingStartTime,
	}

	if timedOut, err := repo.GetTimedOutProcessingFiles(ctx, tenantID, timeout); err != nil || len(timedOut) != 0 {
		t.Fatalf("GetTimedOutProcessingFiles() = (%v, %v), want no candidate before the timeout", fileKeys(timedOut), err)
	}

	// Move the clock past the timeout and reclaim through the lease-checked path.
	clock.Advance(timeout + time.Minute)
	timedOut, err := repo.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
	if err != nil {
		t.Fatalf("GetTimedOutProcessingFiles() error = %v", err)
	}
	if len(timedOut) != 1 || timedOut[0].FileKey != record.FileKey {
		t.Fatalf("timed-out candidates = %v, want %q", fileKeys(timedOut), record.FileKey)
	}

	recovered, err := repo.CompareAndUpdateProcessing(ctx, workerLease, func(metadata *core.FileMetadata) error {
		available := clock.Now()
		metadata.Status = core.FileStatusPending
		metadata.ProcessingStartTime = nil
		metadata.AvailableForProcessingAt = &available
		return nil
	})
	if err != nil {
		t.Fatalf("timeout reclaim error = %v", err)
	}
	if recovered.Status != core.FileStatusPending {
		t.Fatalf("recovered status = %s, want Pending", recovered.Status)
	}

	// A new worker claims the record and gets a new lease.
	newClaim, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("second claim error = %v", err)
	}
	newLease := core.FileProcessingLease{
		TenantID:               tenantID,
		FileKey:                record.FileKey,
		ProcessingStartTimeUTC: *newClaim.ProcessingStartTime,
	}
	if newLease.ProcessingStartTimeUTC.Equal(workerLease.ProcessingStartTimeUTC) {
		t.Fatalf("second claim reused the first lease %s", newLease.ProcessingStartTimeUTC)
	}

	// The stale worker can no longer complete, and the newer claim is untouched.
	_, err = repo.CompareAndUpdateProcessing(ctx, workerLease, func(metadata *core.FileMetadata) error {
		metadata.Status = core.FileStatusCompleted
		return nil
	})
	var mismatch *core.FileProcessingLeaseMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("stale worker error = %v, want *core.FileProcessingLeaseMismatchError", err)
	}
	if mismatch.ActualProcessingStartTimeUTC == nil || !mismatch.ActualProcessingStartTimeUTC.Equal(newLease.ProcessingStartTimeUTC) {
		t.Fatalf("reported active lease = %v, want %s", mismatch.ActualProcessingStartTimeUTC, newLease.ProcessingStartTimeUTC)
	}

	stored, err := repo.Get(ctx, tenantID, record.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusProcessing {
		t.Fatalf("stored status = %s, want the newer claim to survive", stored.Status)
	}
	if stored.ProcessingStartTime == nil || !stored.ProcessingStartTime.Equal(newLease.ProcessingStartTimeUTC) {
		t.Fatalf("stored processing start = %v, want %s", stored.ProcessingStartTime, newLease.ProcessingStartTimeUTC)
	}
}

// ---------------------------------------------------------------------------
// Ordering and paging
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryQueueOrderAndPaging(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-paging"

	base := clock.Now().Add(-time.Hour)
	order := []struct {
		key       string
		available *time.Time
		createdAt time.Time
	}{
		{key: "file-2", createdAt: base.Add(2 * time.Minute)},
		{key: "file-1", createdAt: base.Add(time.Minute)},
		{key: "file-3", createdAt: base.Add(3 * time.Minute)},
	}

	for _, entry := range order {
		record := sqliteRecord(tenantID, entry.key, entry.createdAt)
		record.AvailableForProcessingAt = entry.available
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", entry.key, err)
		}
	}

	// A mix of statuses must not leak into a status scan.
	completed := sqliteRecord(tenantID, "file-completed", base.Add(4*time.Minute))
	completed.Status = core.FileStatusCompleted
	if err := repo.AddOrUpdate(ctx, completed); err != nil {
		t.Fatalf("AddOrUpdate(completed) error = %v", err)
	}

	pending, err := repo.GetPendingFiles(ctx, tenantID, 0)
	if err != nil {
		t.Fatalf("GetPendingFiles() error = %v", err)
	}
	wantOrder := []string{"file-1", "file-2", "file-3"}
	if got := fileKeys(pending); !equalStrings(got, wantOrder) {
		t.Fatalf("GetPendingFiles() order = %v, want %v", got, wantOrder)
	}

	byStatus, err := repo.GetByStatus(ctx, tenantID, core.FileStatusPending, 10)
	if err != nil {
		t.Fatalf("GetByStatus() error = %v", err)
	}
	if got := fileKeys(byStatus); !equalStrings(got, wantOrder) {
		t.Fatalf("GetByStatus() order = %v, want %v", got, wantOrder)
	}

	limited, err := repo.GetByStatus(ctx, tenantID, core.FileStatusPending, 2)
	if err != nil {
		t.Fatalf("GetByStatus(limit 2) error = %v", err)
	}
	if got := fileKeys(limited); !equalStrings(got, []string{"file-1", "file-2"}) {
		t.Fatalf("GetByStatus(limit 2) = %v, want the first two in queue order", got)
	}

	reader, ok := core.MetadataRepository(repo).(core.StatusPageReader)
	if !ok {
		t.Fatalf("repository %T does not implement core.StatusPageReader", repo)
	}

	var (
		collected []string
		cursor    string
		pages     int
	)
	for {
		page, pageErr := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, cursor, 2)
		if pageErr != nil {
			t.Fatalf("GetByStatusPage() error = %v", pageErr)
		}
		pages++
		collected = append(collected, fileKeys(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		if len(page.Records) != 2 {
			t.Fatalf("page with a continuation token carried %d records, want a full page", len(page.Records))
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatalf("paging did not terminate after %d pages", pages)
		}
	}
	if !equalStrings(collected, wantOrder) {
		t.Fatalf("paged scan = %v, want %v", collected, wantOrder)
	}
	if pages != 2 {
		t.Fatalf("pages = %d, want 2 (2 + 1 records)", pages)
	}

	// A limit that exactly divides the set exhausts the scan on the following
	// empty page, which is the contract's empty-cursor completion.
	page, err := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, "", 3)
	if err != nil {
		t.Fatalf("GetByStatusPage(limit 3) error = %v", err)
	}
	if len(page.Records) != 3 || page.NextCursor == "" {
		t.Fatalf("full page = %d records, cursor %q, want 3 records and a token", len(page.Records), page.NextCursor)
	}
	page, err = reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, page.NextCursor, 3)
	if err != nil {
		t.Fatalf("GetByStatusPage(tail) error = %v", err)
	}
	if len(page.Records) != 0 || page.NextCursor != "" {
		t.Fatalf("tail page = %d records, cursor %q, want an empty page with an empty cursor",
			len(page.Records), page.NextCursor)
	}
}

func TestSQLiteMetadataRepositoryPagingOrdersByAvailabilityThenArrival(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-paging-order"

	base := clock.Now().Add(-time.Hour)
	early := base.Add(10 * time.Minute)
	late := base.Add(20 * time.Minute)

	records := []*core.FileMetadata{
		func() *core.FileMetadata {
			record := sqliteRecord(tenantID, "file-b", base.Add(5*time.Minute))
			record.AvailableForProcessingAt = &late
			return record
		}(),
		func() *core.FileMetadata {
			record := sqliteRecord(tenantID, "file-a", base.Add(9*time.Minute))
			record.AvailableForProcessingAt = &early
			return record
		}(),
		func() *core.FileMetadata {
			record := sqliteRecord(tenantID, "file-c", base.Add(time.Minute))
			record.AvailableForProcessingAt = &early
			return record
		}(),
	}
	sqliteStoreRecords(t, repo, tenantID, records)

	reader := core.MetadataRepository(repo).(core.StatusPageReader)
	want := []string{"file-c", "file-a", "file-b"}

	page, err := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, "", 10)
	if err != nil {
		t.Fatalf("GetByStatusPage() error = %v", err)
	}
	if got := fileKeys(page.Records); !equalStrings(got, want) {
		t.Fatalf("paged order = %v, want %v", got, want)
	}

	// The same order must hold when the page boundary falls inside the group.
	var collected []string
	cursor := ""
	for {
		single, singleErr := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, cursor, 1)
		if singleErr != nil {
			t.Fatalf("GetByStatusPage(limit 1) error = %v", singleErr)
		}
		collected = append(collected, fileKeys(single.Records)...)
		if single.NextCursor == "" {
			break
		}
		cursor = single.NextCursor
	}
	if !equalStrings(collected, want) {
		t.Fatalf("single-record paging = %v, want %v", collected, want)
	}
}

func TestSQLiteMetadataRepositoryPagingRejectsBadArguments(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	reader := core.MetadataRepository(repo).(core.StatusPageReader)

	if _, err := reader.GetByStatusPage(ctx, "", core.FileStatusPending, "", 1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("empty tenant error = %v, want core.ErrInvalidArgument", err)
	}
	if _, err := reader.GetByStatusPage(ctx, "tenant", core.FileStatusPending, "", 0); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("non-positive limit error = %v, want core.ErrInvalidArgument", err)
	}
	if _, err := reader.GetByStatusPage(ctx, "tenant", core.FileStatusPending, "not-a-cursor", 1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("malformed cursor error = %v, want core.ErrInvalidArgument", err)
	}

	// A token minted with a different payload version is rejected, not
	// reinterpreted.
	foreign := encodeSQLiteStatusPageCursor(statusPageCursor{available: 1, created: 2, fileKey: "k"})
	decoded, err := decodeSQLiteStatusPageCursor(foreign)
	if err != nil {
		t.Fatalf("decodeSQLiteStatusPageCursor() error = %v", err)
	}
	if decoded.available != 1 || decoded.created != 2 || decoded.fileKey != "k" {
		t.Fatalf("decoded cursor = %+v, want the encoded values", decoded)
	}

	for _, version := range []byte{0x00, 0x02, 0xff} {
		payload := append([]byte{version}, make([]byte, statusPageCursorHeaderSize-1)...)
		token := base64.RawURLEncoding.EncodeToString(payload)
		if _, err := decodeSQLiteStatusPageCursor(token); err == nil {
			t.Fatalf("cursor version %d decoded without error, want a rejection", version)
		}
	}
}

// TestSQLiteMetadataRepositorySmallCacheDoesNotShortenStatusQueries keeps the
// bounded active cache from becoming a query source: a status scan reads the
// database, so a store with more records than the cache capacity still returns
// every record in index order.
func TestSQLiteMetadataRepositorySmallCacheDoesNotShortenStatusQueries(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.MaxCacheEntries = 2
	})
	ctx := context.Background()
	const tenantID = "tenant-small-cache"
	const total = 5

	base := time.Now().UTC().Add(-time.Hour)
	records := make([]*core.FileMetadata, 0, total)
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("cached-%02d", i)
		records = append(records, sqliteRecord(tenantID, key, base.Add(time.Duration(i)*time.Second)))
		want = append(want, key)
	}
	sqliteStoreRecords(t, repo, tenantID, records)

	// The cache is deliberately smaller than the stored set.
	if size := len(repo.cache.entries); size > 2 {
		t.Fatalf("cache holds %d entries, want at most the configured 2", size)
	}

	unlimited, err := repo.GetByStatus(ctx, tenantID, core.FileStatusPending, 0)
	if err != nil {
		t.Fatalf("GetByStatus(unlimited) error = %v", err)
	}
	if got := fileKeys(unlimited); !equalStrings(got, want) {
		t.Fatalf("GetByStatus(unlimited) = %v, want %v", got, want)
	}

	limited, err := repo.GetByStatus(ctx, tenantID, core.FileStatusPending, 3)
	if err != nil {
		t.Fatalf("GetByStatus(limit 3) error = %v", err)
	}
	if got := fileKeys(limited); !equalStrings(got, want[:3]) {
		t.Fatalf("GetByStatus(limit 3) = %v, want %v", got, want[:3])
	}
}

// TestSQLiteMetadataRepositoryPagingCursorResumesAfterTheLastRecord pins the
// opaque-token contract: the token names the position of the last returned
// record, so a caller that keeps deleting returned records still resumes after
// them.
func TestSQLiteMetadataRepositoryPagingCursorResumesAfterTheLastRecord(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	const tenantID = "tenant-paging-cursor"

	base := time.Now().UTC().Add(-time.Hour)
	records := make([]*core.FileMetadata, 0, 12)
	for i := 0; i < 12; i++ {
		records = append(records, sqliteRecord(tenantID, fmt.Sprintf("file-%04d", i), base.Add(time.Duration(i)*time.Millisecond)))
	}
	sqliteStoreRecords(t, repo, tenantID, records)

	reader := core.MetadataRepository(repo).(core.StatusPageReader)
	first, err := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, "", 5)
	if err != nil {
		t.Fatalf("GetByStatusPage() error = %v", err)
	}
	if len(first.Records) != 5 {
		t.Fatalf("records = %d, want 5", len(first.Records))
	}
	if first.NextCursor == "" {
		t.Fatal("NextCursor is empty after a full page, want a continuation token")
	}

	last := first.Records[len(first.Records)-1]
	position, err := decodeSQLiteStatusPageCursor(first.NextCursor)
	if err != nil {
		t.Fatalf("decodeSQLiteStatusPageCursor() error = %v", err)
	}
	want := statusPageCursor{
		available: availableNanos(last.AvailableForProcessingAt),
		created:   last.CreatedAt.UTC().UnixNano(),
		fileKey:   last.FileKey,
	}
	if position != want {
		t.Fatalf("decoded cursor = %+v, want the last returned record %+v", position, want)
	}

	// Deleting every returned record and resuming must not skip or repeat any
	// record that is still there.
	seen := make([]string, 0, len(records))
	cursor := first.NextCursor
	for _, record := range first.Records {
		if err := repo.Delete(ctx, tenantID, record.FileKey); err != nil {
			t.Fatalf("Delete(%s) error = %v", record.FileKey, err)
		}
	}
	for page := 0; ; page++ {
		if page > len(records) {
			t.Fatalf("paging loop did not terminate after %d pages; cursor = %q", page, cursor)
		}
		result, pageErr := reader.GetByStatusPage(ctx, tenantID, core.FileStatusPending, cursor, 5)
		if pageErr != nil {
			t.Fatalf("GetByStatusPage() error = %v", pageErr)
		}
		for _, record := range result.Records {
			seen = append(seen, record.FileKey)
		}
		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	wantKeys := make([]string, 0, len(records)-5)
	for _, record := range records[5:] {
		wantKeys = append(wantKeys, record.FileKey)
	}
	if !slices.Equal(seen, wantKeys) {
		t.Fatalf("resumed scan = %v, want %v", seen, wantKeys)
	}
}

// TestSQLiteMetadataRepositoryPagingSurvivesDeletingReturnedRecords reproduces
// the CleanupCompletedFiles loop: every page is deleted right after it is read,
// and the keyset cursor must still visit every remaining record exactly once.
func TestSQLiteMetadataRepositoryPagingSurvivesDeletingReturnedRecords(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	const (
		tenantID = "tenant-paging-delete"
		total    = 25
		limit    = 7
	)

	base := time.Now().UTC().Add(-time.Hour)
	records := make([]*core.FileMetadata, 0, total)
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("file-%04d", i)
		record := sqliteRecord(tenantID, key, base.Add(time.Duration(i)*time.Millisecond))
		record.Status = core.FileStatusCompleted
		records = append(records, record)
		want = append(want, key)
	}
	sqliteStoreRecords(t, repo, tenantID, records)

	reader := core.MetadataRepository(repo).(core.StatusPageReader)
	seen := make(map[string]int, total)
	order := make([]string, 0, total)
	cursor := ""
	for page := 0; ; page++ {
		if page > total {
			t.Fatalf("paging loop did not terminate after %d pages; cursor = %q", page, cursor)
		}

		result, err := reader.GetByStatusPage(ctx, tenantID, core.FileStatusCompleted, cursor, limit)
		if err != nil {
			t.Fatalf("page %d: GetByStatusPage() error = %v", page, err)
		}
		for _, record := range result.Records {
			seen[record.FileKey]++
			order = append(order, record.FileKey)
			if err := repo.Delete(ctx, tenantID, record.FileKey); err != nil {
				t.Fatalf("Delete(%s) error = %v", record.FileKey, err)
			}
		}
		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	if len(seen) != total {
		t.Fatalf("visited %d records, want %d; seen = %v", len(seen), total, seen)
	}
	for _, key := range want {
		if seen[key] != 1 {
			t.Fatalf("record %s visited %d times, want exactly once", key, seen[key])
		}
	}
	if !slices.Equal(order, want) {
		t.Fatalf("delete-as-you-page scan = %v, want %v", order, want)
	}

	final, err := reader.GetByStatusPage(ctx, tenantID, core.FileStatusCompleted, "", limit)
	if err != nil {
		t.Fatalf("final GetByStatusPage() error = %v", err)
	}
	if len(final.Records) != 0 || final.NextCursor != "" {
		t.Fatalf("final page = {%d records, cursor %q}, want an empty complete page", len(final.Records), final.NextCursor)
	}
}

// ---------------------------------------------------------------------------
// Batch atomicity and statistics
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryBatchOperationsAreAtomic(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-batch"

	good := []*core.FileMetadata{
		sqliteRecord(tenantID, "file-batch-a", time.Now().UTC()),
		sqliteRecord(tenantID, "file-batch-b", time.Now().UTC()),
	}
	if err := repo.AddOrUpdateBatch(ctx, good); err != nil {
		t.Fatalf("AddOrUpdateBatch() error = %v", err)
	}
	for _, record := range good {
		if _, err := repo.Get(ctx, tenantID, record.FileKey); err != nil {
			t.Fatalf("Get(%q) error = %v", record.FileKey, err)
		}
	}

	// One invalid entry rejects the whole batch and writes nothing.
	bad := []*core.FileMetadata{
		sqliteRecord(tenantID, "file-batch-c", time.Now().UTC()),
		nil,
	}
	if err := repo.AddOrUpdateBatch(ctx, bad); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("AddOrUpdateBatch(nil entry) error = %v, want core.ErrInvalidArgument", err)
	}
	if _, err := repo.Get(ctx, tenantID, "file-batch-c"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get(file-batch-c) error = %v, want the rejected batch to write nothing", err)
	}

	if err := repo.DeleteBatch(ctx, tenantID, []string{"file-batch-a", "file-batch-b"}); err != nil {
		t.Fatalf("DeleteBatch() error = %v", err)
	}
	for _, record := range good {
		if _, err := repo.Get(ctx, tenantID, record.FileKey); !errors.Is(err, core.ErrFileNotFound) {
			t.Fatalf("Get(%q) after DeleteBatch error = %v, want core.ErrFileNotFound", record.FileKey, err)
		}
	}

	// One invalid key rejects the whole delete batch.
	if err := repo.DeleteBatch(ctx, tenantID, []string{"a", ""}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("DeleteBatch(empty key) error = %v, want core.ErrInvalidArgument", err)
	}

	if err := repo.AddOrUpdateBatch(ctx, nil); err != nil {
		t.Fatalf("AddOrUpdateBatch(nil) error = %v, want nil", err)
	}
	if err := repo.DeleteBatch(ctx, tenantID, nil); err != nil {
		t.Fatalf("DeleteBatch(nil) error = %v, want nil", err)
	}
}

func TestSQLiteMetadataRepositoryRecordsPersistedBatchStatistics(t *testing.T) {
	recorder := &sqliteStatisticsRecorder{}
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.StatisticsRecorder = recorder
	})
	ctx := context.Background()
	tenantID := "tenant-statistics"

	record := sqliteRecord(tenantID, "file-stats", time.Now().UTC())
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := repo.AddOrUpdateBatch(ctx, []*core.FileMetadata{
		sqliteRecord(tenantID, "file-stats-a", time.Now().UTC()),
		sqliteRecord(tenantID, "file-stats-b", time.Now().UTC()),
	}); err != nil {
		t.Fatalf("AddOrUpdateBatch() error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, tenantID, record.FileKey, core.FileStatusCompleted); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if err := repo.DeleteBatch(ctx, tenantID, []string{"file-stats-a", "file-stats-b"}); err != nil {
		t.Fatalf("DeleteBatch() error = %v", err)
	}

	batches, operations := recorder.totals()
	if batches != 4 {
		t.Fatalf("recorded batches = %d, want 4 committed write paths", batches)
	}
	if operations != 6 {
		t.Fatalf("recorded operations = %d, want 1 + 2 + 1 + 2", operations)
	}
}

// ---------------------------------------------------------------------------
// Cache behaviour
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryCacheKeepsTheNewestRevision(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-cache"

	base := time.Now().UTC().Add(-time.Hour)
	current := sqliteRecord(tenantID, "file-cache", base)
	current.Status = core.FileStatusProcessing
	current.UpdatedAt = base.Add(10 * time.Minute)
	current.FileSize = 999
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{current})

	if _, err := repo.Get(ctx, tenantID, current.FileKey); err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// A stale revision must not replace the newer cached record.
	stale := *current
	stale.FileSize = 111
	stale.UpdatedAt = base.Add(time.Minute)
	repo.cache.set(&stale)

	cached, err := repo.Get(ctx, tenantID, current.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if cached.FileSize != 999 {
		t.Fatalf("cached FileSize = %d, want the newer 999", cached.FileSize)
	}

	// A newer revision does publish.
	fresher := *current
	fresher.FileSize = 222
	fresher.UpdatedAt = base.Add(20 * time.Minute)
	repo.cache.set(&fresher)
	cached, err = repo.Get(ctx, tenantID, current.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if cached.FileSize != 222 {
		t.Fatalf("cached FileSize = %d, want the newer 222", cached.FileSize)
	}

	// An inactive status is never cached, so the next read reports the database.
	completed := *current
	completed.Status = core.FileStatusCompleted
	completed.FileSize = 333
	completed.UpdatedAt = base.Add(30 * time.Minute)
	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{&completed})
	if cached := repo.cache.get(tenantID, current.FileKey); cached != nil {
		t.Fatalf("cache entry = %+v, want an inactive status to be evicted", cached)
	}
	cached, err = repo.Get(ctx, tenantID, current.FileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if cached.Status != core.FileStatusCompleted || cached.FileSize != 333 {
		t.Fatalf("Get() = %+v, want the completed revision from the database", cached)
	}
}

// ---------------------------------------------------------------------------
// Handle bound and idle eviction
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryBoundsOpenHandles(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.MaxOpenDatabases = 1
	})
	ctx := context.Background()

	for _, tenantID := range []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4"} {
		record := sqliteRecord(tenantID, "file-1", time.Now().UTC())
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", tenantID, err)
		}
		if count := repo.openHandleCount(); count > 1 {
			t.Fatalf("open handles = %d after touching %q, want at most 1", count, tenantID)
		}
	}

	// The bound held, and every tenant is still reachable: an evicted handle is
	// reopened on the next access.
	for _, tenantID := range []string{"tenant-1", "tenant-4"} {
		if _, err := repo.Get(ctx, tenantID, "file-1"); err != nil {
			t.Fatalf("Get(%q) after eviction error = %v, want the handle to be reopened", tenantID, err)
		}
	}
}

func TestSQLiteMetadataRepositoryEvictsIdleHandles(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
		options.OpenDatabaseIdleTimeout = time.Minute
	})
	ctx := context.Background()

	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-idle", "file-1", clock.Now())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if count := repo.openHandleCount(); count != 1 {
		t.Fatalf("open handles = %d, want 1", count)
	}

	// Nothing is evicted while the handle is still fresh.
	clock.Advance(30 * time.Second)
	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-other", "file-1", clock.Now())); err != nil {
		t.Fatalf("AddOrUpdate(other) error = %v", err)
	}
	if count := repo.openHandleCount(); count != 2 {
		t.Fatalf("open handles = %d, want both handles kept while they are fresh", count)
	}

	// Once idle, the least recently used handle is closed by the next access.
	clock.Advance(2 * time.Minute)
	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-third", "file-1", clock.Now())); err != nil {
		t.Fatalf("AddOrUpdate(third) error = %v", err)
	}
	if count := repo.openHandleCount(); count != 1 {
		t.Fatalf("open handles = %d, want only the active handle after eviction", count)
	}
	if _, ok := repo.databases["tenant-third"]; !ok {
		t.Fatalf("the actively used tenant was evicted; handle table = %v", repo.databases)
	}
}

// ---------------------------------------------------------------------------
// Concurrency and lifecycle
// ---------------------------------------------------------------------------

func TestSQLiteMetadataRepositoryConcurrentReadsAndWrites(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.MaxOpenDatabases = 0
	})
	ctx := context.Background()

	const (
		tenants   = 4
		perTenant = 8
		readers   = 8
	)

	tenantIDs := make([]string, 0, tenants)
	for i := 0; i < tenants; i++ {
		tenantID := fmt.Sprintf("tenant-concurrent-%d", i)
		tenantIDs = append(tenantIDs, tenantID)
		records := make([]*core.FileMetadata, 0, perTenant)
		for j := 0; j < perTenant; j++ {
			records = append(records, sqliteRecord(tenantID, fmt.Sprintf("file-%02d", j), time.Now().UTC()))
		}
		if err := repo.AddOrUpdateBatch(ctx, records); err != nil {
			t.Fatalf("AddOrUpdateBatch(%q) error = %v", tenantID, err)
		}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			for round := 0; round < 5; round++ {
				tenantID := tenantIDs[index%len(tenantIDs)]
				if _, err := repo.GetByStatus(ctx, tenantID, core.FileStatusPending, 4); err != nil {
					t.Errorf("GetByStatus() error = %v", err)
					return
				}
				if _, err := repo.GetPendingFiles(ctx, tenantID, 4); err != nil {
					t.Errorf("GetPendingFiles() error = %v", err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < tenants; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			tenantID := tenantIDs[index]
			for j := 0; j < perTenant; j++ {
				fileKey := fmt.Sprintf("file-%02d", j)
				if _, err := repo.CompareAndTransitionToProcessing(ctx, tenantID, fileKey); err != nil {
					if errors.Is(err, core.ErrFileNotClaimable) {
						continue
					}
					t.Errorf("CompareAndTransitionToProcessing(%q, %q) error = %v", tenantID, fileKey, err)
					return
				}
			}
		}(i)
	}

	close(start)
	wg.Wait()

	// Every record was claimed exactly once.
	for _, tenantID := range tenantIDs {
		processing, err := repo.GetByStatus(ctx, tenantID, core.FileStatusProcessing, 0)
		if err != nil {
			t.Fatalf("GetByStatus(%q) error = %v", tenantID, err)
		}
		if len(processing) != perTenant {
			t.Fatalf("processing records for %q = %d, want %d", tenantID, len(processing), perTenant)
		}
	}
}

func TestSQLiteMetadataRepositoryCloseIsIdempotentAndRejectsWork(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()

	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-close", "file-1", time.Now().UTC())); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := 0; i < len(errs); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			errs[index] = repo.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Close() #%d error = %v, want nil", i, err)
		}
	}
	if count := repo.openHandleCount(); count != 0 {
		t.Fatalf("open handles after Close = %d, want 0", count)
	}

	if _, err := repo.Get(ctx, "tenant-close", "file-1"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Get() after Close error = %v, want core.ErrDatabaseError", err)
	}
	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-close", "file-2", time.Now().UTC())); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("AddOrUpdate() after Close error = %v, want core.ErrDatabaseError", err)
	}
	if _, err := repo.CompareAndTransitionToProcessing(ctx, "tenant-close", "file-1"); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("claim after Close error = %v, want core.ErrDatabaseError", err)
	}
	if _, err := repo.GetByStatusPage(ctx, "tenant-close", core.FileStatusPending, "", 1); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("GetByStatusPage() after Close error = %v, want core.ErrDatabaseError", err)
	}
	if _, err := repo.Backup(ctx, nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("Backup(nil writer) error = %v, want core.ErrInvalidArgument", err)
	}
	if err := repo.Optimize(ctx); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Optimize() after Close error = %v, want core.ErrDatabaseError", err)
	}
}

func TestSQLiteMetadataRepositoryQueriesHonorContextCancellation(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	tenantID := "tenant-cancel"

	sqliteStoreRecords(t, repo, tenantID, []*core.FileMetadata{
		sqliteRecord(tenantID, "file-1", time.Now().UTC()),
	})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := repo.Get(canceled, tenantID, "file-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get() with a canceled context error = %v, want context.Canceled", err)
	}
	if _, err := repo.GetPendingFiles(canceled, tenantID, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetPendingFiles() with a canceled context error = %v, want context.Canceled", err)
	}
	if _, err := repo.GetByStatusPage(canceled, tenantID, core.FileStatusPending, "", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetByStatusPage() with a canceled context error = %v, want context.Canceled", err)
	}
	if _, err := repo.CompareAndTransitionToProcessing(canceled, tenantID, "file-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("claim with a canceled context error = %v, want context.Canceled", err)
	}
	if err := repo.Delete(canceled, tenantID, "file-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() with a canceled context error = %v, want context.Canceled", err)
	}
	if err := repo.Optimize(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Optimize() with a canceled context error = %v, want context.Canceled", err)
	}
	if _, err := repo.Backup(canceled, ioDiscard{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Backup() with a canceled context error = %v, want context.Canceled", err)
	}
}

func TestSQLiteMetadataRepositoryEmptyArgumentsAreRejected(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()

	checks := []struct {
		name string
		call func() error
	}{
		{name: "AddOrUpdate nil", call: func() error { return repo.AddOrUpdate(ctx, nil) }},
		{name: "AddOrUpdate empty key", call: func() error {
			record := sqliteRecord("tenant", "", time.Now().UTC())
			return repo.AddOrUpdate(ctx, record)
		}},
		{name: "Get empty tenant", call: func() error {
			_, err := repo.Get(ctx, "", "file")
			return err
		}},
		{name: "Get empty key", call: func() error {
			_, err := repo.Get(ctx, "tenant", "")
			return err
		}},
		{name: "UpdateStatus empty tenant", call: func() error {
			return repo.UpdateStatus(ctx, "", "file", core.FileStatusPending)
		}},
		{name: "CompareAndUpdateProcessing nil update", call: func() error {
			_, err := repo.CompareAndUpdateProcessing(ctx, core.FileProcessingLease{
				TenantID: "tenant", FileKey: "file", ProcessingStartTimeUTC: time.Now().UTC(),
			}, nil)
			return err
		}},
		{name: "GetTimedOutProcessingFiles empty tenant", call: func() error {
			_, err := repo.GetTimedOutProcessingFiles(ctx, "", time.Minute)
			return err
		}},
		{name: "invalid tenant id", call: func() error {
			_, err := repo.Get(ctx, "..", "file")
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("error = %v, want core.ErrInvalidArgument", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// sqliteStatisticsRecorder captures recorded statistics counters.
type sqliteStatisticsRecorder struct {
	mu     sync.Mutex
	counts map[string]int64
}

// Record accumulates one statistic.
func (r *sqliteStatisticsRecorder) Record(name string, value int64, _ time.Time, _ map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = make(map[string]int64)
	}
	r.counts[name] += value
}

// totals returns the committed batch and operation counters.
func (r *sqliteStatisticsRecorder) totals() (int64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[core.StatisticMetadataPersistedBatchCount],
		r.counts[core.StatisticMetadataPersistedOperationCount]
}

// ioDiscard is a writer that drops everything it is given.
type ioDiscard struct{}

// Write discards p.
func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

// fileKeys projects the file keys of a result set, in order.
func fileKeys(records []*core.FileMetadata) []string {
	keys := make([]string, 0, len(records))
	for _, record := range records {
		keys = append(keys, record.FileKey)
	}
	return keys
}

// equalStrings reports whether two slices are equal element by element.
func equalStrings(left, right []string) bool {
	return slices.Equal(left, right)
}

// containsSubstring reports whether haystack contains needle.
func containsSubstring(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
