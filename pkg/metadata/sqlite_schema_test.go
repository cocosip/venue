package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
)

// TestApplySchemaKeepsOnlyUsedStatusIndexes pins the index inventory: the unused
// (status, created_at, file_key) index is dropped, while the availability and the
// two expiry indexes stay because the claim and the windowed maintenance readers
// use them.
func TestApplySchemaKeepsOnlyUsedStatusIndexes(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()

	handle, err := repo.begin(ctx, "tenant-schema")
	if err != nil {
		t.Fatalf("begin() error = %v", err)
	}
	defer repo.end(handle)

	indexes := map[string]bool{}
	rows, err := handle.db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'files'`)
	if err != nil {
		t.Fatalf("query index inventory error = %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			t.Fatalf("scan index name error = %v", scanErr)
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read index inventory error = %v", err)
	}

	if indexes["idx_files_status_created_at"] {
		t.Error("idx_files_status_created_at still exists, want it dropped: no query shape orders by it")
	}
	for _, wanted := range []string{
		"idx_files_status_available",
		"idx_files_status_completed_at",
		"idx_files_status_last_failed_at",
		"idx_files_import_operation",
		"idx_files_quota_counts",
	} {
		if !indexes[wanted] {
			t.Errorf("%s is missing, want it present", wanted)
		}
	}
}

// explainQueryPlan returns the planner's detail lines for one statement, so a
// test can assert which access path SQLite chose.
func explainQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN error = %v", err)
	}
	defer func() { _ = rows.Close() }()

	var details []string
	for rows.Next() {
		var (
			identifier int
			parent     int
			unused     int
			detail     string
		)
		if err := rows.Scan(&identifier, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan error = %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read query plan error = %v", err)
	}
	return details
}

// TestSQLiteMetadataRepositoryPlansUseTheHotPathIndexes proves the two lookups
// that must never scan the files table report an index access path. Both were
// full scans: the idempotent-write lookup omitted the partial index's own
// predicate, and the count queries had no index containing tenant_id.
func TestSQLiteMetadataRepositoryPlansUseTheHotPathIndexes(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	const tenantID = "tenant-plan"

	record := sqliteRecord(tenantID, "file-plan", time.Now().UTC())
	record.ImportOperationID = "watcher-operation-plan"
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	handle, err := repo.begin(ctx, tenantID)
	if err != nil {
		t.Fatalf("begin() error = %v", err)
	}
	defer repo.end(handle)

	tests := []struct {
		name      string
		query     string
		args      []any
		wantIndex string
	}{
		{
			name:      "import operation lookup",
			query:     sqliteSelectImportOperationSQL,
			args:      []any{sql.Named("tenant", tenantID), sql.Named("operation_id", record.ImportOperationID)},
			wantIndex: "idx_files_import_operation",
		},
		{
			name:      "count by statuses",
			query:     sqliteCountByStatusesQuery(2),
			args:      []any{tenantID, int(core.FileStatusPending), int(core.FileStatusCompleted)},
			wantIndex: "idx_files_quota_counts",
		},
		{
			name:      "count by directory",
			query:     sqliteCountByDirectoryQuery(2),
			args:      []any{tenantID, int(core.FileStatusPending), int(core.FileStatusCompleted)},
			wantIndex: "idx_files_quota_counts",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			details := explainQueryPlan(t, handle.db, testCase.query, testCase.args...)
			joined := strings.Join(details, " | ")
			if !strings.Contains(joined, "USING INDEX "+testCase.wantIndex) &&
				!strings.Contains(joined, "USING COVERING INDEX "+testCase.wantIndex) {
				t.Fatalf("query plan = %q, want an access path through %s", joined, testCase.wantIndex)
			}
			if strings.Contains(joined, "SCAN files") {
				t.Fatalf("query plan = %q, want no full table scan of files", joined)
			}
		})
	}
}

// TestApplySchemaAddsQuotaCountCoveringIndexToExistingDatabase proves the new
// covering index reaches a database that was created before it existed: the
// schema is migrated in place on open, and the stored records survive.
func TestApplySchemaAddsQuotaCountCoveringIndexToExistingDatabase(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	const tenantID = "tenant-pre-covering-index"

	seeded := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})
	record := sqliteRecord(tenantID, "file-pre-index", time.Now().UTC())
	if err := seeded.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	databasePath := sqliteTenantDatabasePath(dataPath, tenantID)
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Turn the database into one that predates the index without recreating it.
	legacy, err := sqlite.Open(ctx, databasePath, sqlite.DefaultOptions())
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	if _, err := legacy.ExecContext(ctx, `DROP INDEX IF EXISTS idx_files_quota_counts`); err != nil {
		_ = legacy.Close()
		t.Fatalf("DROP INDEX error = %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("Close(legacy) error = %v", err)
	}

	migrated := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.DataPath = dataPath
	})

	if _, err := migrated.Get(ctx, tenantID, record.FileKey); err != nil {
		t.Fatalf("Get() after the schema migration error = %v, want the record preserved", err)
	}

	handle, err := migrated.begin(ctx, tenantID)
	if err != nil {
		t.Fatalf("begin() error = %v", err)
	}
	defer migrated.end(handle)

	var name string
	if err := handle.db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_files_quota_counts'`).
		Scan(&name); err != nil {
		t.Fatalf("idx_files_quota_counts is missing after the migration: %v", err)
	}
	details := explainQueryPlan(t, handle.db, sqliteCountByDirectoryQuery(1),
		tenantID, int(core.FileStatusPending))
	if joined := strings.Join(details, " | "); !strings.Contains(joined, "USING COVERING INDEX idx_files_quota_counts") {
		t.Fatalf("query plan = %q, want the migrated index to serve the aggregate", joined)
	}
}

// TestCheckpointAfterBatchBoundedInterval proves the periodic checkpoint keeps
// committing (it must not deadlock the single pooled connection) and preserves
// every committed record.
func TestCheckpointAfterBatchBoundedInterval(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.Sqlite.CheckpointAfterBatch = true
	})
	ctx := context.Background()
	tenantID := "tenant-checkpoint"

	// More than one interval, so at least one automatic checkpoint runs.
	const recordCount = 2*metadataCheckpointCommitInterval + 2
	for i := 0; i < recordCount; i++ {
		if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, fmt.Sprintf("file-%03d", i), repo.now())); err != nil {
			t.Fatalf("AddOrUpdate(file-%03d) error = %v", i, err)
		}
	}

	count, err := repo.CountByStatuses(ctx, tenantID, []core.FileProcessingStatus{core.FileStatusPending})
	if err != nil {
		t.Fatalf("CountByStatuses() error = %v", err)
	}
	if int(count) != recordCount {
		t.Fatalf("CountByStatuses() = %d, want %d committed records", count, recordCount)
	}
}
