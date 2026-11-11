package metadata

import (
	"context"
	"fmt"
	"testing"

	"github.com/cocosip/venue/pkg/core"
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
	} {
		if !indexes[wanted] {
			t.Errorf("%s is missing, want it present", wanted)
		}
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
