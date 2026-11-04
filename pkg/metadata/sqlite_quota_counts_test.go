package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestSQLiteMetadataRepositoryQuotaCounts verifies the aggregate queries that
// startup reconciliation uses: one count per tenant and one per directory, with
// the same status filter as the paged scan they replace.
func TestSQLiteMetadataRepositoryQuotaCounts(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	now := time.Now().UTC()

	records := []*core.FileMetadata{
		sqliteRecord("tenant-counts", "count-1", now),
		sqliteRecord("tenant-counts", "count-2", now.Add(time.Second)),
		sqliteRecord("tenant-counts", "count-3", now.Add(2*time.Second)),
	}
	records[0].DirectoryPath = "/docs"
	records[1].DirectoryPath = "/docs"
	records[2].DirectoryPath = "/images"
	// One completed record counts, one dead-lettered record must not.
	records[1].Status = core.FileStatusCompleted
	records[2].Status = core.FileStatusDeadLettered
	if err := repo.AddOrUpdateBatch(ctx, records); err != nil {
		t.Fatalf("AddOrUpdateBatch() error = %v", err)
	}

	counted := []core.FileProcessingStatus{core.FileStatusPending, core.FileStatusCompleted}
	count, err := repo.CountByStatuses(ctx, "tenant-counts", counted)
	if err != nil {
		t.Fatalf("CountByStatuses() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("CountByStatuses() = %d, want 2", count)
	}

	perDirectory, err := repo.CountByDirectory(ctx, "tenant-counts", counted)
	if err != nil {
		t.Fatalf("CountByDirectory() error = %v", err)
	}
	if perDirectory["/docs"] != 2 || perDirectory["/images"] != 0 {
		t.Fatalf("CountByDirectory() = %v, want only /docs = 2", perDirectory)
	}

	// A tenant with no matching status has no directory entry and a zero count.
	emptyCount, err := repo.CountByStatuses(ctx, "tenant-counts", []core.FileProcessingStatus{core.FileStatusProcessing})
	if err != nil {
		t.Fatalf("CountByStatuses(processing) error = %v", err)
	}
	if emptyCount != 0 {
		t.Fatalf("CountByStatuses(processing) = %d, want 0", emptyCount)
	}

	if _, err := repo.CountByStatuses(ctx, "", counted); err == nil {
		t.Fatal("CountByStatuses() with an empty tenant error = nil, want ErrInvalidArgument")
	}
}
