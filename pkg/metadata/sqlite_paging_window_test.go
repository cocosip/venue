package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestGetByStatusPageBeforePagesExpiryWindow covers the windowed maintenance
// reader: only records at or before the cutoff are returned, ordered by their own
// expiry column, with non-overlapping pages.
func TestGetByStatusPageBeforePagesExpiryWindow(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-window"
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	for i := 0; i < 5; i++ {
		completedAt := base.Add(time.Duration(i) * time.Minute)
		record := sqliteRecord(tenantID, fmt.Sprintf("done-%d", i), base)
		record.Status = core.FileStatusCompleted
		record.CompletedAt = &completedAt
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(done-%d) error = %v", i, err)
		}
	}
	// Outside the window on purpose.
	outsideAt := base.Add(30 * time.Minute)
	outside := sqliteRecord(tenantID, "done-outside", base)
	outside.Status = core.FileStatusCompleted
	outside.CompletedAt = &outsideAt
	if err := repo.AddOrUpdate(ctx, outside); err != nil {
		t.Fatalf("AddOrUpdate(done-outside) error = %v", err)
	}
	// Another status must never leak into a windowed scan of Completed.
	failedAt := base.Add(time.Minute)
	failed := sqliteRecord(tenantID, "failed-1", base)
	failed.Status = core.FileStatusPermanentlyFailed
	failed.LastFailedAt = &failedAt
	if err := repo.AddOrUpdate(ctx, failed); err != nil {
		t.Fatalf("AddOrUpdate(failed-1) error = %v", err)
	}

	cutoff := base.Add(3 * time.Minute)
	want := []string{"done-0", "done-1", "done-2", "done-3"}

	var (
		collected []string
		cursor    string
	)
	for pageIndex := 0; ; pageIndex++ {
		if pageIndex > 10 {
			t.Fatal("windowed paging did not terminate")
		}
		page, err := repo.GetByStatusPageBefore(ctx, tenantID, core.FileStatusCompleted, cutoff, cursor, 2)
		if err != nil {
			t.Fatalf("GetByStatusPageBefore() error = %v", err)
		}
		collected = append(collected, fileKeys(page.Records)...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if !equalStrings(collected, want) {
		t.Fatalf("windowed scan = %v, want %v", collected, want)
	}

	// The same reader over PermanentlyFailed uses last_failed_at.
	page, err := repo.GetByStatusPageBefore(ctx, tenantID, core.FileStatusPermanentlyFailed, cutoff, "", 10)
	if err != nil {
		t.Fatalf("GetByStatusPageBefore(PermanentlyFailed) error = %v", err)
	}
	if got := fileKeys(page.Records); !equalStrings(got, []string{"failed-1"}) {
		t.Fatalf("PermanentlyFailed window = %v, want [failed-1]", got)
	}
}

// TestGetByStatusPageBeforeRejectsBadArguments covers the argument matrix.
func TestGetByStatusPageBeforeRejectsBadArguments(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	now := time.Now()

	if _, err := repo.GetByStatusPageBefore(ctx, "", core.FileStatusCompleted, now, "", 1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("empty tenant error = %v, want ErrInvalidArgument", err)
	}
	if _, err := repo.GetByStatusPageBefore(ctx, "tenant", core.FileStatusCompleted, now, "", 0); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("zero limit error = %v, want ErrInvalidArgument", err)
	}
	if _, err := repo.GetByStatusPageBefore(ctx, "tenant", core.FileStatusPending, now, "", 1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("unsupported status error = %v, want ErrInvalidArgument", err)
	}
	// A cursor minted by the queue-order reader must not be reinterpreted as a
	// window position.
	foreign := encodeSQLiteStatusPageCursor(statusPageCursor{available: 1, created: 2, fileKey: "k"})
	if _, err := repo.GetByStatusPageBefore(ctx, "tenant", core.FileStatusCompleted, now, foreign, 1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("foreign cursor error = %v, want ErrInvalidArgument", err)
	}
}
