package metadata

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// pagedStatusTestTenant is the tenant used by the paged scan tests. It matches
// createTestMetadata so shared helpers stay interchangeable.
const pagedStatusTestTenant = "test-tenant"

// newPagedStatusTestRepository opens a repository in a temporary directory and
// registers shutdown before the directory is removed.
func newPagedStatusTestRepository(t *testing.T) *BadgerMetadataRepository {
	t.Helper()

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID:   pagedStatusTestTenant,
		DataPath:   t.TempDir(),
		CacheTTL:   time.Minute,
		GCInterval: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	concrete, ok := repo.(*BadgerMetadataRepository)
	if !ok {
		t.Fatalf("repository type = %T, want *BadgerMetadataRepository", repo)
	}
	return concrete
}

// seedStatusRecords stores count records in queue order (oldest arrival first)
// and returns their file keys in that order.
func seedStatusRecords(t *testing.T, repo *BadgerMetadataRepository, tenantID string, status core.FileProcessingStatus, count int) []string {
	t.Helper()

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	records := make([]*core.FileMetadata, 0, count)
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("file-%04d", i)
		created := base.Add(time.Duration(i) * time.Millisecond)
		records = append(records, &core.FileMetadata{
			FileKey:      key,
			TenantID:     tenantID,
			FileSize:     int64(i),
			VolumeID:     "volume-1",
			PhysicalPath: "path/to/" + key,
			Status:       status,
			CreatedAt:    created,
			UpdatedAt:    created,
		})
		keys = append(keys, key)
	}

	if err := repo.AddOrUpdateBatch(context.Background(), records); err != nil {
		t.Fatalf("AddOrUpdateBatch() error = %v", err)
	}
	return keys
}

// deletePrimaryRecord removes only the primary record, leaving its secondary
// index entry behind: that is exactly the stale state a crash between the two
// writes leaves in the database.
func deletePrimaryRecord(t *testing.T, repo *BadgerMetadataRepository, tenantID, fileKey string) {
	t.Helper()

	err := repo.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(repo.buildKey(tenantID, fileKey))
	})
	if err != nil {
		t.Fatalf("delete primary record %s error = %v", fileKey, err)
	}
}

// TestBadgerMetadataRepositoryExposesPagedStatusReads pins the optional
// capability: callers only holding core.MetadataRepository must be able to
// discover paging with the documented type assertion.
func TestBadgerMetadataRepositoryExposesPagedStatusReads(t *testing.T) {
	repo := newPagedStatusTestRepository(t)

	var repository core.MetadataRepository = repo
	reader, ok := repository.(core.StatusPageReader)
	if !ok {
		t.Fatalf("repository type %T does not implement core.StatusPageReader", repository)
	}

	page, err := reader.GetByStatusPage(context.Background(), pagedStatusTestTenant, core.FileStatusPending, "", 10)
	if err != nil {
		t.Fatalf("GetByStatusPage() on an empty store error = %v", err)
	}
	if page == nil {
		t.Fatal("GetByStatusPage() returned a nil page")
	}
	if len(page.Records) != 0 {
		t.Fatalf("GetByStatusPage() records = %d, want 0 on an empty store", len(page.Records))
	}
	if page.NextCursor != "" {
		t.Fatalf("GetByStatusPage() next cursor = %q, want empty when the scan is complete", page.NextCursor)
	}
}

// TestGetByStatusPagePagesWithoutGapsOrDuplicates is the regression test for
// unbounded status scans: a 25-record queue read with limit 10 must produce
// 10/10/5 with a continuation cursor, in index order, without duplicates or
// omissions, and the final page must report completion.
func TestGetByStatusPagePagesWithoutGapsOrDuplicates(t *testing.T) {
	ctx := context.Background()
	repo := newPagedStatusTestRepository(t)
	keys := seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, 25)

	const limit = 10
	var (
		got       []string
		pageSizes []int
		cursors   []string
	)
	cursor := ""
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatalf("paging did not terminate after %d pages", page)
		}

		result, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, cursor, limit)
		if err != nil {
			t.Fatalf("page %d: GetByStatusPage() error = %v", page, err)
		}
		if result == nil {
			t.Fatalf("page %d: GetByStatusPage() returned a nil page", page)
		}

		pageSizes = append(pageSizes, len(result.Records))
		cursors = append(cursors, result.NextCursor)
		for _, record := range result.Records {
			got = append(got, record.FileKey)
		}

		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	if len(got) != len(keys) {
		t.Fatalf("paged records = %d, want %d; got %v", len(got), len(keys), got)
	}
	for i, key := range got {
		if key != keys[i] {
			t.Fatalf("record %d = %s, want %s; paging reordered or skipped records: %v", i, key, keys[i], got)
		}
	}
	if len(pageSizes) != 3 || pageSizes[0] != 10 || pageSizes[1] != 10 || pageSizes[2] != 5 {
		t.Fatalf("page sizes = %v, want [10 10 5]", pageSizes)
	}
	if cursors[0] == "" || cursors[1] == "" {
		t.Fatalf("cursors = %q, want a continuation cursor on every page that filled the limit", cursors)
	}
	if cursors[2] != "" {
		t.Fatalf("last cursor = %q, want empty once the prefix is exhausted", cursors[2])
	}
}

// TestGetByStatusPageCursorIsTheLastIndexKey pins the opaque-token contract:
// the token must decode to the index key of the last returned record so a
// caller that keeps deleting returned records still resumes after them.
func TestGetByStatusPageCursorIsTheLastIndexKey(t *testing.T) {
	ctx := context.Background()
	repo := newPagedStatusTestRepository(t)
	seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, 12)

	first, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, "", 5)
	if err != nil {
		t.Fatalf("GetByStatusPage() error = %v", err)
	}
	if len(first.Records) != 5 {
		t.Fatalf("records = %d, want 5", len(first.Records))
	}
	if first.NextCursor == "" {
		t.Fatal("NextCursor is empty after a full page, want a continuation token")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(first.NextCursor)
	if err != nil {
		t.Fatalf("NextCursor %q is not base64url: %v", first.NextCursor, err)
	}
	if want := string(repo.buildStatusIndexKey(first.Records[len(first.Records)-1])); string(decoded) != want {
		t.Fatalf("decoded cursor = %q, want the last returned index key %q", decoded, want)
	}
}

// TestGetByStatusPageOrdersLikeTheUnboundedQuery keeps the paged reader aligned
// with GetByStatus and GetPendingFiles: availability first, then arrival.
func TestGetByStatusPageOrdersLikeTheUnboundedQuery(t *testing.T) {
	ctx := context.Background()
	repo := newPagedStatusTestRepository(t)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	later := base.Add(30 * time.Minute)
	records := []*core.FileMetadata{
		{FileKey: "scheduled-last", TenantID: pagedStatusTestTenant, Status: core.FileStatusPending, CreatedAt: base, UpdatedAt: base, AvailableForProcessingAt: &later},
		{FileKey: "immediate-second", TenantID: pagedStatusTestTenant, Status: core.FileStatusPending, CreatedAt: base.Add(time.Second), UpdatedAt: base},
		{FileKey: "immediate-third", TenantID: pagedStatusTestTenant, Status: core.FileStatusPending, CreatedAt: base.Add(2 * time.Second), UpdatedAt: base},
	}
	if err := repo.AddOrUpdateBatch(ctx, records); err != nil {
		t.Fatalf("AddOrUpdateBatch() error = %v", err)
	}

	want, err := repo.GetByStatus(ctx, pagedStatusTestTenant, core.FileStatusPending, 0)
	if err != nil {
		t.Fatalf("GetByStatus() error = %v", err)
	}

	var got []string
	cursor := ""
	for page := 0; ; page++ {
		if page > 5 {
			t.Fatal("paging did not terminate")
		}
		result, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, cursor, 1)
		if err != nil {
			t.Fatalf("GetByStatusPage() error = %v", err)
		}
		for _, record := range result.Records {
			got = append(got, record.FileKey)
		}
		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	if len(got) != len(want) {
		t.Fatalf("paged order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i].FileKey {
			t.Fatalf("paged order = %v, want %v", got, want)
		}
	}
}

// TestGetByStatusPageValidatesArguments covers the documented error contract.
func TestGetByStatusPageValidatesArguments(t *testing.T) {
	repo := newPagedStatusTestRepository(t)

	testCases := []struct {
		name     string
		tenantID string
		cursor   string
		limit    int
	}{
		{name: "empty tenant", tenantID: "", cursor: "", limit: 10},
		{name: "zero limit", tenantID: pagedStatusTestTenant, cursor: "", limit: 0},
		{name: "negative limit", tenantID: pagedStatusTestTenant, cursor: "", limit: -1},
		{name: "undecodable cursor", tenantID: pagedStatusTestTenant, cursor: "not-a-token!!", limit: 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			page, err := repo.GetByStatusPage(context.Background(), tc.tenantID, core.FileStatusPending, tc.cursor, tc.limit)
			if err == nil {
				t.Fatalf("GetByStatusPage(%q, %q, %d) error = nil, want %v", tc.tenantID, tc.cursor, tc.limit, core.ErrInvalidArgument)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Errorf("error = %v, want %v", err, core.ErrInvalidArgument)
			}
			if page != nil {
				t.Errorf("page = %+v, want nil on a rejected argument", page)
			}
		})
	}
}

// TestGetByStatusPageSurvivesDeletingReturnedRecords reproduces the
// CleanupCompletedFiles loop: every page is deleted right after it is read, and
// the cursor must still visit every remaining record exactly once.
func TestGetByStatusPageSurvivesDeletingReturnedRecords(t *testing.T) {
	ctx := context.Background()
	repo := newPagedStatusTestRepository(t)
	keys := seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusCompleted, 25)

	seen := make(map[string]int, len(keys))
	cursor := ""
	for page := 0; ; page++ {
		if page > 25 {
			t.Fatalf("paging loop did not terminate after %d pages; cursor = %q", page, cursor)
		}

		result, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusCompleted, cursor, 7)
		if err != nil {
			t.Fatalf("page %d: GetByStatusPage() error = %v", page, err)
		}
		for _, record := range result.Records {
			seen[record.FileKey]++
			if err := repo.Delete(ctx, pagedStatusTestTenant, record.FileKey); err != nil {
				t.Fatalf("Delete(%s) error = %v", record.FileKey, err)
			}
		}
		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	if len(seen) != len(keys) {
		t.Fatalf("visited %d records, want %d; seen = %v", len(seen), len(keys), seen)
	}
	for _, key := range keys {
		if seen[key] != 1 {
			t.Fatalf("record %s visited %d times, want exactly once", key, seen[key])
		}
	}

	final, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusCompleted, "", 7)
	if err != nil {
		t.Fatalf("final GetByStatusPage() error = %v", err)
	}
	if len(final.Records) != 0 || final.NextCursor != "" {
		t.Fatalf("final page = {%d records, cursor %q}, want an empty complete page", len(final.Records), final.NextCursor)
	}
}

// TestGetByStatusPageSkipsStaleIndexEntries covers an index entry whose record
// is gone: the reader must skip it, and a long run of stale entries must stop
// with a cursor instead of returning empty pages forever.
func TestGetByStatusPageSkipsStaleIndexEntries(t *testing.T) {
	ctx := context.Background()

	t.Run("stale entries do not hide live records", func(t *testing.T) {
		repo := newPagedStatusTestRepository(t)
		keys := seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, 10)
		for i := 0; i < len(keys); i += 2 {
			deletePrimaryRecord(t, repo, pagedStatusTestTenant, keys[i])
		}

		result, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, "", 100)
		if err != nil {
			t.Fatalf("GetByStatusPage() error = %v", err)
		}
		if len(result.Records) != 5 {
			t.Fatalf("records = %d, want the 5 live records", len(result.Records))
		}
		for i, record := range result.Records {
			if want := keys[i*2+1]; record.FileKey != want {
				t.Fatalf("record %d = %s, want %s", i, record.FileKey, want)
			}
		}
		if result.NextCursor != "" {
			t.Fatalf("NextCursor = %q, want empty when the prefix is exhausted", result.NextCursor)
		}
	})

	t.Run("a long stale run returns a cursor so the caller makes progress", func(t *testing.T) {
		repo := newPagedStatusTestRepository(t)
		keys := seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, scanStaleLimit+5)
		for _, key := range keys {
			deletePrimaryRecord(t, repo, pagedStatusTestTenant, key)
		}

		cursor := ""
		pages := 0
		for {
			if pages > 5 {
				t.Fatalf("stale-entry scan did not terminate; cursor = %q", cursor)
			}
			result, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, cursor, 10)
			if err != nil {
				t.Fatalf("page %d: GetByStatusPage() error = %v", pages, err)
			}
			if len(result.Records) != 0 {
				t.Fatalf("page %d: records = %d, want 0 (every index entry is stale)", pages, len(result.Records))
			}
			if result.NextCursor == "" {
				break
			}
			if pages == 0 && result.NextCursor == cursor {
				t.Fatalf("page 0 returned the same cursor %q it was given: no forward progress", cursor)
			}
			cursor = result.NextCursor
			pages++
		}
		if pages == 0 {
			t.Fatal("the scan consumed every stale entry in one page, want a bounded page with a cursor first")
		}
	})
}

// TestGetByStatusPageHonoursContextAndClosure keeps the paged scan cancellable
// and consistent with the rest of the repository's closed-state handling.
func TestGetByStatusPageHonoursContextAndClosure(t *testing.T) {
	ctx := context.Background()

	t.Run("cancelled context", func(t *testing.T) {
		repo := newPagedStatusTestRepository(t)
		seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, 3)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		if _, err := repo.GetByStatusPage(cancelled, pagedStatusTestTenant, core.FileStatusPending, "", 10); !errors.Is(err, context.Canceled) {
			t.Fatalf("GetByStatusPage() error = %v, want %v", err, context.Canceled)
		}
	})

	t.Run("closed repository", func(t *testing.T) {
		repo := newPagedStatusTestRepository(t)
		seedStatusRecords(t, repo, pagedStatusTestTenant, core.FileStatusPending, 3)
		if err := repo.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		if _, err := repo.GetByStatusPage(ctx, pagedStatusTestTenant, core.FileStatusPending, "", 10); !errors.Is(err, core.ErrDatabaseError) {
			t.Fatalf("GetByStatusPage() on a closed repository error = %v, want %v", err, core.ErrDatabaseError)
		}
	})
}
