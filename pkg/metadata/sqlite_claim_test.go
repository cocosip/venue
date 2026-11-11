package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestClaimPendingFilesClaimsQueueInOnePass covers the batch claim contract: FIFO
// order, availability respected, one status transition per record, and an empty
// slice (not an error) when nothing is claimable.
func TestClaimPendingFilesClaimsQueueInOnePass(t *testing.T) {
	clock := newTraceClock(time.Now().UTC())
	repo := newSQLiteFixtureRepository(t, func(options *SQLiteRepositoryOptions) {
		options.now = clock.Now
	})
	ctx := context.Background()
	tenantID := "tenant-claim"
	base := clock.Now().Add(-time.Hour)

	notYet := clock.Now().Add(time.Hour)
	for i := 0; i < 4; i++ {
		record := sqliteRecord(tenantID, fmt.Sprintf("file-%d", i), base.Add(time.Duration(i)*time.Minute))
		if i == 2 {
			record.AvailableForProcessingAt = &notYet
		}
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(file-%d) error = %v", i, err)
		}
	}
	otherTenant := sqliteRecord("tenant-claim-other", "file-other", base)
	if err := repo.AddOrUpdate(ctx, otherTenant); err != nil {
		t.Fatalf("AddOrUpdate(other tenant) error = %v", err)
	}

	claimed, err := repo.ClaimPendingFiles(ctx, tenantID, 3)
	if err != nil {
		t.Fatalf("ClaimPendingFiles() error = %v", err)
	}
	if got, want := fileKeys(claimed), []string{"file-0", "file-1", "file-3"}; !equalStrings(got, want) {
		t.Fatalf("ClaimPendingFiles() order = %v, want %v", got, want)
	}
	for _, record := range claimed {
		if record.Status != core.FileStatusProcessing {
			t.Errorf("%s status = %s, want Processing", record.FileKey, record.Status)
		}
		if record.ProcessingStartTime == nil {
			t.Errorf("%s processing start = nil, want the claim lease", record.FileKey)
		}
	}

	// The only unclaimed record is not available yet.
	claimed, err = repo.ClaimPendingFiles(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("ClaimPendingFiles(remaining) error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimPendingFiles(remaining) = %v, want an empty slice while the file is unavailable", fileKeys(claimed))
	}

	clock.Advance(2 * time.Hour)
	claimed, err = repo.ClaimPendingFiles(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("ClaimPendingFiles(available) error = %v", err)
	}
	if got, want := fileKeys(claimed), []string{"file-2"}; !equalStrings(got, want) {
		t.Fatalf("ClaimPendingFiles(available) = %v, want %v", got, want)
	}

	claimed, err = repo.ClaimPendingFiles(ctx, tenantID, 10)
	if err != nil {
		t.Fatalf("ClaimPendingFiles(empty) error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimPendingFiles(empty) = %v, want an empty slice", fileKeys(claimed))
	}

	other, err := repo.Get(ctx, "tenant-claim-other", "file-other")
	if err != nil {
		t.Fatalf("Get(other tenant) error = %v", err)
	}
	if other.Status != core.FileStatusPending {
		t.Fatalf("other tenant file status = %s, want Pending", other.Status)
	}
}

// TestClaimPendingFilesConcurrentWorkersClaimDistinctFiles proves the claim is
// atomic: concurrent workers never observe the same record as claimed.
func TestClaimPendingFilesConcurrentWorkersClaimDistinctFiles(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-claim-race"
	base := time.Now().UTC().Add(-time.Hour)

	const total = 12
	for i := 0; i < total; i++ {
		record := sqliteRecord(tenantID, fmt.Sprintf("file-%02d", i), base.Add(time.Duration(i)*time.Minute))
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(file-%02d) error = %v", i, err)
		}
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		claims = make(map[string]int)
	)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := repo.ClaimPendingFiles(ctx, tenantID, 2)
				if err != nil {
					t.Errorf("ClaimPendingFiles() error = %v", err)
					return
				}
				if len(claimed) == 0 {
					return
				}
				mu.Lock()
				for _, record := range claimed {
					claims[record.FileKey]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claims) != total {
		t.Fatalf("claimed %d distinct files, want %d", len(claims), total)
	}
	for fileKey, count := range claims {
		if count != 1 {
			t.Errorf("%s was claimed %d times, want exactly once", fileKey, count)
		}
	}
}

// TestClaimPendingFilesValidatesArguments covers the argument matrix.
func TestClaimPendingFilesValidatesArguments(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()

	cases := []struct {
		name     string
		tenantID string
		limit    int
	}{
		{name: "empty tenant", tenantID: "", limit: 1},
		{name: "zero limit", tenantID: "tenant", limit: 0},
		{name: "negative limit", tenantID: "tenant", limit: -1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := repo.ClaimPendingFiles(ctx, testCase.tenantID, testCase.limit); !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("ClaimPendingFiles() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}
