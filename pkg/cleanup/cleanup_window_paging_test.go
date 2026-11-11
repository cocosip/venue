package cleanup

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// errWindowScan is the sentinel the windowed stub returns when a test injects a
// repository error.
var errWindowScan = errors.New("windowed status scan failed")

// windowPagingMetadataRepository adds the optional time-window paging
// capability on top of the in-memory paging repository.
//
// Like the real repository it only returns records whose expiry instant is at or
// before before, so a retention sweep does not scan the recent part of the
// partition. The cursor is the last file key of the previous page, matching the
// in-memory fixture's key ordering.
type windowPagingMetadataRepository struct {
	*pagingMetadataRepository

	windowPageCalls int
	beforeValues    []time.Time
	windowErr       error
}

func (r *windowPagingMetadataRepository) GetByStatusPageBefore(
	ctx context.Context,
	tenantID string,
	status core.FileProcessingStatus,
	before time.Time,
	cursor string,
	limit int,
) (*core.MetadataPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.windowPageCalls++
	r.beforeValues = append(r.beforeValues, before)

	if r.windowErr != nil {
		return nil, r.windowErr
	}
	if limit <= 0 {
		return nil, fmt.Errorf("page limit must be positive, got %d: %w", limit, core.ErrInvalidArgument)
	}

	keys := make([]string, 0)
	for key, record := range r.records[tenantID] {
		if record.Status != status || key <= cursor || !withinWindow(record, status, before) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	next := ""
	if len(keys) > limit {
		keys = keys[:limit]
		next = keys[len(keys)-1]
	}

	records := make([]*core.FileMetadata, 0, len(keys))
	for _, key := range keys {
		records = append(records, r.records[tenantID][key])
	}
	r.recordScannedLocked(records)

	return &core.MetadataPage{Records: records, NextCursor: next}, nil
}

// withinWindow reports whether a record's status expiry instant is in the
// window, using the same inclusive boundary as the real repository.
func withinWindow(record *core.FileMetadata, status core.FileProcessingStatus, before time.Time) bool {
	var at *time.Time
	switch status {
	case core.FileStatusCompleted:
		at = record.CompletedAt
	case core.FileStatusPermanentlyFailed:
		at = record.LastFailedAt
	}
	return at != nil && !at.After(before)
}

// windowScanFixture builds the mandatory and recent records of one status and
// registers every physical path on a fake volume.
func windowScanFixture(status core.FileProcessingStatus, eligible, recent int) ([]*core.FileMetadata, []string) {
	now := time.Now()
	old := now.Add(-2 * time.Hour)

	records := make([]*core.FileMetadata, 0, eligible+recent)
	existing := make([]string, 0, eligible+recent)
	wantEligible := make([]string, 0, eligible)

	add := func(index int, at *time.Time, mandatory bool) {
		fileKey := fmt.Sprintf("%s-%04d", status, index)
		record := newPagingRecord(fileKey, status)
		switch status {
		case core.FileStatusCompleted:
			record.CompletedAt = at
		case core.FileStatusPermanentlyFailed:
			record.LastFailedAt = at
		}
		records = append(records, record)
		existing = append(existing, record.PhysicalPath)
		if mandatory {
			wantEligible = append(wantEligible, fileKey)
		}
	}

	for i := 0; i < eligible; i++ {
		add(i, &old, true)
	}
	for i := 0; i < recent; i++ {
		at := now
		add(eligible+i, &at, false)
	}

	return records, existing
}

// TestCleanupTimeWindowSweeps_UseTheWindowedCapability is the regression test for
// the retention sweeps that paged the entire status partition and filtered the
// time window in Go: when the repository provides the windowed capability the
// sweep must read only eligible records, and when it does not the previous
// scanning behaviour (and the same result) must be preserved.
func TestCleanupTimeWindowSweeps_UseTheWindowedCapability(t *testing.T) {
	const eligible = 300
	const recent = 300
	const retention = time.Hour

	tests := []struct {
		name string
		// status is the swept partition.
		status core.FileProcessingStatus
		// run executes the sweep and returns the number of removed records.
		run func(t *testing.T, service core.CleanupService) int
	}{
		{
			name:   "completed files",
			status: core.FileStatusCompleted,
			run: func(t *testing.T, service core.CleanupService) int {
				t.Helper()
				stats, err := service.CleanupCompletedFiles(context.Background(), retention)
				if err != nil {
					t.Fatalf("CleanupCompletedFiles() error = %v", err)
				}
				return stats.CompletedRecordsRemoved
			},
		},
		{
			name:   "permanently failed files",
			status: core.FileStatusPermanentlyFailed,
			run: func(t *testing.T, service core.CleanupService) int {
				t.Helper()
				stats, err := service.CleanupPermanentlyFailedFiles(context.Background(), retention)
				if err != nil {
					t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
				}
				return stats.PermanentlyFailedFilesRemoved
			},
		},
	}

	modes := []struct {
		name string
		// wrap installs the fixture records and returns the repository under test.
		wrap func(memory *memoryMetadataRepository) core.MetadataRepository
	}{
		{
			name: "windowed capability",
			wrap: func(memory *memoryMetadataRepository) core.MetadataRepository {
				return &windowPagingMetadataRepository{pagingMetadataRepository: &pagingMetadataRepository{memory}}
			},
		},
		{
			name: "fallback status paging",
			wrap: func(memory *memoryMetadataRepository) core.MetadataRepository {
				return &pagingMetadataRepository{memory}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					records, existing := windowScanFixture(tc.status, eligible, recent)

					memory := newMemoryMetadataRepository()
					memory.store(records...)
					repo := mode.wrap(memory)
					volume := newFakeStorageVolume(pagingTestVolumeID, existing...)
					service := newPagingCleanupService(repo, volume)

					start := time.Now()
					removed := tc.run(t, service)
					end := time.Now()

					if removed != eligible {
						t.Fatalf("removed = %d, want %d", removed, eligible)
					}
					if got := len(memory.deletedKeys()); got != eligible {
						t.Fatalf("metadata deletions = %d, want %d", got, eligible)
					}
					if got := len(volume.deletedPaths()); got != eligible {
						t.Fatalf("physical deletions = %d, want %d", got, eligible)
					}

					windowed, isWindowed := repo.(*windowPagingMetadataRepository)
					if !isWindowed {
						// The fallback pages the whole partition and filters in
						// Go, exactly as before.
						if got := memory.pageCallCount(); got == 0 {
							t.Fatal("fallback performed no status page calls")
						}
						if got := len(memory.scannedKeys()); got != eligible+recent {
							t.Fatalf("fallback scanned = %d records, want the whole partition (%d)", got, eligible+recent)
						}
						return
					}

					if windowed.windowPageCalls == 0 {
						t.Fatal("windowed capability was never used")
					}
					if got := memory.pageCallCount(); got != 0 {
						t.Errorf("generic status pages = %d, want 0 when the windowed capability is used", got)
					}
					if got := len(memory.scannedKeys()); got != eligible {
						t.Errorf("windowed scan read %d records, want only the %d eligible ones", got, eligible)
					}
					for _, before := range windowed.beforeValues {
						cutoffLow := start.Add(-retention)
						cutoffHigh := end.Add(-retention)
						if before.Before(cutoffLow) || before.After(cutoffHigh) {
							t.Errorf("window boundary = %v, want within [%v, %v]", before, cutoffLow, cutoffHigh)
						}
					}
				})
			}
		})
	}
}

// TestCleanupTimeWindowSweep_ErrorStopsOperation verifies a windowed scan error
// stops the sweep and surfaces, instead of being swallowed.
func TestCleanupTimeWindowSweep_ErrorStopsOperation(t *testing.T) {
	records, existing := windowScanFixture(core.FileStatusCompleted, 10, 10)
	memory := newMemoryMetadataRepository()
	memory.store(records...)

	repo := &windowPagingMetadataRepository{
		pagingMetadataRepository: &pagingMetadataRepository{memory},
		windowErr:                errWindowScan,
	}
	volume := newFakeStorageVolume(pagingTestVolumeID, existing...)
	service := newPagingCleanupService(repo, volume)

	stats, err := service.CleanupCompletedFiles(context.Background(), time.Hour)
	if !errors.Is(err, errWindowScan) {
		t.Fatalf("CleanupCompletedFiles() error = %v, want %v", err, errWindowScan)
	}
	if stats.CompletedRecordsRemoved != 0 {
		t.Errorf("CompletedRecordsRemoved = %d, want 0", stats.CompletedRecordsRemoved)
	}
	if got := len(memory.deletedKeys()); got != 0 {
		t.Errorf("metadata deletions = %d, want 0", got)
	}
}
