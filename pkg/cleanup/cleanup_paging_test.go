package cleanup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

const (
	pagingTestTenant   = "test-tenant"
	pagingTestVolumeID = "test-volume"
)

// errPageScan is the sentinel the paging stub returns when a test injects a
// mid-scan failure.
var errPageScan = errors.New("status page scan failed")

// memoryMetadataRepository is an in-memory MetadataRepository for the paging
// tests. Records are addressed by (tenantID, fileKey) and enumerated in file-key
// order, mirroring the key-ordered status index the real repository scans.
//
// It records how it was queried: an unbounded GetByStatus(limit=0) call is only
// legitimate for the fallback path, so the paging tests assert the optional
// paged capability is actually used when the repository provides it.
type memoryMetadataRepository struct {
	mu      sync.Mutex
	records map[string]map[string]*core.FileMetadata

	getByStatusCalls  int
	getByStatusLimits []int

	pageCalls  int
	pageLimits []int
	pageSizes  []int

	// pageErrOnCall makes GetByStatusPage fail once pageCalls reaches it,
	// modelling a repository error in the middle of a scan.
	pageErrOnCall int
	pageErr       error

	// onPage runs after every successful page, letting a test observe or
	// interrupt the scan between pages.
	onPage func(call int)

	// scanned and deleted record the key order the repository was asked to
	// enumerate and the keys the cleanup removed.
	scanned []string
	deleted []string
}

func newMemoryMetadataRepository() *memoryMetadataRepository {
	return &memoryMetadataRepository{records: make(map[string]map[string]*core.FileMetadata)}
}

// store installs the fixture records without counting as a query.
func (r *memoryMetadataRepository) store(records ...*core.FileMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range records {
		r.storeLocked(record)
	}
}

func (r *memoryMetadataRepository) storeLocked(record *core.FileMetadata) {
	tenant := r.records[record.TenantID]
	if tenant == nil {
		tenant = make(map[string]*core.FileMetadata)
		r.records[record.TenantID] = tenant
	}
	tenant[record.FileKey] = record
}

func (r *memoryMetadataRepository) AddOrUpdate(ctx context.Context, metadata *core.FileMetadata) error {
	r.store(metadata)
	return nil
}

func (r *memoryMetadataRepository) AddOrUpdateBatch(ctx context.Context, metadata []*core.FileMetadata) error {
	r.store(metadata...)
	return nil
}

func (r *memoryMetadataRepository) Get(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.records[tenantID][fileKey]
	if record == nil {
		return nil, core.ErrFileNotFound
	}
	return record, nil
}

func (r *memoryMetadataRepository) Delete(ctx context.Context, tenantID, fileKey string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[tenantID][fileKey]; ok {
		delete(r.records[tenantID], fileKey)
		r.deleted = append(r.deleted, fileKey)
	}
	return nil
}

func (r *memoryMetadataRepository) DeleteBatch(ctx context.Context, tenantID string, fileKeys []string) error {
	for _, fileKey := range fileKeys {
		_ = r.Delete(ctx, tenantID, fileKey)
	}
	return nil
}

func (r *memoryMetadataRepository) GetByStatus(ctx context.Context, tenantID string, status core.FileProcessingStatus, limit int) ([]*core.FileMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getByStatusCalls++
	r.getByStatusLimits = append(r.getByStatusLimits, limit)
	records := r.statusLocked(tenantID, status, "", limit)
	r.recordScannedLocked(records)
	return records, nil
}

func (r *memoryMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (r *memoryMetadataRepository) UpdateStatus(ctx context.Context, tenantID, fileKey string, newStatus core.FileProcessingStatus) error {
	return nil
}

func (r *memoryMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	return nil, core.ErrFileNotFound
}

func (r *memoryMetadataRepository) CompareAndUpdateProcessing(
	ctx context.Context,
	lease core.FileProcessingLease,
	update func(*core.FileMetadata) error,
) (*core.FileMetadata, error) {
	return nil, core.ErrProcessingLeaseMismatch
}

func (r *memoryMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (r *memoryMetadataRepository) Optimize(ctx context.Context) error { return nil }

func (r *memoryMetadataRepository) Close() error { return nil }

// statusLocked returns the records for one tenant/status in file-key order,
// starting strictly after cursor when it is non-empty. limit <= 0 means
// unbounded.
func (r *memoryMetadataRepository) statusLocked(tenantID string, status core.FileProcessingStatus, cursor string, limit int) []*core.FileMetadata {
	keys := make([]string, 0)
	for key, record := range r.records[tenantID] {
		if record.Status != status || key <= cursor {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	records := make([]*core.FileMetadata, 0, len(keys))
	for _, key := range keys {
		records = append(records, r.records[tenantID][key])
	}
	return records
}

func (r *memoryMetadataRepository) recordScannedLocked(records []*core.FileMetadata) {
	for _, record := range records {
		r.scanned = append(r.scanned, record.FileKey)
	}
}

func (r *memoryMetadataRepository) scannedKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.scanned...)
}

func (r *memoryMetadataRepository) deletedKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.deleted...)
}

func (r *memoryMetadataRepository) getByStatusCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getByStatusCalls
}

func (r *memoryMetadataRepository) getByStatusLimitValues() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.getByStatusLimits...)
}

func (r *memoryMetadataRepository) pageCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pageCalls
}

func (r *memoryMetadataRepository) pageLimitValues() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.pageLimits...)
}

func (r *memoryMetadataRepository) pageSizeValues() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.pageSizes...)
}

// pagingMetadataRepository adds the optional core.StatusPageReader capability to
// the in-memory repository.
//
// The cursor is the last file key of the previous page and every page is
// recomputed from the live store with keys strictly greater than the cursor, so
// deleting the record that was just returned cannot shift the window: a deletion
// behind the cursor cannot skip a later record or resurrect an earlier one.
type pagingMetadataRepository struct {
	*memoryMetadataRepository
}

func (r *pagingMetadataRepository) GetByStatusPage(ctx context.Context, tenantID string, status core.FileProcessingStatus, cursor string, limit int) (*core.MetadataPage, error) {
	r.mu.Lock()
	r.pageCalls++
	call := r.pageCalls
	r.pageLimits = append(r.pageLimits, limit)
	fail := r.pageErr != nil && r.pageErrOnCall > 0 && call >= r.pageErrOnCall
	pageErr := r.pageErr
	onPage := r.onPage

	var records []*core.FileMetadata
	next := ""
	switch {
	case fail:
	case limit <= 0:
		pageErr = fmt.Errorf("page limit must be positive, got %d: %w", limit, core.ErrInvalidArgument)
		fail = true
	default:
		records = r.statusLocked(tenantID, status, cursor, limit)
		if len(records) > 0 {
			last := records[len(records)-1].FileKey
			if len(r.statusLocked(tenantID, status, last, 1)) > 0 {
				next = last
			}
		}
		r.recordScannedLocked(records)
	}
	r.pageSizes = append(r.pageSizes, len(records))
	r.mu.Unlock()

	if fail {
		return nil, pageErr
	}
	if onPage != nil {
		onPage(call)
	}

	return &core.MetadataPage{Records: records, NextCursor: next}, nil
}

// fakeStorageVolume is an in-memory StorageVolume for the paging tests. It tracks
// which physical paths exist so a test can model confirmed absence, and it
// records the physical deletions it was asked to perform.
type fakeStorageVolume struct {
	mu        sync.Mutex
	volumeID  string
	mountPath string
	existing  map[string]bool
	deleted   []string

	// onDelete runs after every recorded deletion.
	onDelete func()
}

func newFakeStorageVolume(volumeID string, existing ...string) *fakeStorageVolume {
	volume := &fakeStorageVolume{
		volumeID:  volumeID,
		mountPath: `C:\fake\` + volumeID,
		existing:  make(map[string]bool),
	}
	for _, path := range existing {
		volume.existing[path] = true
	}
	return volume
}

func (v *fakeStorageVolume) VolumeID() string  { return v.volumeID }
func (v *fakeStorageVolume) MountPath() string { return v.mountPath }

func (v *fakeStorageVolume) IsHealthy(ctx context.Context) bool { return true }

func (v *fakeStorageVolume) TotalCapacity(ctx context.Context) (int64, error) { return 0, nil }

func (v *fakeStorageVolume) AvailableSpace(ctx context.Context) (int64, error) { return 0, nil }

func (v *fakeStorageVolume) WriteFile(ctx context.Context, relativePath string, content io.Reader) (int64, error) {
	written, err := io.Copy(io.Discard, content)
	if err != nil {
		return 0, err
	}
	v.mu.Lock()
	v.existing[relativePath] = true
	v.mu.Unlock()
	return written, nil
}

func (v *fakeStorageVolume) ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	return nil, core.ErrFileNotFound
}

func (v *fakeStorageVolume) DeleteFile(ctx context.Context, relativePath string) error {
	v.mu.Lock()
	v.deleted = append(v.deleted, relativePath)
	delete(v.existing, relativePath)
	hook := v.onDelete
	v.mu.Unlock()

	if hook != nil {
		hook()
	}
	return nil
}

func (v *fakeStorageVolume) FileExists(ctx context.Context, relativePath string) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.existing[relativePath], nil
}

func (v *fakeStorageVolume) deletedPaths() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.deleted...)
}

// newPagingRecord builds a metadata fixture for the paging tests.
func newPagingRecord(fileKey string, status core.FileProcessingStatus) *core.FileMetadata {
	now := time.Now()
	return &core.FileMetadata{
		FileKey:      fileKey,
		TenantID:     pagingTestTenant,
		VolumeID:     pagingTestVolumeID,
		PhysicalPath: "tenant-001/" + fileKey + ".txt",
		FileSize:     10,
		Status:       status,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// pagingPhysicalPath mirrors the path newPagingRecord assigns.
func pagingPhysicalPath(fileKey string) string {
	return "tenant-001/" + fileKey + ".txt"
}

// newPagingCleanupService builds a cleanup service over an in-memory repository
// and volume, so scan behaviour can be observed without a database or a disk.
func newPagingCleanupService(repo core.MetadataRepository, volume core.StorageVolume) *cleanupService {
	return &cleanupService{
		tenantManager: newMultiTenantManager(pagingTestTenant),
		metadataRepo:  repo,
		volumes:       map[string]core.StorageVolume{volume.VolumeID(): volume},
		logger:        logging.Disabled(),
		// The paging tests assert the delete path's scan and delete behaviour, so
		// they select the Delete disposition explicitly; the option's zero value
		// is Keep.
		permanentlyFailedDisposition: core.PermanentlyFailedDelete,
	}
}

// TestCleanupStatusScan_PagingMatchesUnbounded is the core test-first regression:
// every converted cleanup path must scan through the optional paged capability
// when the repository provides it, and must visit and delete exactly the same
// records as the unbounded fallback when it does not.
//
// Before the fix each of these paths issued a single unbounded
// GetByStatus(limit=0) query - the exact unbounded-memory behaviour this change
// removes - so the "paged capability" mode failed with pageCalls == 0.
func TestCleanupStatusScan_PagingMatchesUnbounded(t *testing.T) {
	old := time.Now().Add(-2 * time.Hour)
	recent := time.Now()

	cases := []struct {
		name string
		// records builds a fresh fixture for one run.
		records func() []*core.FileMetadata
		// existing lists the physical paths present on the volume.
		existing []string
		// run executes the cleanup path under test.
		run func(t *testing.T, service core.CleanupService)
		// wantScanned is every file key the path must enumerate, exactly once.
		wantScanned []string
		// wantDeleted is every file key the path must delete, exactly once.
		wantDeleted []string
		// wantScans is the number of bounded status queries the path issues for
		// its one tenant.
		wantScans int
	}{
		{
			name: "completed files",
			records: func() []*core.FileMetadata {
				eligible := newPagingRecord("completed-a", core.FileStatusCompleted)
				eligible.CompletedAt = &old
				eligibleB := newPagingRecord("completed-b", core.FileStatusCompleted)
				eligibleB.CompletedAt = &old
				retained := newPagingRecord("completed-recent", core.FileStatusCompleted)
				retained.CompletedAt = &recent
				otherStatus := newPagingRecord("completed-other-status", core.FileStatusPending)

				return []*core.FileMetadata{eligible, eligibleB, retained, otherStatus}
			},
			run: func(t *testing.T, service core.CleanupService) {
				t.Helper()
				stats, err := service.CleanupCompletedFiles(context.Background(), time.Hour)
				if err != nil {
					t.Fatalf("CleanupCompletedFiles() error = %v", err)
				}
				if stats.CompletedRecordsRemoved != 2 {
					t.Errorf("CompletedRecordsRemoved = %d, want 2", stats.CompletedRecordsRemoved)
				}
			},
			wantScanned: []string{"completed-a", "completed-b", "completed-recent"},
			wantDeleted: []string{"completed-a", "completed-b"},
			wantScans:   1,
		},
		{
			name: "permanently failed files",
			records: func() []*core.FileMetadata {
				eligible := newPagingRecord("failed-old", core.FileStatusPermanentlyFailed)
				eligible.LastFailedAt = &old
				tooRecent := newPagingRecord("failed-recent", core.FileStatusPermanentlyFailed)
				tooRecent.LastFailedAt = &recent
				noTimestamp := newPagingRecord("failed-nil-timestamp", core.FileStatusPermanentlyFailed)
				noTimestamp.LastFailedAt = nil

				return []*core.FileMetadata{eligible, tooRecent, noTimestamp}
			},
			run: func(t *testing.T, service core.CleanupService) {
				t.Helper()
				stats, err := service.CleanupPermanentlyFailedFiles(context.Background(), time.Hour)
				if err != nil {
					t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
				}
				if stats.PermanentlyFailedFilesRemoved != 1 {
					t.Errorf("PermanentlyFailedFilesRemoved = %d, want 1", stats.PermanentlyFailedFilesRemoved)
				}
			},
			wantScanned: []string{"failed-old", "failed-recent", "failed-nil-timestamp"},
			wantDeleted: []string{"failed-old"},
			wantScans:   1,
		},
		{
			name: "orphaned metadata",
			records: func() []*core.FileMetadata {
				return []*core.FileMetadata{
					newPagingRecord("orphan-pending", core.FileStatusPending),
					newPagingRecord("orphan-processing", core.FileStatusProcessing),
					newPagingRecord("orphan-failed", core.FileStatusFailed),
					newPagingRecord("orphan-permanently-failed", core.FileStatusPermanentlyFailed),
					newPagingRecord("orphan-live", core.FileStatusPending),
					newPagingRecord("orphan-completed", core.FileStatusCompleted),
				}
			},
			// Only orphan-live exists physically; the other scanned records are
			// confirmed absent.
			existing: []string{pagingPhysicalPath("orphan-live")},
			run: func(t *testing.T, service core.CleanupService) {
				t.Helper()
				stats, err := service.CleanupOrphanedMetadata(context.Background())
				if err != nil {
					t.Fatalf("CleanupOrphanedMetadata() error = %v", err)
				}
				if stats.OrphanedMetadataRemoved != 4 {
					t.Errorf("OrphanedMetadataRemoved = %d, want 4", stats.OrphanedMetadataRemoved)
				}
			},
			// Completed files are not part of the orphan scan.
			wantScanned: []string{
				"orphan-pending", "orphan-processing", "orphan-failed",
				"orphan-permanently-failed", "orphan-live",
			},
			wantDeleted: []string{
				"orphan-pending", "orphan-processing", "orphan-failed",
				"orphan-permanently-failed",
			},
			wantScans: 4,
		},
	}

	modes := []struct {
		name     string
		paging   bool
		memory   func() *memoryMetadataRepository
		wrapRepo func(*memoryMetadataRepository) core.MetadataRepository
	}{
		{
			name:   "paged capability",
			paging: true,
			memory: newMemoryMetadataRepository,
			wrapRepo: func(memory *memoryMetadataRepository) core.MetadataRepository {
				return &pagingMetadataRepository{memory}
			},
		},
		{
			name:     "fallback unbounded query",
			paging:   false,
			memory:   newMemoryMetadataRepository,
			wrapRepo: func(memory *memoryMetadataRepository) core.MetadataRepository { return memory },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					memory := mode.memory()
					memory.store(tc.records()...)
					repo := mode.wrapRepo(memory)
					volume := newFakeStorageVolume(pagingTestVolumeID, tc.existing...)
					service := newPagingCleanupService(repo, volume)

					tc.run(t, service)

					if got := memory.getByStatusCallCount(); mode.paging && got != 0 {
						t.Errorf("paged repository used the unbounded GetByStatus %d time(s), want 0", got)
					}
					if got := memory.pageCallCount(); mode.paging {
						if got != tc.wantScans {
							t.Errorf("page calls = %d, want %d", got, tc.wantScans)
						}
						for _, limit := range memory.pageLimitValues() {
							if limit != cleanupPageSize {
								t.Errorf("page limit = %d, want %d", limit, cleanupPageSize)
							}
						}
					} else {
						if got != 0 {
							t.Errorf("fallback repository recorded %d page calls, want 0", got)
						}
						if got := memory.getByStatusCallCount(); got != tc.wantScans {
							t.Errorf("unbounded GetByStatus calls = %d, want %d", got, tc.wantScans)
						}
						for _, limit := range memory.getByStatusLimitValues() {
							if limit != 0 {
								t.Errorf("fallback query limit = %d, want 0 (unbounded)", limit)
							}
						}
					}

					assertSameKeys(t, "scanned", memory.scannedKeys(), tc.wantScanned)
					assertSameKeys(t, "deleted", memory.deletedKeys(), tc.wantDeleted)
				})
			}
		})
	}
}

// TestCleanupCompletedFiles_PagesEveryRecordOnce proves that a store larger than
// one page is processed completely and exactly once, including while the scan
// deletes the record it just returned.
//
// The stub's cursor is a file key and each page is recomputed from the live
// store, so a deletion behind the cursor cannot shift the window; if the pager
// were offset-based or the cleanup reset the cursor, this test would see skipped
// or duplicated records instead of all 1200.
func TestCleanupCompletedFiles_PagesEveryRecordOnce(t *testing.T) {
	const total = 1200

	completedAt := time.Now().Add(-2 * time.Hour)

	records := make([]*core.FileMetadata, 0, total)
	existing := make([]string, 0, total)
	wantKeys := make([]string, 0, total)
	var wantFreed int64
	for i := 0; i < total; i++ {
		fileKey := fmt.Sprintf("completed-%04d", i)
		record := newPagingRecord(fileKey, core.FileStatusCompleted)
		record.CompletedAt = &completedAt
		record.FileSize = int64(i)
		records = append(records, record)
		existing = append(existing, record.PhysicalPath)
		wantKeys = append(wantKeys, fileKey)
		wantFreed += record.FileSize
	}

	memory := newMemoryMetadataRepository()
	memory.store(records...)
	volume := newFakeStorageVolume(pagingTestVolumeID, existing...)
	service := newPagingCleanupService(&pagingMetadataRepository{memory}, volume)

	stats, err := service.CleanupCompletedFiles(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("CleanupCompletedFiles() error = %v", err)
	}
	if stats.CompletedRecordsRemoved != total {
		t.Errorf("CompletedRecordsRemoved = %d, want %d", stats.CompletedRecordsRemoved, total)
	}
	if stats.SpaceFreed != wantFreed {
		t.Errorf("SpaceFreed = %d, want %d", stats.SpaceFreed, wantFreed)
	}

	if got := memory.getByStatusCallCount(); got != 0 {
		t.Errorf("unbounded GetByStatus calls = %d, want 0", got)
	}

	deleted := memory.deletedKeys()
	if len(deleted) != total {
		t.Errorf("metadata deletions = %d, want %d (a record was skipped or handled twice)", len(deleted), total)
	}
	assertSameKeys(t, "deleted", deleted, wantKeys)

	scanned := memory.scannedKeys()
	if len(scanned) != total {
		t.Errorf("scanned records = %d, want %d", len(scanned), total)
	}
	assertSameKeys(t, "scanned", scanned, wantKeys)
	if !sort.SliceIsSorted(scanned, func(i, j int) bool { return scanned[i] < scanned[j] }) {
		t.Errorf("scan must stay forward-only in key order, got %v", scanned)
	}

	physical := volume.deletedPaths()
	if len(physical) != total {
		t.Errorf("physical deletions = %d, want %d", len(physical), total)
	}

	// 1200 records at the frozen page size of 500 must be three pages: 500, 500,
	// 200 - the last page ends the scan with an empty cursor.
	if got := memory.pageCallCount(); got != 3 {
		t.Errorf("page calls = %d, want 3", got)
	}
	var paged int
	for _, size := range memory.pageSizeValues() {
		if size > cleanupPageSize {
			t.Errorf("page size = %d, must never exceed %d", size, cleanupPageSize)
		}
		paged += size
	}
	if paged != total {
		t.Errorf("records returned across pages = %d, want %d", paged, total)
	}

	// A second run must find nothing left to do.
	second, err := service.CleanupCompletedFiles(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("second CleanupCompletedFiles() error = %v", err)
	}
	if second.CompletedRecordsRemoved != 0 {
		t.Errorf("second run CompletedRecordsRemoved = %d, want 0", second.CompletedRecordsRemoved)
	}
}

// TestCleanupPagedScan_ErrorStopsOperation verifies that a repository error in
// the middle of a paged scan stops the operation and surfaces the error instead
// of being swallowed or silently truncating the scan.
func TestCleanupPagedScan_ErrorStopsOperation(t *testing.T) {
	t.Run("completed files stop after the failing page", func(t *testing.T) {
		const total = 600

		completedAt := time.Now().Add(-2 * time.Hour)
		records := make([]*core.FileMetadata, 0, total)
		existing := make([]string, 0, total)
		for i := 0; i < total; i++ {
			record := newPagingRecord(fmt.Sprintf("completed-%04d", i), core.FileStatusCompleted)
			record.CompletedAt = &completedAt
			records = append(records, record)
			existing = append(existing, record.PhysicalPath)
		}

		memory := newMemoryMetadataRepository()
		memory.store(records...)
		memory.pageErrOnCall = 2
		memory.pageErr = errPageScan
		volume := newFakeStorageVolume(pagingTestVolumeID, existing...)
		service := newPagingCleanupService(&pagingMetadataRepository{memory}, volume)

		stats, err := service.CleanupCompletedFiles(context.Background(), time.Hour)
		if !errors.Is(err, errPageScan) {
			t.Fatalf("CleanupCompletedFiles() error = %v, want %v", err, errPageScan)
		}
		if !strings.Contains(err.Error(), pagingTestTenant) {
			t.Errorf("error %q must carry the tenant context", err)
		}
		if got := memory.pageCallCount(); got != 2 {
			t.Errorf("page calls = %d, want 2 (the scan must stop at the failure)", got)
		}
		if got := memory.getByStatusCallCount(); got != 0 {
			t.Errorf("unbounded GetByStatus calls = %d, want 0", got)
		}
		if got := stats.CompletedRecordsRemoved; got != cleanupPageSize {
			t.Errorf("CompletedRecordsRemoved = %d, want %d (only the first page was processed)", got, cleanupPageSize)
		}
		if got := len(memory.deletedKeys()); got != cleanupPageSize {
			t.Errorf("metadata deletions = %d, want %d", got, cleanupPageSize)
		}
		if _, getErr := memory.Get(context.Background(), pagingTestTenant, "completed-0500"); getErr != nil {
			t.Errorf("records after the failing page must be retained, Get() error = %v", getErr)
		}
	})

	t.Run("orphaned metadata surfaces the scan error", func(t *testing.T) {
		memory := newMemoryMetadataRepository()
		memory.store(
			newPagingRecord("orphan-a", core.FileStatusPending),
			newPagingRecord("orphan-b", core.FileStatusPending),
		)
		memory.pageErrOnCall = 1
		memory.pageErr = errPageScan
		service := newPagingCleanupService(&pagingMetadataRepository{memory}, newFakeStorageVolume(pagingTestVolumeID))

		stats, err := service.CleanupOrphanedMetadata(context.Background())
		if !errors.Is(err, errPageScan) {
			t.Fatalf("CleanupOrphanedMetadata() error = %v, want %v", err, errPageScan)
		}
		if got := memory.pageCallCount(); got != 1 {
			t.Errorf("page calls = %d, want 1 (the scan must stop at the failure)", got)
		}
		if stats.OrphanedMetadataRemoved != 0 {
			t.Errorf("OrphanedMetadataRemoved = %d, want 0", stats.OrphanedMetadataRemoved)
		}
		if got := len(memory.deletedKeys()); got != 0 {
			t.Errorf("metadata deletions = %d, want 0", got)
		}
	})
}

// TestCleanupPagedScan_HonoursContext verifies that cancellation is observed
// before the first page, between pages and between records, so a cancelled
// maintenance run cannot keep deleting after shutdown began.
func TestCleanupPagedScan_HonoursContext(t *testing.T) {
	completedAt := time.Now().Add(-2 * time.Hour)
	newFixture := func(count int) (*memoryMetadataRepository, []string) {
		records := make([]*core.FileMetadata, 0, count)
		existing := make([]string, 0, count)
		for i := 0; i < count; i++ {
			record := newPagingRecord(fmt.Sprintf("completed-%04d", i), core.FileStatusCompleted)
			record.CompletedAt = &completedAt
			records = append(records, record)
			existing = append(existing, record.PhysicalPath)
		}
		memory := newMemoryMetadataRepository()
		memory.store(records...)
		return memory, existing
	}

	t.Run("cancelled before the first page", func(t *testing.T) {
		memory, existing := newFixture(10)
		service := newPagingCleanupService(&pagingMetadataRepository{memory}, newFakeStorageVolume(pagingTestVolumeID, existing...))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		stats, err := service.CleanupCompletedFiles(ctx, time.Hour)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CleanupCompletedFiles() error = %v, want context.Canceled", err)
		}
		if got := memory.pageCallCount(); got != 0 {
			t.Errorf("page calls = %d, want 0", got)
		}
		if stats.CompletedRecordsRemoved != 0 {
			t.Errorf("CompletedRecordsRemoved = %d, want 0", stats.CompletedRecordsRemoved)
		}
	})

	t.Run("cancelled between pages", func(t *testing.T) {
		memory, existing := newFixture(600)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		memory.onPage = func(call int) {
			if call == 1 {
				cancel()
			}
		}
		service := newPagingCleanupService(&pagingMetadataRepository{memory}, newFakeStorageVolume(pagingTestVolumeID, existing...))

		stats, err := service.CleanupCompletedFiles(ctx, time.Hour)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CleanupCompletedFiles() error = %v, want context.Canceled", err)
		}
		if got := memory.pageCallCount(); got != 1 {
			t.Errorf("page calls = %d, want 1", got)
		}
		if stats.CompletedRecordsRemoved != 0 {
			t.Errorf("CompletedRecordsRemoved = %d, want 0", stats.CompletedRecordsRemoved)
		}
	})

	t.Run("cancelled between records", func(t *testing.T) {
		memory, existing := newFixture(10)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		volume := newFakeStorageVolume(pagingTestVolumeID, existing...)
		volume.onDelete = cancel
		service := newPagingCleanupService(&pagingMetadataRepository{memory}, volume)

		stats, err := service.CleanupCompletedFiles(ctx, time.Hour)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CleanupCompletedFiles() error = %v, want context.Canceled", err)
		}
		if stats.CompletedRecordsRemoved != 1 {
			t.Errorf("CompletedRecordsRemoved = %d, want 1 (must stop after the first record)", stats.CompletedRecordsRemoved)
		}
		if got := len(memory.deletedKeys()); got != 1 {
			t.Errorf("metadata deletions = %d, want 1", got)
		}
		if got := len(volume.deletedPaths()); got != 1 {
			t.Errorf("physical deletions = %d, want 1", got)
		}
	})
}

// assertSameKeys compares two key multisets, so a duplicate is reported as a
// mismatch rather than being hidden by set semantics.
func assertSameKeys(t *testing.T, label string, got, want []string) {
	t.Helper()

	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)

	if len(gotSorted) != len(wantSorted) {
		t.Errorf("%s keys = %v, want %v", label, gotSorted, wantSorted)
		return
	}
	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Errorf("%s keys = %v, want %v", label, gotSorted, wantSorted)
			return
		}
	}
}
