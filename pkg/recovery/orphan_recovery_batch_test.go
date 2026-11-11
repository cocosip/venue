package recovery

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// batchCountingRepository records the batched existence queries and the point
// queries made through it, so a test can tell which recovery path ran.
type batchCountingRepository struct {
	core.MetadataRepository

	mu       sync.Mutex
	batches  []int
	batchErr error
	getCalls int
}

func (r *batchCountingRepository) ExistingFileKeys(ctx context.Context, tenantID string, fileKeys []string) (map[string]bool, error) {
	r.mu.Lock()
	r.batches = append(r.batches, len(fileKeys))
	batchErr := r.batchErr
	r.mu.Unlock()

	if batchErr != nil {
		return nil, batchErr
	}

	existing := make(map[string]bool, len(fileKeys))
	for _, fileKey := range fileKeys {
		if _, err := r.MetadataRepository.Get(ctx, tenantID, fileKey); err == nil {
			existing[fileKey] = true
		} else if !errors.Is(err, core.ErrFileNotFound) {
			return nil, err
		}
	}
	return existing, nil
}

func (r *batchCountingRepository) Get(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	r.mu.Lock()
	r.getCalls++
	r.mu.Unlock()
	return r.MetadataRepository.Get(ctx, tenantID, fileKey)
}

func (r *batchCountingRepository) batchSizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.batches...)
}

func (r *batchCountingRepository) pointQueries() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getCalls
}

// writeOrphanFiles creates count physically present, valid managed payloads under
// one tenant and returns their file keys.
func writeOrphanFiles(t *testing.T, volumeRoot string, count int) []string {
	t.Helper()

	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		fileKey := fmt.Sprintf("%032x", i)
		keys = append(keys, fileKey)
		writeFile(t, filepath.Join(volumeRoot, "tenant-001", fileKey+".txt"), "payload")
	}
	return keys
}

// newBatchRecoveryService builds a recovery service over the batch-counting
// wrapper and returns it with the wrapper.
func newBatchRecoveryService(t *testing.T, volumeRoot string, batchSize int) (*OrphanRecoveryService, *batchCountingRepository) {
	t.Helper()

	repo := &batchCountingRepository{MetadataRepository: newTestRepository(t, volumeRoot)}
	service, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository: repo,
		Volumes:            map[string]core.StorageVolume{"vol-1": &stubVolume{id: "vol-1", mount: volumeRoot}},
		RecoveryInterval:   time.Hour,
		BatchSize:          batchSize,
	})
	if err != nil {
		t.Fatalf("NewOrphanRecoveryService() error = %v", err)
	}
	return service, repo
}

// TestOrphanRecoveryBatchesExistenceChecks is the regression test for the
// per-file point query: a full-volume scan must resolve whole bounded batches of
// keys with one metadata query instead of one Get per physical file, and it must
// rebuild exactly the missing ones.
func TestOrphanRecoveryBatchesExistenceChecks(t *testing.T) {
	const total = 300
	const tracked = 50

	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	keys := writeOrphanFiles(t, volumeRoot, total)

	service, repo := newBatchRecoveryService(t, volumeRoot, 0)

	// Pre-register the first tracked keys so the batch must skip them.
	ctx := context.Background()
	for _, fileKey := range keys[:tracked] {
		if err := repo.AddOrUpdate(ctx, &core.FileMetadata{
			FileKey: fileKey, TenantID: "tenant-001", VolumeID: "vol-1",
			PhysicalPath: "tenant-001/" + fileKey + ".txt", Status: core.FileStatusPending,
			CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}); err != nil {
			t.Fatalf("AddOrUpdate() error = %v", err)
		}
	}

	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}

	if report.FilesRecovered != total-tracked {
		t.Fatalf("FilesRecovered = %d, want %d", report.FilesRecovered, total-tracked)
	}
	if report.FilesSkipped != tracked {
		t.Fatalf("FilesSkipped = %d, want %d", report.FilesSkipped, tracked)
	}
	if report.FilesFailed != 0 {
		t.Fatalf("FilesFailed = %d, want 0", report.FilesFailed)
	}

	batches := repo.batchSizes()
	if len(batches) == 0 {
		t.Fatal("no batched existence query was issued")
	}
	queried := 0
	for _, size := range batches {
		if size > orphanLookupBatchSize {
			t.Errorf("batch size = %d, must never exceed %d", size, orphanLookupBatchSize)
		}
		queried += size
	}
	if queried != total {
		t.Errorf("keys covered by batches = %d, want %d", queried, total)
	}
	if got := repo.pointQueries(); got != 0 {
		t.Errorf("point queries = %d, want 0 when the batch capability is available", got)
	}

	// Every orphan now has metadata; the tracked ones were not duplicated.
	for _, fileKey := range keys {
		if _, getErr := repo.MetadataRepository.Get(ctx, "tenant-001", fileKey); getErr != nil {
			t.Fatalf("Get(%s) error = %v", fileKey, getErr)
		}
	}
}

// TestOrphanRecoveryFallsBackToPointQueries covers the repository without the
// batch capability: the per-key Get path must still recover every orphan.
func TestOrphanRecoveryFallsBackToPointQueries(t *testing.T) {
	const total = 20

	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeOrphanFiles(t, volumeRoot, total)

	// The wrapper embeds only core.MetadataRepository, so the batch capability
	// is hidden: this models a third-party repository.
	service, repo := newBatchRecoveryService(t, volumeRoot, 0)
	service.metadataRepo = repo.MetadataRepository

	report, err := service.RecoverNow(context.Background())
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != total {
		t.Fatalf("FilesRecovered = %d, want %d", report.FilesRecovered, total)
	}
	if got := len(repo.batchSizes()); got != 0 {
		t.Errorf("batched queries = %d, want 0 for a repository without the capability", got)
	}
}

// TestOrphanRecoveryFallsBackWhenTheBatchQueryFails keeps the per-file failure
// accounting intact: a failed batch must not lose the scan, so the same batch is
// resolved with point queries.
func TestOrphanRecoveryFallsBackWhenTheBatchQueryFails(t *testing.T) {
	const total = 20

	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeOrphanFiles(t, volumeRoot, total)

	service, repo := newBatchRecoveryService(t, volumeRoot, 0)
	repo.batchErr = errors.New("injected batch failure")

	report, err := service.RecoverNow(context.Background())
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != total {
		t.Fatalf("FilesRecovered = %d, want %d", report.FilesRecovered, total)
	}
	if report.FilesFailed != 0 {
		t.Fatalf("FilesFailed = %d, want 0 (point queries must recover the batch)", report.FilesFailed)
	}
	if got := repo.pointQueries(); got != total {
		t.Errorf("point queries = %d, want %d", got, total)
	}
}

// TestOrphanRecoveryBatchPreservesTheRecoveryLimit verifies the report counters
// stay exactly what the file-by-file scan produced: a capped scan stops at the
// same file and scans no further. Earlier passes leave tracked files behind, which
// every later pass re-scans as skipped, exactly like the point-query scan.
func TestOrphanRecoveryBatchPreservesTheRecoveryLimit(t *testing.T) {
	const total = 25
	const batchLimit = 10

	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeOrphanFiles(t, volumeRoot, total)

	service, _ := newBatchRecoveryService(t, volumeRoot, batchLimit)

	// Each pass scans everything up to the file that fills the recovery limit:
	// pass 1 recovers files 1-10, pass 2 re-scans them as tracked before
	// recovering files 11-20, and pass 3 re-scans 1-20 before recovering 21-25.
	wantScanned := []int{10, 20, 25}
	wantRecovered := []int{10, 10, 5}

	for pass, want := range wantScanned {
		report, err := service.RecoverNow(context.Background())
		if err != nil {
			t.Fatalf("RecoverNow() pass %d error = %v", pass, err)
		}
		if report.FilesRecovered != wantRecovered[pass] {
			t.Errorf("pass %d FilesRecovered = %d, want %d", pass, report.FilesRecovered, wantRecovered[pass])
		}
		if report.FilesScanned != want {
			t.Errorf("pass %d FilesScanned = %d, want %d (a capped scan must stop at the same file)", pass, report.FilesScanned, want)
		}
	}
}

// TestParsePhysicalPathSeparatorEdgeCases pins the allocation-free parser against
// the segment-splitting behaviour it replaced, including the strict key rules.
func TestParsePhysicalPathSeparatorEdgeCases(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef"

	tests := []struct {
		path       string
		wantTenant string
		wantKey    string
		wantOK     bool
	}{
		{path: "tenant-001/" + key + ".tar.gz", wantTenant: "tenant-001", wantKey: key, wantOK: true},
		{path: "tenant-001/aa/bb/" + key, wantTenant: "tenant-001", wantKey: key, wantOK: true},
		{path: "tenant-001//" + key, wantTenant: "tenant-001", wantKey: key, wantOK: true},
		{path: "tenant-001/", wantOK: false},
		{path: "tenant-001", wantOK: false},
		{path: "/" + key, wantOK: false},
		{path: "tenant-001/" + key + ".TXT", wantTenant: "tenant-001", wantKey: key, wantOK: true},
		{path: "tenant-001/" + "0123456789ABCDEF0123456789abcdef", wantOK: false},
		{path: "tenant-001/" + "0123456789abcdef0123456789abcdeg", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			tenantID, fileKey, ok := ParsePhysicalPath(test.path)
			if ok != test.wantOK {
				t.Fatalf("ParsePhysicalPath(%q) ok = %v, want %v", test.path, ok, test.wantOK)
			}
			if !ok {
				return
			}
			if tenantID != test.wantTenant || fileKey != test.wantKey {
				t.Fatalf("ParsePhysicalPath(%q) = (%q, %q), want (%q, %q)",
					test.path, tenantID, fileKey, test.wantTenant, test.wantKey)
			}
		})
	}
}
