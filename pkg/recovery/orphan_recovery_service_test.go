package recovery

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/sqlite"
)

const testFileKey = "0123456789abcdef0123456789abcdef"

func TestParsePhysicalPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantTenant string
		wantKey    string
		wantOK     bool
	}{
		{
			name:       "sharded layout",
			path:       "tenant-001/01/23/" + testFileKey + ".pdf",
			wantTenant: "tenant-001",
			wantKey:    testFileKey,
			wantOK:     true,
		},
		{
			name:       "flat layout without extension",
			path:       "tenant-001/" + testFileKey,
			wantTenant: "tenant-001",
			wantKey:    testFileKey,
			wantOK:     true,
		},
		{
			name:   "not a key",
			path:   "tenant-001/01/23/not-a-key.pdf",
			wantOK: false,
		},
		{
			name:   "wrong key length",
			path:   "tenant-001/01/23/0123456789abcdef.txt",
			wantOK: false,
		},
		{
			name:   "uppercase key",
			path:   "tenant-001/01/23/" + strings.ToUpper(testFileKey) + ".txt",
			wantOK: false,
		},
		{
			name:   "no tenant segment",
			path:   testFileKey + ".txt",
			wantOK: false,
		},
		{
			name:   "unsafe tenant segment",
			path:   "../01/23/" + testFileKey + ".txt",
			wantOK: false,
		},
		{
			name:   "empty",
			path:   "",
			wantOK: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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

func TestOrphanRecoveryRecoversStrayFileAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	payload := "orphaned payload"
	relative := filepath.Join("tenant-001", "01", "23", testFileKey+".pdf")
	writeFile(t, filepath.Join(volumeRoot, relative), payload)

	repo := newTestRepository(t, root)
	tenantQuota := newStubTenantQuota()
	dirQuota := newStubDirQuota()
	service := newTestService(t, repo, volumeRoot, tenantQuota, dirQuota)

	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 1 {
		t.Fatalf("FilesRecovered = %d, want 1 (%#v)", report.FilesRecovered, report)
	}
	if report.BytesRecovered != int64(len(payload)) {
		t.Fatalf("BytesRecovered = %d, want %d", report.BytesRecovered, len(payload))
	}

	restored, err := repo.Get(ctx, "tenant-001", testFileKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if restored == nil {
		t.Fatal("Get() = nil, want recovered metadata")
	}
	if restored.Status != core.FileStatusPending {
		t.Errorf("Status = %v, want Pending", restored.Status)
	}
	if restored.VolumeID != "vol-1" {
		t.Errorf("VolumeID = %q, want vol-1", restored.VolumeID)
	}
	if filepath.ToSlash(restored.PhysicalPath) != filepath.ToSlash(relative) {
		t.Errorf("PhysicalPath = %q, want %q", restored.PhysicalPath, relative)
	}
	if restored.FileExtension != ".pdf" {
		t.Errorf("FileExtension = %q, want .pdf", restored.FileExtension)
	}
	if restored.FileSize != int64(len(payload)) {
		t.Errorf("FileSize = %d, want %d", restored.FileSize, len(payload))
	}

	if tenantQuota.increments() != 1 || dirQuota.increments() != 1 {
		t.Errorf("quota increments = %d/%d, want 1/1", tenantQuota.increments(), dirQuota.increments())
	}

	// A second scan must not duplicate the record or the quota charge.
	second, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("second RecoverNow() error = %v", err)
	}
	if second.FilesRecovered != 0 {
		t.Fatalf("second FilesRecovered = %d, want 0", second.FilesRecovered)
	}
	if tenantQuota.increments() != 1 || dirQuota.increments() != 1 {
		t.Errorf("quota increments after second scan = %d/%d, want 1/1", tenantQuota.increments(), dirQuota.increments())
	}
}

func TestOrphanRecoverySkipsTrackedAndUnrecognizedFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")

	trackedRelative := filepath.Join("tenant-001", "aa", "bb", testFileKey+".txt")
	writeFile(t, filepath.Join(volumeRoot, trackedRelative), "tracked")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", "not-a-key.txt"), "noise")
	// A 32-character non-hex name must not be treated as a file key. The case
	// differs from testFileKey as well, but Windows paths are case-insensitive,
	// so the invalid key must differ by more than case.
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", strings.Repeat("z", 32)+".txt"), "noise")

	repo := newTestRepository(t, root)
	if err := repo.AddOrUpdate(ctx, &core.FileMetadata{
		FileKey: testFileKey, TenantID: "tenant-001", VolumeID: "vol-1",
		PhysicalPath: filepath.ToSlash(trackedRelative), Status: core.FileStatusPending,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("AddOrUpdate: %v", err)
	}

	service := newTestService(t, repo, volumeRoot, nil, nil)
	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 0 {
		t.Fatalf("FilesRecovered = %d, want 0 (%#v)", report.FilesRecovered, report)
	}
	if report.FilesSkipped != 3 {
		t.Fatalf("FilesSkipped = %d, want 3 (%#v)", report.FilesSkipped, report)
	}
}

func TestOrphanRecoveryRespectsTenantQuota(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "payload")

	repo := newTestRepository(t, root)
	tenantQuota := newStubTenantQuota()
	tenantQuota.failIncrement = true
	service := newTestService(t, repo, volumeRoot, tenantQuota, nil)

	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 0 || report.FilesSkipped != 1 {
		t.Fatalf("recovered/skipped = %d/%d, want 0/1 (%#v)", report.FilesRecovered, report.FilesSkipped, report)
	}
	if _, err := repo.Get(ctx, "tenant-001", testFileKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() error = %v, want ErrFileNotFound (recovery must not exceed quota)", err)
	}
}

func TestOrphanRecoveryCompensatesQuotaWhenPersistFails(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "payload")

	tenantQuota := newStubTenantQuota()
	dirQuota := newStubDirQuota()
	service, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository:    failingRepository{},
		Volumes:               map[string]core.StorageVolume{"vol-1": &stubVolume{id: "vol-1", mount: volumeRoot}},
		TenantQuotaManager:    tenantQuota,
		DirectoryQuotaManager: dirQuota,
	})
	if err != nil {
		t.Fatalf("NewOrphanRecoveryService() error = %v", err)
	}

	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesFailed != 1 {
		t.Fatalf("FilesFailed = %d, want 1 (%#v)", report.FilesFailed, report)
	}
	if tenantQuota.increments() != 0 || dirQuota.increments() != 0 {
		t.Errorf("quota not compensated: tenant=%d dir=%d, want 0/0", tenantQuota.increments(), dirQuota.increments())
	}
}

func TestOrphanRecoverySkipsFilesYoungerThanMinimumAge(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "payload")

	repo := newTestRepository(t, root)
	service, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository: repo,
		Volumes:            map[string]core.StorageVolume{"vol-1": &stubVolume{id: "vol-1", mount: volumeRoot}},
		RecoveryInterval:   time.Hour,
		MinimumFileAge:     time.Hour,
	})
	if err != nil {
		t.Fatalf("NewOrphanRecoveryService() error = %v", err)
	}

	// A file written moments ago must not be recovered while a writer could
	// still be filling it.
	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 0 || report.FilesSkipped != 1 {
		t.Fatalf("recovered/skipped = %d/%d, want 0/1 (%#v)", report.FilesRecovered, report.FilesSkipped, report)
	}
	if _, err := repo.Get(ctx, "tenant-001", testFileKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("Get() error = %v, want ErrFileNotFound", err)
	}
}

func TestOrphanRecoverySerializesConcurrentScans(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "payload")

	repo := newTestRepository(t, root)
	service := newTestService(t, repo, volumeRoot, nil, nil)

	const scans = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		recovered int
		failed    int
	)
	for i := 0; i < scans; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report, err := service.RecoverNow(ctx)
			if err != nil {
				t.Errorf("RecoverNow() error = %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			recovered += report.FilesRecovered
			failed += report.FilesFailed
		}()
	}
	wg.Wait()

	// Exactly one scan may register the file; the others observe metadata.
	if recovered != 1 {
		t.Errorf("recovered across %d concurrent scans = %d, want 1", scans, recovered)
	}
	if failed != 0 {
		t.Errorf("failed = %d, want 0 (scans must be serialized, not conflicting)", failed)
	}
}

func TestOrphanRecoveryServiceLifecycle(t *testing.T) {
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	if err := os.MkdirAll(volumeRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	service := newTestService(t, newTestRepository(t, root), volumeRoot, nil, nil)
	if service.IsRunning() {
		t.Fatal("IsRunning() = true before Start")
	}
	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !service.IsRunning() {
		t.Fatal("IsRunning() = false after Start")
	}
	if err := service.Start(); err == nil {
		t.Fatal("second Start() error = nil, want error")
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if service.IsRunning() {
		t.Fatal("IsRunning() = true after Stop")
	}
	if err := service.Stop(); err == nil {
		t.Fatal("second Stop() error = nil, want error")
	}
}

func TestNewOrphanRecoveryServiceValidatesOptions(t *testing.T) {
	volumeRoot := t.TempDir()
	volume := &stubVolume{id: "vol-1", mount: volumeRoot}

	if _, err := NewOrphanRecoveryService(nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("nil options error = %v, want ErrInvalidArgument", err)
	}
	if _, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		Volumes: map[string]core.StorageVolume{"vol-1": volume},
	}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("nil repository error = %v, want ErrInvalidArgument", err)
	}
	if _, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository: failingRepository{},
	}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("no volumes error = %v, want ErrInvalidArgument", err)
	}
}

// --- test doubles ---

type stubVolume struct {
	id    string
	mount string
}

func (v *stubVolume) VolumeID() string                                 { return v.id }
func (v *stubVolume) MountPath() string                                { return v.mount }
func (v *stubVolume) IsHealthy(context.Context) bool                   { return true }
func (v *stubVolume) TotalCapacity(context.Context) (int64, error)     { return 0, nil }
func (v *stubVolume) AvailableSpace(context.Context) (int64, error)    { return 0, nil }
func (v *stubVolume) DeleteFile(context.Context, string) error         { return nil }
func (v *stubVolume) FileExists(context.Context, string) (bool, error) { return false, nil }

func (v *stubVolume) WriteFile(context.Context, string, io.Reader) (int64, error) {
	return 0, errors.New("not implemented")
}

func (v *stubVolume) ReadFile(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

type stubCounter struct {
	mu            sync.Mutex
	count         int
	failIncrement bool
}

func (c *stubCounter) increments() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *stubCounter) increment() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failIncrement {
		return core.ErrTenantQuotaExceeded
	}
	c.count++
	return nil
}

func (c *stubCounter) decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count--
}

// stubTenantQuota implements core.TenantQuotaManager.
type stubTenantQuota struct{ stubCounter }

func newStubTenantQuota() *stubTenantQuota { return &stubTenantQuota{} }

func (q *stubTenantQuota) CanAddFile(context.Context, string) (bool, error) { return true, nil }
func (q *stubTenantQuota) IncrementFileCount(context.Context, string) error { return q.increment() }
func (q *stubTenantQuota) DecrementFileCount(context.Context, string) error {
	q.decrement()
	return nil
}
func (q *stubTenantQuota) GetFileCount(context.Context, string) (int, error) {
	return q.increments(), nil
}
func (q *stubTenantQuota) SetQuota(context.Context, string, int) error     { return nil }
func (q *stubTenantQuota) SetFileCount(context.Context, string, int) error { return nil }

// stubDirQuota implements core.DirectoryQuotaManager.
type stubDirQuota struct{ stubCounter }

func newStubDirQuota() *stubDirQuota { return &stubDirQuota{} }

func (q *stubDirQuota) CanAddFile(context.Context, string, string) (bool, error) { return true, nil }
func (q *stubDirQuota) IncrementFileCount(context.Context, string, string) error {
	return q.increment()
}
func (q *stubDirQuota) DecrementFileCount(context.Context, string, string) error {
	q.decrement()
	return nil
}
func (q *stubDirQuota) GetFileCount(context.Context, string, string) (int, error) {
	return q.increments(), nil
}
func (q *stubDirQuota) SetQuota(context.Context, string, string, int) error     { return nil }
func (q *stubDirQuota) SetFileCount(context.Context, string, string, int) error { return nil }
func (q *stubDirQuota) GetQuota(context.Context, string, string) (*core.DirectoryQuota, error) {
	return &core.DirectoryQuota{DirectoryPath: "/"}, nil
}

// failingRepository fails every metadata write, to exercise quota compensation.
type failingRepository struct{}

func (failingRepository) AddOrUpdate(context.Context, *core.FileMetadata) error {
	return errors.New("write failed")
}

func (failingRepository) AddOrUpdateBatch(context.Context, []*core.FileMetadata) error {
	return errors.New("write failed")
}

func (failingRepository) Get(context.Context, string, string) (*core.FileMetadata, error) {
	return nil, core.ErrFileNotFound
}

func (failingRepository) Delete(context.Context, string, string) error { return nil }

func (failingRepository) DeleteBatch(context.Context, string, []string) error { return nil }

func (failingRepository) GetByStatus(context.Context, string, core.FileProcessingStatus, int) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (failingRepository) GetPendingFiles(context.Context, string, int) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (failingRepository) UpdateStatus(context.Context, string, string, core.FileProcessingStatus) error {
	return nil
}

func (failingRepository) CompareAndTransitionToProcessing(context.Context, string, string) (*core.FileMetadata, error) {
	return nil, errors.New("not implemented")
}

func (failingRepository) CompareAndUpdateProcessing(context.Context, core.FileProcessingLease, func(*core.FileMetadata) error) (*core.FileMetadata, error) {
	return nil, errors.New("not implemented")
}

func (failingRepository) GetTimedOutProcessingFiles(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
	return nil, nil
}

func (failingRepository) Optimize(context.Context) error { return nil }
func (failingRepository) Close() error                   { return nil }

// newTestRepository builds a SQLite metadata repository inside a temporary
// directory of its own, so its tenant database
// ({DataPath}/{tenantId}/metadata.db) never falls inside the volume tree a test
// scans. The caller's root is deliberately unused: that tree is the only thing
// the orphan scan walks, and a database left inside it would be recovered as an
// orphaned file.
func newTestRepository(t *testing.T, _ string) core.MetadataRepository {
	t.Helper()

	// Declared first so it is registered first: t.Cleanup runs in reverse
	// registration order, which closes the repository before the directory is
	// removed.
	metadataRoot := t.TempDir()

	repo, err := metadata.NewSQLiteMetadataRepository(&metadata.SQLiteRepositoryOptions{
		DataPath: metadataRoot,
		Sqlite:   sqlite.DefaultOptions(),
	})
	if err != nil {
		t.Fatalf("NewSQLiteMetadataRepository() error = %v", err)
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Errorf("close repository: %v", err)
		}
	})
	return repo
}

func newTestService(
	t *testing.T,
	repo core.MetadataRepository,
	volumeRoot string,
	tenantQuota core.TenantQuotaManager,
	dirQuota core.DirectoryQuotaManager,
) *OrphanRecoveryService {
	t.Helper()
	service, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository:    repo,
		Volumes:               map[string]core.StorageVolume{"vol-1": &stubVolume{id: "vol-1", mount: volumeRoot}},
		TenantQuotaManager:    tenantQuota,
		DirectoryQuotaManager: dirQuota,
		RecoveryInterval:      time.Hour,
	})
	if err != nil {
		t.Fatalf("NewOrphanRecoveryService() error = %v", err)
	}
	return service
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}
