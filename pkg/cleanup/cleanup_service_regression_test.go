package cleanup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/scheduler"
)

// multiTenantManager is a TenantManager stub that returns a fixed tenant list.
type multiTenantManager struct {
	tenants []core.TenantContext
}

func (m *multiTenantManager) GetTenant(ctx context.Context, tenantID string) (core.TenantContext, error) {
	return core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled}, nil
}

func (m *multiTenantManager) TryGetTenant(ctx context.Context, tenantID string) (core.TenantContext, bool, error) {
	return core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled}, true, nil
}

func (m *multiTenantManager) IsTenantEnabled(ctx context.Context, tenantID string) (bool, error) {
	return true, nil
}

func (m *multiTenantManager) CreateTenant(ctx context.Context, tenantID string) error  { return nil }
func (m *multiTenantManager) EnableTenant(ctx context.Context, tenantID string) error  { return nil }
func (m *multiTenantManager) DisableTenant(ctx context.Context, tenantID string) error { return nil }

func (m *multiTenantManager) GetAllTenants(ctx context.Context) ([]core.TenantContext, error) {
	return m.tenants, nil
}

func newMultiTenantManager(tenantIDs ...string) *multiTenantManager {
	tenants := make([]core.TenantContext, 0, len(tenantIDs))
	for _, id := range tenantIDs {
		tenants = append(tenants, core.TenantContext{ID: id, Status: core.TenantStatusEnabled})
	}
	return &multiTenantManager{tenants: tenants}
}

// injectingVolume wraps a real volume so tests can force path-check and delete
// failures and observe context cancellation.
type injectingVolume struct {
	core.StorageVolume
	mu               sync.Mutex
	fileExistsErr    error
	fileExistsImpl   func(ctx context.Context, relativePath string) (bool, error)
	deleteFileErr    error
	fileExistsCalls  int
	deleteFileCalls  int
	lastDeleteCtxErr error
}

func (v *injectingVolume) FileExists(ctx context.Context, relativePath string) (bool, error) {
	v.mu.Lock()
	v.fileExistsCalls++
	err := v.fileExistsErr
	impl := v.fileExistsImpl
	v.mu.Unlock()

	if err != nil {
		return false, err
	}
	if impl != nil {
		return impl(ctx, relativePath)
	}
	return v.StorageVolume.FileExists(ctx, relativePath)
}

func (v *injectingVolume) DeleteFile(ctx context.Context, relativePath string) error {
	v.mu.Lock()
	v.deleteFileCalls++
	v.lastDeleteCtxErr = ctx.Err()
	err := v.deleteFileErr
	v.mu.Unlock()

	if err != nil {
		return err
	}
	return v.StorageVolume.DeleteFile(ctx, relativePath)
}

func (v *injectingVolume) deleteCalls() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.deleteFileCalls
}

func (v *injectingVolume) fileExistsQueries() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.fileExistsCalls
}

// countingDeleteRepository records Delete calls on top of a real repository.
type countingDeleteRepository struct {
	core.MetadataRepository
	mu          sync.Mutex
	deleteCalls int
}

func (r *countingDeleteRepository) Delete(ctx context.Context, tenantID, fileKey string) error {
	r.mu.Lock()
	r.deleteCalls++
	r.mu.Unlock()
	return r.MetadataRepository.Delete(ctx, tenantID, fileKey)
}

func (r *countingDeleteRepository) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deleteCalls
}

// stubTenantQuotaManager fails DecrementFileCount on demand and records counts.
type stubTenantQuotaManager struct {
	mu           sync.Mutex
	decrementErr error
	decrements   int
	increments   int
	counts       map[string]int
}

func newStubTenantQuotaManager() *stubTenantQuotaManager {
	return &stubTenantQuotaManager{counts: make(map[string]int)}
}

func (m *stubTenantQuotaManager) CanAddFile(ctx context.Context, tenantID string) (bool, error) {
	return true, nil
}

func (m *stubTenantQuotaManager) IncrementFileCount(ctx context.Context, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.increments++
	m.counts[tenantID]++
	return nil
}

func (m *stubTenantQuotaManager) DecrementFileCount(ctx context.Context, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.decrementErr != nil {
		return m.decrementErr
	}
	m.decrements++
	m.counts[tenantID]--
	return nil
}

func (m *stubTenantQuotaManager) GetFileCount(ctx context.Context, tenantID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[tenantID], nil
}

func (m *stubTenantQuotaManager) SetQuota(ctx context.Context, tenantID string, maxCount int) error {
	return nil
}

func (m *stubTenantQuotaManager) SetFileCount(ctx context.Context, tenantID string, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[tenantID] = count
	return nil
}

func (m *stubTenantQuotaManager) snapshot() (decrements, increments int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.decrements, m.increments
}

// stubDirectoryQuotaManager fails DecrementFileCount on demand and records counts.
type stubDirectoryQuotaManager struct {
	mu           sync.Mutex
	decrementErr error
	decrements   []string
	increments   []string
}

func (m *stubDirectoryQuotaManager) CanAddFile(ctx context.Context, tenantID string, directoryPath string) (bool, error) {
	return true, nil
}

func (m *stubDirectoryQuotaManager) IncrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.increments = append(m.increments, directoryPath)
	return nil
}

func (m *stubDirectoryQuotaManager) DecrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.decrementErr != nil {
		return m.decrementErr
	}
	m.decrements = append(m.decrements, directoryPath)
	return nil
}

func (m *stubDirectoryQuotaManager) GetFileCount(ctx context.Context, tenantID string, directoryPath string) (int, error) {
	return 0, nil
}

func (m *stubDirectoryQuotaManager) GetQuota(ctx context.Context, tenantID string, directoryPath string) (*core.DirectoryQuota, error) {
	return &core.DirectoryQuota{DirectoryPath: directoryPath}, nil
}

func (m *stubDirectoryQuotaManager) SetQuota(ctx context.Context, tenantID string, directoryPath string, maxCount int) error {
	return nil
}

func (m *stubDirectoryQuotaManager) SetFileCount(ctx context.Context, tenantID string, directoryPath string, count int) error {
	return nil
}

func (m *stubDirectoryQuotaManager) Snapshot() (decrements int, compensations int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.decrements), len(m.increments)
}

// newRegressionCleanupService builds a cleanup service over a real metadata
// repository, a real volume and an isolated tenant list.
func newRegressionCleanupService(
	t *testing.T,
	tenantIDs []string,
	customize func(opts *CleanupServiceOptions),
) (core.CleanupService, core.MetadataRepository, map[string]core.StorageVolume) {
	t.Helper()

	repo, repoDir := createTestRepository(t)
	t.Cleanup(func() { _ = repo.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(repoDir) })

	volumes := createTestVolumes(t)
	t.Cleanup(func() { cleanupVolumes(volumes) })

	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	opts := &CleanupServiceOptions{
		TenantManager:      newMultiTenantManager(tenantIDs...),
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
		// These regression tests cover the recorded delete ordering and the
		// missing-volume skip, so they select the Delete disposition explicitly.
		// The zero value is Keep; the Locus default (MoveToDeadLetter) is supplied
		// by the configuration model.
		PermanentlyFailedDisposition: core.PermanentlyFailedDelete,
	}
	if customize != nil {
		customize(opts)
	}

	service, err := NewCleanupService(opts)
	if err != nil {
		t.Fatalf("NewCleanupService() error = %v", err)
	}

	return service, repo, volumes
}

// TestCleanupEmptyDirectories_HonoursContext verifies that a cancelled context
// aborts the sweep instead of reporting success.
func TestCleanupEmptyDirectories_HonoursContext(t *testing.T) {
	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	mount := volumes["test-volume"].MountPath()
	if err := os.MkdirAll(filepath.Join(mount, "tenant-001", "a", "b", "c"), 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.CleanupEmptyDirectories(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CleanupEmptyDirectories() error = %v, want context.Canceled", err)
	}

	if _, statErr := os.Stat(filepath.Join(mount, "tenant-001", "a", "b", "c")); statErr != nil {
		t.Fatalf("cancelled sweep must not remove directories, stat error = %v", statErr)
	}
}

// TestCleanupEmptyDirectories_RemovesBottomUp verifies that nested empty
// directories are all reclaimed within a single cycle.
func TestCleanupEmptyDirectories_RemovesBottomUp(t *testing.T) {
	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	mount := volumes["test-volume"].MountPath()
	nested := filepath.Join(mount, "tenant-001", "a", "b", "c")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	stats, err := service.CleanupEmptyDirectories(context.Background())
	if err != nil {
		t.Fatalf("CleanupEmptyDirectories() error = %v", err)
	}
	if stats.EmptyDirectoriesRemoved != 3 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 3", stats.EmptyDirectoriesRemoved)
	}

	for _, path := range []string{
		filepath.Join(mount, "tenant-001", "a"),
		filepath.Join(mount, "tenant-001", "a", "b"),
		nested,
	} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Errorf("expected %s to be removed, stat error = %v", path, statErr)
		}
	}
	if _, statErr := os.Stat(filepath.Join(mount, "tenant-001")); statErr != nil {
		t.Errorf("tenant directory must remain, stat error = %v", statErr)
	}
}

// TestCleanupEmptyDirectories_ProtectsShardDirectories verifies that the shard
// hierarchy is protected for realistic tenant identifiers, so a sweep cannot
// race WriteFile's MkdirAll -> Create window.
func TestCleanupEmptyDirectories_ProtectsShardDirectories(t *testing.T) {
	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	mount := volumes["test-volume"].MountPath()
	protected := []string{
		filepath.Join(mount, "tenant-001", "ab"),
		filepath.Join(mount, "tenant-001", "ab", "cd"),
		filepath.Join(mount, "tenant-001", "2026", "04", "04", "15"),
	}
	customEmpty := filepath.Join(mount, "tenant-001", "custom-empty")

	for _, path := range protected {
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", path, err)
		}
	}
	if err := os.MkdirAll(customEmpty, 0755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", customEmpty, err)
	}

	if _, err := service.CleanupEmptyDirectories(context.Background()); err != nil {
		t.Fatalf("CleanupEmptyDirectories() error = %v", err)
	}

	for _, path := range protected {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Errorf("expected shard/date directory %s to be protected, stat error = %v", path, statErr)
		}
	}
	if _, statErr := os.Stat(customEmpty); !os.IsNotExist(statErr) {
		t.Errorf("expected non-system empty directory to be removed, stat error = %v", statErr)
	}
}

// TestCleanupOrphanedMetadata_SkipsOnUncertainty is the regression test for
// deleting live metadata when the physical check is inconclusive.
func TestCleanupOrphanedMetadata_SkipsOnUncertainty(t *testing.T) {
	const tenantID = "test-tenant"

	uncertainErr := errors.New("volume temporarily unavailable")

	tests := []struct {
		name string
		// file builds the metadata fixture for the case.
		file func(t *testing.T, vol core.StorageVolume) *core.FileMetadata
		// customize wires the injected volume behaviour and returns the expected
		// MetadataRepository that the service must use.
		customize func(t *testing.T, vols map[string]core.StorageVolume) core.MetadataRepository
	}{
		{
			name: "FileExists returns an error",
			file: func(t *testing.T, vol core.StorageVolume) *core.FileMetadata {
				t.Helper()
				path := "tenant-001/live.txt"
				writeErr := writeVolumeFile(t, vol, path)
				if writeErr != nil {
					t.Fatalf("WriteFile() error = %v", writeErr)
				}
				file := createTestFileMetadata("orphan-exists-error", core.FileStatusPending)
				file.PhysicalPath = path
				return file
			},
			customize: func(t *testing.T, vols map[string]core.StorageVolume) core.MetadataRepository {
				t.Helper()
				base := vols["test-volume"]
				vols["test-volume"] = &injectingVolume{StorageVolume: base, fileExistsErr: uncertainErr}
				return nil
			},
		},
		{
			name: "PhysicalPath is empty",
			file: func(t *testing.T, _ core.StorageVolume) *core.FileMetadata {
				t.Helper()
				file := createTestFileMetadata("orphan-empty-path", core.FileStatusPending)
				file.PhysicalPath = ""
				return file
			},
		},
		{
			name: "volume is missing from the map",
			file: func(t *testing.T, _ core.StorageVolume) *core.FileMetadata {
				t.Helper()
				file := createTestFileMetadata("orphan-unknown-volume", core.FileStatusPending)
				file.VolumeID = "missing-volume"
				return file
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var counts *countingDeleteRepository

			service, repo, volumes := newRegressionCleanupService(t, []string{tenantID}, func(opts *CleanupServiceOptions) {
				opts.TenantQuotaManager = newStubTenantQuotaManager()
			})

			if tc.customize != nil {
				_ = tc.customize(t, volumes)
			}

			file := tc.file(t, volumes["test-volume"])
			if err := repo.AddOrUpdate(context.Background(), file); err != nil {
				t.Fatalf("AddOrUpdate() error = %v", err)
			}

			stored, storedErr := repo.Get(context.Background(), tenantID, file.FileKey)
			if storedErr != nil {
				t.Fatalf("Get() after AddOrUpdate error = %v", storedErr)
			}
			if stored.VolumeID != file.VolumeID {
				t.Fatalf("stored VolumeID = %q, want %q", stored.VolumeID, file.VolumeID)
			}

			counts = &countingDeleteRepository{MetadataRepository: repo}
			replaceMetadataRepository(t, service, counts)

			stats, err := service.CleanupOrphanedMetadata(context.Background())
			if err != nil {
				t.Fatalf("CleanupOrphanedMetadata() error = %v", err)
			}
			if stats.OrphanedMetadataRemoved != 0 {
				t.Errorf("OrphanedMetadataRemoved = %d, want 0", stats.OrphanedMetadataRemoved)
			}
			if got := counts.calls(); got != 0 {
				t.Errorf("metadata Delete calls = %d, want 0", got)
			}

			if _, getErr := repo.Get(context.Background(), tenantID, file.FileKey); getErr != nil {
				t.Errorf("metadata must be retained, Get() error = %v", getErr)
			}

			// The unsafe path must have been reachable: for the FileExists-error
			// case the stub volume was actually queried.
			if injected, ok := volumes["test-volume"].(*injectingVolume); ok {
				if got := injected.fileExistsQueries(); got == 0 {
					t.Error("injected volume FileExists was never queried")
				}
			}
		})
	}
}

// TestCleanupOrphanedMetadata_ConfirmedMissing verifies that a confirmed absence
// deletes metadata exactly once, keeps quota accounting paired with it, and
// stays idempotent.
func TestCleanupOrphanedMetadata_ConfirmedMissing(t *testing.T) {
	const tenantID = "test-tenant"
	ctx := context.Background()

	dirQuotaRepo, dirQuotaDir := createTestDirQuotaRepository(t)
	t.Cleanup(func() { _ = dirQuotaRepo.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(dirQuotaDir) })

	dirQuotaMgr, err := quota.NewDirectoryQuotaManager(dirQuotaRepo)
	if err != nil {
		t.Fatalf("NewDirectoryQuotaManager() error = %v", err)
	}

	tenantQuotaMgr := newStubTenantQuotaManager()
	files := []*core.FileMetadata{
		createTestFileMetadata("orphan-a", core.FileStatusPending),
		createTestFileMetadata("orphan-b", core.FileStatusFailed),
	}

	service, repo, volumes := newRegressionCleanupService(t, []string{tenantID}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
	})

	vol := volumes["test-volume"]

	for _, file := range files {
		dir := filepath.Join(vol.MountPath(), "tenant-001", "dir")
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("MkdirAll() error = %v", err)
		}
		// Create, then remove, so the physical absence is genuine.
		path := filepath.Join("tenant-001", "dir", file.FileKey+".txt")
		if err := writeVolumeFile(t, vol, path); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		if err := vol.DeleteFile(ctx, path); err != nil {
			t.Fatalf("DeleteFile() error = %v", err)
		}
		file.PhysicalPath = path
		file.DirectoryPath = "/dir"
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("AddOrUpdate() error = %v", err)
		}
		if err := dirQuotaMgr.IncrementFileCount(ctx, tenantID, "/dir"); err != nil {
			t.Fatalf("IncrementFileCount(dir) error = %v", err)
		}
		if err := tenantQuotaMgr.IncrementFileCount(ctx, tenantID); err != nil {
			t.Fatalf("IncrementFileCount(tenant) error = %v", err)
		}
	}

	stats, err := service.CleanupOrphanedMetadata(ctx)
	if err != nil {
		t.Fatalf("CleanupOrphanedMetadata() error = %v", err)
	}
	if stats.OrphanedMetadataRemoved != len(files) {
		t.Fatalf("OrphanedMetadataRemoved = %d, want %d", stats.OrphanedMetadataRemoved, len(files))
	}

	for _, file := range files {
		if _, getErr := repo.Get(ctx, tenantID, file.FileKey); !errors.Is(getErr, core.ErrFileNotFound) {
			t.Errorf("metadata %s should be deleted, Get() error = %v", file.FileKey, getErr)
		}
	}

	if _, decrements := tenantQuotaMgr.snapshot(); decrements != len(files) {
		t.Errorf("tenant quota decrements = %d, want %d", decrements, len(files))
	}
	dirCount, dirErr := dirQuotaMgr.GetFileCount(ctx, tenantID, "/dir")
	if dirErr != nil {
		t.Fatalf("GetFileCount() error = %v", dirErr)
	}
	if dirCount != 0 {
		t.Errorf("directory count = %d, want 0", dirCount)
	}

	// Second run must be a no-op.
	second, err := service.CleanupOrphanedMetadata(ctx)
	if err != nil {
		t.Fatalf("second CleanupOrphanedMetadata() error = %v", err)
	}
	if second.OrphanedMetadataRemoved != 0 {
		t.Errorf("second run OrphanedMetadataRemoved = %d, want 0", second.OrphanedMetadataRemoved)
	}
	if _, decrements := tenantQuotaMgr.snapshot(); decrements != len(files) {
		t.Errorf("second run must not decrement again, decrements = %d, want %d", decrements, len(files))
	}
}

// TestCleanupPermanentlyFailedFiles_DeleteFailureCompensates verifies that a
// physical delete failure leaves quota accounting untouched.
func TestCleanupPermanentlyFailedFiles_DeleteFailureCompensates(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
	})

	base := volumes["test-volume"]
	injected := &injectingVolume{StorageVolume: base, deleteFileErr: errors.New("delete failed")}
	volumes["test-volume"] = injected

	file := createTestFileMetadata("failed-delete-error", core.FileStatusPermanentlyFailed)
	file.PhysicalPath = filepath.Join("tenant-001", "failed.txt")
	file.DirectoryPath = "/dir"
	old := time.Now().Add(-48 * time.Hour)
	file.LastFailedAt = &old
	if err := writeVolumeFile(t, base, file.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.PermanentlyFailedFilesRemoved != 0 {
		t.Errorf("PermanentlyFailedFilesRemoved = %d, want 0", stats.PermanentlyFailedFilesRemoved)
	}

	if _, getErr := repo.Get(ctx, "test-tenant", file.FileKey); getErr != nil {
		t.Errorf("metadata must be retained after a failed physical delete, Get() error = %v", getErr)
	}
	tenantDecrements, tenantIncrements := tenantQuotaMgr.snapshot()
	if tenantDecrements != 0 || tenantIncrements != 1 {
		t.Errorf("tenant quota decrements = %d increments = %d, want 0/1", tenantDecrements, tenantIncrements)
	}
	dirDecrements, dirCompensations := dirQuotaMgr.Snapshot()
	if dirDecrements != 0 || dirCompensations != 1 {
		t.Errorf("directory quota decrements = %d compensations = %d, want 0/1", dirDecrements, dirCompensations)
	}
	if got := injected.deleteCalls(); got != 1 {
		t.Errorf("physical delete attempts = %d, want 1", got)
	}
}

// TestCleanupPermanentlyFailedFiles_NilLastFailedAtIsNotEligible verifies that a
// permanently failed row without a failure timestamp is blocked instead of being
// deleted immediately.
func TestCleanupPermanentlyFailedFiles_NilLastFailedAtIsNotEligible(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
	})

	vol := volumes["test-volume"]
	file := createTestFileMetadata("failed-nil-timestamp", core.FileStatusPermanentlyFailed)
	file.PhysicalPath = filepath.Join("tenant-001", "nil-timestamp.txt")
	file.LastFailedAt = nil
	if err := writeVolumeFile(t, vol, file.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, 0)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.PermanentlyFailedFilesRemoved != 0 {
		t.Errorf("PermanentlyFailedFilesRemoved = %d, want 0", stats.PermanentlyFailedFilesRemoved)
	}
	if _, getErr := repo.Get(ctx, "test-tenant", file.FileKey); getErr != nil {
		t.Errorf("metadata with nil LastFailedAt must be retained, Get() error = %v", getErr)
	}
	exists, err := vol.FileExists(ctx, file.PhysicalPath)
	if err != nil || !exists {
		t.Errorf("physical file must be retained, exists = %v error = %v", exists, err)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0", decrements)
	}
}

// TestCleanupPermanentlyFailedFiles_DirectoryQuotaFailureKeepsAccounting verifies
// that a failed directory-quota decrement stops the chain before metadata is
// deleted or the tenant count is touched.
func TestCleanupPermanentlyFailedFiles_DirectoryQuotaFailureKeepsAccounting(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{decrementErr: errors.New("directory quota unavailable")}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
	})

	vol := volumes["test-volume"]
	file := createTestFileMetadata("failed-dirquota-error", core.FileStatusPermanentlyFailed)
	file.PhysicalPath = filepath.Join("tenant-001", "dirquota.txt")
	file.DirectoryPath = "/dir"
	old := time.Now().Add(-48 * time.Hour)
	file.LastFailedAt = &old
	if err := writeVolumeFile(t, vol, file.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.PermanentlyFailedFilesRemoved != 0 {
		t.Errorf("PermanentlyFailedFilesRemoved = %d, want 0", stats.PermanentlyFailedFilesRemoved)
	}
	if _, getErr := repo.Get(ctx, "test-tenant", file.FileKey); getErr != nil {
		t.Errorf("metadata must be retained, Get() error = %v", getErr)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0 (must stop before the tenant count)", decrements)
	}
	if dirDecrements, dirCompensations := dirQuotaMgr.Snapshot(); dirDecrements != 0 || dirCompensations != 1 {
		t.Errorf("directory quota decrements = %d compensations = %d, want 0/1", dirDecrements, dirCompensations)
	}
}

// TestCleanupPermanentlyFailedFiles_DeletedOnce verifies the happy path: the
// physical file goes first, quota counts are decremented once each, and metadata
// is removed once.
func TestCleanupPermanentlyFailedFiles_DeletedOnce(t *testing.T) {
	ctx := context.Background()

	dirQuotaRepo, dirQuotaDir := createTestDirQuotaRepository(t)
	t.Cleanup(func() { _ = dirQuotaRepo.Close() })
	t.Cleanup(func() { _ = os.RemoveAll(dirQuotaDir) })

	dirQuotaMgr, err := quota.NewDirectoryQuotaManager(dirQuotaRepo)
	if err != nil {
		t.Fatalf("NewDirectoryQuotaManager() error = %v", err)
	}
	tenantQuotaMgr := newStubTenantQuotaManager()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
	})

	vol := volumes["test-volume"]
	file := createTestFileMetadata("failed-happy", core.FileStatusPermanentlyFailed)
	file.PhysicalPath = filepath.Join("tenant-001", "happy.txt")
	file.DirectoryPath = "/dir"
	old := time.Now().Add(-48 * time.Hour)
	file.LastFailedAt = &old
	if err := writeVolumeFile(t, vol, file.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.PermanentlyFailedFilesRemoved != 1 {
		t.Fatalf("PermanentlyFailedFilesRemoved = %d, want 1", stats.PermanentlyFailedFilesRemoved)
	}

	if _, getErr := repo.Get(ctx, "test-tenant", file.FileKey); !errors.Is(getErr, core.ErrFileNotFound) {
		t.Errorf("metadata should be deleted, Get() error = %v", getErr)
	}
	exists, err := vol.FileExists(ctx, file.PhysicalPath)
	if err != nil || exists {
		t.Errorf("physical file should be deleted, exists = %v error = %v", exists, err)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
	dirCount, dirErr := dirQuotaMgr.GetFileCount(ctx, "test-tenant", "/dir")
	if dirErr != nil {
		t.Fatalf("GetFileCount() error = %v", dirErr)
	}
	if dirCount != 0 {
		t.Errorf("directory count = %d, want 0", dirCount)
	}

	second, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("second CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if second.PermanentlyFailedFilesRemoved != 0 {
		t.Errorf("second run PermanentlyFailedFilesRemoved = %d, want 0", second.PermanentlyFailedFilesRemoved)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 1 {
		t.Errorf("second run tenant quota decrements = %d, want 1", decrements)
	}
}

// writeVolumeFile writes through a volume or writes directly when the volume
// does not expose a writer (multi-tenant path helper).
func writeVolumeFile(t *testing.T, vol core.StorageVolume, relativePath string) error {
	t.Helper()
	fullPath := filepath.Join(vol.MountPath(), relativePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}
	_, err := vol.WriteFile(context.Background(), relativePath, bytes.NewReader([]byte("content")))
	return err
}

// replaceMetadataRepository swaps the repository the concrete cleanup service
// uses, so tests can observe Delete calls without changing production code.
func replaceMetadataRepository(t *testing.T, service core.CleanupService, repo core.MetadataRepository) {
	t.Helper()
	concrete, ok := service.(*cleanupService)
	if !ok {
		t.Fatalf("unexpected cleanup service type %T", service)
	}
	concrete.metadataRepo = repo
}
