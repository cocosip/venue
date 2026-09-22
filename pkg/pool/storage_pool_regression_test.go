package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/volume"
)

// fakeVolume is a controllable core.StorageVolume used by the storage-pool
// regression tests. Call counters are atomic so the tests stay race-free.
type fakeVolume struct {
	id        string
	mountPath string
	healthy   bool
	available int64
	total     int64

	availableErr error
	totalErr     error
	writeErr     error
	readErr      error
	deleteErr    error
	buildPathErr error

	// consumeBeforeWriteFailure drains this many bytes from the content reader
	// before reporting writeErr, simulating a partial write.
	consumeBeforeWriteFailure int

	writeCalls     atomic.Int64
	deleteCalls    atomic.Int64
	healthyCalls   atomic.Int64
	totalCalls     atomic.Int64
	availableCalls atomic.Int64

	mu      sync.Mutex
	written map[string][]byte
}

func newFakeVolume(id string, healthy bool, availableSpace int64) *fakeVolume {
	return &fakeVolume{
		id:        id,
		mountPath: filepath.Join("fake-mount", id),
		healthy:   healthy,
		available: availableSpace,
		total:     availableSpace * 2,
		written:   make(map[string][]byte),
	}
}

func (v *fakeVolume) VolumeID() string { return v.id }

func (v *fakeVolume) MountPath() string { return v.mountPath }

func (v *fakeVolume) IsHealthy(context.Context) bool {
	v.healthyCalls.Add(1)
	return v.healthy
}

func (v *fakeVolume) TotalCapacity(context.Context) (int64, error) {
	v.totalCalls.Add(1)
	return v.total, v.totalErr
}

func (v *fakeVolume) AvailableSpace(context.Context) (int64, error) {
	v.availableCalls.Add(1)
	return v.available, v.availableErr
}

func (v *fakeVolume) WriteFile(_ context.Context, relativePath string, content io.Reader) (int64, error) {
	v.writeCalls.Add(1)
	if v.writeErr != nil {
		if v.consumeBeforeWriteFailure > 0 {
			_, _ = io.CopyN(io.Discard, content, int64(v.consumeBeforeWriteFailure))
		}
		return 0, v.writeErr
	}
	data, err := io.ReadAll(content)
	if err != nil {
		return 0, err
	}
	v.mu.Lock()
	v.written[relativePath] = data
	v.mu.Unlock()
	return int64(len(data)), nil
}

func (v *fakeVolume) ReadFile(_ context.Context, relativePath string) (io.ReadCloser, error) {
	if v.readErr != nil {
		return nil, v.readErr
	}
	v.mu.Lock()
	data, ok := v.written[relativePath]
	v.mu.Unlock()
	if !ok {
		return nil, core.ErrFileNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (v *fakeVolume) DeleteFile(_ context.Context, relativePath string) error {
	v.deleteCalls.Add(1)
	if v.deleteErr != nil {
		return v.deleteErr
	}
	v.mu.Lock()
	delete(v.written, relativePath)
	v.mu.Unlock()
	return nil
}

func (v *fakeVolume) FileExists(_ context.Context, relativePath string) (bool, error) {
	v.mu.Lock()
	_, ok := v.written[relativePath]
	v.mu.Unlock()
	return ok, nil
}

// BuildPhysicalPath makes the fake volume a core.StorageVolumePathBuilder so the
// pool produces deterministic relative paths in tests.
func (v *fakeVolume) BuildPhysicalPath(tenantID string, fileKey string, fileExtension string) (string, error) {
	if v.buildPathErr != nil {
		return "", v.buildPathErr
	}
	return filepath.Join(tenantID, fileKey+fileExtension), nil
}

func (v *fakeVolume) writtenContent(relativePath string) ([]byte, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	data, ok := v.written[relativePath]
	return data, ok
}

func (v *fakeVolume) writtenCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.written)
}

// fakeTenantQuotaManager counts tenant-level quota mutations.
type fakeTenantQuotaManager struct {
	mu           sync.Mutex
	incrementErr error
	increments   int
	decrements   int
}

func (m *fakeTenantQuotaManager) CanAddFile(context.Context, string) (bool, error) {
	return true, nil
}

func (m *fakeTenantQuotaManager) IncrementFileCount(context.Context, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.incrementErr != nil {
		return m.incrementErr
	}
	m.increments++
	return nil
}

func (m *fakeTenantQuotaManager) DecrementFileCount(context.Context, string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decrements++
	return nil
}

func (m *fakeTenantQuotaManager) GetFileCount(context.Context, string) (int, error) {
	return 0, nil
}

func (m *fakeTenantQuotaManager) SetQuota(context.Context, string, int) error { return nil }

func (m *fakeTenantQuotaManager) SetFileCount(context.Context, string, int) error { return nil }

func (m *fakeTenantQuotaManager) counts() (increments, decrements int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.increments, m.decrements
}

// fakeDirectoryQuotaManager counts directory-level quota mutations.
type fakeDirectoryQuotaManager struct {
	mu           sync.Mutex
	incrementErr error
	increments   []string
	decrements   []string
}

func (m *fakeDirectoryQuotaManager) CanAddFile(context.Context, string, string) (bool, error) {
	return true, nil
}

func (m *fakeDirectoryQuotaManager) IncrementFileCount(_ context.Context, _ string, directoryPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.incrementErr != nil {
		return m.incrementErr
	}
	m.increments = append(m.increments, directoryPath)
	return nil
}

func (m *fakeDirectoryQuotaManager) DecrementFileCount(_ context.Context, _ string, directoryPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decrements = append(m.decrements, directoryPath)
	return nil
}

func (m *fakeDirectoryQuotaManager) GetFileCount(context.Context, string, string) (int, error) {
	return 0, nil
}

func (m *fakeDirectoryQuotaManager) SetQuota(context.Context, string, string, int) error { return nil }

func (m *fakeDirectoryQuotaManager) GetQuota(context.Context, string, string) (*core.DirectoryQuota, error) {
	return nil, nil
}

func (m *fakeDirectoryQuotaManager) SetFileCount(context.Context, string, string, int) error {
	return nil
}

func (m *fakeDirectoryQuotaManager) decrementCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.decrements)
}

// fakeMetadataRepo is an in-memory core.MetadataRepository used by the
// regression tests. Unused interface methods are intentionally not implemented.
type fakeMetadataRepo struct {
	core.MetadataRepository

	mu       sync.Mutex
	files    map[string]*core.FileMetadata
	addErr   error
	getErr   error
	addCalls int
}

func newFakeMetadataRepo() *fakeMetadataRepo {
	return &fakeMetadataRepo{files: make(map[string]*core.FileMetadata)}
}

func fakeMetadataKey(tenantID, fileKey string) string {
	return tenantID + "\x00" + fileKey
}

func (r *fakeMetadataRepo) AddOrUpdate(_ context.Context, metadata *core.FileMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addCalls++
	if r.addErr != nil {
		return r.addErr
	}
	if metadata == nil {
		return core.ErrInvalidArgument
	}
	copied := *metadata
	r.files[fakeMetadataKey(metadata.TenantID, metadata.FileKey)] = &copied
	return nil
}

func (r *fakeMetadataRepo) Get(_ context.Context, tenantID string, fileKey string) (*core.FileMetadata, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, r.getErr
	}
	metadata, ok := r.files[fakeMetadataKey(tenantID, fileKey)]
	if !ok {
		return nil, core.ErrFileNotFound
	}
	copied := *metadata
	return &copied, nil
}

func (r *fakeMetadataRepo) Delete(_ context.Context, tenantID string, fileKey string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.files, fakeMetadataKey(tenantID, fileKey))
	return nil
}

func (r *fakeMetadataRepo) addCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addCalls
}

func (r *fakeMetadataRepo) fileCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.files)
}

// newRegressionPool builds a storage pool directly so tests can inject fake
// collaborators without opening a real metadata database.
func newRegressionPool(
	volumes map[string]core.StorageVolume,
	repo core.MetadataRepository,
	tenantQuota core.TenantQuotaManager,
	dirQuota core.DirectoryQuotaManager,
) *storagePool {
	return &storagePool{
		tenantManager:  &mockTenantManager{},
		metadataRepo:   repo,
		volumes:        volumes,
		tenantQuotaMgr: tenantQuota,
		dirQuotaMgr:    dirQuota,
		volumeSelector: &MostAvailableSpaceSelector{},
	}
}

// panickingReader returns some bytes and then panics, simulating an
// application reader that fails while io.Copy is streaming.
type panickingReader struct {
	remaining int
}

func (r *panickingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		panic("application reader failed")
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	return n, nil
}

func filesUnder(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to walk %s: %v", root, err)
	}
	return files
}

// TestWriteFile_PanicRollsBackEveryResource verifies that a panic raised by the
// content reader rolls back the tenant quota, the directory quota, and the
// partial physical file before the panic continues to propagate.
func TestWriteFile_PanicRollsBackEveryResource(t *testing.T) {
	ctx := context.Background()

	mountDir := t.TempDir()
	realVolume, err := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
		VolumeID:  "panic-volume",
		MountPath: mountDir,
	})
	if err != nil {
		t.Fatalf("failed to create volume: %v", err)
	}

	repo := newFakeMetadataRepo()
	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}
	pool := newRegressionPool(
		map[string]core.StorageVolume{"panic-volume": realVolume},
		repo,
		tenantQuota,
		dirQuota,
	)

	recovered := func() (recoveredValue any) {
		defer func() { recoveredValue = recover() }()
		_, _ = pool.WriteFile(ctx, createTestTenant(), &panickingReader{remaining: 16}, nil)
		return nil
	}()

	if recovered == nil {
		t.Fatal("expected the reader panic to propagate out of WriteFile")
	}
	if message, ok := recovered.(string); !ok || message != "application reader failed" {
		t.Errorf("propagated panic value = %v, want the original reader panic", recovered)
	}

	if _, decrements := tenantQuota.counts(); decrements != 1 {
		t.Errorf("tenant quota rollbacks = %d, want 1", decrements)
	}
	if got := dirQuota.decrementCount(); got != 1 {
		t.Errorf("directory quota rollbacks = %d, want 1", got)
	}
	if files := filesUnder(t, mountDir); len(files) != 0 {
		t.Errorf("partial physical file left behind: %v", files)
	}
}

// TestWriteFile_MetadataFailureKeepsQuotaWhenPhysicalDeleteFails verifies that
// quotas are not released while the written bytes are still on disk, and that
// the rollback failure is surfaced without leaking the physical path.
func TestWriteFile_MetadataFailureKeepsQuotaWhenPhysicalDeleteFails(t *testing.T) {
	ctx := context.Background()

	const pathMarker = "leaked-physical-path-marker"
	vol := newFakeVolume("v1", true, 4096)
	vol.deleteErr = fmt.Errorf("failed to delete file: %s: access denied", pathMarker)

	repo := newFakeMetadataRepo()
	repo.addErr = errors.New("metadata store unavailable")
	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}

	pool := newRegressionPool(
		map[string]core.StorageVolume{"v1": vol},
		repo,
		tenantQuota,
		dirQuota,
	)

	_, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader([]byte("payload")), nil)
	if err == nil {
		t.Fatal("expected WriteFile to fail when metadata persistence fails")
	}
	if !errors.Is(err, repo.addErr) {
		t.Errorf("expected the metadata error to stay wrapped, got %v", err)
	}

	if _, decrements := tenantQuota.counts(); decrements != 0 {
		t.Errorf("tenant quota rollbacks = %d, want 0 while bytes remain on disk", decrements)
	}
	if got := dirQuota.decrementCount(); got != 0 {
		t.Errorf("directory quota rollbacks = %d, want 0 while bytes remain on disk", got)
	}
	if strings.Contains(err.Error(), pathMarker) {
		t.Errorf("error leaked the physical path: %v", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "rollback") {
		t.Errorf("expected the rollback failure to be reported, got %v", err)
	}
}

// TestWriteFile_MetadataFailureWithSuccessfulDeleteReleasesQuota verifies the
// existing behavior is preserved when the physical rollback succeeds.
func TestWriteFile_MetadataFailureWithSuccessfulDeleteReleasesQuota(t *testing.T) {
	ctx := context.Background()

	vol := newFakeVolume("v1", true, 4096)
	repo := newFakeMetadataRepo()
	repo.addErr = errors.New("metadata store unavailable")
	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}

	pool := newRegressionPool(
		map[string]core.StorageVolume{"v1": vol},
		repo,
		tenantQuota,
		dirQuota,
	)

	_, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader([]byte("payload")), nil)
	if err == nil {
		t.Fatal("expected WriteFile to fail when metadata persistence fails")
	}

	if _, decrements := tenantQuota.counts(); decrements != 1 {
		t.Errorf("tenant quota rollbacks = %d, want 1", decrements)
	}
	if got := dirQuota.decrementCount(); got != 1 {
		t.Errorf("directory quota rollbacks = %d, want 1", got)
	}
	if vol.deleteCalls.Load() != 1 {
		t.Errorf("physical delete calls = %d, want 1", vol.deleteCalls.Load())
	}
	if got := vol.writtenCount(); got != 0 {
		t.Errorf("failed write must not remain in the volume (%d entries)", got)
	}
}

// TestWriteFile_RetriesNextVolumeWhenFirstFails verifies that a failed write on
// the preferred volume is retried on the next healthy candidate.
func TestWriteFile_RetriesNextVolumeWhenFirstFails(t *testing.T) {
	ctx := context.Background()

	failing := newFakeVolume("failing", true, 9000)
	failing.writeErr = errors.New("volume write failed")
	backup := newFakeVolume("backup", true, 1000)

	repo := newFakeMetadataRepo()
	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}

	pool := newRegressionPool(
		map[string]core.StorageVolume{"failing": failing, "backup": backup},
		repo,
		tenantQuota,
		dirQuota,
	)

	// Warm the capacity snapshot so the retry path can invalidate it.
	if _, err := pool.GetTotalCapacity(ctx); err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	capacityProbesBefore := failing.totalCalls.Load() + backup.totalCalls.Load()

	fileName := "retry.txt"
	fileKey, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader([]byte("payload")), &fileName)
	if err != nil {
		t.Fatalf("expected the write to succeed on the next healthy volume, got %v", err)
	}
	if failing.writeCalls.Load() != 1 {
		t.Errorf("failing volume write calls = %d, want 1", failing.writeCalls.Load())
	}
	if failing.deleteCalls.Load() != 1 {
		t.Errorf("partial write on the failing volume was not cleaned up (delete calls = %d)", failing.deleteCalls.Load())
	}
	if backup.writeCalls.Load() != 1 {
		t.Errorf("backup volume write calls = %d, want 1", backup.writeCalls.Load())
	}

	metadata, err := repo.Get(ctx, "test-tenant", fileKey)
	if err != nil {
		t.Fatalf("failed to load metadata: %v", err)
	}
	if metadata.VolumeID != "backup" {
		t.Errorf("metadata volume = %q, want %q", metadata.VolumeID, "backup")
	}
	if content, ok := backup.writtenContent(metadata.PhysicalPath); !ok || string(content) != "payload" {
		t.Errorf("backup volume content = %q (ok=%v), want %q", string(content), ok, "payload")
	}

	if _, err := pool.GetAvailableSpace(ctx); err != nil {
		t.Fatalf("GetAvailableSpace failed: %v", err)
	}
	capacityProbesAfter := failing.totalCalls.Load() + backup.totalCalls.Load()
	if capacityProbesAfter == capacityProbesBefore {
		t.Error("failed write must invalidate the cached volume selection state")
	}
}

// TestReadFile_CorrectsDriftedPhysicalPath verifies that a read recovers when
// the stored physical path no longer matches the canonical layout, and that the
// correction is persisted.
func TestReadFile_CorrectsDriftedPhysicalPath(t *testing.T) {
	ctx := context.Background()

	vol := newFakeVolume("v1", true, 4096)
	repo := newFakeMetadataRepo()
	pool := newRegressionPool(
		map[string]core.StorageVolume{"v1": vol},
		repo,
		nil,
		nil,
	)

	tenant := createTestTenant()
	fileName := "drift.txt"
	fileKey, err := pool.WriteFile(ctx, tenant, bytes.NewReader([]byte("canonical content")), &fileName)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	metadata, err := repo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		t.Fatalf("failed to load metadata: %v", err)
	}
	canonicalPath := metadata.PhysicalPath
	if canonicalPath == "" {
		t.Fatal("expected a physical path in metadata")
	}

	// Simulate path drift: metadata points at a stale location while the bytes
	// still live at the canonical location.
	metadata.PhysicalPath = filepath.Join("stale", "old-layout", fileKey+".txt")
	if err := repo.AddOrUpdate(ctx, metadata); err != nil {
		t.Fatalf("failed to corrupt metadata: %v", err)
	}

	reader, err := pool.ReadFile(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("expected the read to recover from the drifted path, got %v", err)
	}
	defer func() { _ = reader.Close() }()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read recovered content: %v", err)
	}
	if string(content) != "canonical content" {
		t.Errorf("recovered content = %q, want %q", string(content), "canonical content")
	}

	corrected, err := repo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		t.Fatalf("failed to reload metadata: %v", err)
	}
	if corrected.PhysicalPath != canonicalPath {
		t.Errorf("persisted physical path = %q, want the corrected path %q", corrected.PhysicalPath, canonicalPath)
	}
}

// TestWriteFile_RejectsUnsafeTenantIdentifier verifies that a tenant identifier
// is validated before it can become a storage path segment, and that no quota,
// volume, or metadata side effect happens for a rejected identifier.
func TestWriteFile_RejectsUnsafeTenantIdentifier(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		tenantID string
	}{
		{name: "parent traversal", tenantID: "../evil"},
		{name: "embedded forward separator", tenantID: "tenant/child"},
		{name: "embedded back separator", tenantID: `tenant\child`},
		{name: "windows reserved device", tenantID: "CON"},
		{name: "reserved dot prefix", tenantID: ".hidden"},
		{name: "trailing dot", tenantID: "tenant."},
		{name: "alternate data stream", tenantID: "tenant:ads"},
		{name: "leading whitespace", tenantID: " tenant"},
		{name: "overlong", tenantID: strings.Repeat("a", core.MaxTenantIDLength+1)},
		{name: "invalid utf-8", tenantID: "bad\xff"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vol := newFakeVolume("v1", true, 4096)
			repo := newFakeMetadataRepo()
			tenantQuota := &fakeTenantQuotaManager{}
			dirQuota := &fakeDirectoryQuotaManager{}
			pool := newRegressionPool(
				map[string]core.StorageVolume{"v1": vol},
				repo,
				tenantQuota,
				dirQuota,
			)

			tenant := core.TenantContext{ID: tc.tenantID, Status: core.TenantStatusEnabled}
			_, err := pool.WriteFile(ctx, tenant, bytes.NewReader([]byte("payload")), nil)
			if err == nil {
				t.Fatalf("expected tenant ID %q to be rejected", tc.tenantID)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Errorf("error = %v, want ErrInvalidArgument", err)
			}
			if vol.writeCalls.Load() != 0 {
				t.Errorf("volume write calls = %d, want 0", vol.writeCalls.Load())
			}
			if repo.fileCount() != 0 {
				t.Errorf("metadata records = %d, want 0", repo.fileCount())
			}
			if increments, _ := tenantQuota.counts(); increments != 0 {
				t.Errorf("tenant quota increments = %d, want 0", increments)
			}
			if len(dirQuota.increments) != 0 {
				t.Errorf("directory quota increments = %d, want 0", len(dirQuota.increments))
			}
		})
	}
}

// TestReadFile_SuccessPathDoesNotRewriteMetadata verifies that a successful
// read leaves metadata untouched.
func TestReadFile_SuccessPathDoesNotRewriteMetadata(t *testing.T) {
	ctx := context.Background()

	vol := newFakeVolume("v1", true, 4096)
	repo := newFakeMetadataRepo()
	pool := newRegressionPool(map[string]core.StorageVolume{"v1": vol}, repo, nil, nil)

	tenant := createTestTenant()
	fileKey, err := pool.WriteFile(ctx, tenant, bytes.NewReader([]byte("content")), nil)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	addCallsBefore := repo.addCount()

	reader, err := pool.ReadFile(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	_ = reader.Close()

	if got := repo.addCount(); got != addCallsBefore {
		t.Errorf("metadata writes on the success path = %d, want %d", got, addCallsBefore)
	}
}

// TestReadFile_UnrecoverableDriftStillFails verifies that a missing file is
// still reported as an error.
func TestReadFile_UnrecoverableDriftStillFails(t *testing.T) {
	ctx := context.Background()

	vol := newFakeVolume("v1", true, 4096)
	repo := newFakeMetadataRepo()
	pool := newRegressionPool(map[string]core.StorageVolume{"v1": vol}, repo, nil, nil)

	tenant := createTestTenant()
	if err := repo.AddOrUpdate(ctx, &core.FileMetadata{
		TenantID:     tenant.ID,
		FileKey:      "missing-key",
		VolumeID:     "v1",
		PhysicalPath: filepath.Join("stale", "missing-key"),
	}); err != nil {
		t.Fatalf("failed to seed metadata: %v", err)
	}

	if _, err := pool.ReadFile(ctx, tenant, "missing-key"); err == nil {
		t.Fatal("expected an error when neither the stored nor the canonical path exists")
	}
}

// TestCapacity_ExcludesUnhealthyVolumesAndCaches verifies that capacity
// reporting ignores unhealthy volumes and caches the aggregate briefly.
func TestCapacity_ExcludesUnhealthyVolumesAndCaches(t *testing.T) {
	ctx := context.Background()

	healthy := newFakeVolume("healthy", true, 500)
	healthy.total = 1000
	unhealthy := newFakeVolume("unhealthy", false, 700)
	unhealthy.total = 2000

	repo := newFakeMetadataRepo()
	pool := newRegressionPool(
		map[string]core.StorageVolume{"healthy": healthy, "unhealthy": unhealthy},
		repo,
		nil,
		nil,
	)

	total, err := pool.GetTotalCapacity(ctx)
	if err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	if total != 1000 {
		t.Errorf("total capacity = %d, want 1000 (unhealthy volume excluded)", total)
	}

	available, err := pool.GetAvailableSpace(ctx)
	if err != nil {
		t.Fatalf("GetAvailableSpace failed: %v", err)
	}
	if available != 500 {
		t.Errorf("available space = %d, want 500 (unhealthy volume excluded)", available)
	}

	probesBefore := healthy.totalCalls.Load() + healthy.availableCalls.Load() +
		unhealthy.totalCalls.Load() + unhealthy.availableCalls.Load()

	if _, err := pool.GetTotalCapacity(ctx); err != nil {
		t.Fatalf("GetTotalCapacity (cached) failed: %v", err)
	}
	if _, err := pool.GetAvailableSpace(ctx); err != nil {
		t.Fatalf("GetAvailableSpace (cached) failed: %v", err)
	}

	probesAfter := healthy.totalCalls.Load() + healthy.availableCalls.Load() +
		unhealthy.totalCalls.Load() + unhealthy.availableCalls.Load()

	if probesAfter != probesBefore {
		t.Errorf("capacity probes after the cache window = %d, want %d (aggregate must be cached)", probesAfter, probesBefore)
	}
}

// TestCapacity_ReturnsPartialSumsOnVolumeErrors verifies that a failing volume
// only removes its own contribution.
func TestCapacity_ReturnsPartialSumsOnVolumeErrors(t *testing.T) {
	ctx := context.Background()

	good := newFakeVolume("good", true, 300)
	good.total = 900
	broken := newFakeVolume("broken", true, 0)
	broken.totalErr = errors.New("disk capacity unavailable")
	broken.availableErr = errors.New("disk capacity unavailable")

	pool := newRegressionPool(
		map[string]core.StorageVolume{"good": good, "broken": broken},
		newFakeMetadataRepo(),
		nil,
		nil,
	)

	total, err := pool.GetTotalCapacity(ctx)
	if err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	if total != 900 {
		t.Errorf("total capacity = %d, want 900", total)
	}

	available, err := pool.GetAvailableSpace(ctx)
	if err != nil {
		t.Fatalf("GetAvailableSpace failed: %v", err)
	}
	if available != 300 {
		t.Errorf("available space = %d, want 300", available)
	}
}

// TestCapacity_CacheExpiresAfterTTL verifies the capacity cache window with an
// injected clock.
func TestCapacity_CacheExpiresAfterTTL(t *testing.T) {
	ctx := context.Background()

	vol := newFakeVolume("v1", true, 500)
	vol.total = 1000

	pool := newRegressionPool(map[string]core.StorageVolume{"v1": vol}, newFakeMetadataRepo(), nil, nil)
	current := time.Unix(1_700_000_000, 0)
	pool.now = func() time.Time { return current }

	if _, err := pool.GetTotalCapacity(ctx); err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	probes := vol.totalCalls.Load()

	// Inside the window the cached aggregate is reused.
	current = current.Add(defaultCapacityCacheTTL - time.Millisecond)
	if _, err := pool.GetTotalCapacity(ctx); err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	if got := vol.totalCalls.Load(); got != probes {
		t.Errorf("capacity probes inside the window = %d, want %d", got, probes)
	}

	// Past the window the volumes are probed again.
	current = current.Add(2 * defaultCapacityCacheTTL)
	if _, err := pool.GetTotalCapacity(ctx); err != nil {
		t.Fatalf("GetTotalCapacity failed: %v", err)
	}
	if got := vol.totalCalls.Load(); got <= probes {
		t.Errorf("capacity probes after the window = %d, want more than %d", got, probes)
	}
}

// TestWriteFile_RetryRewindsSeekableContent verifies that a retry after a
// partially consumed reader writes the complete payload to the next volume.
func TestWriteFile_RetryRewindsSeekableContent(t *testing.T) {
	ctx := context.Background()

	failing := newFakeVolume("failing", true, 9000)
	failing.writeErr = errors.New("volume write failed")
	failing.consumeBeforeWriteFailure = 3
	backup := newFakeVolume("backup", true, 1000)

	repo := newFakeMetadataRepo()
	pool := newRegressionPool(
		map[string]core.StorageVolume{"failing": failing, "backup": backup},
		repo,
		nil,
		nil,
	)

	fileKey, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader([]byte("payload")), nil)
	if err != nil {
		t.Fatalf("expected the retry to succeed, got %v", err)
	}

	metadata, err := repo.Get(ctx, "test-tenant", fileKey)
	if err != nil {
		t.Fatalf("failed to load metadata: %v", err)
	}
	content, ok := backup.writtenContent(metadata.PhysicalPath)
	if !ok {
		t.Fatalf("expected the retried write on %q to exist", metadata.PhysicalPath)
	}
	if string(content) != "payload" {
		t.Errorf("content after retry = %q, want %q", string(content), "payload")
	}
}

// TestWriteFile_AllCandidatesFail verifies that quotas are released when every
// candidate volume rejects the write.
func TestWriteFile_AllCandidatesFail(t *testing.T) {
	ctx := context.Background()

	first := newFakeVolume("first", true, 900)
	first.writeErr = errors.New("volume write failed")
	second := newFakeVolume("second", true, 500)
	second.writeErr = errors.New("volume write failed")

	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}
	pool := newRegressionPool(
		map[string]core.StorageVolume{"first": first, "second": second},
		newFakeMetadataRepo(),
		tenantQuota,
		dirQuota,
	)

	if _, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader([]byte("payload")), nil); err == nil {
		t.Fatal("expected the write to fail when every candidate rejects it")
	}

	if first.writeCalls.Load() != 1 || second.writeCalls.Load() != 1 {
		t.Errorf("write attempts = (%d, %d), want (1, 1)", first.writeCalls.Load(), second.writeCalls.Load())
	}
	if _, decrements := tenantQuota.counts(); decrements != 1 {
		t.Errorf("tenant quota rollbacks = %d, want 1", decrements)
	}
	if got := dirQuota.decrementCount(); got != 1 {
		t.Errorf("directory quota rollbacks = %d, want 1", got)
	}
}

// TestWriteFile_RejectsVolumeTooSmallForSeekableContent verifies the
// payload-size constraint: a seekable reader whose payload fits nowhere must
// fail before touching a volume.
func TestWriteFile_RejectsVolumeTooSmallForSeekableContent(t *testing.T) {
	ctx := context.Background()

	small := newFakeVolume("small", true, 100)
	medium := newFakeVolume("medium", true, 200)

	tenantQuota := &fakeTenantQuotaManager{}
	dirQuota := &fakeDirectoryQuotaManager{}
	pool := newRegressionPool(
		map[string]core.StorageVolume{"small": small, "medium": medium},
		newFakeMetadataRepo(),
		tenantQuota,
		dirQuota,
	)

	payload := bytes.Repeat([]byte("x"), 500)
	_, err := pool.WriteFile(ctx, createTestTenant(), bytes.NewReader(payload), nil)
	if !errors.Is(err, core.ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}
	if small.writeCalls.Load() != 0 || medium.writeCalls.Load() != 0 {
		t.Errorf("no volume may be written when the payload cannot fit (calls: %d, %d)",
			small.writeCalls.Load(), medium.writeCalls.Load())
	}
	if _, decrements := tenantQuota.counts(); decrements != 1 {
		t.Errorf("tenant quota rollbacks = %d, want 1", decrements)
	}
}

// TestWriteFile_NonSeekableContentSkipsSizeConstraint verifies that streams
// without a known size are still accepted.
func TestWriteFile_NonSeekableContentSkipsSizeConstraint(t *testing.T) {
	ctx := context.Background()

	small := newFakeVolume("small", true, 100)
	medium := newFakeVolume("medium", true, 200)

	repo := newFakeMetadataRepo()
	pool := newRegressionPool(
		map[string]core.StorageVolume{"small": small, "medium": medium},
		repo,
		nil,
		nil,
	)

	payload := bytes.Repeat([]byte("x"), 500)
	stream := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))

	fileKey, err := pool.WriteFile(ctx, createTestTenant(), stream, nil)
	if err != nil {
		t.Fatalf("expected the stream to be accepted, got %v", err)
	}

	metadata, err := repo.Get(ctx, "test-tenant", fileKey)
	if err != nil {
		t.Fatalf("failed to load metadata: %v", err)
	}
	if metadata.VolumeID != "medium" {
		t.Errorf("selected volume = %q, want the largest available %q", metadata.VolumeID, "medium")
	}
	if metadata.FileSize != int64(len(payload)) {
		t.Errorf("file size = %d, want %d", metadata.FileSize, len(payload))
	}
}

// TestRemainingContentSize_DoesNotConsumeReader verifies the size probe.
func TestRemainingContentSize_DoesNotConsumeReader(t *testing.T) {
	reader := bytes.NewReader([]byte("0123456789"))
	if _, err := reader.Seek(4, io.SeekStart); err != nil {
		t.Fatalf("failed to position reader: %v", err)
	}

	if got := remainingContentSize(reader); got != 6 {
		t.Errorf("remainingContentSize = %d, want 6", got)
	}

	position, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("failed to read position: %v", err)
	}
	if position != 4 {
		t.Errorf("reader position after probing = %d, want 4", position)
	}

	if got := remainingContentSize(io.LimitReader(reader, 3)); got != 0 {
		t.Errorf("remainingContentSize for a non-seekable reader = %d, want 0", got)
	}
}
