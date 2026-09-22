package pool

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/sqlite"
	"github.com/cocosip/venue/pkg/volume"
)

// poolRecorderCall is one captured Record call of poolRecorder.
type poolRecorderCall struct {
	name       string
	value      int64
	timestamp  time.Time
	dimensions map[string]string
}

// poolRecorder is a minimal core.StatisticsRecorder that captures every delta
// the storage pool reports.
type poolRecorder struct {
	mu    sync.Mutex
	calls []poolRecorderCall
}

func (r *poolRecorder) Record(name string, value int64, timestamp time.Time, dimensions map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, poolRecorderCall{
		name:       name,
		value:      value,
		timestamp:  timestamp,
		dimensions: dimensions,
	})
}

func (r *poolRecorder) recorded() []poolRecorderCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]poolRecorderCall, len(r.calls))
	copy(out, r.calls)

	return out
}

// TestStoragePoolRecordsStatistics asserts the exact statistics tuples of one
// write -> claim -> read -> complete flow.
func TestStoragePoolRecordsStatistics(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo := newStatisticsTestRepository(t)

	volumes := newStatisticsTestVolumes(t)

	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	recorder := &poolRecorder{}

	pool, err := NewStoragePool(&StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
		StatisticsRecorder: recorder,
	})
	if err != nil {
		t.Fatalf("NewStoragePool() error = %v", err)
	}

	tenant := createTestTenant()
	content := []byte("statistics payload")
	fileName := "statistics.txt"

	fileKey, err := pool.WriteFile(ctx, tenant, bytes.NewReader(content), &fileName)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	location, err := pool.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("GetNextFileForProcessing() error = %v", err)
	}
	if location == nil {
		t.Fatal("GetNextFileForProcessing() = nil, want the written file")
	}

	reader, err := pool.ReadFile(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := pool.MarkAsCompleted(ctx, requirePoolProcessingLease(t, location)); err != nil {
		t.Fatalf("MarkAsCompleted() error = %v", err)
	}

	volumeID := location.VolumeID
	if volumeID == "" {
		t.Fatal("location.VolumeID is empty")
	}

	want := []poolRecorderCall{
		{
			name:  core.StatisticStorageWriteSuccessCount,
			value: 1,
			dimensions: map[string]string{
				core.StatisticsDimensionTenantID: tenant.ID,
				core.StatisticsDimensionVolumeID: volumeID,
			},
		},
		{
			name:  core.StatisticStorageWriteBytes,
			value: int64(len(content)),
			dimensions: map[string]string{
				core.StatisticsDimensionTenantID: tenant.ID,
				core.StatisticsDimensionVolumeID: volumeID,
			},
		},
		{
			name:  core.StatisticStorageFileDequeuedCount,
			value: 1,
			dimensions: map[string]string{
				core.StatisticsDimensionTenantID: tenant.ID,
				core.StatisticsDimensionVolumeID: volumeID,
			},
		},
		{
			name:  core.StatisticStorageFileReadCount,
			value: 1,
			dimensions: map[string]string{
				core.StatisticsDimensionTenantID: tenant.ID,
				core.StatisticsDimensionVolumeID: volumeID,
			},
		},
		{
			name:  core.StatisticStorageFileCompletedCount,
			value: 1,
			dimensions: map[string]string{
				core.StatisticsDimensionTenantID: tenant.ID,
				core.StatisticsDimensionVolumeID: volumeID,
			},
		},
	}

	got := recorder.recorded()
	if len(got) != len(want) {
		t.Fatalf("recorded %d deltas, want %d: %+v", len(got), len(want), got)
	}

	for i, expected := range want {
		assertRecorderCall(t, i, got[i], expected)
	}

	// Every delta must carry a real timestamp so a reader can bucket it.
	for i, call := range got {
		if call.timestamp.IsZero() {
			t.Errorf("delta %d (%s) has a zero timestamp", i, call.name)
		}
	}
}

// TestStoragePoolRecordsBatchDequeues asserts that a batch claim reports one
// dequeue per claimed file location.
func TestStoragePoolRecordsBatchDequeues(t *testing.T) {
	ctx := context.Background()

	recorder := &poolRecorder{}

	repo := newStatisticsTestRepository(t)
	volumes := newStatisticsTestVolumes(t)

	pool, err := NewStoragePool(&StoragePoolOptions{
		TenantManager:      &mockTenantManager{},
		MetadataRepository: repo,
		FileScheduler:      mustStatisticsScheduler(t, repo, volumes),
		Volumes:            volumes,
		StatisticsRecorder: recorder,
	})
	if err != nil {
		t.Fatalf("NewStoragePool() error = %v", err)
	}

	tenant := createTestTenant()
	fileName := "batch.txt"

	const files = 3

	for i := 0; i < files; i++ {
		if _, err := pool.WriteFile(ctx, tenant, bytes.NewReader([]byte("payload")), &fileName); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
	}

	locations, err := pool.GetNextBatchForProcessing(ctx, tenant, files)
	if err != nil {
		t.Fatalf("GetNextBatchForProcessing() error = %v", err)
	}
	if len(locations) != files {
		t.Fatalf("claimed %d files, want %d", len(locations), files)
	}

	dequeues := 0

	for _, call := range recorder.recorded() {
		if call.name != core.StatisticStorageFileDequeuedCount {
			continue
		}

		dequeues++

		if call.value != 1 {
			t.Errorf("dequeue delta value = %d, want 1", call.value)
		}
		if call.dimensions[core.StatisticsDimensionTenantID] != tenant.ID {
			t.Errorf("dequeue tenant_id = %q, want %q", call.dimensions[core.StatisticsDimensionTenantID], tenant.ID)
		}
		if call.dimensions[core.StatisticsDimensionVolumeID] != locations[0].VolumeID {
			t.Errorf("dequeue volume_id = %q, want %q", call.dimensions[core.StatisticsDimensionVolumeID], locations[0].VolumeID)
		}
	}

	if dequeues != files {
		t.Fatalf("recorded %d dequeue deltas, want %d", dequeues, files)
	}
}

// TestStoragePoolRecordsNoStatisticsWithoutRecorder verifies that a nil
// recorder is a supported configuration, not a nil dereference.
func TestStoragePoolRecordsNoStatisticsWithoutRecorder(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo := newStatisticsTestRepository(t)

	volumes := newStatisticsTestVolumes(t)

	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	pool, err := NewStoragePool(&StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	})
	if err != nil {
		t.Fatalf("NewStoragePool() error = %v", err)
	}

	tenant := createTestTenant()
	fileName := "no-recorder.txt"

	if _, err := pool.WriteFile(ctx, tenant, bytes.NewReader([]byte("payload")), &fileName); err != nil {
		t.Fatalf("WriteFile() without a recorder error = %v", err)
	}
}

// assertRecorderCall compares one recorded delta against its expectation.
func assertRecorderCall(t *testing.T, index int, got, want poolRecorderCall) {
	t.Helper()

	if got.name != want.name {
		t.Errorf("delta %d name = %q, want %q", index, got.name, want.name)
	}
	if got.value != want.value {
		t.Errorf("delta %d (%s) value = %d, want %d", index, got.name, got.value, want.value)
	}
	if len(got.dimensions) != len(want.dimensions) {
		t.Fatalf("delta %d (%s) dimensions = %v, want %v", index, got.name, got.dimensions, want.dimensions)
	}

	for key, value := range want.dimensions {
		if got.dimensions[key] != value {
			t.Errorf("delta %d (%s) dimension %q = %q, want %q", index, got.name, key, got.dimensions[key], value)
		}
	}
}

// newStatisticsTestRepository builds a SQLite metadata repository rooted in the
// test's own temporary directory, which testing removes after the repository is
// closed.
func newStatisticsTestRepository(t *testing.T) core.MetadataRepository {
	t.Helper()

	repo, err := metadata.NewSQLiteMetadataRepository(&metadata.SQLiteRepositoryOptions{
		DataPath:        t.TempDir(),
		CacheTTL:        5 * time.Minute,
		MaxCacheEntries: 10000,
		Sqlite:          sqlite.DefaultOptions(),
	})
	if err != nil {
		t.Fatalf("NewSQLiteMetadataRepository() error = %v", err)
	}

	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return repo
}

// newStatisticsTestVolumes builds a local filesystem volume rooted in the test's
// own temporary directory.
func newStatisticsTestVolumes(t *testing.T) map[string]core.StorageVolume {
	t.Helper()

	vol, err := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
		VolumeID:  "test-volume",
		MountPath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}

	return map[string]core.StorageVolume{"test-volume": vol}
}

// mustStatisticsScheduler builds a file scheduler over the given repository and
// volumes.
func mustStatisticsScheduler(
	t *testing.T,
	repo core.MetadataRepository,
	volumes map[string]core.StorageVolume,
) core.FileScheduler {
	t.Helper()

	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	return sched
}
