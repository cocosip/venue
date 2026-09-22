package cleanup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/scheduler"
)

// newDetailedOptimizationService builds a cleanup service whose database
// directories are known to the test, so the detailed optimization report can be
// inspected.
func newDetailedOptimizationService(
	t *testing.T,
	opts *CleanupServiceOptions,
) (core.CleanupService, core.DatabaseOptimizationService) {
	t.Helper()

	service, err := NewCleanupService(opts)
	if err != nil {
		t.Fatalf("NewCleanupService() error = %v", err)
	}

	detailed, ok := service.(core.DatabaseOptimizationService)
	if !ok {
		t.Fatal("cleanup service does not implement core.DatabaseOptimizationService")
	}

	return service, detailed
}

// TestOptimizeDatabasesDetailed_ReportsMeasuredSizes runs the detailed pass
// against real BadgerDB-backed metadata and quota repositories. The sizes may be
// unchanged by an optimization of a nearly empty database, so the test asserts
// the invariants that always hold: the counts, a measured post-pass size, a
// non-negative reclaim, and the reclaim/measurement relationship.
func TestOptimizeDatabasesDetailed_ReportsMeasuredSizes(t *testing.T) {
	ctx := context.Background()

	metaRepo, metaDir := createTestRepository(t)
	defer func() { _ = metaRepo.Close() }()
	defer func() { _ = os.RemoveAll(metaDir) }()

	quotaRepo, quotaDir := createTestDirQuotaRepository(t)
	defer func() { _ = quotaRepo.Close() }()
	defer func() { _ = os.RemoveAll(quotaDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, err := scheduler.NewFileScheduler(metaRepo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	service, detailed := newDetailedOptimizationService(t, &CleanupServiceOptions{
		TenantManager:            &stubTenantManager{},
		MetadataRepository:       metaRepo,
		FileScheduler:            sched,
		Volumes:                  volumes,
		DirectoryQuotaRepository: quotaRepo,
		MetadataDirectory:        metaDir,
		QuotaDirectory:           quotaDir,
	})

	result, err := detailed.OptimizeDatabasesDetailed(ctx)
	if err != nil {
		t.Fatalf("OptimizeDatabasesDetailed() error = %v", err)
	}

	if result.MetadataDatabasesOptimized != 1 {
		t.Errorf("MetadataDatabasesOptimized = %d, want 1", result.MetadataDatabasesOptimized)
	}
	if result.QuotaDatabasesOptimized != 1 {
		t.Errorf("QuotaDatabasesOptimized = %d, want 1", result.QuotaDatabasesOptimized)
	}
	if result.SizeBefore <= 0 {
		t.Errorf("SizeBefore = %d, want a measured, positive pre-optimization size", result.SizeBefore)
	}
	if result.SizeAfter <= 0 {
		t.Errorf("SizeAfter = %d, want a measured, positive post-optimization size", result.SizeAfter)
	}
	if result.SpaceReclaimed < 0 {
		t.Errorf("SpaceReclaimed = %d, want a non-negative reclaim", result.SpaceReclaimed)
	}
	if result.SizeAfter <= result.SizeBefore && result.SpaceReclaimed != result.SizeBefore-result.SizeAfter {
		t.Errorf("SpaceReclaimed = %d, want SizeBefore-SizeAfter = %d",
			result.SpaceReclaimed, result.SizeBefore-result.SizeAfter)
	}

	// The pass is also part of the process-lifetime totals.
	if cumulative := service.CumulativeStatistics(); cumulative.MetadataDatabasesOptimized != 1 {
		t.Errorf("cumulative MetadataDatabasesOptimized = %d, want 1", cumulative.MetadataDatabasesOptimized)
	}
}

// TestOptimizeDatabasesDetailed_SkipsUnmeasurableDatabases verifies that a
// database tree whose size cannot be measured is skipped instead of failing the
// pass, and that the reported counters stay consistent with the trees that were
// actually optimized.
func TestOptimizeDatabasesDetailed_SkipsUnmeasurableDatabases(t *testing.T) {
	metaRepo := &stubMetadataRepository{}
	quotaRepo := &stubDirectoryQuotaRepository{}

	// No MetadataDirectory and no QuotaDirectory are configured, so neither tree
	// can be measured.
	service := &cleanupService{metadataRepo: metaRepo, dirQuotaRepo: quotaRepo}

	result, err := service.OptimizeDatabasesDetailed(context.Background())
	if err != nil {
		t.Fatalf("OptimizeDatabasesDetailed() error = %v", err)
	}

	if result.MetadataDatabasesOptimized != 0 || result.QuotaDatabasesOptimized != 0 {
		t.Errorf("optimized counts = %d/%d, want 0/0",
			result.MetadataDatabasesOptimized, result.QuotaDatabasesOptimized)
	}
	if result.SizeBefore != 0 || result.SizeAfter != 0 || result.SpaceReclaimed != 0 {
		t.Errorf("sizes = %d/%d/%d, want zero", result.SizeBefore, result.SizeAfter, result.SpaceReclaimed)
	}
	if metaRepo.optimizeCalls != 0 || quotaRepo.optimizeCalls != 0 {
		t.Errorf("unmeasurable databases must not be optimized, got metadata=%d quota=%d",
			metaRepo.optimizeCalls, quotaRepo.optimizeCalls)
	}
}

// TestOptimizeDatabasesDetailed_SkipsUnmeasurableQuotaTree verifies that a
// measurable metadata tree is still optimized when the quota tree cannot be
// measured, and that the quota repository is not touched.
func TestOptimizeDatabasesDetailed_SkipsUnmeasurableQuotaTree(t *testing.T) {
	metaDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(metaDir, "metadata.db"), []byte("0123456789"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	quotaRepo := &stubDirectoryQuotaRepository{}
	service := &cleanupService{
		metadataRepo:      &stubMetadataRepository{},
		dirQuotaRepo:      quotaRepo,
		metadataDirectory: metaDir,
	}

	result, err := service.OptimizeDatabasesDetailed(context.Background())
	if err != nil {
		t.Fatalf("OptimizeDatabasesDetailed() error = %v", err)
	}

	if result.MetadataDatabasesOptimized != 1 {
		t.Errorf("MetadataDatabasesOptimized = %d, want 1", result.MetadataDatabasesOptimized)
	}
	if result.QuotaDatabasesOptimized != 0 {
		t.Errorf("QuotaDatabasesOptimized = %d, want 0", result.QuotaDatabasesOptimized)
	}
	if result.SpaceReclaimed != 0 || result.SizeBefore != result.SizeAfter {
		t.Errorf("sizes = before %d after %d reclaimed %d, want an unchanged measured size",
			result.SizeBefore, result.SizeAfter, result.SpaceReclaimed)
	}
	if quotaRepo.optimizeCalls != 0 {
		t.Errorf("quota optimize calls = %d, want 0", quotaRepo.optimizeCalls)
	}
}

// TestOptimizeDatabasesDetailed_ReportsMetadataOptimizationFailure verifies
// that a repository failure is reported and stops the pass before the quota
// repository is optimized, matching OptimizeDatabases.
func TestOptimizeDatabasesDetailed_ReportsMetadataOptimizationFailure(t *testing.T) {
	metaDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(metaDir, "metadata.db"), []byte("0123456789"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	optimizeErr := errors.New("metadata optimize failed")
	metaRepo := &stubMetadataRepository{optimizeErr: optimizeErr}
	quotaRepo := &stubDirectoryQuotaRepository{}
	service := &cleanupService{
		metadataRepo:      metaRepo,
		dirQuotaRepo:      quotaRepo,
		metadataDirectory: metaDir,
	}

	_, err := service.OptimizeDatabasesDetailed(context.Background())
	if !errors.Is(err, optimizeErr) {
		t.Fatalf("OptimizeDatabasesDetailed() error = %v, want the repository error", err)
	}
	if quotaRepo.optimizeCalls != 0 {
		t.Errorf("quota optimize calls = %d, want 0 after a metadata failure", quotaRepo.optimizeCalls)
	}
}

// TestDatabaseDirectorySize covers the measurement used by the detailed report.
func TestDatabaseDirectorySize(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), []byte("12345"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nested", "b.bin"), []byte("123"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	file := filepath.Join(dir, "a.bin")

	tests := []struct {
		name           string
		path           string
		wantSize       int64
		wantMeasurable bool
	}{
		{name: "directory tree", path: dir, wantSize: 8, wantMeasurable: true},
		{name: "subdirectory", path: filepath.Join(dir, "nested"), wantSize: 3, wantMeasurable: true},
		{name: "empty path", path: "", wantMeasurable: false},
		{name: "missing path", path: filepath.Join(dir, "missing"), wantMeasurable: false},
		{name: "regular file", path: file, wantMeasurable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			size, measurable := databaseDirectorySize(test.path)
			if measurable != test.wantMeasurable {
				t.Fatalf("databaseDirectorySize(%q) measurable = %v, want %v", test.path, measurable, test.wantMeasurable)
			}
			if measurable && size != test.wantSize {
				t.Errorf("databaseDirectorySize(%q) = %d, want %d", test.path, size, test.wantSize)
			}
		})
	}
}
