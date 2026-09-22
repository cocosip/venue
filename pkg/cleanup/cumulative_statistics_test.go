package cleanup

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestCumulativeStatistics_AccumulatesAcrossOperations is the regression test for
// the Locus GetCleanupStatisticsAsync parity: every cleanup operation adds its
// results into process-lifetime counters that CumulativeStatistics reports.
func TestCumulativeStatistics_AccumulatesAcrossOperations(t *testing.T) {
	ctx := context.Background()

	metadataRoot := t.TempDir()
	quotaRoot := t.TempDir()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.MetadataDirectory = metadataRoot
		opts.QuotaDirectory = quotaRoot
		opts.CorruptedDatabaseRetention = time.Hour
	})

	mount := volumes["test-volume"].MountPath()

	// Junk sweep: two 4-byte junk files.
	writeTestFile(t, mount, filepath.Join("tenant-001", "Thumbs.db"), "junk")
	writeTestFile(t, mount, filepath.Join("tenant-001", ".DS_Store"), "junk")
	if _, err := service.CleanupJunkFiles(ctx); err != nil {
		t.Fatalf("CleanupJunkFiles() error = %v", err)
	}

	// Quarantine sweep: one 32-byte expired directory.
	writeAgedQuarantine(
		t,
		filepath.Join(metadataRoot, "shared", "metadata.corrupted.20200101T000000Z"),
		[]int{32},
		time.Now().Add(-10*time.Hour),
	)
	if _, err := service.CleanupInvalidDatabaseBackups(ctx); err != nil {
		t.Fatalf("CleanupInvalidDatabaseBackups() error = %v", err)
	}

	// Timed-out reset: one record.
	longAgo := time.Now().Add(-2 * time.Hour)
	timedOut := createTestFileMetadata("cumulative-timed-out", core.FileStatusProcessing)
	timedOut.ProcessingStartTime = &longAgo
	if err := repo.AddOrUpdate(ctx, timedOut); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if _, err := service.CleanupTimedOutProcessingFiles(ctx, time.Hour); err != nil {
		t.Fatalf("CleanupTimedOutProcessingFiles() error = %v", err)
	}

	// Completed sweep: one 7-byte payload.
	completed := createTestFileMetadata("cumulative-completed", core.FileStatusCompleted)
	completed.CompletedAt = &longAgo
	completed.FileSize = 7
	completed.PhysicalPath = filepath.Join("tenant-001", "cumulative-completed.txt")
	writeTestFile(t, mount, completed.PhysicalPath, "payload")
	if err := repo.AddOrUpdate(ctx, completed); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if _, err := service.CleanupCompletedFiles(ctx, time.Hour); err != nil {
		t.Fatalf("CleanupCompletedFiles() error = %v", err)
	}

	cumulative := service.CumulativeStatistics()
	if cumulative == nil {
		t.Fatal("CumulativeStatistics() = nil, want a snapshot")
	}
	if cumulative.JunkFilesRemoved != 2 {
		t.Errorf("JunkFilesRemoved = %d, want 2", cumulative.JunkFilesRemoved)
	}
	if cumulative.InvalidDatabaseBackupsRemoved != 1 {
		t.Errorf("InvalidDatabaseBackupsRemoved = %d, want 1", cumulative.InvalidDatabaseBackupsRemoved)
	}
	if cumulative.TimedOutFilesReset != 1 {
		t.Errorf("TimedOutFilesReset = %d, want 1", cumulative.TimedOutFilesReset)
	}
	if cumulative.CompletedRecordsRemoved != 1 {
		t.Errorf("CompletedRecordsRemoved = %d, want 1", cumulative.CompletedRecordsRemoved)
	}
	if want := int64(4 + 4 + 32 + 7); cumulative.SpaceFreed != want {
		t.Errorf("SpaceFreed = %d, want %d", cumulative.SpaceFreed, want)
	}
}

// TestCumulativeStatistics_OrphanedMetadataAndOptimization verifies the two
// remaining operations report into the same lifetime totals.
func TestCumulativeStatistics_OrphanedMetadataAndOptimization(t *testing.T) {
	ctx := context.Background()

	service, repo, _ := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	for _, fileKey := range []string{"cumulative-orphan-a", "cumulative-orphan-b"} {
		record := createTestFileMetadata(fileKey, core.FileStatusPending)
		record.PhysicalPath = filepath.Join("tenant-001", fileKey+".txt")
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(%s) error = %v", fileKey, err)
		}
	}

	if _, err := service.CleanupOrphanedMetadata(ctx); err != nil {
		t.Fatalf("CleanupOrphanedMetadata() error = %v", err)
	}
	if _, err := service.OptimizeDatabases(ctx); err != nil {
		t.Fatalf("OptimizeDatabases() error = %v", err)
	}

	cumulative := service.CumulativeStatistics()
	if cumulative.OrphanedMetadataRemoved != 2 {
		t.Errorf("OrphanedMetadataRemoved = %d, want 2", cumulative.OrphanedMetadataRemoved)
	}
	if cumulative.MetadataDatabasesOptimized != 1 {
		t.Errorf("MetadataDatabasesOptimized = %d, want 1", cumulative.MetadataDatabasesOptimized)
	}
}

// TestCumulativeStatistics_IsMonotonicAndIndependent verifies the snapshot is an
// independent copy and that counters never decrease.
func TestCumulativeStatistics_IsMonotonicAndIndependent(t *testing.T) {
	ctx := context.Background()

	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)
	mount := volumes["test-volume"].MountPath()

	writeTestFile(t, mount, filepath.Join("tenant-001", "Thumbs.db"), "junk")
	if _, err := service.CleanupJunkFiles(ctx); err != nil {
		t.Fatalf("CleanupJunkFiles() error = %v", err)
	}

	first := service.CumulativeStatistics()
	if first.JunkFilesRemoved != 1 {
		t.Fatalf("JunkFilesRemoved = %d, want 1", first.JunkFilesRemoved)
	}

	// Mutating the returned snapshot must not affect later reads.
	first.JunkFilesRemoved = 999
	first.SpaceFreed = -1

	second := service.CumulativeStatistics()
	if second.JunkFilesRemoved != 1 {
		t.Errorf("JunkFilesRemoved = %d after mutating a previous snapshot, want 1", second.JunkFilesRemoved)
	}
	if second.SpaceFreed != 4 {
		t.Errorf("SpaceFreed = %d after mutating a previous snapshot, want 4", second.SpaceFreed)
	}

	writeTestFile(t, mount, filepath.Join("tenant-001", "desktop.ini"), "junk")
	if _, err := service.CleanupJunkFiles(ctx); err != nil {
		t.Fatalf("second CleanupJunkFiles() error = %v", err)
	}

	third := service.CumulativeStatistics()
	if third.JunkFilesRemoved != 2 {
		t.Errorf("JunkFilesRemoved = %d, want 2 (monotonic accumulation)", third.JunkFilesRemoved)
	}
	if third.JunkFilesRemoved < second.JunkFilesRemoved {
		t.Errorf("JunkFilesRemoved decreased from %d to %d", second.JunkFilesRemoved, third.JunkFilesRemoved)
	}
}

// TestCumulativeStatistics_ConcurrentReadsAreRaceFree exercises the counters from
// concurrent readers and writers; it must stay clean under -race.
func TestCumulativeStatistics_ConcurrentReadsAreRaceFree(t *testing.T) {
	ctx := context.Background()

	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)
	mount := volumes["test-volume"].MountPath()

	for i := 0; i < 4; i++ {
		writeTestFile(t, mount, filepath.Join("tenant-001", "Thumbs.db"), "junk")
		if _, err := service.CleanupJunkFiles(ctx); err != nil {
			t.Fatalf("CleanupJunkFiles() error = %v", err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				_ = service.CumulativeStatistics()
				_, _ = service.CleanupJunkFiles(ctx)
			}
		}()
	}
	wg.Wait()

	if got := service.CumulativeStatistics().JunkFilesRemoved; got < 4 {
		t.Errorf("JunkFilesRemoved = %d, want at least the 4 initial removals", got)
	}
}
