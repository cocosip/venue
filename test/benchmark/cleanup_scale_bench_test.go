package benchmark

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
)

// These benchmarks measure the cost of one cleanup pass over a large number of
// completed records. They exist so the "cleanup used to load every record into
// memory" concern has numbers attached to it.
//
// Records are seeded through the public metadata repository rather than by
// writing payloads: the measurement target is the scan/delete/accounting path,
// not the write path. Each record's physical file is intentionally absent, which
// the volume treats as an already-deleted file.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkCleanupCompletedRecords -benchtime=1x ./test/benchmark

const cleanupBenchTenant = "cleanup-bench"

func seedCompletedRecords(tb testing.TB, runtime *venue.Venue, count int) {
	tb.Helper()
	ctx := context.Background()
	completedAt := time.Now().Add(-time.Hour)
	now := time.Now()

	const batchSize = 500
	batch := make([]*core.FileMetadata, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := runtime.MetadataRepository().AddOrUpdateBatch(ctx, batch); err != nil {
			tb.Fatalf("seed batch: %v", err)
		}
		batch = batch[:0]
	}

	for i := 0; i < count; i++ {
		fileKey := fmt.Sprintf("%032x", i)
		batch = append(batch, &core.FileMetadata{
			FileKey:       fileKey,
			TenantID:      cleanupBenchTenant,
			VolumeID:      "primary",
			PhysicalPath:  fmt.Sprintf("%s/ab/cd/%s.bin", cleanupBenchTenant, fileKey),
			DirectoryPath: "/",
			FileSize:      1024,
			FileExtension: ".bin",
			Status:        core.FileStatusCompleted,
			CompletedAt:   &completedAt,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
		if len(batch) == batchSize {
			flush()
		}
	}
	flush()
}

func newCleanupBenchVenue(tb testing.TB) *venue.Venue {
	tb.Helper()
	root := tb.TempDir()
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		tb.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.TenantManager().CreateTenant(context.Background(), cleanupBenchTenant); err != nil {
		tb.Fatalf("CreateTenant() error = %v", err)
	}
	tb.Cleanup(func() { _ = runtime.Stop() })
	return runtime
}

// BenchmarkCleanupCompletedRecords measures one completed-file cleanup pass.
func BenchmarkCleanupCompletedRecords(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("records=%d", count), func(b *testing.B) {
			runtime := newCleanupBenchVenue(b)
			ctx := context.Background()
			seed := func() {
				seedCompletedRecords(b, runtime, count)
				// Give the quota counters rows that match the seeded records so
				// the per-record decrement path is exercised, not just skipped.
				if err := runtime.ReconcileQuotaCounts(ctx); err != nil {
					b.Fatalf("ReconcileQuotaCounts() error = %v", err)
				}
			}
			seed()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i > 0 {
					b.StopTimer()
					seed()
					b.StartTimer()
				}
				stats, err := runtime.CleanupService().CleanupCompletedFiles(ctx, 0)
				if err != nil {
					b.Fatalf("CleanupCompletedFiles() error = %v", err)
				}
				if stats.CompletedRecordsRemoved != count {
					b.Fatalf("removed %d records, want %d", stats.CompletedRecordsRemoved, count)
				}
			}
		})
	}
}
