package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	venue "github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/quota"
)

// BenchmarkSystem contains the public Venue runtime used by system benchmarks.
type BenchmarkSystem struct {
	metadataRepo core.MetadataRepository
	storagePool  core.StoragePool
	tenantCtx    core.TenantContext
}

// setupBenchmarkSystem creates a complete system through Venue's public API.
func setupBenchmarkSystem(b *testing.B) *BenchmarkSystem {
	b.Helper()

	ctx := context.Background()
	dataDir := b.TempDir()
	tenantID := "bench-tenant"
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(dataDir, "metadata")).
		WithQuotaDirectory(filepath.Join(dataDir, "quotas")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("default-volume").
			WithMountPath(filepath.Join(dataDir, "volumes", "default")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig(tenantID))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		b.Fatalf("NewVenue failed: %v", err)
	}
	if err := runtime.Start(); err != nil {
		b.Fatalf("Venue.Start failed: %v", err)
	}
	b.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			b.Errorf("Venue.Stop failed: %v", err)
		}
	})

	tenantCtx, err := runtime.TenantManager().GetTenant(ctx, tenantID)
	if err != nil {
		b.Fatalf("GetTenant failed: %v", err)
	}

	return &BenchmarkSystem{
		metadataRepo: runtime.MetadataRepository(),
		storagePool:  runtime.StoragePool(),
		tenantCtx:    tenantCtx,
	}
}

// BenchmarkWriteFile benchmarks file upload performance
func BenchmarkWriteFile(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()
	content := []byte("benchmark test content")
	fileName := "bench.txt"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
		if err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
	}
}

// BenchmarkWriteFile_Parallel benchmarks parallel file uploads
func BenchmarkWriteFile_Parallel(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()
	content := []byte("benchmark test content")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fileName := fmt.Sprintf("bench-%d.txt", i)
			_, err := sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
			if err != nil {
				b.Errorf("WriteFile failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkReadFile benchmarks file read performance
func BenchmarkReadFile(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()

	// Upload a file first
	content := []byte("benchmark test content for reading")
	fileName := "read-bench.txt"
	fileKey, _ := sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := sys.storagePool.ReadFile(ctx, sys.tenantCtx, fileKey)
		if err != nil {
			b.Fatalf("ReadFile failed: %v", err)
		}
		_ = reader.Close()
	}
}

// BenchmarkGetNextFileForProcessing benchmarks queue retrieval
func BenchmarkGetNextFileForProcessing(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()

	// Upload files
	for i := 0; i < 100; i++ {
		content := []byte(fmt.Sprintf("file %d", i))
		fileName := fmt.Sprintf("file-%d.txt", i)
		_, _ = sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := sys.storagePool.GetNextFileForProcessing(ctx, sys.tenantCtx)
		if err != nil {
			// No more files, upload more
			content := []byte("replenish")
			fileName := "replenish.txt"
			_, _ = sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
		}
	}
}

// BenchmarkCompleteWorkflow benchmarks the complete file lifecycle
func BenchmarkCompleteWorkflow(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()
	content := []byte("complete workflow benchmark")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Upload
		fileName := fmt.Sprintf("workflow-%d.txt", i)
		_, err := sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
		if err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}

		// 2. Get for processing
		location, err := sys.storagePool.GetNextFileForProcessing(ctx, sys.tenantCtx)
		if err != nil {
			b.Fatalf("GetNextFileForProcessing failed: %v", err)
		}
		if location.Lease == nil {
			b.Fatal("GetNextFileForProcessing returned a nil lease")
		}

		// 3. Mark as completed (deletes)
		err = sys.storagePool.MarkAsCompleted(ctx, *location.Lease)
		if err != nil {
			b.Fatalf("MarkAsCompleted failed: %v", err)
		}
	}
}

// BenchmarkMetadataOperations benchmarks metadata operations
func BenchmarkMetadataOperations(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()

	// Create test metadata
	now := time.Now()
	meta := &core.FileMetadata{
		FileKey:          "test-key",
		TenantID:         "bench-tenant",
		VolumeID:         "default-volume",
		PhysicalPath:     "test/path/file.txt",
		FileSize:         1024,
		FileExtension:    ".txt",
		OriginalFileName: "test.txt",
		Status:           core.FileStatusPending,
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	b.Run("AddOrUpdate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			meta.FileKey = fmt.Sprintf("key-%d", i)
			_ = sys.metadataRepo.AddOrUpdate(ctx, meta)
		}
	})

	b.Run("Get", func(b *testing.B) {
		// Add one file
		_ = sys.metadataRepo.AddOrUpdate(ctx, meta)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = sys.metadataRepo.Get(ctx, sys.tenantCtx.ID, meta.FileKey)
		}
	})

	b.Run("GetPendingFiles", func(b *testing.B) {
		// Add some pending files
		for i := 0; i < 10; i++ {
			m := *meta
			m.FileKey = fmt.Sprintf("pending-%d", i)
			_ = sys.metadataRepo.AddOrUpdate(ctx, &m)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = sys.metadataRepo.GetPendingFiles(ctx, "bench-tenant", 10)
		}
	})
}

// BenchmarkQuotaOperations benchmarks quota management
func BenchmarkQuotaOperations(b *testing.B) {
	ctx := context.Background()
	tenantQuotaMgr := quota.NewTenantQuotaManager()

	b.Run("IncrementFileCount", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tenantQuotaMgr.IncrementFileCount(ctx, "bench-tenant")
		}
	})

	b.Run("DecrementFileCount", func(b *testing.B) {
		// Pre-increment
		for i := 0; i < b.N; i++ {
			_ = tenantQuotaMgr.IncrementFileCount(ctx, "bench-tenant")
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = tenantQuotaMgr.DecrementFileCount(ctx, "bench-tenant")
		}
	})

	b.Run("GetFileCount", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = tenantQuotaMgr.GetFileCount(ctx, "bench-tenant")
		}
	})
}

// BenchmarkConcurrentProcessing benchmarks concurrent file processing
func BenchmarkConcurrentProcessing(b *testing.B) {
	sys := setupBenchmarkSystem(b)

	ctx := context.Background()

	// Upload files
	for i := 0; i < 1000; i++ {
		content := []byte(fmt.Sprintf("file %d", i))
		fileName := fmt.Sprintf("concurrent-%d.txt", i)
		_, _ = sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			location, err := sys.storagePool.GetNextFileForProcessing(ctx, sys.tenantCtx)
			if err != nil {
				continue
			}

			// Simulate processing
			reader, _ := sys.storagePool.ReadFile(ctx, sys.tenantCtx, location.FileKey)
			_ = reader.Close()

			// Mark as completed
			if location.Lease != nil {
				_ = sys.storagePool.MarkAsCompleted(ctx, *location.Lease)
			}
		}
	})
}
