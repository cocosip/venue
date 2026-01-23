package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/pool"
	"github.com/cocosip/venue/pkg/quota"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/tenant"
	"github.com/cocosip/venue/pkg/volume"
)

// BenchmarkSystem contains the system setup for benchmarks
type BenchmarkSystem struct {
	tenantManager core.TenantManager
	metadataRepo  core.MetadataRepository
	fileScheduler core.FileScheduler
	volumes       map[string]core.StorageVolume
	storagePool   core.StoragePool
	tenantCtx     core.TenantContext
	dataDir       string
}

// setupBenchmarkSystem creates a complete system for benchmarking
func setupBenchmarkSystem(b *testing.B) *BenchmarkSystem {
	ctx := context.Background()

	// Create temp directory
	dataDir, err := os.MkdirTemp("", "bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.MetadataDirectory = filepath.Join(dataDir, "metadata")
	cfg.QuotaDirectory = filepath.Join(dataDir, "quotas")
	cfg.Volumes[0].MountPath = filepath.Join(dataDir, "volumes", "default")

	// Tenant manager
	tenantMgr, _ := tenant.NewTenantManager(&tenant.TenantManagerOptions{
		RootPath:         cfg.MetadataDirectory,
		CacheTTL:         5 * time.Minute, // Use reasonable default
		EnableAutoCreate: false,
	})

	tenantID := "bench-tenant"
	_ = tenantMgr.CreateTenant(ctx, tenantID)
	tenantCtx, _ := tenantMgr.GetTenant(ctx, tenantID)

	// Storage volume
	vol, _ := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
		VolumeID:   cfg.Volumes[0].VolumeId,
		VolumeType: cfg.Volumes[0].VolumeType,
		MountPath:  cfg.Volumes[0].MountPath,
		ShardDepth: cfg.Volumes[0].ShardingDepth,
	})

	volumes := map[string]core.StorageVolume{
		cfg.Volumes[0].VolumeId: vol,
	}

	// Metadata repository
	metaRepo, _ := metadata.NewBadgerMetadataRepository(&metadata.BadgerRepositoryOptions{
		TenantID:       tenantID,
		DataPath:       filepath.Join(cfg.MetadataDirectory, tenantID),
		CacheTTL:       cfg.MetadataOptions.CacheTTL,
		GCInterval:     cfg.BadgerDBOptions.GCInterval,
		GCDiscardRatio: cfg.BadgerDBOptions.GCDiscardRatio,
	})

	// Quota managers
	tenantQuotaMgr := quota.NewTenantQuotaManager()
	dirQuotaRepo, _ := quota.NewBadgerDirectoryQuotaRepository(&quota.BadgerDirectoryQuotaRepositoryOptions{
		DataPath:       cfg.QuotaDirectory,
		GCInterval:     cfg.BadgerDBOptions.GCInterval,
		GCDiscardRatio: cfg.BadgerDBOptions.GCDiscardRatio,
	})
	dirQuotaMgr, _ := quota.NewDirectoryQuotaManager(dirQuotaRepo)

	// File scheduler
	fileScheduler, _ := scheduler.NewFileScheduler(metaRepo, volumes, &scheduler.FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         cfg.RetryPolicy.MaxRetryCount,
			InitialRetryDelay:     cfg.RetryPolicy.InitialRetryDelay,
			UseExponentialBackoff: cfg.RetryPolicy.UseExponentialBackoff,
			MaxRetryDelay:         cfg.RetryPolicy.MaxRetryDelay,
		},
		ProcessingTimeout: cfg.CleanupOptions.ProcessingTimeout,
	})

	// Storage pool
	storagePool, _ := pool.NewStoragePool(&pool.StoragePoolOptions{
		TenantManager:         tenantMgr,
		MetadataRepository:    metaRepo,
		FileScheduler:         fileScheduler,
		Volumes:               volumes,
		TenantQuotaManager:    tenantQuotaMgr,
		DirectoryQuotaManager: dirQuotaMgr,
	})

	return &BenchmarkSystem{
		tenantManager: tenantMgr,
		metadataRepo:  metaRepo,
		fileScheduler: fileScheduler,
		volumes:       volumes,
		storagePool:   storagePool,
		tenantCtx:     tenantCtx,
		dataDir:       dataDir,
	}
}

// cleanup cleans up benchmark system resources
func (s *BenchmarkSystem) cleanup() {
	_ = os.RemoveAll(s.dataDir)
}

// BenchmarkWriteFile benchmarks file upload performance
func BenchmarkWriteFile(b *testing.B) {
	sys := setupBenchmarkSystem(b)
	defer sys.cleanup()

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
	defer sys.cleanup()

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
	defer sys.cleanup()

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
	defer sys.cleanup()

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
	defer sys.cleanup()

	ctx := context.Background()
	content := []byte("complete workflow benchmark")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Upload
		fileName := fmt.Sprintf("workflow-%d.txt", i)
		fileKey, err := sys.storagePool.WriteFile(ctx, sys.tenantCtx, bytes.NewReader(content), &fileName)
		if err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}

		// 2. Get for processing
		_, err = sys.storagePool.GetNextFileForProcessing(ctx, sys.tenantCtx)
		if err != nil {
			b.Fatalf("GetNextFileForProcessing failed: %v", err)
		}

		// 3. Mark as completed (deletes)
		err = sys.storagePool.MarkAsCompleted(ctx, fileKey)
		if err != nil {
			b.Fatalf("MarkAsCompleted failed: %v", err)
		}
	}
}

// BenchmarkMetadataOperations benchmarks metadata operations
func BenchmarkMetadataOperations(b *testing.B) {
	sys := setupBenchmarkSystem(b)
	defer sys.cleanup()

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
			_, _ = sys.metadataRepo.Get(ctx, meta.FileKey)
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
	sys := setupBenchmarkSystem(b)
	defer sys.cleanup()

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
	defer sys.cleanup()

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
			_ = sys.storagePool.MarkAsCompleted(ctx, location.FileKey)
		}
	})
}
