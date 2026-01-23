package metadata

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// BenchmarkAddOrUpdate measures write performance.
func BenchmarkAddOrUpdate(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		err := repo.AddOrUpdate(ctx, metadata)
		if err != nil {
			b.Fatalf("AddOrUpdate failed: %v", err)
		}
	}
}

// BenchmarkGet measures read performance.
func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with test data
	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileKey := fmt.Sprintf("file-%d", i%numFiles)
		_, err := repo.Get(ctx, fileKey)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkGetCached measures cached read performance.
func BenchmarkGetCached(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Add a single file and ensure it's cached
	metadata := createTestMetadata("cached-file", core.FileStatusPending)
	_ = repo.AddOrUpdate(ctx, metadata)
	_, _ = repo.Get(ctx, "cached-file") // Warm up cache

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := repo.Get(ctx, "cached-file")
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkGetUncached measures uncached read performance.
func BenchmarkGetUncached(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with completed files (not cached)
	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusCompleted)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileKey := fmt.Sprintf("file-%d", i%numFiles)
		_, err := repo.Get(ctx, fileKey)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkUpdateStatus measures status update performance.
func BenchmarkUpdateStatus(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with test data
	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileKey := fmt.Sprintf("file-%d", i%numFiles)
		status := core.FileStatusProcessing
		if i%2 == 0 {
			status = core.FileStatusCompleted
		}
		err := repo.UpdateStatus(ctx, fileKey, status)
		if err != nil {
			b.Fatalf("UpdateStatus failed: %v", err)
		}
	}
}

// BenchmarkGetByStatus measures query by status performance.
func BenchmarkGetByStatus(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with mixed status files
	for i := 0; i < 1000; i++ {
		var status core.FileProcessingStatus
		switch i % 3 {
		case 0:
			status = core.FileStatusProcessing
		case 1:
			status = core.FileStatusCompleted
		default:
			status = core.FileStatusPending
		}
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), status)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 10)
		if err != nil {
			b.Fatalf("GetByStatus failed: %v", err)
		}
	}
}

// BenchmarkGetPendingFiles measures pending file query performance.
func BenchmarkGetPendingFiles(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with pending files
	for i := 0; i < 1000; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := repo.GetPendingFiles(ctx, "test-tenant", 10)
		if err != nil {
			b.Fatalf("GetPendingFiles failed: %v", err)
		}
	}
}

// BenchmarkDelete measures delete performance.
func BenchmarkDelete(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with test data
	for i := 0; i < b.N; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileKey := fmt.Sprintf("file-%d", i)
		err := repo.Delete(ctx, fileKey)
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}
	}
}

// BenchmarkConcurrentWrites measures concurrent write performance.
func BenchmarkConcurrentWrites(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
			err := repo.AddOrUpdate(ctx, metadata)
			if err != nil {
				b.Fatalf("AddOrUpdate failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkConcurrentReads measures concurrent read performance.
func BenchmarkConcurrentReads(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with test data
	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fileKey := fmt.Sprintf("file-%d", i%numFiles)
			_, err := repo.Get(ctx, fileKey)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkMixedOperations measures mixed read/write performance.
func BenchmarkMixedOperations(b *testing.B) {
	ctx := context.Background()
	repo, tmpDir := createBenchRepository(b)
	defer func() { _ = os.RemoveAll(tmpDir) }()
	defer func() { _ = repo.(*BadgerMetadataRepository).Close() }()

	// Pre-populate with test data
	for i := 0; i < 100; i++ {
		metadata := createTestMetadata(fmt.Sprintf("file-%d", i), core.FileStatusPending)
		_ = repo.AddOrUpdate(ctx, metadata)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			operation := i % 4
			fileKey := fmt.Sprintf("file-%d", i%100)

			switch operation {
			case 0: // Read
				_, _ = repo.Get(ctx, fileKey)
			case 1: // Write
				metadata := createTestMetadata(fileKey, core.FileStatusPending)
				_ = repo.AddOrUpdate(ctx, metadata)
			case 2: // Update status
				_ = repo.UpdateStatus(ctx, fileKey, core.FileStatusProcessing)
			case 3: // Query
				_, _ = repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 10)
			}
			i++
		}
	})
}

// createBenchRepository creates a repository for benchmarking.
func createBenchRepository(b *testing.B) (core.MetadataRepository, string) {
	tmpDir, err := os.MkdirTemp("", "venue-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &BadgerRepositoryOptions{
		TenantID:       "bench-tenant",
		DataPath:       tmpDir,
		CacheTTL:       5 * time.Minute,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := NewBadgerMetadataRepository(opts)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		b.Fatalf("Failed to create repository: %v", err)
	}

	return repo, tmpDir
}
