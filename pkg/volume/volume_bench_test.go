package volume

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Helper function to create a temporary test volume for benchmarks
func createBenchVolume(shardDepth int) (*LocalFileSystemVolume, string, error) {
	tempDir, err := os.MkdirTemp("", "bench-volume-*")
	if err != nil {
		return nil, "", err
	}

	opts := &LocalFileSystemVolumeOptions{
		VolumeID:   "bench-volume",
		MountPath:  tempDir,
		ShardDepth: shardDepth,
	}

	volume, err := NewLocalFileSystemVolume(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, "", err
	}

	concreteVolume := volume.(*LocalFileSystemVolume)
	return concreteVolume, tempDir, nil
}

// BenchmarkPathSanitizer_SanitizeAndJoin benchmarks path sanitization.
func BenchmarkPathSanitizer_SanitizeAndJoin(b *testing.B) {
	sanitizer := NewPathSanitizer("/tmp/test")
	relativePath := "a/b/c/file.txt"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sanitizer.SanitizeAndJoin(relativePath)
	}
}

// BenchmarkPathSanitizer_ValidateRelativePath benchmarks path validation.
func BenchmarkPathSanitizer_ValidateRelativePath(b *testing.B) {
	sanitizer := NewPathSanitizer("/tmp/test")
	relativePath := "a/b/c/file.txt"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizer.ValidateRelativePath(relativePath)
	}
}

// BenchmarkBuildShardedPath benchmarks sharded path building with different depths.
func BenchmarkBuildShardedPath(b *testing.B) {
	fileKey := "550e8400e29b41d4a716446655440000"

	testCases := []struct {
		name       string
		shardDepth int
	}{
		{"NoSharding", 0},
		{"OneLevel", 1},
		{"TwoLevel", 2},
		{"ThreeLevel", 3},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = BuildShardedPath(fileKey, tc.shardDepth)
			}
		})
	}
}

// BenchmarkLocalFileSystemVolume_WriteFile benchmarks file writing with different sizes.
func BenchmarkLocalFileSystemVolume_WriteFile(b *testing.B) {
	ctx := context.Background()

	testCases := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			volume, tempDir, err := createBenchVolume(0)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tempDir)

			content := strings.Repeat("A", tc.size)

			b.ResetTimer()
			b.SetBytes(int64(tc.size))

			for i := 0; i < b.N; i++ {
				relativePath := fmt.Sprintf("bench-%d.txt", i)
				reader := bytes.NewReader([]byte(content))
				_, _ = volume.WriteFile(ctx, relativePath, reader)
			}
		})
	}
}

// BenchmarkLocalFileSystemVolume_ReadFile benchmarks file reading with different sizes.
func BenchmarkLocalFileSystemVolume_ReadFile(b *testing.B) {
	ctx := context.Background()

	testCases := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			volume, tempDir, err := createBenchVolume(0)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tempDir)

			content := strings.Repeat("A", tc.size)

			// Prepare files
			files := make([]string, b.N)
			for i := 0; i < b.N; i++ {
				relativePath := fmt.Sprintf("bench-read-%d.txt", i)
				reader := bytes.NewReader([]byte(content))
				_, _ = volume.WriteFile(ctx, relativePath, reader)
				files[i] = relativePath
			}

			b.ResetTimer()
			b.SetBytes(int64(tc.size))

			for i := 0; i < b.N; i++ {
				readCloser, _ := volume.ReadFile(ctx, files[i])
				if readCloser != nil {
					readCloser.Close()
				}
			}
		})
	}
}

// BenchmarkLocalFileSystemVolume_FileExists benchmarks file existence check.
func BenchmarkLocalFileSystemVolume_FileExists(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(0)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	relativePath := "bench-exists.txt"
	reader := bytes.NewReader([]byte("test"))
	_, _ = volume.WriteFile(ctx, relativePath, reader)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = volume.FileExists(ctx, relativePath)
	}
}

// BenchmarkLocalFileSystemVolume_GetFileSize benchmarks file size retrieval.
func BenchmarkLocalFileSystemVolume_GetFileSize(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(0)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test file
	relativePath := "bench-size.txt"
	content := strings.Repeat("A", 10000)
	reader := bytes.NewReader([]byte(content))
	_, _ = volume.WriteFile(ctx, relativePath, reader)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = volume.GetFileSize(ctx, relativePath)
	}
}

// BenchmarkLocalFileSystemVolume_DeleteFile benchmarks file deletion.
func BenchmarkLocalFileSystemVolume_DeleteFile(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(0)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Prepare files
	files := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		relativePath := fmt.Sprintf("bench-delete-%d.txt", i)
		reader := bytes.NewReader([]byte("test"))
		_, _ = volume.WriteFile(ctx, relativePath, reader)
		files[i] = relativePath
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = volume.DeleteFile(ctx, files[i])
	}
}

// BenchmarkLocalFileSystemVolume_ShardedWrite benchmarks writing with different shard depths.
func BenchmarkLocalFileSystemVolume_ShardedWrite(b *testing.B) {
	ctx := context.Background()
	content := strings.Repeat("A", 1024) // 1KB

	testCases := []struct {
		name       string
		shardDepth int
	}{
		{"NoSharding", 0},
		{"OneLevel", 1},
		{"TwoLevel", 2},
		{"ThreeLevel", 3},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			volume, tempDir, err := createBenchVolume(tc.shardDepth)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tempDir)

			b.ResetTimer()
			b.SetBytes(int64(len(content)))

			for i := 0; i < b.N; i++ {
				fileKey := fmt.Sprintf("bench%032d", i)
				relativePath, _ := volume.BuildFilePath(fileKey, ".txt")
				reader := bytes.NewReader([]byte(content))
				_, _ = volume.WriteFile(ctx, relativePath, reader)
			}
		})
	}
}

// BenchmarkLocalFileSystemVolume_IsHealthy benchmarks health check.
func BenchmarkLocalFileSystemVolume_IsHealthy(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(0)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = volume.IsHealthy(ctx)
	}
}

// BenchmarkLocalFileSystemVolume_BuildFilePath benchmarks path building.
func BenchmarkLocalFileSystemVolume_BuildFilePath(b *testing.B) {
	volume, tempDir, err := createBenchVolume(2)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	fileKey := "550e8400e29b41d4a716446655440000"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = volume.BuildFilePath(fileKey, ".pdf")
	}
}

// BenchmarkLocalFileSystemVolume_ConcurrentWrites benchmarks concurrent writes.
func BenchmarkLocalFileSystemVolume_ConcurrentWrites(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(2)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	content := strings.Repeat("A", 1024) // 1KB

	b.ResetTimer()
	b.SetBytes(1024)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fileKey := fmt.Sprintf("concurrent%032d", i)
			relativePath, _ := volume.BuildFilePath(fileKey, ".txt")
			reader := bytes.NewReader([]byte(content))
			_, _ = volume.WriteFile(ctx, relativePath, reader)
			i++
		}
	})
}

// BenchmarkLocalFileSystemVolume_ConcurrentReads benchmarks concurrent reads.
func BenchmarkLocalFileSystemVolume_ConcurrentReads(b *testing.B) {
	ctx := context.Background()
	volume, tempDir, err := createBenchVolume(2)
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	content := strings.Repeat("A", 1024) // 1KB

	// Prepare files
	numFiles := 100
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		fileKey := fmt.Sprintf("read%032d", i)
		relativePath, _ := volume.BuildFilePath(fileKey, ".txt")
		reader := bytes.NewReader([]byte(content))
		_, _ = volume.WriteFile(ctx, relativePath, reader)
		files[i] = relativePath
	}

	b.ResetTimer()
	b.SetBytes(1024)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			readCloser, _ := volume.ReadFile(ctx, files[i%numFiles])
			if readCloser != nil {
				readCloser.Close()
			}
			i++
		}
	})
}

// BenchmarkSanitizeFilename benchmarks filename sanitization.
func BenchmarkSanitizeFilename(b *testing.B) {
	filename := "my-file.with.dots.txt"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = SanitizeFilename(filename)
	}
}

// BenchmarkExtractExtension benchmarks extension extraction.
func BenchmarkExtractExtension(b *testing.B) {
	filename := "document.tar.gz"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = ExtractExtension(filename)
	}
}
