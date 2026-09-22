package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// This file measures the one-off startup cost of the schema-v3 status-index
// rebuild that runs the first time a store written by the previous schema is
// opened. It exists so "how expensive is the upgrade" is an answered question
// rather than an unknown.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkStartupIndexRebuild -benchtime=1x ./pkg/metadata

const reindexBenchTenant = "reindex-bench"

// staleSchemaStorePath returns the Badger directory for the benchmark tenant.
func staleSchemaStorePath(dataPath string) string {
	return filepath.Join(dataPath, reindexBenchTenant, "metadata")
}

// prepareStaleSchema writes count primary records and then rewinds the store to
// the pre-v3 shape: no v3 status index, stale v2 index keys, schema version 2.
// The next repository open therefore has to rebuild every index entry.
func prepareStaleSchema(b *testing.B, dataPath string, count int) {
	b.Helper()
	ctx := context.Background()

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
		TenantID: reindexBenchTenant,
		DataPath: dataPath,
	})
	if err != nil {
		b.Fatalf("create repository: %v", err)
	}
	base := time.Now()
	for i := 0; i < count; i++ {
		record := createTestMetadata(fmt.Sprintf("reindex-%08d", i), core.FileStatusPending)
		record.TenantID = reindexBenchTenant
		record.CreatedAt = base.Add(time.Duration(i) * time.Millisecond)
		record.UpdatedAt = record.CreatedAt
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			_ = repo.Close()
			b.Fatalf("seed record %d: %v", i, err)
		}
	}
	if err := repo.Close(); err != nil {
		b.Fatalf("close seeded repository: %v", err)
	}
	rewindToPreviousSchema(b, dataPath)
}

// rewindToPreviousSchema drops the v3 index, plants stale v2 index keys and
// sets the stored schema version back to 2.
func rewindToPreviousSchema(b *testing.B, dataPath string) {
	b.Helper()
	db, err := badger.Open(badger.DefaultOptions(staleSchemaStorePath(dataPath)).WithLogger(nil))
	if err != nil {
		b.Fatalf("open raw store: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			b.Fatalf("close raw store: %v", err)
		}
	}()

	if err := deletePrefixInBatches(db, statusIndexPrefix, 512); err != nil {
		b.Fatalf("drop v3 index: %v", err)
	}

	// Plant stale v2 index keys, exactly what the previous schema left behind.
	var keys [][]byte
	if err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(metadataPrimaryPrefix); it.ValidForPrefix(metadataPrimaryPrefix) && len(keys) < 512; it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		b.Fatalf("scan primary keys: %v", err)
	}
	if len(keys) == 0 {
		b.Fatal("seeded store has no primary records")
	}
	if err := db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			legacy := append([]byte("v2:idx:status:"), key...)
			if err := txn.Set(legacy, key); err != nil {
				return err
			}
		}
		return txn.Set(metadataSchemaKey, []byte("2"))
	}); err != nil {
		b.Fatalf("plant stale schema: %v", err)
	}
}

// BenchmarkStartupIndexRebuild measures the repository open that has to rebuild
// the status indexes, including the heap allocated by the rebuild.
func BenchmarkStartupIndexRebuild(b *testing.B) {
	for _, count := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("records=%d", count), func(b *testing.B) {
			dataPath := b.TempDir()
			prepareStaleSchema(b, dataPath, count)

			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				if i > 0 {
					b.StopTimer()
					rewindToPreviousSchema(b, dataPath)
					b.StartTimer()
				}

				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)

				repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{
					TenantID: reindexBenchTenant,
					DataPath: dataPath,
				})
				if err != nil {
					b.Fatalf("open repository (iteration %d): %v", i, err)
				}

				// The rebuild must make every record visible through the index.
				pending, err := repo.GetPendingFiles(ctx, reindexBenchTenant, 1)
				if err != nil {
					_ = repo.Close()
					b.Fatalf("index not usable after rebuild: %v", err)
				}
				if len(pending) != 1 {
					_ = repo.Close()
					b.Fatalf("rebuilt index returned %d records, want 1", len(pending))
				}
				if err := repo.Close(); err != nil {
					b.Fatalf("close repository: %v", err)
				}

				runtime.ReadMemStats(&after)
				heapDelta := int64(after.TotalAlloc - before.TotalAlloc)
				b.ReportMetric(float64(heapDelta)/(1024*1024), "MB-alloc/op")
			}
		})
	}
}
