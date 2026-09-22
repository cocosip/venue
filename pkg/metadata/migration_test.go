package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// TestBuildStatusIndexKeyUsesFixedWidthUTC pins the schema-v3 secondary index
// key layout. Every segment after the tenant must be fixed width so that Badger
// iteration order equals queue order: availability first (nil availability uses
// the lexicographically smallest sentinel), then the arrival timestamp.
func TestBuildStatusIndexKeyUsesFixedWidthUTC(t *testing.T) {
	zone := time.FixedZone("UTC+8", 8*60*60)
	available := time.Date(2026, time.March, 4, 5, 6, 7, 500_000_000, zone)

	tests := []struct {
		name      string
		available *time.Time
		created   time.Time
		wantBody  string
	}{
		{
			name:     "nil availability sorts first",
			created:  time.Date(2026, time.March, 4, 4, 0, 0, 0, time.UTC),
			wantBody: "0000-00-00T00:00:00.000000000Z:2026-03-04T04:00:00.000000000Z",
		},
		{
			name:      "availability and creation are converted to fixed width UTC",
			available: &available,
			created:   time.Date(2026, time.March, 4, 4, 0, 0, 0, zone),
			wantBody:  "2026-03-03T21:06:07.500000000Z:2026-03-03T20:00:00.000000000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := &core.FileMetadata{
				FileKey:                  "file-a",
				TenantID:                 "test-tenant",
				Status:                   core.FileStatusPending,
				AvailableForProcessingAt: tt.available,
				CreatedAt:                tt.created,
			}

			want := fmt.Sprintf("v3:idx:status:%s:0:%s:file-a", encodeTenantID("test-tenant"), tt.wantBody)
			if got := string(buildStatusIndexKey(metadata)); got != want {
				t.Errorf("buildStatusIndexKey() = %q, want %q", got, want)
			}
		})
	}

	t.Run("nil availability sorts before any real timestamp", func(t *testing.T) {
		later := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
		immediate := buildStatusIndexKey(&core.FileMetadata{
			FileKey: "a", TenantID: "test-tenant", Status: core.FileStatusPending,
			CreatedAt: later,
		})
		scheduled := buildStatusIndexKey(&core.FileMetadata{
			FileKey: "b", TenantID: "test-tenant", Status: core.FileStatusPending,
			AvailableForProcessingAt: &later, CreatedAt: later,
		})
		if string(immediate) >= string(scheduled) {
			t.Errorf("immediately available key %q does not sort before scheduled key %q", immediate, scheduled)
		}
	})
}

// TestGetPendingFilesPreservesArrivalOrder is the FIFO regression test. Freshly
// written files have no AvailableForProcessingAt, so their creation time must
// decide the order instead of the random file key.
func TestGetPendingFilesPreservesArrivalOrder(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.(*BadgerMetadataRepository).Close() })

	base := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	fixtures := []struct {
		fileKey string
		created time.Time
	}{
		{"ccc-written-first", base},
		{"bbb-written-second", base.Add(time.Second)},
		{"aaa-written-third", base.Add(2 * time.Second)},
	}

	for _, fixture := range fixtures {
		metadata := createTestMetadata(fixture.fileKey, core.FileStatusPending)
		metadata.CreatedAt = fixture.created
		metadata.UpdatedAt = fixture.created
		if err := repo.AddOrUpdate(ctx, metadata); err != nil {
			t.Fatalf("add %s: %v", fixture.fileKey, err)
		}
	}

	queries := map[string]func() ([]*core.FileMetadata, error){
		"GetPendingFiles": func() ([]*core.FileMetadata, error) {
			return repo.GetPendingFiles(ctx, "test-tenant", 0)
		},
		"GetByStatus": func() ([]*core.FileMetadata, error) {
			return repo.GetByStatus(ctx, "test-tenant", core.FileStatusPending, 0)
		},
	}

	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			results, err := query()
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(results) != len(fixtures) {
				t.Fatalf("got %d records, want %d", len(results), len(fixtures))
			}
			for i, fixture := range fixtures {
				if results[i].FileKey != fixture.fileKey {
					t.Errorf("result[%d] = %q, want %q (arrival order)", i, results[i].FileKey, fixture.fileKey)
				}
			}
		})
	}
}

// TestRepositoryRebuildsStatusIndexesForNewSchemaVersion verifies the
// schema-versioned startup reindex: a database still carrying v2 index keys is
// rebuilt from primary records, the stale prefix is removed, and the recorded
// schema version is upgraded. A second open must be a no-op.
func TestRepositoryRebuildsStatusIndexesForNewSchemaVersion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dbPath := filepath.Join(root, "shared", "metadata")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	fixtures := []struct {
		fileKey string
		created time.Time
	}{
		{"ccc-oldest", base},
		{"bbb-middle", base.Add(time.Second)},
		{"aaa-newest", base.Add(2 * time.Second)},
	}

	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		for _, fixture := range fixtures {
			metadata := &core.FileMetadata{
				FileKey: fixture.fileKey, TenantID: "tenant-a", VolumeID: "v1",
				PhysicalPath: "path/" + fixture.fileKey, Status: core.FileStatusPending,
				CreatedAt: fixture.created, UpdatedAt: fixture.created,
			}
			data, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			if err := txn.Set(buildMetadataKey(metadata.TenantID, metadata.FileKey), data); err != nil {
				return err
			}
			// A stale v2 index shares one availability sentinel for every fresh
			// file, so iteration falls back to the random file key.
			stale := fmt.Sprintf("v2:idx:status:%s:0:0000-00-00T00:00:00Z:%s",
				encodeTenantID(metadata.TenantID), metadata.FileKey)
			if err := txn.Set([]byte(stale), []byte(metadata.FileKey)); err != nil {
				return err
			}
		}
		return txn.Set(metadataSchemaKey, []byte("2"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	assertOrderedPending := func(t *testing.T, repo core.MetadataRepository) {
		t.Helper()
		results, err := repo.GetPendingFiles(ctx, "tenant-a", 0)
		if err != nil {
			t.Fatalf("get pending files: %v", err)
		}
		if len(results) != len(fixtures) {
			t.Fatalf("got %d pending records, want %d", len(results), len(fixtures))
		}
		for i, fixture := range fixtures {
			if results[i].FileKey != fixture.fileKey {
				t.Errorf("pending[%d] = %q, want %q", i, results[i].FileKey, fixture.fileKey)
			}
		}
	}

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: "shared", DataPath: root})
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	concrete := repo.(*BadgerMetadataRepository)

	assertOrderedPending(t, repo)

	if metadataSchemaVersion != "3" {
		t.Errorf("metadataSchemaVersion = %q, want %q", metadataSchemaVersion, "3")
	}
	if err := concrete.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metadataSchemaKey)
		if err != nil {
			return fmt.Errorf("schema version missing: %w", err)
		}
		var version string
		if err := item.Value(func(val []byte) error {
			version = string(val)
			return nil
		}); err != nil {
			return err
		}
		if version != metadataSchemaVersion {
			t.Errorf("schema version = %q, want %q", version, metadataSchemaVersion)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for _, prefix := range [][]byte{[]byte("v2:idx:status:"), legacyStatusIndexPrefix} {
		stale, err := countKeysWithPrefix(concrete.db, prefix)
		if err != nil {
			t.Fatal(err)
		}
		if stale != 0 {
			t.Errorf("%d stale %q index keys remain", stale, prefix)
		}
	}
	if v3, err := countKeysWithPrefix(concrete.db, []byte("v3:idx:status:")); err != nil {
		t.Fatal(err)
	} else if v3 != len(fixtures) {
		t.Errorf("%d v3 index keys, want %d", v3, len(fixtures))
	}

	if err := concrete.Close(); err != nil {
		t.Fatalf("close repository: %v", err)
	}

	// Restarting with the current schema version must not rebuild or lose data.
	restarted, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: "shared", DataPath: root})
	if err != nil {
		t.Fatalf("reopen repository: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	assertOrderedPending(t, restarted)
}

// countKeysWithPrefix counts keys carrying prefix in the given database.
func countKeysWithPrefix(db *badger.DB, prefix []byte) (int, error) {
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}
