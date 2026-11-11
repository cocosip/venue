package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestExistingFileKeysReportsOnlyStoredKeys covers the batched existence lookup
// orphan recovery uses instead of one point query per physical file.
func TestExistingFileKeysReportsOnlyStoredKeys(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-keys"
	base := time.Now().UTC().Add(-time.Hour)

	stored := []string{"k1", "k3"}
	for _, fileKey := range stored {
		if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, fileKey, base)); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", fileKey, err)
		}
	}
	// Same key under another tenant must not count as existing for this tenant.
	if err := repo.AddOrUpdate(ctx, sqliteRecord("tenant-keys-other", "k2", base)); err != nil {
		t.Fatalf("AddOrUpdate(other tenant) error = %v", err)
	}

	existing, err := repo.ExistingFileKeys(ctx, tenantID, []string{"k1", "k2", "k3", "k4"})
	if err != nil {
		t.Fatalf("ExistingFileKeys() error = %v", err)
	}
	if len(existing) != 2 || !existing["k1"] || !existing["k3"] {
		t.Fatalf("ExistingFileKeys() = %v, want only k1 and k3", existing)
	}

	empty, err := repo.ExistingFileKeys(ctx, tenantID, nil)
	if err != nil {
		t.Fatalf("ExistingFileKeys(nil) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ExistingFileKeys(nil) = %v, want an empty map", empty)
	}

	if _, err := repo.ExistingFileKeys(ctx, "", []string{"k1"}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("empty tenant error = %v, want ErrInvalidArgument", err)
	}
	if _, err := repo.ExistingFileKeys(ctx, tenantID, []string{"k1", ""}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("empty key error = %v, want ErrInvalidArgument", err)
	}
}

// TestExistingFileKeysChunksLargeBatches proves the lookup stays correct across
// the internal chunk boundary instead of exceeding SQLite's parameter limit.
func TestExistingFileKeysChunksLargeBatches(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)
	ctx := context.Background()
	tenantID := "tenant-keys-chunk"
	base := time.Now().UTC().Add(-time.Hour)

	keys := make([]string, 0, fileKeyExistenceChunkSize+20)
	for i := 0; i < fileKeyExistenceChunkSize+20; i++ {
		fileKey := fmt.Sprintf("key-%04d", i)
		keys = append(keys, fileKey)
		// Every other key exists, so both chunks carry hits and misses.
		if i%2 != 0 {
			continue
		}
		if err := repo.AddOrUpdate(ctx, sqliteRecord(tenantID, fileKey, base)); err != nil {
			t.Fatalf("AddOrUpdate(%q) error = %v", fileKey, err)
		}
	}

	existing, err := repo.ExistingFileKeys(ctx, tenantID, keys)
	if err != nil {
		t.Fatalf("ExistingFileKeys() error = %v", err)
	}
	wantCount := len(keys) / 2
	if len(existing) != wantCount {
		t.Fatalf("ExistingFileKeys() returned %d keys, want %d", len(existing), wantCount)
	}
	for i := 0; i < len(keys); i += 2 {
		if !existing[keys[i]] {
			t.Fatalf("ExistingFileKeys() is missing stored key %q", keys[i])
		}
	}
}
