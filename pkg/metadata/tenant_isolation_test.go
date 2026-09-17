package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

func TestRepositoryScopesSameFileKeyByTenant(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	t.Cleanup(func() { _ = repo.Close() })

	tenantA := createTestMetadata("shared-key", core.FileStatusPending)
	tenantA.TenantID = "tenant-a"
	tenantB := createTestMetadata("shared-key", core.FileStatusPending)
	tenantB.TenantID = "tenant-b"
	tenantB.OriginalFileName = "tenant-b.txt"

	if err := repo.AddOrUpdate(ctx, tenantA); err != nil {
		t.Fatalf("add tenant A: %v", err)
	}
	if err := repo.AddOrUpdate(ctx, tenantB); err != nil {
		t.Fatalf("add tenant B: %v", err)
	}

	gotA, err := repo.Get(ctx, "tenant-a", "shared-key")
	if err != nil {
		t.Fatalf("get tenant A: %v", err)
	}
	gotB, err := repo.Get(ctx, "tenant-b", "shared-key")
	if err != nil {
		t.Fatalf("get tenant B: %v", err)
	}
	if gotA.TenantID != "tenant-a" || gotB.TenantID != "tenant-b" {
		t.Fatalf("cross-tenant read: A=%q B=%q", gotA.TenantID, gotB.TenantID)
	}

	pendingA, err := repo.GetPendingFiles(ctx, "tenant-a", 0)
	if err != nil || len(pendingA) != 1 || pendingA[0].TenantID != "tenant-a" {
		t.Fatalf("tenant A pending = %#v, err = %v", pendingA, err)
	}
	if err := repo.Delete(ctx, "tenant-a", "shared-key"); err != nil {
		t.Fatalf("delete tenant A: %v", err)
	}
	if _, err := repo.Get(ctx, "tenant-a", "shared-key"); !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("tenant A get after delete error = %v", err)
	}
	if _, err := repo.Get(ctx, "tenant-b", "shared-key"); err != nil {
		t.Fatalf("tenant B was affected by tenant A delete: %v", err)
	}
}

func TestRepositoryMigratesLiteralLegacyRecords(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dbPath := filepath.Join(root, "shared", "metadata")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	legacy := &core.FileMetadata{
		FileKey: "legacy-key", TenantID: "tenant-a", VolumeID: "v1",
		PhysicalPath: "legacy/path", Status: core.FileStatusPending,
		CreatedAt: now, UpdatedAt: now,
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("file:legacy-key"), data); err != nil {
			return err
		}
		return txn.Set([]byte("idx:status:0:00000000000000000000:legacy-key"), []byte("legacy-key"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	repo, err := NewBadgerMetadataRepository(&BadgerRepositoryOptions{TenantID: "shared", DataPath: root})
	if err != nil {
		t.Fatalf("open repository: %v", err)
	}
	concrete := repo.(*BadgerMetadataRepository)

	got, err := repo.Get(ctx, "tenant-a", "legacy-key")
	if err != nil || got.TenantID != "tenant-a" {
		t.Fatalf("migrated get = %#v, err = %v", got, err)
	}
	if err := concrete.Close(); err != nil {
		t.Fatal(err)
	}

	verify, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = verify.Close() }()
	if err := verify.View(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte("file:legacy-key")); err != badger.ErrKeyNotFound {
			return errors.New("legacy primary key still exists")
		}
		_, err := txn.Get([]byte("schema:metadata"))
		return err
	}); err != nil {
		t.Fatalf("migration verification: %v", err)
	}
}
