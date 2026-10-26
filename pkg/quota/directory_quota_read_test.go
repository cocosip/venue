package quota

import (
	"context"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestReadPathsDoNotMaterializeQuotaRows pins the read/write asymmetry of the
// quota manager: CanAddFile, GetFileCount and GetQuota are monitoring and
// pre-check calls, so they must answer from the stored state without
// persisting a default row for every directory they look at. Only a real write
// (IncrementFileCount) or an administrative SetQuota may create a row.
func TestReadPathsDoNotMaterializeQuotaRows(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: dataPath})

	manager, err := NewDirectoryQuotaManager(repository)
	if err != nil {
		t.Fatalf("NewDirectoryQuotaManager() error = %v", err)
	}

	allowed, err := manager.CanAddFile(ctx, sqliteQuotaTenant, "/watched")
	if err != nil {
		t.Fatalf("CanAddFile() error = %v", err)
	}
	if !allowed {
		t.Error("CanAddFile() = false, want true for a directory without a quota")
	}

	count, err := manager.GetFileCount(ctx, sqliteQuotaTenant, "/watched")
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 0 {
		t.Errorf("GetFileCount() = %d, want 0 for a directory without a row", count)
	}

	quota, err := manager.GetQuota(ctx, sqliteQuotaTenant, "/watched")
	if err != nil {
		t.Fatalf("GetQuota() error = %v", err)
	}
	if quota == nil {
		t.Fatal("GetQuota() = nil, want the domain defaults")
	}
	if quota.DirectoryPath != "/watched" || quota.CurrentCount != 0 || quota.MaxCount != 0 || quota.Enabled {
		t.Errorf("GetQuota() = %+v, want the unpersisted domain defaults", quota)
	}

	rows, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("GetAll() = %d rows, want 0: a read must not materialize quota rows", len(rows))
	}

	// A real write still materializes the row, so the fix cannot have broken
	// quota enforcement.
	if err := manager.IncrementFileCount(ctx, sqliteQuotaTenant, "/watched"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}
	rows, err = repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() after increment error = %v", err)
	}
	if len(rows) != 1 || rows[0].CurrentCount != 1 {
		t.Errorf("GetAll() after increment = %+v, want one row with count 1", rows)
	}
}

// getOrCreateOnlyRepository implements core.DirectoryQuotaRepository without the
// optional Get capability, so the manager's fallback path is exercised.
type getOrCreateOnlyRepository struct {
	getOrCreateCalls int
}

func (r *getOrCreateOnlyRepository) GetOrCreate(context.Context, string, string) (*core.DirectoryQuota, error) {
	r.getOrCreateCalls++
	return &core.DirectoryQuota{DirectoryPath: "/legacy"}, nil
}

func (r *getOrCreateOnlyRepository) Update(context.Context, string, *core.DirectoryQuota) error {
	return nil
}

func (r *getOrCreateOnlyRepository) IncrementCount(context.Context, string, string) error {
	return nil
}

func (r *getOrCreateOnlyRepository) DecrementCount(context.Context, string, string) error {
	return nil
}

func (r *getOrCreateOnlyRepository) GetAll(context.Context, string) ([]*core.DirectoryQuota, error) {
	return nil, nil
}

func (r *getOrCreateOnlyRepository) Optimize(context.Context) error { return nil }

func (r *getOrCreateOnlyRepository) Close() error { return nil }

// TestReadPathsFallBackToGetOrCreate keeps repositories that do not implement
// core.DirectoryQuotaReader working: the manager then answers its read paths
// through GetOrCreate exactly as before.
func TestReadPathsFallBackToGetOrCreate(t *testing.T) {
	repository := &getOrCreateOnlyRepository{}

	manager, err := NewDirectoryQuotaManager(repository)
	if err != nil {
		t.Fatalf("NewDirectoryQuotaManager() error = %v", err)
	}

	quota, err := manager.GetQuota(context.Background(), "tenant", "/legacy")
	if err != nil {
		t.Fatalf("GetQuota() error = %v", err)
	}
	if quota.DirectoryPath != "/legacy" {
		t.Errorf("GetQuota() = %+v, want the GetOrCreate answer", quota)
	}
	if repository.getOrCreateCalls != 1 {
		t.Errorf("GetOrCreate calls = %d, want 1 through the fallback", repository.getOrCreateCalls)
	}
}
