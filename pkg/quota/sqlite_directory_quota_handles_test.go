package quota

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestSQLiteDirectoryQuotaRepositoryBoundsOpenDatabases proves MaxOpenDatabases
// reclaims the least recently used idle handle instead of growing without bound.
func TestSQLiteDirectoryQuotaRepositoryBoundsOpenDatabases(t *testing.T) {
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:         t.TempDir(),
		MaxOpenDatabases: 2,
	})

	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}
	for _, tenantID := range tenants {
		mustGetOrCreate(t, repository, tenantID, "/bounded")
	}

	if got := repository.openHandleCount(); got != 2 {
		t.Fatalf("open handle count = %d, want 2", got)
	}
	if got, want := repository.openTenantIDs(), []string{"tenant-b", "tenant-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("open tenants = %v, want %v (the least recently used handle is reclaimed)", got, want)
	}

	// Using the reclaimed tenant again reopens its database with the same data.
	quota := mustGetOrCreate(t, repository, tenants[0], "/bounded")
	if quota.CurrentCount != 0 {
		t.Fatalf("CurrentCount = %d, want 0 after the handle was reclaimed", quota.CurrentCount)
	}
	if got := repository.openHandleCount(); got != 2 {
		t.Fatalf("open handle count = %d, want 2 after reopening", got)
	}

	// Zero means unlimited: every touched tenant keeps its handle.
	unbounded := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})
	for _, tenantID := range tenants {
		mustGetOrCreate(t, unbounded, tenantID, "/unbounded")
	}
	if got := unbounded.openHandleCount(); got != len(tenants) {
		t.Fatalf("open handle count = %d, want %d with MaxOpenDatabases = 0", got, len(tenants))
	}
}

// TestSQLiteDirectoryQuotaRepositoryEvictsIdleDatabases proves
// OpenDatabaseIdleTimeout reclaims a handle that has been idle for longer than the
// configured duration, on the next acquisition and on the maintenance pass.
func TestSQLiteDirectoryQuotaRepositoryEvictsIdleDatabases(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                t.TempDir(),
		OpenDatabaseIdleTimeout: time.Hour,
		now:                     func() time.Time { return current },
	})

	mustGetOrCreate(t, repository, "tenant-a", "/idle")
	if got := repository.openTenantIDs(); !reflect.DeepEqual(got, []string{"tenant-a"}) {
		t.Fatalf("open tenants = %v, want [tenant-a]", got)
	}

	// A second access in the same instant must not evict anything.
	current = current.Add(30 * time.Minute)
	mustGetOrCreate(t, repository, "tenant-a", "/idle")
	if got := repository.openHandleCount(); got != 1 {
		t.Fatalf("open handle count = %d, want 1 while the handle is still fresh", got)
	}

	// Past the timeout, the next acquisition reclaims the idle handle.
	current = current.Add(2 * time.Hour)
	mustGetOrCreate(t, repository, "tenant-b", "/idle")
	if got, want := repository.openTenantIDs(), []string{"tenant-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("open tenants = %v, want %v after the idle timeout", got, want)
	}

	// The maintenance pass reclaims whatever is idle from the earlier round.
	current = current.Add(2 * time.Hour)
	if err := repository.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}
	if got := repository.openHandleCount(); got != 0 {
		t.Fatalf("open handle count = %d, want 0 after Optimize reclaimed the idle handles", got)
	}
}

// TestSQLiteDirectoryQuotaRepositoryEvictionKeepsData proves a reclaimed handle
// was checkpointed and closed, and that reopening it reads the persisted counts.
func TestSQLiteDirectoryQuotaRepositoryEvictionKeepsData(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	dataPath := t.TempDir()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:                dataPath,
		OpenDatabaseIdleTimeout: time.Minute,
		now:                     func() time.Time { return current },
	})

	for i := 0; i < 4; i++ {
		if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/persisted"); err != nil {
			t.Fatalf("IncrementCount() error = %v", err)
		}
	}

	current = current.Add(time.Hour)
	if err := repository.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}
	if got := repository.openHandleCount(); got != 0 {
		t.Fatalf("open handle count = %d, want 0", got)
	}

	// A clean close removes the write-ahead log sidecar; its absence is the
	// observable proof that the handle really released the file.
	if _, err := os.Lstat(sqliteQuotaDatabasePath(dataPath, sqliteQuotaTenant) + quotaWalSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("write-ahead log still present after the eviction: %v", err)
	}

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/persisted")
	if quota.CurrentCount != 4 {
		t.Fatalf("CurrentCount = %d, want 4 after the handle was reclaimed", quota.CurrentCount)
	}
}

// TestSQLiteDirectoryQuotaRepositoryConcurrentTenantsWithHandleBound runs
// concurrent counters across more tenants than the bound allows, so handles are
// continuously reclaimed while operations are in flight. Every count must still
// be exact, which is the real invariant: an eviction never closes a handle that
// an operation is using.
func TestSQLiteDirectoryQuotaRepositoryConcurrentTenantsWithHandleBound(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{
		DataPath:         t.TempDir(),
		MaxOpenDatabases: 2,
	})

	tenants := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5", "tenant-6", "tenant-7", "tenant-8"}
	const incrementsPerTenant = 20

	var group sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, len(tenants))
	for index, tenantID := range tenants {
		group.Add(1)
		go func(slot int, tenant string) {
			defer group.Done()
			<-start
			for i := 0; i < incrementsPerTenant; i++ {
				if err := repository.IncrementCount(ctx, tenant, "/bounded"); err != nil {
					errs[slot] = err
					return
				}
			}
		}(index, tenantID)
	}
	close(start)
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("tenant %s: IncrementCount() error = %v", tenants[index], err)
		}
	}

	// The bound converges once the in-flight operations are done; the maintenance
	// pass is the design's deterministic point for it.
	if err := repository.Optimize(ctx); err != nil {
		t.Fatalf("Optimize() error = %v", err)
	}
	if got := repository.openHandleCount(); got > 2 {
		t.Fatalf("open handle count = %d, want at most 2", got)
	}

	for _, tenantID := range tenants {
		quota := mustGetOrCreate(t, repository, tenantID, "/bounded")
		if quota.CurrentCount != incrementsPerTenant {
			t.Fatalf("tenant %s count = %d, want %d", tenantID, quota.CurrentCount, incrementsPerTenant)
		}
	}
}
