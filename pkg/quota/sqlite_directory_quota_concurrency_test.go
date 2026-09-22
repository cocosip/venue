package quota

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestSQLiteDirectoryQuotaRepositoryDecrementFloorsAtZero pins the zero floor and
// the "nothing to decrement" no-op of a missing row.
func TestSQLiteDirectoryQuotaRepositoryDecrementFloorsAtZero(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	// A missing row must not be created by a decrement.
	if err := repository.DecrementCount(ctx, sqliteQuotaTenant, "/missing"); err != nil {
		t.Fatalf("DecrementCount() on a missing row error = %v", err)
	}
	quotas, err := repository.GetAll(ctx, sqliteQuotaTenant)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(quotas) != 0 {
		t.Fatalf("GetAll() = %+v, want no quotas after a decrement of a missing row", quotas)
	}

	mustGetOrCreate(t, repository, sqliteQuotaTenant, "/floored")
	for i := 0; i < 5; i++ {
		if err := repository.DecrementCount(ctx, sqliteQuotaTenant, "/floored"); err != nil {
			t.Fatalf("DecrementCount() error = %v", err)
		}
	}
	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/floored")
	if quota.CurrentCount != 0 {
		t.Fatalf("CurrentCount = %d, want 0: a decrement must never go negative", quota.CurrentCount)
	}

	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/floored"); err != nil {
		t.Fatalf("IncrementCount() error = %v", err)
	}
	if err := repository.DecrementCount(ctx, sqliteQuotaTenant, "/floored"); err != nil {
		t.Fatalf("DecrementCount() error = %v", err)
	}
	if quota = mustGetOrCreate(t, repository, sqliteQuotaTenant, "/floored"); quota.CurrentCount != 0 {
		t.Fatalf("CurrentCount = %d, want 0", quota.CurrentCount)
	}
}

// TestSQLiteDirectoryQuotaRepositoryIncrementCreatesRowWithUnitCount matches the
// Badger implementation's create-on-first-increment behavior.
func TestSQLiteDirectoryQuotaRepositoryIncrementCreatesRowWithUnitCount(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, "/created-by-increment"); err != nil {
		t.Fatalf("IncrementCount() error = %v", err)
	}

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, "/created-by-increment")
	if quota.CurrentCount != 1 {
		t.Fatalf("CurrentCount = %d, want 1", quota.CurrentCount)
	}
	if quota.MaxCount != 0 || quota.Enabled {
		t.Fatalf("quota = %+v, want the create defaults (unlimited, disabled)", quota)
	}
}

// TestSQLiteDirectoryQuotaRepositoryConcurrentIncrements proves no increment is
// lost when many goroutines share one directory. It is meaningful under -race.
func TestSQLiteDirectoryQuotaRepositoryConcurrentIncrements(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	const (
		workers               = 32
		incrementsPerWorker   = 25
		wantTotalCurrentCount = workers * incrementsPerWorker
	)
	const directoryPath = "/concurrent"

	var group sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, workers)
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(index int) {
			defer group.Done()
			<-start
			for i := 0; i < incrementsPerWorker; i++ {
				if err := repository.IncrementCount(ctx, sqliteQuotaTenant, directoryPath); err != nil {
					errs[index] = err
					return
				}
			}
		}(worker)
	}
	close(start)
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: IncrementCount() error = %v", index, err)
		}
	}

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, directoryPath)
	if quota.CurrentCount != wantTotalCurrentCount {
		t.Fatalf("CurrentCount = %d, want %d", quota.CurrentCount, wantTotalCurrentCount)
	}
}

// TestSQLiteDirectoryQuotaRepositoryConcurrentMixedCounters proves decrements can
// never drive a concurrent count below zero.
func TestSQLiteDirectoryQuotaRepositoryConcurrentMixedCounters(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})

	const (
		workers    = 16
		operations = 20
		directory  = "/mixed"
	)

	var group sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, 2*workers)
	for worker := 0; worker < workers; worker++ {
		group.Add(2)
		go func(index int) {
			defer group.Done()
			<-start
			for i := 0; i < operations; i++ {
				if err := repository.IncrementCount(ctx, sqliteQuotaTenant, directory); err != nil {
					errs[index] = err
					return
				}
			}
		}(worker)
		go func(index int) {
			defer group.Done()
			<-start
			for i := 0; i < operations; i++ {
				if err := repository.DecrementCount(ctx, sqliteQuotaTenant, directory); err != nil {
					errs[workers+index] = err
					return
				}
			}
		}(worker)
	}
	close(start)
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: counter error = %v", index, err)
		}
	}

	quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, directory)
	if quota.CurrentCount < 0 {
		t.Fatalf("CurrentCount = %d, want a non-negative count", quota.CurrentCount)
	}
	if maximum := workers * operations; quota.CurrentCount > maximum {
		t.Fatalf("CurrentCount = %d, want at most %d", quota.CurrentCount, maximum)
	}
}

// TestSQLiteDirectoryQuotaRepositoryConcurrentDecrementsFloorAtZero starts at 1
// and runs many concurrent decrements: the stored count must be exactly 0.
func TestSQLiteDirectoryQuotaRepositoryConcurrentDecrementsFloorAtZero(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})
	const directory = "/floor"

	mustGetOrCreate(t, repository, sqliteQuotaTenant, directory)
	if err := repository.IncrementCount(ctx, sqliteQuotaTenant, directory); err != nil {
		t.Fatalf("IncrementCount() error = %v", err)
	}

	const workers = 16

	var group sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, workers)
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(index int) {
			defer group.Done()
			<-start
			errs[index] = repository.DecrementCount(ctx, sqliteQuotaTenant, directory)
		}(worker)
	}
	close(start)
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: DecrementCount() error = %v", index, err)
		}
	}
	if quota := mustGetOrCreate(t, repository, sqliteQuotaTenant, directory); quota.CurrentCount != 0 {
		t.Fatalf("CurrentCount = %d, want 0", quota.CurrentCount)
	}
}

// TestSQLiteDirectoryQuotaRepositoryConcurrentGetOrCreate proves the upsert is
// idempotent: every concurrent first read observes the same stored row.
func TestSQLiteDirectoryQuotaRepositoryConcurrentGetOrCreate(t *testing.T) {
	ctx := context.Background()
	repository := newSQLiteQuotaRepository(t, &SQLiteDirectoryQuotaRepositoryOptions{DataPath: t.TempDir()})
	const directory = "/raced"

	const workers = 16

	var group sync.WaitGroup
	start := make(chan struct{})
	observations := make([]string, workers)
	errs := make([]error, workers)
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(index int) {
			defer group.Done()
			<-start
			quota, err := repository.GetOrCreate(ctx, sqliteQuotaTenant, directory)
			if err != nil {
				errs[index] = err
				return
			}
			observations[index] = fmt.Sprintf("%s|%d|%d|%t",
				quota.CreatedAt.UTC(), quota.CurrentCount, quota.MaxCount, quota.Enabled)
		}(worker)
	}
	close(start)
	group.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: GetOrCreate() error = %v", index, err)
		}
	}
	for index, observation := range observations {
		if observation != observations[0] {
			t.Fatalf("worker %d observed %q, want %q: concurrent GetOrCreate must not create two rows",
				index, observation, observations[0])
		}
	}
}
