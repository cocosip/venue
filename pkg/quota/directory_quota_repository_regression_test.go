package quota

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestBadgerDirectoryQuotaRepository_CloseConcurrentWithGCRun is the regression
// test for the shutdown deadlock: Close() must not hold mu while it waits for
// the GC goroutine, because the GC goroutine's Optimize() takes mu.RLock().
//
// The deadlock needs the GC goroutine to be waiting on mu.RLock() while Close()
// holds mu.Lock(). Tests can only widen that window, so the whole scenario is
// repeated until either the deadlock is observed or the attempt budget is
// exhausted; the fixed implementation must survive every repetition.
func TestBadgerDirectoryQuotaRepository_CloseConcurrentWithGCRun(t *testing.T) {
	const (
		attemptsThisMany = 60
		workers          = 32
		gcTicker         = time.Nanosecond
		watchdog         = 5 * time.Second
		seedPaths        = 3
	)

	for attempt := 0; attempt < attemptsThisMany; attempt++ {
		func() {
			ctx := context.Background()

			tmpDir, err := os.MkdirTemp("", "quota-close-race-*")
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer func() { _ = os.RemoveAll(tmpDir) }()

			repo, err := NewBadgerDirectoryQuotaRepository(&BadgerDirectoryQuotaRepositoryOptions{
				DataPath:       tmpDir,
				GCInterval:     gcTicker,
				GCDiscardRatio: 0.5,
			})
			if err != nil {
				t.Fatalf("NewBadgerDirectoryQuotaRepository() error = %v", err)
			}

			concrete, ok := repo.(*badgerDirectoryQuotaRepository)
			if !ok {
				t.Fatalf("unexpected repository type %T", repo)
			}

			for i := 0; i < seedPaths; i++ {
				path := fmt.Sprintf("/path/%d", i)
				if _, err := concrete.GetOrCreate(ctx, "tenant1", path); err != nil {
					t.Fatalf("GetOrCreate() error = %v", err)
				}
				if err := concrete.IncrementCount(ctx, "tenant1", path); err != nil {
					t.Fatalf("IncrementCount() error = %v", err)
				}
			}

			// Keep concurrent Optimize callers inside the critical section while
			// the GC goroutine ticker fires and Close() runs.
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for {
						if err := concrete.Optimize(ctx); err != nil {
							return
						}
					}
				}()
			}
			close(start)

			// Give the GC ticker a chance to enter Optimize.
			time.Sleep(time.Millisecond)

			closed := make(chan error, 1)
			go func() { closed <- concrete.Close() }()

			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("attempt %d: Close() error = %v", attempt, err)
				}
			case <-time.After(watchdog):
				t.Fatalf(
					"attempt %d: Close() did not return within %s while GC was running: shutdown deadlock",
					attempt, watchdog,
				)
			}

			wg.Wait()

			if err := concrete.Close(); err != nil {
				t.Fatalf("attempt %d: second Close() error = %v", attempt, err)
			}
			if _, err := concrete.GetOrCreate(ctx, "tenant1", "/path/0"); err == nil {
				t.Fatalf("attempt %d: GetOrCreate() on a closed repository must fail", attempt)
			}
		}()
	}
}
