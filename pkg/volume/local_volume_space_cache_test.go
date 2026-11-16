package volume

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestCapacityCacheReusesOneReading verifies that TotalCapacity and
// AvailableSpace share one cached filesystem reading, so volume selection does
// not pay a statfs round trip per write.
func TestCapacityCacheReusesOneReading(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	current := time.Unix(1_700_000_000, 0)
	vol.now = func() time.Time { return current }

	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		probes.Add(1)
		return 1_000, 400, nil
	}

	total, err := vol.TotalCapacity(context.Background())
	if err != nil {
		t.Fatalf("TotalCapacity() error = %v", err)
	}
	available, err := vol.AvailableSpace(context.Background())
	if err != nil {
		t.Fatalf("AvailableSpace() error = %v", err)
	}
	if total != 1_000 || available != 400 {
		t.Fatalf("capacity = (%d, %d), want (1000, 400)", total, available)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("space probes = %d, want 1 shared reading", got)
	}

	// Inside the cache window no new probe runs.
	current = current.Add(DefaultSpaceCacheTTL / 2)
	if _, err := vol.AvailableSpace(context.Background()); err != nil {
		t.Fatalf("AvailableSpace() error = %v", err)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("space probes = %d, want the cached reading", got)
	}
}

// TestCapacityCacheRefreshesInBackground verifies stale-while-revalidate: an
// expired reading is returned immediately and the refresh runs off the caller's
// goroutine.
func TestCapacityCacheRefreshesInBackground(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	current := time.Unix(1_700_000_000, 0)
	vol.now = func() time.Time { return current }

	var available atomic.Int64
	available.Store(400)
	refreshed := make(chan struct{}, 1)
	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		if probes.Add(1) > 1 {
			refreshed <- struct{}{}
		}
		return 1_000, available.Load(), nil
	}

	if _, err := vol.AvailableSpace(context.Background()); err != nil {
		t.Fatalf("AvailableSpace() error = %v", err)
	}
	available.Store(100)
	current = current.Add(DefaultSpaceCacheTTL + time.Second)

	// The stale reading is returned and the background probe is started.
	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want the stale (400, nil)", got, err)
	}
	select {
	case <-refreshed:
	case <-time.After(2 * time.Second):
		t.Fatal("background capacity refresh did not run")
	}

	// Wait for the refresh to publish before observing the new value.
	deadline := time.Now().Add(2 * time.Second)
	for {
		vol.spaceMu.Lock()
		refreshing := vol.spaceRefreshing
		vol.spaceMu.Unlock()
		if !refreshing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background capacity refresh did not publish")
		}
		time.Sleep(time.Millisecond)
	}

	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 100 {
		t.Fatalf("AvailableSpace() = (%d, %v), want the refreshed (100, nil)", got, err)
	}
}

// TestInvalidateSpaceCacheForcesFreshReading verifies the capability the storage
// pool calls after a write fails on a volume: the cached reading is dropped so
// the next selection observes the real free space instead of the stale value.
func TestInvalidateSpaceCacheForcesFreshReading(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	var available atomic.Int64
	available.Store(400)
	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		probes.Add(1)
		return 1_000, available.Load(), nil
	}

	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want (400, nil)", got, err)
	}

	// The warm cache hides the changed reading.
	available.Store(100)
	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want the cached (400, nil)", got, err)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("space probes = %d, want the cached reading", got)
	}

	vol.InvalidateSpaceCache()

	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 100 {
		t.Fatalf("AvailableSpace() after invalidation = (%d, %v), want a fresh (100, nil)", got, err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("space probes after invalidation = %d, want a fresh probe", got)
	}
}

// TestInvalidateSpaceCacheDropsInFlightRefresh is the regression test for a
// background refresh that republishes the reading the caller just invalidated:
// it started before the invalidation, so publishing it would restore exactly the
// stale value the pool asked to drop, and the next selection would reuse it
// instead of observing the real free space.
func TestInvalidateSpaceCacheDropsInFlightRefresh(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	current := time.Unix(1_700_000_000, 0)
	vol.now = func() time.Time { return current }

	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		switch probes.Add(1) {
		case 1:
			return 1_000, 400, nil
		case 2:
			close(refreshStarted)
			<-releaseRefresh
			// The reading the refresh observed before the invalidation.
			return 1_000, 100, nil
		default:
			return 1_000, 50, nil
		}
	}

	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want (400, nil)", got, err)
	}

	// Expire the reading so the next call returns it stale and refreshes in the
	// background.
	current = current.Add(DefaultSpaceCacheTTL + time.Second)
	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want the stale (400, nil)", got, err)
	}
	<-refreshStarted

	// The pool invalidates the cache while that refresh is still probing.
	vol.InvalidateSpaceCache()
	close(releaseRefresh)

	// Wait for the refresh to finish so the assertion below cannot race it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		vol.spaceMu.Lock()
		refreshing := vol.spaceRefreshing
		vol.spaceMu.Unlock()
		if !refreshing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background capacity refresh did not finish")
		}
		time.Sleep(time.Millisecond)
	}

	// The invalidated refresh must not have published: the next call has to
	// probe the filesystem again and report the fresh reading.
	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 50 {
		t.Fatalf("AvailableSpace() after invalidation = (%d, %v), want a fresh (50, nil)", got, err)
	}
	if got := probes.Load(); got != 3 {
		t.Fatalf("space probes = %d, want a fresh probe after the invalidated refresh", got)
	}
}

// TestFirstSpaceProbeDoesNotHoldLock is the regression test for a first capacity
// probe that runs inside spaceMu: every concurrent caller blocked on the lock
// behind one filesystem syscall, so a hung or networked mount stalled the whole
// write path. The probe now runs outside the lock and concurrent first callers
// wait for that probe, sharing one reading.
func TestFirstSpaceProbeDoesNotHoldLock(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		if probes.Add(1) == 1 {
			close(probeStarted)
			<-releaseProbe
		}
		return 1_000, 400, nil
	}

	type capacityResult struct {
		available int64
		err       error
	}
	first := make(chan capacityResult, 1)
	go func() {
		available, err := vol.AvailableSpace(context.Background())
		first <- capacityResult{available: available, err: err}
	}()
	<-probeStarted

	// The in-flight probe must not hold the cache mutex.
	if !vol.spaceMu.TryLock() {
		t.Fatal("spaceMu is held while the first capacity probe runs")
	}
	vol.spaceMu.Unlock()

	second := make(chan capacityResult, 1)
	go func() {
		available, err := vol.AvailableSpace(context.Background())
		second <- capacityResult{available: available, err: err}
	}()

	// The second caller has no cached reading either, so it waits for the shared
	// probe instead of returning or starting its own.
	select {
	case result := <-second:
		t.Fatalf("second first-call returned (%d, %v) before the shared probe finished", result.available, result.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseProbe)

	for name, results := range map[string]chan capacityResult{"first": first, "second": second} {
		select {
		case result := <-results:
			if result.err != nil || result.available != 400 {
				t.Errorf("%s caller = (%d, %v), want the shared (400, nil)", name, result.available, result.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s caller did not observe the shared probe", name)
		}
	}

	if got := probes.Load(); got != 1 {
		t.Errorf("space probes = %d, want 1 shared first reading", got)
	}
}

// TestCapacityCacheCanBeDisabled verifies the negative TTL escape hatch.
func TestCapacityCacheCanBeDisabled(t *testing.T) {
	vol, _ := createTestVolume(t, 0)
	vol.spaceCacheTTL = -1

	var probes atomic.Int64
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		probes.Add(1)
		return 1_000, 400, nil
	}

	if _, err := vol.AvailableSpace(context.Background()); err != nil {
		t.Fatalf("AvailableSpace() error = %v", err)
	}
	if _, err := vol.TotalCapacity(context.Background()); err != nil {
		t.Fatalf("TotalCapacity() error = %v", err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("space probes = %d, want 2 with caching disabled", got)
	}
}

// TestCapacityErrorsAreNotCached verifies a failed reading is retried instead of
// pinning a failure for the whole TTL.
func TestCapacityErrorsAreNotCached(t *testing.T) {
	vol, _ := createTestVolume(t, 0)

	calls := atomic.Int64{}
	vol.probeSpace = func(context.Context) (int64, int64, error) {
		if calls.Add(1) == 1 {
			return 0, 0, errors.New("disk probe failed")
		}
		return 1_000, 400, nil
	}

	if _, err := vol.AvailableSpace(context.Background()); err == nil {
		t.Fatal("AvailableSpace() error = nil, want the probe error")
	}
	if got, err := vol.AvailableSpace(context.Background()); err != nil || got != 400 {
		t.Fatalf("AvailableSpace() = (%d, %v), want (400, nil) on retry", got, err)
	}
}
