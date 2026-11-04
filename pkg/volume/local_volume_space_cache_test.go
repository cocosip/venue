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
