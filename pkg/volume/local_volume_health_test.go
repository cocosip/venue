package volume

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIsHealthy_CachesProbeResults is the regression test for the missing
// health-probe cache: a probe result must be reused for the TTL window instead
// of writing and deleting a probe file on every call. Failed probes are cached
// as well.
func TestIsHealthy_CachesProbeResults(t *testing.T) {
	ctx := context.Background()

	t.Run("healthy result is cached", func(t *testing.T) {
		vol, mountDir := createTestVolume(t, 0)

		if !vol.IsHealthy(ctx) {
			t.Fatal("expected the fresh volume to be healthy")
		}

		// Break the volume after the successful probe.
		if err := os.RemoveAll(mountDir); err != nil {
			t.Fatalf("failed to remove mount path: %v", err)
		}

		if !vol.IsHealthy(ctx) {
			t.Error("expected the successful probe result to be reused within the TTL window")
		}
	})

	t.Run("failed result is cached", func(t *testing.T) {
		vol, mountDir := createTestVolume(t, 0)

		if err := os.RemoveAll(mountDir); err != nil {
			t.Fatalf("failed to remove mount path: %v", err)
		}

		if vol.IsHealthy(ctx) {
			t.Fatal("expected the volume to be unhealthy while the mount path is missing")
		}

		// Restore the volume after the failed probe.
		if err := os.MkdirAll(mountDir, 0755); err != nil {
			t.Fatalf("failed to recreate mount path: %v", err)
		}

		if vol.IsHealthy(ctx) {
			t.Error("expected the failed probe result to be reused within the TTL window")
		}
	})
}

// TestIsHealthy_ProbeCacheExpires verifies TTL expiry with an injected clock.
func TestIsHealthy_ProbeCacheExpires(t *testing.T) {
	ctx := context.Background()

	vol, mountDir := createTestVolume(t, 0)

	current := time.Unix(1_700_000_000, 0)
	vol.now = func() time.Time { return current }

	if !vol.IsHealthy(ctx) {
		t.Fatal("expected the fresh volume to be healthy")
	}

	if err := os.RemoveAll(mountDir); err != nil {
		t.Fatalf("failed to remove mount path: %v", err)
	}

	// Still inside the TTL window: the cached (healthy) result is reused.
	current = current.Add(DefaultHealthCheckCacheTTL - time.Second)
	if !vol.IsHealthy(ctx) {
		t.Error("expected the cached result to be reused before the TTL expires")
	}

	// Past the TTL window: the probe runs again and observes the broken mount.
	current = current.Add(2 * time.Second)
	if vol.IsHealthy(ctx) {
		t.Error("expected a fresh probe after the TTL expired")
	}

	// The fresh failed result is cached in turn.
	if err := os.MkdirAll(mountDir, 0755); err != nil {
		t.Fatalf("failed to recreate mount path: %v", err)
	}
	current = current.Add(DefaultHealthCheckCacheTTL - time.Second)
	if vol.IsHealthy(ctx) {
		t.Error("expected the failed probe result to stay cached before the TTL expires")
	}
}

// TestIsHealthy_ConcurrentCallersShareOneProbe verifies singleflight-style
// de-duplication: concurrent callers must not run one probe each.
func TestIsHealthy_ConcurrentCallersShareOneProbe(t *testing.T) {
	ctx := context.Background()

	vol, _ := createTestVolume(t, 0)

	var probes atomic.Int64
	vol.probeHealth = func(context.Context) bool {
		probes.Add(1)
		time.Sleep(10 * time.Millisecond)
		return true
	}

	const callers = 16
	results := make([]bool, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = vol.IsHealthy(ctx)
		}(i)
	}
	wg.Wait()

	if got := probes.Load(); got != 1 {
		t.Errorf("health probes = %d, want 1 (concurrent callers must share one probe)", got)
	}
	for i, healthy := range results {
		if !healthy {
			t.Errorf("caller %d observed an unhealthy volume", i)
		}
	}
}

// TestIsHealthy_CachingCanBeDisabled verifies the negative TTL escape hatch.
func TestIsHealthy_CachingCanBeDisabled(t *testing.T) {
	ctx := context.Background()

	vol, _ := createTestVolume(t, 0)
	vol.healthCacheTTL = -1

	var probes atomic.Int64
	vol.probeHealth = func(context.Context) bool {
		probes.Add(1)
		return true
	}

	if !vol.IsHealthy(ctx) {
		t.Fatal("expected the volume to stay healthy")
	}
	if !vol.IsHealthy(ctx) {
		t.Fatal("expected the volume to stay healthy on the second probe")
	}
	if got := probes.Load(); got != 2 {
		t.Errorf("health probes = %d, want 2 when caching is disabled", got)
	}
}

// TestNewLocalFileSystemVolume_HealthCacheTTLDefaults verifies the option
// default and override behavior.
func TestNewLocalFileSystemVolume_HealthCacheTTLDefaults(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{name: "zero selects the default", ttl: 0, want: DefaultHealthCheckCacheTTL},
		{name: "positive overrides", ttl: 5 * time.Second, want: 5 * time.Second},
		{name: "negative disables caching", ttl: -1, want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			created, err := NewLocalFileSystemVolume(&LocalFileSystemVolumeOptions{
				VolumeID:            "test-volume",
				MountPath:           t.TempDir(),
				HealthCheckCacheTTL: tc.ttl,
			})
			if err != nil {
				t.Fatalf("failed to create volume: %v", err)
			}

			vol, ok := created.(*LocalFileSystemVolume)
			if !ok {
				t.Fatalf("expected *LocalFileSystemVolume, got %T", created)
			}
			if vol.healthCacheTTL != tc.want {
				t.Errorf("healthCacheTTL = %v, want %v", vol.healthCacheTTL, tc.want)
			}
		})
	}
}
