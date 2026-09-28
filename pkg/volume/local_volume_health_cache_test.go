package volume

import (
	"context"
	"testing"
	"time"
)

// TestIsHealthyDoesNotCacheCancelledContext verifies that a verdict produced
// under a dead context never enters the health cache: the cancelled request
// says nothing about the volume, and caching it would make one cancelled write
// reject every write for the whole TTL.
func TestIsHealthyDoesNotCacheCancelledContext(t *testing.T) {
	volume, err := NewLocalFileSystemVolume(&LocalFileSystemVolumeOptions{
		VolumeID:  "v",
		MountPath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}
	concrete := volume.(*LocalFileSystemVolume)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if concrete.IsHealthy(cancelled) {
		t.Fatal("IsHealthy(cancelled) = true, want false")
	}
	if concrete.healthKnown {
		t.Fatal("the cancelled-context verdict leaked into the health cache")
	}

	// A healthy context immediately afterwards must observe a healthy volume;
	// with the poisoned-cache bug it reused the cached false verdict for the
	// whole TTL.
	if !concrete.IsHealthy(context.Background()) {
		t.Fatal("IsHealthy() = false right after a cancelled probe, want a fresh healthy verdict")
	}
}

// TestIsHealthyCachesRealFailureForTTL keeps the documented behaviour for real
// probe outcomes: a genuine failure is cached for the window.
func TestIsHealthyCachesRealFailureForTTL(t *testing.T) {
	volume, err := NewLocalFileSystemVolume(&LocalFileSystemVolumeOptions{
		VolumeID:  "v",
		MountPath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}
	concrete := volume.(*LocalFileSystemVolume)

	base := time.Now()
	current := base
	concrete.now = func() time.Time { return current }
	probes := 0
	concrete.probeHealth = func(context.Context) bool {
		probes++
		return false
	}

	if concrete.IsHealthy(context.Background()) {
		t.Fatal("IsHealthy() = true with a failing probe, want false")
	}

	// Advance less than the TTL: the cached failure must be reused without a
	// second probe.
	current = base.Add(DefaultHealthCheckCacheTTL / 2)
	if concrete.IsHealthy(context.Background()) {
		t.Fatal("IsHealthy() inside the cache window reused a healthy verdict, want the cached failure")
	}
	if probes != 1 {
		t.Fatalf("probe count = %d, want exactly 1 inside the cache window", probes)
	}
}
