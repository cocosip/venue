package volume

import (
	"context"
	"sync/atomic"
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

// TestFirstHealthProbeDoesNotHoldLock is the regression test for a first health
// probe that runs inside healthMu: every concurrent writer blocked on the lock
// behind one filesystem syscall, so a hung or networked mount stalled the whole
// write path. The probe now runs outside the lock and concurrent first callers
// wait for that probe, sharing one verdict.
func TestFirstHealthProbeDoesNotHoldLock(t *testing.T) {
	volume, err := NewLocalFileSystemVolume(&LocalFileSystemVolumeOptions{
		VolumeID:  "v",
		MountPath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}
	concrete := volume.(*LocalFileSystemVolume)

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	probes := atomic.Int64{}
	concrete.probeHealth = func(context.Context) bool {
		if probes.Add(1) == 1 {
			close(probeStarted)
			<-releaseProbe
		}
		return true
	}

	first := make(chan bool, 1)
	go func() { first <- concrete.IsHealthy(context.Background()) }()
	<-probeStarted

	// The in-flight probe must not hold the health mutex.
	if !concrete.healthMu.TryLock() {
		t.Fatal("healthMu is held while the first health probe runs")
	}
	concrete.healthMu.Unlock()

	second := make(chan bool, 1)
	go func() { second <- concrete.IsHealthy(context.Background()) }()

	// The second caller has no cached verdict either, so it waits for the shared
	// probe instead of returning or starting its own.
	select {
	case healthy := <-second:
		t.Fatalf("second first-call returned %t before the shared probe finished", healthy)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseProbe)

	for name, result := range map[string]chan bool{"first": first, "second": second} {
		select {
		case healthy := <-result:
			if !healthy {
				t.Errorf("%s caller = false, want the shared healthy verdict", name)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s caller did not observe the shared probe", name)
		}
	}

	if got := probes.Load(); got != 1 {
		t.Errorf("health probes = %d, want 1 shared first verdict", got)
	}
}
