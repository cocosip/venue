package volume

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// createWritePathVolume creates a volume with an explicit fsync setting so the
// write-path observations can be asserted for both durability modes.
func createWritePathVolume(t *testing.T, enableFsync bool) (*LocalFileSystemVolume, string) {
	t.Helper()

	mountDir := t.TempDir()
	created, err := NewLocalFileSystemVolume(&LocalFileSystemVolumeOptions{
		VolumeID:    "write-path-volume",
		MountPath:   mountDir,
		EnableFsync: enableFsync,
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}

	volume, ok := created.(*LocalFileSystemVolume)
	if !ok {
		t.Fatalf("expected *LocalFileSystemVolume, got %T", created)
	}

	return volume, mountDir
}

// collectVolumeEntries returns every path below root, relative to root and
// sorted, so a test can assert that a helper left nothing behind.
func collectVolumeEntries(t *testing.T, root string) []string {
	t.Helper()

	var entries []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		entries = append(entries, relative)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s failed: %v", root, err)
	}

	sort.Strings(entries)
	return entries
}

// failingReader fails a payload copy after the destination file was created, so
// the failure happens inside the write path instead of before it.
type failingReader struct{}

// Read always reports an injected failure.
func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("injected read failure")
}

// TestLocalFileSystemVolume_NewOptionalCapabilitiesAreDiscoverable verifies the
// three newly implemented capabilities are reachable through the
// core.StorageVolume value that callers actually hold.
func TestLocalFileSystemVolume_NewOptionalCapabilitiesAreDiscoverable(t *testing.T) {
	volume, _ := createTestVolume(t, 2)

	var storageVolume core.StorageVolume = volume

	if _, ok := storageVolume.(core.StorageVolumeHealthProbe); !ok {
		t.Error("LocalFileSystemVolume does not implement core.StorageVolumeHealthProbe")
	}
	if _, ok := storageVolume.(core.StorageVolumeWritePathWarmup); !ok {
		t.Error("LocalFileSystemVolume does not implement core.StorageVolumeWritePathWarmup")
	}
	if _, ok := storageVolume.(core.StorageVolumeWritePathDiagnostics); !ok {
		t.Error("LocalFileSystemVolume does not implement core.StorageVolumeWritePathDiagnostics")
	}
}

// TestProbeHealth_BypassesAndRefreshesCache is the regression test for the
// forced probe: it must run even while a warm cached outcome exists, and it must
// publish its outcome so a later IsHealthy reports it.
func TestProbeHealth_BypassesAndRefreshesCache(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	var probes atomic.Int64
	var outcome atomic.Bool
	volume.probeHealth = func(context.Context) bool {
		probes.Add(1)
		return outcome.Load()
	}

	if volume.IsHealthy(ctx) {
		t.Fatal("expected the first IsHealthy call to report the injected unsuccessful probe")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("health probes = %d, want 1 after the first IsHealthy call", got)
	}

	// The cached negative outcome is reused; no second probe runs.
	if volume.IsHealthy(ctx) {
		t.Error("expected the cached negative outcome to be reused")
	}
	if got := probes.Load(); got != 1 {
		t.Errorf("health probes = %d, want 1 while the cached outcome is warm", got)
	}

	// A forced probe must ignore the warm cache.
	outcome.Store(true)
	if !volume.ProbeHealth(ctx) {
		t.Fatal("expected ProbeHealth to bypass the cached negative outcome")
	}
	if got := probes.Load(); got != 2 {
		t.Errorf("health probes = %d, want 2 after a forced probe", got)
	}

	// The forced outcome must be the one IsHealthy now reports, without probing.
	if !volume.IsHealthy(ctx) {
		t.Error("expected IsHealthy to report the outcome ProbeHealth refreshed")
	}
	if got := probes.Load(); got != 2 {
		t.Errorf("health probes = %d, want 2 after IsHealthy reused the refreshed cache", got)
	}
}

// TestProbeHealth_ObservesBrokenAndRestoredVolume verifies the forced probe
// against the real filesystem instead of an injected seam.
func TestProbeHealth_ObservesBrokenAndRestoredVolume(t *testing.T) {
	ctx := context.Background()
	volume, mountDir := createTestVolume(t, 0)

	if !volume.IsHealthy(ctx) {
		t.Fatal("expected the fresh volume to be healthy")
	}

	if err := os.RemoveAll(mountDir); err != nil {
		t.Fatalf("failed to remove mount path: %v", err)
	}

	// The warm positive cache still hides the broken mount.
	if !volume.IsHealthy(ctx) {
		t.Error("expected the cached positive outcome to be reused before the forced probe")
	}
	if volume.ProbeHealth(ctx) {
		t.Error("expected the forced probe to observe the broken mount")
	}
	if volume.IsHealthy(ctx) {
		t.Error("expected IsHealthy to reuse the refreshed negative outcome")
	}

	if err := os.MkdirAll(mountDir, 0755); err != nil {
		t.Fatalf("failed to recreate mount path: %v", err)
	}
	if !volume.ProbeHealth(ctx) {
		t.Error("expected the forced probe to observe the restored mount")
	}
	if !volume.IsHealthy(ctx) {
		t.Error("expected IsHealthy to reuse the refreshed positive outcome")
	}
}

// TestProbeHealth_CancelledOrNilContextIsUnsuccessful verifies a cancelled (or
// absent) context is treated as an unsuccessful probe instead of panicking.
func TestProbeHealth_CancelledOrNilContextIsUnsuccessful(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "cancelled context", ctx: cancelled},
		{name: "nil context", ctx: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, _ := createTestVolume(t, 0)

			if volume.ProbeHealth(tc.ctx) {
				t.Error("expected an unusable context to report an unhealthy volume")
			}

			// The unsuccessful outcome is cached like any other probe outcome.
			if volume.IsHealthy(context.Background()) {
				t.Error("expected IsHealthy to reuse the refreshed unsuccessful outcome")
			}
		})
	}
}

// TestProbeHealth_DoesNotDisturbWritePathStatistics verifies the health probe is
// not counted as write-path work: it does not go through the volume write path.
func TestProbeHealth_DoesNotDisturbWritePathStatistics(t *testing.T) {
	ctx := context.Background()
	volume, _ := createTestVolume(t, 0)

	volume.ProbeHealth(ctx)
	volume.IsHealthy(ctx)

	if got := volume.WritePathStatistics(); got != (core.StorageVolumeWritePathStatistics{}) {
		t.Errorf("write path statistics = %+v, want the zero snapshot after health probes only", got)
	}
}

// TestWarmWritePathCache_LeavesNothingBehind verifies exactly one real write
// happens and neither outcome leaves the throwaway file in the volume.
func TestWarmWritePathCache_LeavesNothingBehind(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name        string
		enableFsync bool
	}{
		{name: "fsync disabled", enableFsync: false},
		{name: "fsync enabled", enableFsync: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, mountDir := createWritePathVolume(t, tc.enableFsync)

			if err := volume.WarmWritePathCache(ctx); err != nil {
				t.Fatalf("WarmWritePathCache() error = %v", err)
			}

			if entries := collectVolumeEntries(t, mountDir); len(entries) != 0 {
				t.Errorf("warmup left %v behind in the volume root, want nothing", entries)
			}

			stats := volume.WritePathStatistics()
			if stats.TotalWrites != 1 {
				t.Errorf("TotalWrites = %d, want 1 for one warmup write", stats.TotalWrites)
			}
			if stats.TotalBytes != int64(len(writeWarmupPayload)) {
				t.Errorf("TotalBytes = %d, want %d", stats.TotalBytes, len(writeWarmupPayload))
			}
			if stats.FailedWrites != 0 {
				t.Errorf("FailedWrites = %d, want 0", stats.FailedWrites)
			}
			if stats.CopyOperationCount != 1 {
				t.Errorf("CopyOperationCount = %d, want 1", stats.CopyOperationCount)
			}

			wantFsync := int64(0)
			if tc.enableFsync {
				wantFsync = 1
			}
			if stats.FsyncCount != wantFsync {
				t.Errorf("FsyncCount = %d, want %d", stats.FsyncCount, wantFsync)
			}
		})
	}
}

// TestWarmWritePathCache_ReportsWriteFailure verifies a failed warmup write is
// reported wrapped and is counted as a failed write.
func TestWarmWritePathCache_ReportsWriteFailure(t *testing.T) {
	ctx := context.Background()
	volume, mountDir := createWritePathVolume(t, false)

	// Replace the volume root with a regular file so the probe file cannot be
	// created: directory preparation fails inside the real write path.
	if err := os.RemoveAll(mountDir); err != nil {
		t.Fatalf("failed to remove mount path: %v", err)
	}
	if err := os.WriteFile(mountDir, []byte("not a directory"), 0644); err != nil {
		t.Fatalf("failed to replace mount path with a file: %v", err)
	}

	err := volume.WarmWritePathCache(ctx)
	if err == nil {
		t.Fatal("expected the failed warmup write to be reported")
	}
	if !strings.Contains(err.Error(), "warmup write failed") {
		t.Errorf("error = %v, want it to wrap the failed warmup write", err)
	}

	stats := volume.WritePathStatistics()
	if stats.FailedWrites != 1 {
		t.Errorf("FailedWrites = %d, want 1", stats.FailedWrites)
	}
	if stats.TotalWrites != 0 {
		t.Errorf("TotalWrites = %d, want 0 for a failed warmup", stats.TotalWrites)
	}
}

// TestWarmWritePathCache_ReportsCleanupFailure verifies both causes are reported
// when the failure leaves the probe file impossible to remove.
func TestWarmWritePathCache_ReportsCleanupFailure(t *testing.T) {
	ctx := context.Background()
	volume, mountDir := createWritePathVolume(t, false)

	// A non-empty directory cannot be replaced by the probe file and cannot be
	// removed with os.Remove, which is exactly the "cleanup failed" outcome.
	blockedName := "blocked-warmup"
	blockedPath := filepath.Join(mountDir, blockedName)
	if err := os.MkdirAll(blockedPath, 0755); err != nil {
		t.Fatalf("failed to create the blocking directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(blockedPath, "child"), []byte("x"), 0644); err != nil {
		t.Fatalf("failed to populate the blocking directory: %v", err)
	}

	volume.warmupName = func() string { return blockedName }

	err := volume.WarmWritePathCache(ctx)
	if err == nil {
		t.Fatal("expected the failed warmup to be reported")
	}
	if !strings.Contains(err.Error(), "warmup write failed") {
		t.Errorf("error = %v, want it to wrap the failed warmup write", err)
	}
	if !strings.Contains(err.Error(), "warmup cleanup failed") {
		t.Errorf("error = %v, want it to wrap the failed warmup cleanup", err)
	}
}

// TestWarmWritePathCache_CancelledContextTouchesNothing verifies a cancelled
// warmup is reported and performs no write at all.
func TestWarmWritePathCache_CancelledContextTouchesNothing(t *testing.T) {
	volume, mountDir := createWritePathVolume(t, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := volume.WarmWritePathCache(ctx)
	if err == nil {
		t.Fatal("expected the cancelled warmup to be reported")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}

	if entries := collectVolumeEntries(t, mountDir); len(entries) != 0 {
		t.Errorf("cancelled warmup created %v, want nothing", entries)
	}

	stats := volume.WritePathStatistics()
	if stats.TotalWrites != 0 || stats.FailedWrites != 0 {
		t.Errorf("statistics = %+v, want a skipped warmup to be uncounted", stats)
	}
}

// TestWritePathStatistics_CountsRealWritePathWork verifies the counters that a
// real successful write must move, including the fsync phase when enabled.
func TestWritePathStatistics_CountsRealWritePathWork(t *testing.T) {
	ctx := context.Background()
	volume, _ := createWritePathVolume(t, true)

	if got := volume.WritePathStatistics(); got != (core.StorageVolumeWritePathStatistics{}) {
		t.Fatalf("fresh volume statistics = %+v, want the zero snapshot", got)
	}

	payloads := []struct {
		relativePath string
		content      string
	}{
		{relativePath: "first.txt", content: "hello"},
		{relativePath: "nested/second.txt", content: "seventh"},
	}

	var wantBytes int64
	for _, payload := range payloads {
		written, err := volume.WriteFile(ctx, payload.relativePath, strings.NewReader(payload.content))
		if err != nil {
			t.Fatalf("WriteFile(%q) error = %v", payload.relativePath, err)
		}
		if written != int64(len(payload.content)) {
			t.Fatalf("WriteFile(%q) = %d bytes, want %d", payload.relativePath, written, len(payload.content))
		}
		wantBytes += written
	}

	got := volume.WritePathStatistics()
	if got.TotalWrites != int64(len(payloads)) {
		t.Errorf("TotalWrites = %d, want %d", got.TotalWrites, len(payloads))
	}
	if got.TotalBytes != wantBytes {
		t.Errorf("TotalBytes = %d, want %d", got.TotalBytes, wantBytes)
	}
	if got.FailedWrites != 0 {
		t.Errorf("FailedWrites = %d, want 0", got.FailedWrites)
	}
	if got.DirectoryPreparationCount != int64(len(payloads)) {
		t.Errorf("DirectoryPreparationCount = %d, want %d", got.DirectoryPreparationCount, len(payloads))
	}
	if got.CopyOperationCount != int64(len(payloads)) {
		t.Errorf("CopyOperationCount = %d, want %d", got.CopyOperationCount, len(payloads))
	}
	if got.FsyncCount != int64(len(payloads)) {
		t.Errorf("FsyncCount = %d, want %d while fsync is enabled", got.FsyncCount, len(payloads))
	}
	if got.DirectoryPreparationDuration < 0 || got.CopyDuration < 0 || got.FsyncDuration < 0 {
		t.Errorf("durations must not be negative: %+v", got)
	}
}

// TestWritePathStatistics_CountsFailedWrites verifies every failing write is
// counted once and contributes no success counters.
func TestWritePathStatistics_CountsFailedWrites(t *testing.T) {
	ctx := context.Background()
	volume, _ := createWritePathVolume(t, false)

	if _, err := volume.WriteFile(ctx, "../escape.txt", strings.NewReader("x")); err == nil {
		t.Fatal("expected the escaping path to be rejected")
	}

	stats := volume.WritePathStatistics()
	if stats.FailedWrites != 1 {
		t.Errorf("FailedWrites = %d, want 1 after a rejected path", stats.FailedWrites)
	}
	if stats.TotalWrites != 0 || stats.TotalBytes != 0 {
		t.Errorf("statistics = %+v, want no successful write to be counted", stats)
	}
	if stats.DirectoryPreparationCount != 0 || stats.CopyOperationCount != 0 {
		t.Errorf("statistics = %+v, want no phase observation for a rejected path", stats)
	}

	// A failure in the middle of the payload copy is a failed write too, and the
	// copy phase it did perform is still observed.
	if _, err := volume.WriteFile(ctx, "partial.txt", failingReader{}); err == nil {
		t.Fatal("expected the failing reader to be reported")
	}

	stats = volume.WritePathStatistics()
	if stats.FailedWrites != 2 {
		t.Errorf("FailedWrites = %d, want 2 after a failed copy", stats.FailedWrites)
	}
	if stats.TotalWrites != 0 || stats.TotalBytes != 0 {
		t.Errorf("statistics = %+v, want no successful write to be counted", stats)
	}
	if stats.CopyOperationCount != 1 {
		t.Errorf("CopyOperationCount = %d, want 1 for the attempted copy", stats.CopyOperationCount)
	}
	if stats.FsyncCount != 0 {
		t.Errorf("FsyncCount = %d, want 0 while fsync is disabled", stats.FsyncCount)
	}
}

// TestWritePathStatistics_ReturnsIndependentCopy verifies a snapshot copy can be
// mutated without changing the volume's counters.
func TestWritePathStatistics_ReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	volume, _ := createWritePathVolume(t, true)

	if _, err := volume.WriteFile(ctx, "payload.txt", strings.NewReader("abcdef")); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	before := volume.WritePathStatistics()
	if before.TotalWrites != 1 || before.TotalBytes != 6 {
		t.Fatalf("statistics = %+v, want one write of six bytes", before)
	}

	mutated := before
	mutated.TotalWrites = -1
	mutated.TotalBytes = -1
	mutated.FailedWrites = -1
	mutated.DirectoryPreparationCount = -1
	mutated.DirectoryPreparationDuration = -1
	mutated.CopyOperationCount = -1
	mutated.CopyDuration = -1
	mutated.FsyncCount = -1
	mutated.FsyncDuration = -1

	if mutated == before {
		t.Fatal("the mutation did not change the copy, so the test is vacuous")
	}

	after := volume.WritePathStatistics()
	if after != before {
		t.Errorf("statistics = %+v, want the unchanged %+v", after, before)
	}
}

// TestWriteCapabilities_ConcurrentProbeWriteAndStatistics exercises the probe,
// the write path, the warmup and the diagnostics together so the race detector
// can prove the counters need no lock and the health cache stays consistent.
func TestWriteCapabilities_ConcurrentProbeWriteAndStatistics(t *testing.T) {
	ctx := context.Background()
	volume, mountDir := createWritePathVolume(t, true)

	const (
		probers   = 4
		writers   = 4
		iteration = 8
		wantBytes = "payload"
	)

	var (
		wg            sync.WaitGroup
		successes     atomic.Int64
		warmupWrites  atomic.Int64
		probeHealthy  atomic.Int64
		probeUnhealth atomic.Int64
	)

	for i := 0; i < probers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iteration; j++ {
				if volume.ProbeHealth(ctx) {
					probeHealthy.Add(1)
				} else {
					probeUnhealth.Add(1)
				}
				_ = volume.IsHealthy(ctx)
			}
		}()
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < iteration; j++ {
				relativePath := fmt.Sprintf("concurrent-%d-%d.txt", worker, j)
				if _, err := volume.WriteFile(ctx, relativePath, strings.NewReader(wantBytes)); err != nil {
					t.Errorf("WriteFile(%q) error = %v", relativePath, err)
					continue
				}
				successes.Add(1)
				_ = volume.WritePathStatistics()
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iteration*2; j++ {
			_ = volume.WritePathStatistics()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := volume.WarmWritePathCache(ctx); err == nil {
			warmupWrites.Add(1)
		} else {
			t.Errorf("WarmWritePathCache() error = %v", err)
		}
	}()

	wg.Wait()

	if got := probeUnhealth.Load(); got != 0 {
		t.Errorf("unhealthy probe results = %d, want 0 for a writable volume", got)
	}
	if got := probeHealthy.Load(); got != probers*iteration {
		t.Errorf("healthy probe results = %d, want %d", got, probers*iteration)
	}

	stats := volume.WritePathStatistics()
	wantWrites := successes.Load() + warmupWrites.Load()
	if stats.TotalWrites != wantWrites {
		t.Errorf("TotalWrites = %d, want %d", stats.TotalWrites, wantWrites)
	}
	wantTotal := successes.Load()*int64(len(wantBytes)) + warmupWrites.Load()*int64(len(writeWarmupPayload))
	if stats.TotalBytes != wantTotal {
		t.Errorf("TotalBytes = %d, want %d", stats.TotalBytes, wantTotal)
	}
	if stats.FailedWrites != 0 {
		t.Errorf("FailedWrites = %d, want 0", stats.FailedWrites)
	}

	for _, entry := range collectVolumeEntries(t, mountDir) {
		if strings.HasPrefix(filepath.Base(entry), writeWarmupFilePrefix) {
			t.Errorf("concurrent warmup left %q behind", entry)
		}
	}
}
