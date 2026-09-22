package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// stabilitySpy replaces the delayed stability wait so a test controls exactly
// when the second probe happens (and what the file looks like at that moment)
// instead of depending on wall-clock timing.
type stabilitySpy struct {
	mu    sync.Mutex
	calls int
	delay time.Duration
	probe func() error
}

func (s *stabilitySpy) wait(_ context.Context, delay time.Duration) error {
	s.mu.Lock()
	s.calls++
	s.delay = delay
	probe := s.probe
	s.mu.Unlock()

	if probe != nil {
		return probe()
	}

	return nil
}

func (s *stabilitySpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

func (s *stabilitySpy) observedDelay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.delay
}

// youngFileAge is the age given to a stability-test candidate: old enough to
// pass any MinFileAge gate, young enough that the probe is not skipped by age.
const youngFileAge = time.Second

// writeYoungFile writes content and backdates the modification time by
// youngFileAge, so the candidate passes the MinFileAge gate while staying well
// below the stability skip age.
func writeYoungFile(t *testing.T, path string, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	modified := time.Now().Add(-youngFileAge)
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

// --- delayed second probe --------------------------------------------------

func TestScanNow_SkipsFileThatChangesDuringStabilityProbe(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	target := filepath.Join(watchPath, "growing.csv")
	writeYoungFile(t, target, "first")

	// The writer appends while the watcher waits: the second probe must see the
	// changed size and modification time.
	spy := &stabilitySpy{probe: func() error {
		return os.WriteFile(target, []byte("first-and-a-lot-more"), 0o644)
	}}
	w.stabilityWait = spy.wait

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FileStabilityCheckDelay = 50 * time.Millisecond
		config.SkipStabilityCheckAfterAge = time.Hour
	}))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesImported != 0 {
		t.Fatalf("FilesImported = %d, want 0 for a file still being written", result.FilesImported)
	}
	if result.FilesSkipped != 1 {
		t.Fatalf("FilesSkipped = %d, want 1 for a file that changed during the probe", result.FilesSkipped)
	}
	if result.FilesFailed != 0 {
		t.Fatalf("FilesFailed = %d, want 0: an unstable file is skipped, not failed", result.FilesFailed)
	}
	if got := pool.writeCount(); got != 0 {
		t.Fatalf("WriteFile calls = %d, want 0 for an unstable file", got)
	}
	if got := spy.callCount(); got != 1 {
		t.Fatalf("stability probes = %d, want exactly 1", got)
	}
	if got := spy.observedDelay(); got != 50*time.Millisecond {
		t.Fatalf("stability delay = %v, want the configured 50ms", got)
	}
}

func TestScanNow_ImportsFileThatStaysStableDuringProbe(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	writeYoungFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	spy := &stabilitySpy{}
	w.stabilityWait = spy.wait

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FileStabilityCheckDelay = 25 * time.Millisecond
		config.SkipStabilityCheckAfterAge = time.Hour
	}))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1 for a stable file (%v)", result.FilesImported, result.Errors)
	}
	if got := pool.writeCount(); got != 1 {
		t.Fatalf("WriteFile calls = %d, want 1", got)
	}
	if got := spy.callCount(); got != 1 {
		t.Fatalf("stability probes = %d, want exactly 1", got)
	}
}

func TestScanNow_StabilityProbeObservesDisabledDelay(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	writeYoungFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	spy := &stabilitySpy{}
	w.stabilityWait = spy.wait

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.MinFileAge = time.Nanosecond
		// A non-positive delay disables the delayed second probe entirely.
		config.FileStabilityCheckDelay = 0
		config.SkipStabilityCheckAfterAge = time.Hour
	}))

	if _, err := w.ScanNow(context.Background(), "w1"); err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}

	if got := spy.callCount(); got != 0 {
		t.Fatalf("stability probes = %d, want 0 when the delay disables the probe", got)
	}
}

// TestConfirmFileStable_SkipAgeShortCircuitsProbe pins the age shortcut and the
// documented default for a zero skip age.
func TestConfirmFileStable_SkipAgeShortCircuitsProbe(t *testing.T) {
	tests := []struct {
		name       string
		delay      time.Duration
		fileAge    time.Duration
		skipAge    time.Duration
		wantProbes int
	}{
		{
			name: "zero skip age defaults to one minute for a young file", delay: time.Second,
			fileAge: 30 * time.Second, skipAge: 0, wantProbes: 1,
		},
		{
			name: "zero skip age defaults to one minute for an old file", delay: time.Second,
			fileAge: 2 * time.Minute, skipAge: 0, wantProbes: 0,
		},
		{
			name: "negative skip age always probes", delay: time.Second,
			fileAge: time.Hour, skipAge: -time.Second, wantProbes: 1,
		},
		{
			name: "file below the skip age is probed", delay: time.Second,
			fileAge: time.Second, skipAge: time.Minute, wantProbes: 1,
		},
		{
			name: "file above the skip age is not probed", delay: time.Second,
			fileAge: time.Hour, skipAge: time.Minute, wantProbes: 0,
		},
		{
			name: "non-positive delay disables the probe", delay: 0,
			fileAge: time.Second, skipAge: time.Minute, wantProbes: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, _ := newTestWatcher(t)

			current := time.Unix(1_700_000_000, 0)
			w.now = func() time.Time { return current }

			target := filepath.Join(t.TempDir(), "candidate.csv")
			writeYoungFile(t, target, "payload")

			modified := current.Add(-test.fileAge)
			if err := os.Chtimes(target, modified, modified); err != nil {
				t.Fatalf("chtimes: %v", err)
			}

			spy := &stabilitySpy{}
			w.stabilityWait = spy.wait

			stable, err := w.confirmFileStable(context.Background(), target, &core.FileWatcherConfiguration{
				FileStabilityCheckDelay:    test.delay,
				SkipStabilityCheckAfterAge: test.skipAge,
			})
			if err != nil {
				t.Fatalf("confirmFileStable() error = %v", err)
			}
			if !stable {
				t.Fatalf("confirmFileStable() = false, want true for an unchanged file")
			}
			if got := spy.callCount(); got != test.wantProbes {
				t.Fatalf("stability probes = %d, want %d", got, test.wantProbes)
			}
		})
	}
}

func TestConfirmFileStable_SkipsFileThatDisappearsDuringProbe(t *testing.T) {
	w, _ := newTestWatcher(t)

	target := filepath.Join(t.TempDir(), "vanishing.csv")
	writeYoungFile(t, target, "payload")

	spy := &stabilitySpy{probe: func() error {
		return os.Remove(target)
	}}
	w.stabilityWait = spy.wait

	stable, err := w.confirmFileStable(context.Background(), target, &core.FileWatcherConfiguration{
		FileStabilityCheckDelay:    time.Millisecond,
		SkipStabilityCheckAfterAge: time.Hour,
	})
	if err != nil {
		t.Fatalf("confirmFileStable() error = %v, want the vanished candidate to be skipped instead", err)
	}
	if stable {
		t.Fatalf("confirmFileStable() = true, want false for a candidate that disappeared")
	}
}

func TestScanNow_StabilityProbeHonoursScanCancellation(t *testing.T) {
	w, pool := newTestWatcher(t)

	watchPath := t.TempDir()
	writeYoungFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the scan from inside the wait, exactly as a stopping service does.
	w.stabilityWait = func(context.Context, time.Duration) error {
		cancel()

		return context.Canceled
	}

	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.FileStabilityCheckDelay = time.Hour
		config.SkipStabilityCheckAfterAge = time.Hour
	}))

	result, err := w.ScanNow(ctx, "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 0 {
		t.Fatalf("FilesImported = %d, want 0 after the scan was cancelled", result.FilesImported)
	}
	if got := pool.writeCount(); got != 0 {
		t.Fatalf("WriteFile calls = %d, want 0 after the scan was cancelled", got)
	}
}

func TestWaitForStabilityDelay_ObservesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()

	err := waitForStabilityDelay(ctx, time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForStabilityDelay() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("waitForStabilityDelay() waited %v for a cancelled scan", elapsed)
	}
}
