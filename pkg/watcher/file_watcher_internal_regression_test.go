package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// Regression tests whose call sites use internal signatures: discovery result
// plumbing, the cheap schedule snapshot, and the cancellable move-name probe.
// Unreadable discovery entries are reported

func TestDiscoverFilesReportsUnreadableEntries(t *testing.T) {
	w, _ := newTestWatcher(t)

	result := &core.FileWatcherScanResult{Errors: make([]string, 0)}
	missing := filepath.Join(t.TempDir(), "gone")

	files, err := w.discoverFiles(context.Background(), missing, &core.FileWatcherConfiguration{
		WatcherID:             "w1",
		IncludeSubdirectories: true,
	}, result)
	if err != nil {
		t.Fatalf("discoverFiles() error = %v, want the unreadable entry reported on the scan result", err)
	}
	if len(files) != 0 {
		t.Fatalf("discoverFiles() = %v, want no files", namesOf(files))
	}
	if len(result.Errors) == 0 {
		t.Fatalf("an unreadable directory was silently swallowed")
	}
	for _, message := range result.Errors {
		if strings.Contains(message, missing) {
			t.Fatalf("discovery error %q leaked the physical path", message)
		}
	}
}

// The dispatch path uses the cheap schedule snapshot

// snapshotCountingWatcher proves which accessor the dispatch path uses.
type snapshotCountingWatcher struct {
	*stubFileWatcher

	mu            sync.Mutex
	getAllCalls   int
	snapshotCalls int
}

func (s *snapshotCountingWatcher) GetAllWatchers(ctx context.Context) ([]*core.FileWatcherConfiguration, error) {
	s.mu.Lock()
	s.getAllCalls++
	s.mu.Unlock()

	return s.stubFileWatcher.GetAllWatchers(ctx)
}

func (s *snapshotCountingWatcher) scheduleSnapshot() []watcherScheduleEntry {
	s.mu.Lock()
	s.snapshotCalls++
	s.mu.Unlock()

	return []watcherScheduleEntry{{WatcherID: "w1", Enabled: true, PollingInterval: time.Second}}
}

func (s *snapshotCountingWatcher) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.getAllCalls, s.snapshotCalls
}

func TestDispatchScanCycleUsesScheduleSnapshot(t *testing.T) {
	watcher := &snapshotCountingWatcher{stubFileWatcher: newStubFileWatcher()}

	service := newBackgroundService(t, watcher, nil)
	service.dispatchScanCycleAt(context.Background(), time.Unix(0, 0))
	service.wg.Wait()

	getAllCalls, snapshotCalls := watcher.counts()
	if getAllCalls != 0 {
		t.Fatalf("GetAllWatchers calls = %d, want the lightweight snapshot instead", getAllCalls)
	}
	if snapshotCalls == 0 {
		t.Fatalf("scheduleSnapshot was never used by the dispatch path")
	}
	if scanned := watcher.scannedIDs(); len(scanned) != 1 || scanned[0] != "w1" {
		t.Fatalf("scanned = %v, want [w1]", scanned)
	}
}

// The move-name probe is cancellable and bounded

func TestResolveMoveTargetPathHonoursCancellation(t *testing.T) {
	targetDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := resolveMoveTargetPath(ctx, filepath.Join(targetDir, "source.csv"), targetDir); !errors.Is(err, context.Canceled) {
		t.Fatalf("resolveMoveTargetPath() error = %v, want context.Canceled", err)
	}
}

func TestResolveMoveTargetPathStopsAtAttemptCap(t *testing.T) {
	watchDir := t.TempDir()
	targetDir := t.TempDir()

	source := filepath.Join(watchDir, "report.csv")

	// Fill every candidate name the probe may try.
	if err := os.WriteFile(filepath.Join(targetDir, "report.csv"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write base candidate: %v", err)
	}
	for counter := 1; counter < maxMoveTargetNameAttempts; counter++ {
		name := fmt.Sprintf("report_%d.csv", counter)
		if err := os.WriteFile(filepath.Join(targetDir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write candidate %s: %v", name, err)
		}
	}

	if _, err := resolveMoveTargetPath(context.Background(), source, targetDir); err == nil {
		t.Fatalf("resolveMoveTargetPath() error = nil, want an error after %d collisions", maxMoveTargetNameAttempts)
	}
}
