package cleanup

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// mergingCleanupService adds the combined volume traversal to the mock cleanup
// service, modelling the concrete cleanupService the background service uses in
// production.
type mergingCleanupService struct {
	*mockCleanupService
	sweepCalls atomic.Int32
	stats      core.CleanupStatistics
}

func (m *mergingCleanupService) sweepVolumes(context.Context) (*core.CleanupStatistics, error) {
	m.sweepCalls.Add(1)
	stats := m.stats
	return &stats, nil
}

// TestBackgroundCleanupService_UsesOneCombinedVolumeTraversal proves the wiring:
// when empty-directory cleanup and a due junk sweep both run, the background
// service asks the cleanup service for the single combined traversal instead of
// walking every volume once per sweep.
func TestBackgroundCleanupService_UsesOneCombinedVolumeTraversal(t *testing.T) {
	merging := &mergingCleanupService{
		mockCleanupService: &mockCleanupService{},
		stats: core.CleanupStatistics{
			EmptyDirectoriesRemoved: 3,
			JunkFilesRemoved:        4,
			SpaceFreed:              12,
		},
	}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          merging,
		InitialDelay:            time.Millisecond,
		CleanupInterval:         time.Hour,
		CleanupEmptyDirectories: true,
		CleanupJunkFiles:        true,
		JunkFileCleanupInterval: -time.Second, // sweep on every cycle
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for merging.sweepCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := merging.sweepCalls.Load(); got != 1 {
		t.Fatalf("sweepVolumes calls = %d, want 1", got)
	}
	if got := merging.cleanupEmptyDirsCalled.Load(); got != 0 {
		t.Errorf("CleanupEmptyDirectories calls = %d, want 0 when the combined traversal ran", got)
	}
	if got := merging.cleanupJunkFilesCalled.Load(); got != 0 {
		t.Errorf("CleanupJunkFiles calls = %d, want 0 when the combined traversal ran", got)
	}
	if svc.lastJunkFileCleanup.IsZero() {
		t.Error("lastJunkFileCleanup was not recorded after the combined sweep")
	}
}

// TestBackgroundCleanupService_CombinedTraversalFallsBackForPlainServices keeps
// the documented separate sweeps working for a CleanupService that does not
// implement the combined traversal (test doubles, third-party implementations).
func TestBackgroundCleanupService_CombinedTraversalFallsBackForPlainServices(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          mockSvc,
		InitialDelay:            time.Millisecond,
		CleanupInterval:         time.Hour,
		CleanupEmptyDirectories: true,
		CleanupJunkFiles:        true,
		JunkFileCleanupInterval: -time.Second,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for mockSvc.cleanupJunkFilesCalled.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := mockSvc.cleanupEmptyDirsCalled.Load(); got != 1 {
		t.Errorf("CleanupEmptyDirectories calls = %d, want 1", got)
	}
	if got := mockSvc.cleanupJunkFilesCalled.Load(); got != 1 {
		t.Errorf("CleanupJunkFiles calls = %d, want 1", got)
	}
}

// TestSweepVolumes_RemovesJunkAndEmptiedDirectoriesInOneTraversal verifies the
// combined traversal's outcome on a real volume: junk files are deleted during
// the shared walk and a directory that only held junk is reclaimed in the same
// pass, while a directory with a managed payload survives.
func TestSweepVolumes_RemovesJunkAndEmptiedDirectoriesInOneTraversal(t *testing.T) {
	ctx := context.Background()

	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)
	mount := volumes["test-volume"].MountPath()

	const junkContent = "junk"
	junkDirectory := filepath.Join("tenant-001", "junk-dir")
	writeTestFile(t, mount, filepath.Join(junkDirectory, "Thumbs.db"), junkContent)

	payloadDirectory := filepath.Join("tenant-001", "keep-dir")
	payloadPath := filepath.Join(payloadDirectory, managedPayloadKey+".txt")
	writeTestFile(t, mount, payloadPath, "payload")

	stats, err := service.sweepVolumes(ctx)
	if err != nil {
		t.Fatalf("sweepVolumes() error = %v", err)
	}

	if stats.JunkFilesRemoved != 1 {
		t.Errorf("JunkFilesRemoved = %d, want 1", stats.JunkFilesRemoved)
	}
	if wantFreed := int64(len(junkContent)); stats.SpaceFreed != wantFreed {
		t.Errorf("SpaceFreed = %d, want %d", stats.SpaceFreed, wantFreed)
	}
	if stats.EmptyDirectoriesRemoved != 1 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 1 (the junk-only directory must be reclaimed in the same pass)", stats.EmptyDirectoriesRemoved)
	}

	if _, statErr := os.Stat(filepath.Join(mount, junkDirectory)); !os.IsNotExist(statErr) {
		t.Errorf("junk-only directory %s still exists, stat error = %v", junkDirectory, statErr)
	}
	if _, statErr := os.Stat(filepath.Join(mount, payloadPath)); statErr != nil {
		t.Errorf("managed payload must survive, stat error = %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(mount, payloadDirectory)); statErr != nil {
		t.Errorf("payload directory must survive, stat error = %v", statErr)
	}

	// The combined sweep counts toward the lifetime totals exactly once.
	cumulative := service.CumulativeStatistics()
	if cumulative.JunkFilesRemoved != 1 || cumulative.EmptyDirectoriesRemoved != 1 {
		t.Errorf("cumulative = %+v, want one junk file and one directory", cumulative)
	}
}

// TestSweepVolumes_HonoursContextCancellation verifies a cancelled combined sweep
// reports the cancellation instead of a successful empty sweep.
func TestSweepVolumes_HonoursContextCancellation(t *testing.T) {
	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)
	writeTestFile(t, volumes["test-volume"].MountPath(), filepath.Join("tenant-001", "Thumbs.db"), "junk")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := service.sweepVolumes(ctx)
	if err == nil {
		t.Fatal("sweepVolumes() error = nil, want context.Canceled")
	}
	if stats.JunkFilesRemoved != 0 {
		t.Errorf("JunkFilesRemoved = %d, want 0 for a cancelled sweep", stats.JunkFilesRemoved)
	}
}
