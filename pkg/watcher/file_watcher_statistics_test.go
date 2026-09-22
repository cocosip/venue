package watcher

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// watcherRecorderCall is one captured Record call of watcherRecorder.
type watcherRecorderCall struct {
	name       string
	value      int64
	timestamp  time.Time
	dimensions map[string]string
}

// watcherRecorder is a minimal core.StatisticsRecorder that captures every
// delta the file watcher reports.
type watcherRecorder struct {
	mu    sync.Mutex
	calls []watcherRecorderCall
}

func (r *watcherRecorder) Record(name string, value int64, timestamp time.Time, dimensions map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, watcherRecorderCall{
		name:       name,
		value:      value,
		timestamp:  timestamp,
		dimensions: dimensions,
	})
}

func (r *watcherRecorder) recorded() []watcherRecorderCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]watcherRecorderCall, len(r.calls))
	copy(out, r.calls)

	return out
}

func (r *watcherRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.calls)
}

// TestScanNowRecordsStatistics asserts the exact statistics tuples of one
// manual scan.
func TestScanNowRecordsStatistics(t *testing.T) {
	recorder := &watcherRecorder{}

	w, pool := newTestWatcherWithOptions(t, func(opts *FileWatcherOptions) {
		opts.StatisticsRecorder = recorder
	})

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	result, err := w.ScanNow(context.Background(), "w1")
	if err != nil {
		t.Fatalf("ScanNow() error = %v", err)
	}
	if result.FilesImported != 1 {
		t.Fatalf("FilesImported = %d, want 1", result.FilesImported)
	}
	if pool.writeCount() != 1 {
		t.Fatalf("storage writes = %d, want 1", pool.writeCount())
	}

	want := []watcherRecorderCall{
		{name: core.StatisticWatcherScanCount, value: 1},
		{name: core.StatisticWatcherFilesDiscovered, value: int64(result.FilesDiscovered)},
		{name: core.StatisticWatcherFilesImported, value: int64(result.FilesImported)},
		{name: core.StatisticWatcherFilesSkipped, value: int64(result.FilesSkipped)},
		{name: core.StatisticWatcherFilesFailed, value: int64(result.FilesFailed)},
		{name: core.StatisticWatcherBytesImported, value: result.BytesImported},
	}

	got := recorder.recorded()
	if len(got) != len(want) {
		t.Fatalf("recorded %d deltas, want %d: %+v", len(got), len(want), got)
	}

	for i, expected := range want {
		assertWatcherRecorderCall(t, i, got[i], expected, "w1")
	}
}

// TestBackgroundServiceScanRecordsStatisticsOnce asserts that a background
// service scan is recorded by the scan path exactly once, with no double
// counting from the service itself.
func TestBackgroundServiceScanRecordsStatisticsOnce(t *testing.T) {
	recorder := &watcherRecorder{}

	w, _ := newTestWatcherWithOptions(t, func(opts *FileWatcherOptions) {
		opts.StatisticsRecorder = recorder
	})

	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	registerWatcher(t, w, newConfig("w1", watchPath, nil))

	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          w,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: t.TempDir(),
		InitialDelay:         time.Millisecond,
		ServiceOptions: core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  time.Hour,
			MinimumPollingInterval:  time.Millisecond,
			MaximumPollingInterval:  time.Hour,
			DisabledCheckInterval:   time.Hour,
			MaxParallelWatcherScans: 1,
		},
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(func() {
		if service.IsRunning() {
			if err := service.Stop(); err != nil {
				t.Errorf("Stop() error = %v", err)
			}
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for recorder.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if err := service.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	// The service schedules the next scan an hour out, so exactly one scan
	// happened and it must have produced exactly one result tuple set: six
	// deltas, not twelve.
	want := []watcherRecorderCall{
		{name: core.StatisticWatcherScanCount, value: 1},
		{name: core.StatisticWatcherFilesDiscovered, value: 1},
		{name: core.StatisticWatcherFilesImported, value: 1},
		{name: core.StatisticWatcherFilesSkipped, value: 0},
		{name: core.StatisticWatcherFilesFailed, value: 0},
		{name: core.StatisticWatcherBytesImported, value: int64(len("payload"))},
	}

	got := recorder.recorded()
	if len(got) != len(want) {
		t.Fatalf("recorded %d deltas, want %d (double counting?): %+v", len(got), len(want), got)
	}

	for i, expected := range want {
		assertWatcherRecorderCall(t, i, got[i], expected, "w1")
	}
}

// TestScanAllWatchersRecordsStatisticsOnce asserts that the aggregate scan path
// records each watcher's scan exactly once.
func TestScanAllWatchersRecordsStatisticsOnce(t *testing.T) {
	recorder := &watcherRecorder{}

	w, _ := newTestWatcherWithOptions(t, func(opts *FileWatcherOptions) {
		opts.StatisticsRecorder = recorder
	})

	firstPath := t.TempDir()
	secondPath := t.TempDir()
	writeAgedFile(t, filepath.Join(firstPath, "first.csv"), "first")
	writeAgedFile(t, filepath.Join(secondPath, "second.csv"), "second")

	registerWatcher(t, w, newConfig("w1", firstPath, nil))
	registerWatcher(t, w, newConfig("w2", secondPath, nil))

	results, err := w.ScanAllWatchers(context.Background())
	if err != nil {
		t.Fatalf("ScanAllWatchers() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("scan results = %d, want 2", len(results))
	}

	counts := make(map[string]int)

	for _, call := range recorder.recorded() {
		if call.name != core.StatisticWatcherScanCount {
			continue
		}

		if call.value != 1 {
			t.Errorf("scan count delta value = %d, want 1", call.value)
		}

		counts[call.dimensions[core.StatisticsDimensionWatcherID]]++
	}

	if counts["w1"] != 1 || counts["w2"] != 1 {
		t.Fatalf("scan count deltas per watcher = %v, want exactly one each", counts)
	}

	// Two watchers x six deltas: the aggregate path must not double count.
	if got := recorder.callCount(); got != 12 {
		t.Fatalf("recorded %d deltas, want 12 (double counting?)", got)
	}
}

// assertWatcherRecorderCall compares one recorded delta against its
// expectation, including the empty-safe tenant and watcher dimensions and the
// scan operation.
func assertWatcherRecorderCall(t *testing.T, index int, got, want watcherRecorderCall, watcherID string) {
	t.Helper()

	if got.name != want.name {
		t.Errorf("delta %d name = %q, want %q", index, got.name, want.name)
	}
	if got.value != want.value {
		t.Errorf("delta %d (%s) value = %d, want %d", index, got.name, got.value, want.value)
	}
	if got.timestamp.IsZero() {
		t.Errorf("delta %d (%s) has a zero timestamp", index, got.name)
	}
	if got.dimensions[core.StatisticsDimensionTenantID] != "tenant-a" {
		t.Errorf("delta %d (%s) tenant_id = %q, want %q", index, got.name, got.dimensions[core.StatisticsDimensionTenantID], "tenant-a")
	}
	if got.dimensions[core.StatisticsDimensionWatcherID] != watcherID {
		t.Errorf("delta %d (%s) watcher_id = %q, want %q", index, got.name, got.dimensions[core.StatisticsDimensionWatcherID], watcherID)
	}
	if got.dimensions[core.StatisticsDimensionOperation] != "scan" {
		t.Errorf("delta %d (%s) operation = %q, want %q", index, got.name, got.dimensions[core.StatisticsDimensionOperation], "scan")
	}
	if len(got.dimensions) != 3 {
		t.Errorf("delta %d (%s) dimensions = %v, want exactly tenant_id, watcher_id and operation", index, got.name, got.dimensions)
	}
}
