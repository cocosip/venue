package watcher

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// testWatcherStateFileName is the persisted runtime enable/disable state file
// name, spelled literally so these tests fail on behaviour rather than on a
// missing symbol.
const testWatcherStateFileName = "watcher-state.json"

// --- runtime enable/disable state persistence -------------------------------

func TestWatcherState_EnableDisableSurvivesRestart(t *testing.T) {
	const watcherID = "watcher-vip-tenant-001"

	tests := []struct {
		name        string
		configured  bool
		apply       func(t *testing.T, w *fileWatcher)
		wantEnabled bool
	}{
		{
			name:       "disable persists across a restart",
			configured: true,
			apply: func(t *testing.T, w *fileWatcher) {
				t.Helper()

				if err := w.DisableWatcher(context.Background(), watcherID); err != nil {
					t.Fatalf("DisableWatcher() error = %v", err)
				}
			},
			wantEnabled: false,
		},
		{
			name:       "enable persists across a restart",
			configured: false,
			apply: func(t *testing.T, w *fileWatcher) {
				t.Helper()

				if err := w.EnableWatcher(context.Background(), watcherID); err != nil {
					t.Fatalf("EnableWatcher() error = %v", err)
				}
			},
			wantEnabled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configRoot := filepath.Join(t.TempDir(), "watchers")
			watchPath := t.TempDir()

			first, _ := newTestWatcherAt(t, configRoot)
			registerWatcher(t, first, newConfig(watcherID, watchPath, func(config *core.FileWatcherConfiguration) {
				config.Enabled = test.configured
			}))

			test.apply(t, first)

			version, watchers := readWatcherStateFile(t, configRoot)
			if version != 1 {
				t.Fatalf("state document version = %d, want 1", version)
			}
			persisted, ok := watchers[watcherID]
			if !ok {
				t.Fatalf("state document has no entry for %q: %v", watcherID, watchers)
			}
			if persisted != test.wantEnabled {
				t.Fatalf("persisted %q = %v, want %v", watcherID, persisted, test.wantEnabled)
			}

			// A restart: a brand new instance over the same configuration root
			// re-registers the watcher from configuration with the opposite flag,
			// so only the persisted runtime decision can produce the expectation.
			restarted, _ := newTestWatcherAt(t, configRoot)
			registerWatcher(t, restarted, newConfig(watcherID, watchPath, func(config *core.FileWatcherConfiguration) {
				config.Enabled = !test.wantEnabled
			}))

			if got := watcherEnabled(t, restarted, watcherID); got != test.wantEnabled {
				t.Fatalf("Enabled after restart = %v, want %v (persisted runtime state must win over configuration)",
					got, test.wantEnabled)
			}
		})
	}
}

func TestWatcherState_UnregisterRemovesPersistedEntry(t *testing.T) {
	const watcherID = "w1"

	configRoot := filepath.Join(t.TempDir(), "watchers")
	watchPath := t.TempDir()

	w, _ := newTestWatcherAt(t, configRoot)
	registerWatcher(t, w, newConfig(watcherID, watchPath, nil))

	if err := w.DisableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("DisableWatcher() error = %v", err)
	}

	_, watchers := readWatcherStateFile(t, configRoot)
	if _, ok := watchers[watcherID]; !ok {
		t.Fatalf("no persisted entry for %q after DisableWatcher()", watcherID)
	}

	if err := w.UnregisterWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("UnregisterWatcher() error = %v", err)
	}

	_, watchers = readWatcherStateFile(t, configRoot)
	if _, ok := watchers[watcherID]; ok {
		t.Fatalf("persisted entry for %q survived UnregisterWatcher(): %v", watcherID, watchers)
	}

	// A new instance must not resurrect the removed decision.
	restarted, _ := newTestWatcherAt(t, configRoot)
	registerWatcher(t, restarted, newConfig(watcherID, watchPath, nil))

	if !watcherEnabled(t, restarted, watcherID) {
		t.Fatalf("unregistered watcher came back disabled after a restart")
	}
}

func TestWatcherState_WriteIsAtomicAndLeavesNoTempFile(t *testing.T) {
	const watcherID = "w1"

	configRoot := filepath.Join(t.TempDir(), "watchers")

	w, _ := newTestWatcherAt(t, configRoot)
	registerWatcher(t, w, newConfig(watcherID, t.TempDir(), nil))

	if err := w.DisableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("DisableWatcher() error = %v", err)
	}

	entries, err := os.ReadDir(configRoot)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", configRoot, err)
	}

	stateFound := false
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("atomic write left a staging file behind: %q", entry.Name())
		}
		if entry.Name() == testWatcherStateFileName {
			stateFound = true
		}
	}

	if !stateFound {
		t.Fatalf("%s was not written", testWatcherStateFileName)
	}

	// The published document must be complete and parseable.
	version, watchers := readWatcherStateFile(t, configRoot)
	if version != 1 {
		t.Fatalf("state document version = %d, want 1", version)
	}
	if enabled, ok := watchers[watcherID]; !ok || enabled {
		t.Fatalf("published state = %v, want %q present and false", watchers, watcherID)
	}
}

func TestWatcherState_CorruptStateFileIsIgnored(t *testing.T) {
	configRoot := filepath.Join(t.TempDir(), "watchers")
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", configRoot, err)
	}

	if err := os.WriteFile(filepath.Join(configRoot, testWatcherStateFileName), []byte("{ this is not a watcher state document"), 0o644); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}

	handler := &recordingHandler{}

	logRuntime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	// Construction must succeed and must not lose the configured flag.
	w, _ := newTestWatcherAt(t, configRoot, func(opts *FileWatcherOptions) {
		opts.Logging = logRuntime
	})

	registerWatcher(t, w, newConfig("w1", t.TempDir(), func(config *core.FileWatcherConfiguration) {
		config.Enabled = true
	}))

	if !watcherEnabled(t, w, "w1") {
		t.Fatalf("configured Enabled flag was lost after a corrupt state file")
	}

	// The warning must be structured, stable, and free of physical paths.
	records := handler.snapshot()

	warned := false
	for _, record := range records {
		if record.event == "watcher_state_load_failed" {
			warned = true

			if record.level != slog.LevelWarn {
				t.Fatalf("watcher_state_load_failed level = %v, want %v", record.level, slog.LevelWarn)
			}
		}

		for _, attr := range record.attrs {
			if strings.Contains(attr.Value.String(), configRoot) {
				t.Fatalf("log attribute %q leaked the physical state directory: %q", attr.Key, attr.Value.String())
			}
		}
	}

	if !warned {
		t.Fatalf("no watcher_state_load_failed event was emitted for a corrupt state file")
	}
}

func TestWatcherState_PersistFailureIsBestEffort(t *testing.T) {
	const watcherID = "w1"

	configRoot := filepath.Join(t.TempDir(), "watchers")

	w, _ := newTestWatcherAt(t, configRoot)
	registerWatcher(t, w, newConfig(watcherID, t.TempDir(), nil))

	// Make the state path unwritable without touching construction: replace the
	// state directory with a regular file so the staging file cannot be created.
	if err := os.RemoveAll(configRoot); err != nil {
		t.Skipf("cannot remove the state directory to simulate an unwritable state path: %v", err)
	}

	if err := os.WriteFile(configRoot, []byte("blocker"), 0o644); err != nil {
		t.Skipf("cannot create a blocker file at the state path: %v", err)
	}

	// Restore the directory before newTestWatcherAt's cleanup closes the watcher
	// and flushes its history.
	t.Cleanup(func() {
		_ = os.Remove(configRoot)
		_ = os.MkdirAll(configRoot, 0o755)
	})

	probe, probeErr := os.CreateTemp(configRoot, "probe-*.tmp")
	if probeErr == nil {
		probePath := probe.Name()
		_ = probe.Close()
		_ = os.Remove(probePath)

		t.Skip("this platform allows creating files below a regular file; an unwritable state path cannot be simulated")
	}

	if err := w.DisableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("DisableWatcher() error = %v, want nil (state persistence is best-effort)", err)
	}

	if watcherEnabled(t, w, watcherID) {
		t.Fatalf("in-memory Enabled = true after DisableWatcher() whose state write failed")
	}

	if err := w.EnableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("EnableWatcher() error = %v, want nil (state persistence is best-effort)", err)
	}

	if !watcherEnabled(t, w, watcherID) {
		t.Fatalf("in-memory Enabled = false after EnableWatcher() whose state write failed")
	}
}

func TestWatcherState_UnregisterPersistFailureIsBestEffort(t *testing.T) {
	const watcherID = "w1"

	configRoot := filepath.Join(t.TempDir(), "watchers")

	w, _ := newTestWatcherAt(t, configRoot)
	registerWatcher(t, w, newConfig(watcherID, t.TempDir(), nil))

	if err := w.DisableWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("DisableWatcher() error = %v", err)
	}

	if err := os.RemoveAll(configRoot); err != nil {
		t.Skipf("cannot remove the state directory to simulate an unwritable state path: %v", err)
	}

	if err := os.WriteFile(configRoot, []byte("blocker"), 0o644); err != nil {
		t.Skipf("cannot create a blocker file at the state path: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Remove(configRoot)
		_ = os.MkdirAll(configRoot, 0o755)
	})

	if err := w.UnregisterWatcher(context.Background(), watcherID); err != nil {
		t.Fatalf("UnregisterWatcher() error = %v, want nil (state persistence is best-effort)", err)
	}

	if _, err := w.GetWatcher(context.Background(), watcherID); err == nil {
		t.Fatalf("GetWatcher() error = nil after UnregisterWatcher(), want not found")
	}
}

// --- helpers ----------------------------------------------------------------

// readWatcherStateFile decodes the persisted state document literally, so the
// assertion does not depend on the implementation's own types.
func readWatcherStateFile(t *testing.T, configRoot string) (int, map[string]bool) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(configRoot, testWatcherStateFileName))
	if err != nil {
		t.Fatalf("read %s: %v", testWatcherStateFileName, err)
	}

	var document struct {
		Version  int             `json:"version"`
		Watchers map[string]bool `json:"watchers"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("parse %s: %v (content %q)", testWatcherStateFileName, err, data)
	}

	return document.Version, document.Watchers
}

// watcherEnabled returns the stored Enabled flag for a registered watcher.
func watcherEnabled(t *testing.T, w *fileWatcher, watcherID string) bool {
	t.Helper()

	config, err := w.GetWatcher(context.Background(), watcherID)
	if err != nil {
		t.Fatalf("GetWatcher(%q) error = %v", watcherID, err)
	}

	return config.Enabled
}

// capturedLog is one record captured by recordingHandler.
type capturedLog struct {
	level slog.Level
	event string
	attrs []slog.Attr
}

// recordingHandler captures emitted records so a test can assert on the stable
// event name and on attribute safety.
type recordingHandler struct {
	mu      sync.Mutex
	records []capturedLog
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	captured := capturedLog{level: record.Level}

	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			captured.event = attr.Value.String()

			return true
		}

		captured.attrs = append(captured.attrs, attr)

		return true
	})

	h.mu.Lock()
	h.records = append(h.records, captured)
	h.mu.Unlock()

	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) snapshot() []capturedLog {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]capturedLog(nil), h.records...)
}
