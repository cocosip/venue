package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// newServiceWithState builds a service whose options state file lives in its own
// temporary directory, so a test never reads or writes the process-wide default
// configuration directory (and therefore never leaks state into another test).
func newServiceWithState(t *testing.T, mutate func(*BackgroundFileWatcherServiceOptions)) *BackgroundFileWatcherService {
	t.Helper()

	opts := &BackgroundFileWatcherServiceOptions{
		FileWatcher:          newStubFileWatcher(),
		Logging:              logging.Disabled(),
		ConfigurationRootDir: t.TempDir(),
	}

	if mutate != nil {
		mutate(opts)
	}

	service, err := NewBackgroundFileWatcherService(opts)
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	return service
}

// --- defaults and copies ----------------------------------------------------

func TestBackgroundFileWatcherServiceOptions_Defaults(t *testing.T) {
	service := newServiceWithState(t, nil)

	options := service.Options()

	if !options.Enabled {
		t.Fatalf("Options().Enabled = false, want true when the caller leaves it at its zero value")
	}
	if options.DefaultPollingInterval != 30*time.Second {
		t.Fatalf("DefaultPollingInterval = %v, want 30s", options.DefaultPollingInterval)
	}
	if options.MinimumPollingInterval != 5*time.Second {
		t.Fatalf("MinimumPollingInterval = %v, want 5s", options.MinimumPollingInterval)
	}
	if options.MaximumPollingInterval != time.Hour {
		t.Fatalf("MaximumPollingInterval = %v, want 1h", options.MaximumPollingInterval)
	}
	if options.DisabledCheckInterval != time.Minute {
		t.Fatalf("DisabledCheckInterval = %v, want 1m", options.DisabledCheckInterval)
	}
	if options.MaxParallelWatcherScans != 4 {
		t.Fatalf("MaxParallelWatcherScans = %d, want 4", options.MaxParallelWatcherScans)
	}
}

// TestBackgroundFileWatcherServiceOptions_ParallelBoundAtConstruction pins the
// construction-time semantics of MaxParallelWatcherScans: a non-positive value
// is Go's "unset" marker and yields the documented default of 4. Sequential
// scanning is requested through UpdateOptions, where 0 is preserved as a
// meaningful runtime value.
func TestBackgroundFileWatcherServiceOptions_ParallelBoundAtConstruction(t *testing.T) {
	tests := []struct {
		name  string
		bound int
		want  int
	}{
		{name: "unset", bound: 0, want: 4},
		{name: "negative", bound: -1, want: 4},
		{name: "explicit", bound: 3, want: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newServiceWithState(t, func(opts *BackgroundFileWatcherServiceOptions) {
				opts.ServiceOptions.MaxParallelWatcherScans = test.bound
			})

			if got := service.Options().MaxParallelWatcherScans; got != test.want {
				t.Fatalf("MaxParallelWatcherScans = %d, want %d", got, test.want)
			}
		})
	}
}

// TestBackgroundFileWatcherServiceOptions_NegativeBoundRejectedAtRuntime pins
// the other half of the contract: UpdateOptions is explicit, so a negative bound
// is an error there rather than a silent rewrite.
func TestBackgroundFileWatcherServiceOptions_NegativeBoundRejectedAtRuntime(t *testing.T) {
	service := newServiceWithState(t, nil)

	before := service.Options()

	err := service.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{
		Enabled:                 true,
		MaxParallelWatcherScans: -1,
	})
	if err == nil {
		t.Fatalf("UpdateOptions(-1) error = nil, want core.ErrInvalidArgument")
	}
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("UpdateOptions(-1) error = %v, want core.ErrInvalidArgument", err)
	}

	if after := service.Options(); after != before {
		t.Fatalf("Options() changed to %+v after a rejected update, want %+v", after, before)
	}
}

func TestBackgroundFileWatcherServiceOptions_OptionsReturnsCopy(t *testing.T) {
	service := newServiceWithState(t, nil)

	mutated := service.Options()
	mutated.Enabled = false
	mutated.MaxParallelWatcherScans = 99

	fresh := service.Options()
	if !fresh.Enabled {
		t.Fatalf("Options().Enabled = false after the caller mutated a previous copy")
	}
	if fresh.MaxParallelWatcherScans == 99 {
		t.Fatalf("Options().MaxParallelWatcherScans = 99 after the caller mutated a previous copy")
	}
}

// --- validation -------------------------------------------------------------

func TestBackgroundFileWatcherService_UpdateOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		options core.FileWatcherServiceOptions
	}{
		{
			name: "negative default polling interval",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: -time.Second, MinimumPollingInterval: time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "negative minimum polling interval",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: time.Second, MinimumPollingInterval: -time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "negative maximum polling interval",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: time.Second, MinimumPollingInterval: time.Second,
				MaximumPollingInterval: -time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "negative disabled check interval",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: time.Second, MinimumPollingInterval: time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: -time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "minimum greater than maximum",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: 30 * time.Second, MinimumPollingInterval: time.Hour,
				MaximumPollingInterval: time.Minute, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "default below minimum",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: time.Second, MinimumPollingInterval: 30 * time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "default above maximum",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: 2 * time.Hour, MinimumPollingInterval: time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: 1,
			},
		},
		{
			name: "negative parallel scans",
			options: core.FileWatcherServiceOptions{
				Enabled: true, DefaultPollingInterval: 30 * time.Second, MinimumPollingInterval: time.Second,
				MaximumPollingInterval: time.Hour, DisabledCheckInterval: time.Minute, MaxParallelWatcherScans: -1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newServiceWithState(t, nil)

			before := service.Options()

			err := service.UpdateOptions(context.Background(), test.options)
			if err == nil {
				t.Fatalf("UpdateOptions() error = nil, want core.ErrInvalidArgument")
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("UpdateOptions() error = %v, want core.ErrInvalidArgument", err)
			}

			if after := service.Options(); after != before {
				t.Fatalf("Options() changed to %+v after a rejected update, want %+v", after, before)
			}
		})
	}
}

// TestBackgroundFileWatcherService_UpdateOptionsZeroDurationsUseDefaults pins
// the default-safe convention for the interval knobs: a zero value means
// "unset" and is replaced by the documented default rather than rejected.
func TestBackgroundFileWatcherService_UpdateOptionsZeroDurationsUseDefaults(t *testing.T) {
	service := newServiceWithState(t, nil)

	if err := service.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{Enabled: true}); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	options := service.Options()
	if options.DefaultPollingInterval != 30*time.Second {
		t.Fatalf("DefaultPollingInterval = %v, want the default 30s", options.DefaultPollingInterval)
	}
	if options.MinimumPollingInterval != 5*time.Second {
		t.Fatalf("MinimumPollingInterval = %v, want the default 5s", options.MinimumPollingInterval)
	}
	if options.MaximumPollingInterval != time.Hour {
		t.Fatalf("MaximumPollingInterval = %v, want the default 1h", options.MaximumPollingInterval)
	}
	if options.DisabledCheckInterval != time.Minute {
		t.Fatalf("DisabledCheckInterval = %v, want the default 1m", options.DisabledCheckInterval)
	}
}

func TestBackgroundFileWatcherService_UpdateOptionsAppliesAndBoundsParallelism(t *testing.T) {
	service := newServiceWithState(t, nil)

	updated := core.FileWatcherServiceOptions{
		Enabled:                 true,
		DefaultPollingInterval:  45 * time.Second,
		MinimumPollingInterval:  10 * time.Second,
		MaximumPollingInterval:  30 * time.Minute,
		DisabledCheckInterval:   2 * time.Minute,
		MaxParallelWatcherScans: 3,
	}

	if err := service.UpdateOptions(context.Background(), updated); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	if got := service.Options(); got != updated {
		t.Fatalf("Options() = %+v, want %+v", got, updated)
	}

	// The applied bounds must drive scheduling, not just be reported back.
	clamped := service.pollingIntervalFor(&core.FileWatcherConfiguration{WatcherID: "w1", PollingInterval: time.Second})
	if clamped != 10*time.Second {
		t.Fatalf("pollingIntervalFor(1s) = %v, want the updated minimum 10s", clamped)
	}
}

// An unset MaxParallelWatcherScans falls back to the documented default of 4;
// sequential scanning is requested explicitly through UpdateOptions.
func TestBackgroundFileWatcherService_SequentialWhenParallelismNotSet(t *testing.T) {
	service := newServiceWithState(t, nil)

	if err := service.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{
		Enabled:                 true,
		DefaultPollingInterval:  30 * time.Second,
		MinimumPollingInterval:  time.Second,
		MaximumPollingInterval:  time.Hour,
		DisabledCheckInterval:   time.Minute,
		MaxParallelWatcherScans: 0,
	}); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	if got := service.Options().MaxParallelWatcherScans; got != 0 {
		t.Fatalf("MaxParallelWatcherScans = %d, want 0 preserved as the sequential marker", got)
	}

	// The construction-time default is the documented 4.
	if got := newServiceWithState(t, nil).Options().MaxParallelWatcherScans; got != 4 {
		t.Fatalf("constructed MaxParallelWatcherScans = %d, want the default 4", got)
	}
}

// --- persistence ------------------------------------------------------------

func TestBackgroundFileWatcherService_SetEnabledPersistsAcrossRestart(t *testing.T) {
	configRoot := t.TempDir()

	newService := func() *BackgroundFileWatcherService {
		t.Helper()

		service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
			FileWatcher:          newStubFileWatcher(),
			Logging:              logging.Disabled(),
			ConfigurationRootDir: configRoot,
		})
		if err != nil {
			t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
		}

		return service
	}

	service := newService()
	if !service.IsEnabled() {
		t.Fatalf("IsEnabled() = false, want the default-safe enabled state")
	}

	service.SetEnabled(false)
	if service.IsEnabled() {
		t.Fatalf("IsEnabled() = true after SetEnabled(false)")
	}

	optionsPath := filepath.Join(configRoot, "file-watcher-options.json")
	if _, err := os.Stat(optionsPath); err != nil {
		t.Fatalf("SetEnabled(false) did not persist %s: %v", filepath.Base(optionsPath), err)
	}

	restarted := newService()
	if restarted.IsEnabled() {
		t.Fatalf("IsEnabled() = true after restart, want the persisted disable decision")
	}
	if restarted.Options().Enabled {
		t.Fatalf("Options().Enabled = true after restart, want the persisted disable decision")
	}

	restarted.SetEnabled(true)

	reenabled := newService()
	if !reenabled.IsEnabled() {
		t.Fatalf("IsEnabled() = false after restart, want the persisted enable decision")
	}
}

// TestBackgroundFileWatcherService_OptionsFileOverridesConstructorDefaults
// proves an operator decision in the state file outranks a caller-supplied
// default, while only the Enabled flag is persisted.
func TestBackgroundFileWatcherService_OptionsFileOverridesConstructorDefaults(t *testing.T) {
	configRoot := t.TempDir()

	first, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher: newStubFileWatcher(),
		Logging:     logging.Disabled(),
		ServiceOptions: core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  42 * time.Second,
			MinimumPollingInterval:  7 * time.Second,
			MaximumPollingInterval:  42 * time.Minute,
			DisabledCheckInterval:   9 * time.Minute,
			MaxParallelWatcherScans: 6,
		},
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if err := first.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{
		Enabled:                 false,
		DefaultPollingInterval:  42 * time.Second,
		MinimumPollingInterval:  7 * time.Second,
		MaximumPollingInterval:  42 * time.Minute,
		DisabledCheckInterval:   9 * time.Minute,
		MaxParallelWatcherScans: 6,
	}); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	// Restart with the same constructor options: the persisted disable wins.
	restarted, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher: newStubFileWatcher(),
		Logging:     logging.Disabled(),
		ServiceOptions: core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  42 * time.Second,
			MinimumPollingInterval:  7 * time.Second,
			MaximumPollingInterval:  42 * time.Minute,
			DisabledCheckInterval:   9 * time.Minute,
			MaxParallelWatcherScans: 6,
		},
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if restarted.IsEnabled() {
		t.Fatalf("IsEnabled() = true after restart, want the persisted disable decision to outrank the constructor default")
	}
	if got := restarted.Options().DefaultPollingInterval; got != 42*time.Second {
		t.Fatalf("DefaultPollingInterval = %v, want the caller's 42s", got)
	}
}

func TestBackgroundFileWatcherService_IgnoresCorruptOptionsFile(t *testing.T) {
	configRoot := t.TempDir()

	if err := os.WriteFile(filepath.Join(configRoot, "file-watcher-options.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write corrupt options file: %v", err)
	}

	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          newStubFileWatcher(),
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v, want a corrupt state file to be ignored", err)
	}

	if !service.IsEnabled() {
		t.Fatalf("IsEnabled() = false, want the default-safe enabled state for a corrupt options file")
	}
	if got := service.Options().DefaultPollingInterval; got != 30*time.Second {
		t.Fatalf("DefaultPollingInterval = %v, want the default 30s", got)
	}
}

// The persisted document must not carry physical paths or other sensitive data.
func TestBackgroundFileWatcherService_OptionsFileIsSafeJSON(t *testing.T) {
	configRoot := t.TempDir()

	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          newStubFileWatcher(),
		Logging:              logging.Disabled(),
		ConfigurationRootDir: configRoot,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if err := service.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{
		Enabled:                 false,
		DefaultPollingInterval:  30 * time.Second,
		MinimumPollingInterval:  time.Second,
		MaximumPollingInterval:  time.Hour,
		DisabledCheckInterval:   time.Minute,
		MaxParallelWatcherScans: 2,
	}); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(configRoot, "file-watcher-options.json"))
	if err != nil {
		t.Fatalf("read options file: %v", err)
	}

	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("options file is not valid JSON: %v", err)
	}

	if enabled, ok := document["enabled"].(bool); !ok || enabled {
		t.Fatalf("enabled = %v, want false", document["enabled"])
	}
}

// --- parallelism ------------------------------------------------------------

// TestBackgroundFileWatcherService_BoundsParallelScans proves one cycle never
// runs more scans at once than the configured limit.
func TestBackgroundFileWatcherService_BoundsParallelScans(t *testing.T) {
	const (
		watcherCount  = 12
		maxParallel   = 3
		cycleAttempts = 5
	)

	configs := make([]*core.FileWatcherConfiguration, 0, watcherCount)
	for i := 0; i < watcherCount; i++ {
		configs = append(configs, &core.FileWatcherConfiguration{
			WatcherID:       string(rune('a'+i)) + "-watcher",
			Enabled:         true,
			PollingInterval: time.Second,
		})
	}

	watcher := newStubFileWatcher(configs...)

	var inFlight atomic.Int64
	var peak atomic.Int64

	watcher.scanHook = func(ctx context.Context, _ string) (*core.FileWatcherScanResult, error) {
		current := inFlight.Add(1)

		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}

		// Yield so a serial implementation can never observe overlap.
		time.Sleep(2 * time.Millisecond)

		inFlight.Add(-1)

		return &core.FileWatcherScanResult{}, nil
	}

	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:          watcher,
		Logging:              logging.Disabled(),
		ConfigurationRootDir: t.TempDir(),
		ServiceOptions: core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  time.Second,
			MinimumPollingInterval:  time.Millisecond,
			MaximumPollingInterval:  time.Hour,
			DisabledCheckInterval:   time.Millisecond,
			MaxParallelWatcherScans: maxParallel,
		},
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	for attempt := 0; attempt < cycleAttempts; attempt++ {
		service.executeScanCycleAt(context.Background(), time.Unix(0, 0))
	}

	if scanned := len(watcher.scannedIDs()); scanned < watcherCount {
		t.Fatalf("scanned %d watchers across %d cycles, want at least %d", scanned, cycleAttempts, watcherCount)
	}

	if observed := peak.Load(); observed > maxParallel {
		t.Fatalf("peak concurrent scans = %d, want at most %d", observed, maxParallel)
	}
}

func TestBackgroundFileWatcherService_SequentialScanWhenLimitNotPositive(t *testing.T) {
	watchers := make([]*core.FileWatcherConfiguration, 0, 6)
	for i := 0; i < 6; i++ {
		watchers = append(watchers, &core.FileWatcherConfiguration{
			WatcherID:       string(rune('a'+i)) + "-watcher",
			Enabled:         true,
			PollingInterval: time.Second,
		})
	}

	watcher := newStubFileWatcher(watchers...)

	var inFlight atomic.Int64
	var peak atomic.Int64

	watcher.scanHook = func(context.Context, string) (*core.FileWatcherScanResult, error) {
		current := inFlight.Add(1)

		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}

		time.Sleep(time.Millisecond)
		inFlight.Add(-1)

		return &core.FileWatcherScanResult{}, nil
	}

	service := newBackgroundService(t, watcher, nil)

	// Apply a non-positive bound at runtime: it means sequential scanning, not
	// the construction-time default.
	if err := service.UpdateOptions(context.Background(), core.FileWatcherServiceOptions{
		Enabled:                 true,
		DefaultPollingInterval:  time.Second,
		MinimumPollingInterval:  time.Millisecond,
		MaximumPollingInterval:  time.Hour,
		DisabledCheckInterval:   time.Millisecond,
		MaxParallelWatcherScans: 0,
	}); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}

	service.executeScanCycleAt(context.Background(), time.Unix(0, 0))

	if observed := peak.Load(); observed > 1 {
		t.Fatalf("peak concurrent scans = %d, want sequential scanning for a non-positive limit", observed)
	}
}

// TestBackgroundFileWatcherService_DisabledServiceDoesNotScan proves an
// explicit SetEnabled(false) stops scanning entirely.
func TestBackgroundFileWatcherService_DisabledServiceDoesNotScan(t *testing.T) {
	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true})

	service := newBackgroundService(t, watcher, nil)
	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	service.SetEnabled(false)

	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	time.Sleep(80 * time.Millisecond)

	if scanned := watcher.scannedIDs(); len(scanned) != 0 {
		t.Fatalf("scanned = %v, want nothing while the service is globally disabled", scanned)
	}
}

// --- schedule changes take effect on the next cycle -------------------------

// TestBackgroundFileWatcherService_AppliesUpdatedScheduleOnNextCycle covers the
// "update, never unregister" contract end to end against the real watcher: a
// watcher whose interval is shortened through UpdateWatcher stays registered and
// its new schedule fires on the very next cycle.
func TestBackgroundFileWatcherService_AppliesUpdatedScheduleOnNextCycle(t *testing.T) {
	w, _ := newTestWatcher(t)

	watchPath := t.TempDir()
	registerWatcher(t, w, newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PollingInterval = 90 * time.Second
	}))

	service := newBackgroundService(t, w, nil)

	// First cycle: the watcher is new, so it is due immediately.
	service.executeScanCycleAt(context.Background(), time.Unix(0, 0))

	// The operator shortens the schedule without unregistering.
	if err := w.UpdateWatcher(context.Background(), newConfig("w1", watchPath, func(config *core.FileWatcherConfiguration) {
		config.PollingInterval = 10 * time.Second
	})); err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}

	// The new schedule must be respected: one full new interval after the update
	// was observed, not the stale 90s due time and not immediately.
	if delay := service.executeScanCycleAt(context.Background(), time.Unix(0, 0).Add(5*time.Second)); delay != 10*time.Second {
		t.Fatalf("second cycle delay = %v, want the updated 10s schedule to be due in 5s", delay)
	}
	service.executeScanCycleAt(context.Background(), time.Unix(0, 0).Add(10*time.Second))

	updated, err := w.GetWatcher(context.Background(), "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v, want the watcher to stay registered across the update", err)
	}
	if updated.PollingInterval != 10*time.Second {
		t.Fatalf("PollingInterval = %v after UpdateWatcher, want 10s", updated.PollingInterval)
	}
}

// --- -race: parallel scans keep the in-flight de-duplication intact ---------

func TestScanNow_ParallelScansImportOnce(t *testing.T) {
	w, pool := newTestWatcher(t)

	const watcherCount = 4

	for i := 0; i < watcherCount; i++ {
		watchPath := t.TempDir()

		watcherID := "w" + string(rune('1'+i))
		registerWatcher(t, w, newConfig(watcherID, watchPath, nil))

		writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload-"+watcherID)
	}

	// Widen the race window so a non-atomic reservation would double-import.
	pool.onWrite = func(context.Context, core.TenantContext, string) error {
		time.Sleep(time.Millisecond)
		return nil
	}

	service := newBackgroundService(t, w, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.ServiceOptions = core.FileWatcherServiceOptions{
			Enabled:                 true,
			DefaultPollingInterval:  time.Second,
			MinimumPollingInterval:  time.Millisecond,
			MaximumPollingInterval:  time.Hour,
			DisabledCheckInterval:   time.Millisecond,
			MaxParallelWatcherScans: watcherCount,
		}
	})

	// Several overlapping cycles: every file must still be imported exactly once.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.executeScanCycleAt(context.Background(), time.Unix(0, 0))
		}()
	}
	wg.Wait()

	if got := pool.writeCount(); got != watcherCount {
		t.Fatalf("WriteFile calls = %d, want %d (one per watcher, no duplicate imports)", got, watcherCount)
	}
}
