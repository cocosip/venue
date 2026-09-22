package watcher

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// stubFileWatcher is a scriptable core.FileWatcher double.
type stubFileWatcher struct {
	mu sync.Mutex

	configs map[string]*core.FileWatcherConfiguration

	scanHook func(ctx context.Context, watcherID string) (*core.FileWatcherScanResult, error)
	scans    []string
}

func newStubFileWatcher(configs ...*core.FileWatcherConfiguration) *stubFileWatcher {
	registry := make(map[string]*core.FileWatcherConfiguration, len(configs))
	for _, config := range configs {
		registry[config.WatcherID] = config
	}

	return &stubFileWatcher{configs: registry}
}

func (s *stubFileWatcher) RegisterWatcher(_ context.Context, config *core.FileWatcherConfiguration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.configs[config.WatcherID] = config

	return nil
}

// UpdateWatcher replaces a stored configuration, mirroring the contract that an
// unknown watcher ID is reported as core.ErrWatcherNotFound.
func (s *stubFileWatcher) UpdateWatcher(_ context.Context, config *core.FileWatcherConfiguration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.configs[config.WatcherID]; !ok {
		return fmt.Errorf("watcher not found: %s: %w", config.WatcherID, core.ErrWatcherNotFound)
	}

	s.configs[config.WatcherID] = config

	return nil
}

func (s *stubFileWatcher) UnregisterWatcher(_ context.Context, watcherID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.configs, watcherID)

	return nil
}

func (s *stubFileWatcher) GetWatcher(_ context.Context, watcherID string) (*core.FileWatcherConfiguration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	config, ok := s.configs[watcherID]
	if !ok {
		return nil, fmt.Errorf("watcher not found: %s: %w", watcherID, core.ErrWatcherNotFound)
	}

	copied := *config

	return &copied, nil
}

// GetWatchersForTenant returns the single-tenant watchers of one tenant.
func (s *stubFileWatcher) GetWatchersForTenant(_ context.Context, tenantID string) ([]*core.FileWatcherConfiguration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*core.FileWatcherConfiguration, 0)
	for _, config := range s.configs {
		if config.MultiTenantMode || config.TenantID != tenantID {
			continue
		}

		copied := *config
		out = append(out, &copied)
	}

	return out, nil
}

func (s *stubFileWatcher) GetAllWatchers(context.Context) ([]*core.FileWatcherConfiguration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*core.FileWatcherConfiguration, 0, len(s.configs))
	for _, config := range s.configs {
		copied := *config
		out = append(out, &copied)
	}

	return out, nil
}

func (s *stubFileWatcher) EnableWatcher(_ context.Context, watcherID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if config, ok := s.configs[watcherID]; ok {
		config.Enabled = true
	}

	return nil
}

func (s *stubFileWatcher) DisableWatcher(_ context.Context, watcherID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if config, ok := s.configs[watcherID]; ok {
		config.Enabled = false
	}

	return nil
}

func (s *stubFileWatcher) ScanNow(ctx context.Context, watcherID string) (*core.FileWatcherScanResult, error) {
	s.mu.Lock()
	s.scans = append(s.scans, watcherID)
	hook := s.scanHook
	s.mu.Unlock()

	if hook != nil {
		return hook(ctx, watcherID)
	}

	return &core.FileWatcherScanResult{}, nil
}

func (s *stubFileWatcher) ScanAllWatchers(context.Context) (map[string]*core.FileWatcherScanResult, error) {
	return map[string]*core.FileWatcherScanResult{}, nil
}

func (s *stubFileWatcher) scannedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.scans))
	copy(out, s.scans)

	return out
}

func (s *stubFileWatcher) resetScans() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scans = nil
}

func newBackgroundService(t *testing.T, watcher core.FileWatcher, mutate func(*BackgroundFileWatcherServiceOptions)) *BackgroundFileWatcherService {
	t.Helper()

	opts := &BackgroundFileWatcherServiceOptions{
		FileWatcher: watcher,
		Logging:     logging.Disabled(),
		// Isolate the options state file: a test must never read or write the
		// process-wide default configuration directory.
		ConfigurationRootDir:   t.TempDir(),
		InitialDelay:           time.Millisecond,
		MinimumPollingInterval: time.Millisecond,
		DefaultPollingInterval: time.Millisecond,
		DisabledCheckInterval:  time.Millisecond,
		MaximumPollingInterval: time.Hour,
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

// --- defect 4/1.4: the service is enabled by default -----------------------

func TestBackgroundFileWatcherService_EnabledByDefault(t *testing.T) {
	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{
		WatcherID: "w1",
		Enabled:   true,
	})

	// Zero-valued ServiceEnabled must not silently disable the service.
	service := newBackgroundService(t, watcher, nil)

	if !service.IsEnabled() {
		t.Fatalf("IsEnabled() = false, want true when ServiceEnabled is left at its zero value")
	}
}

func TestBackgroundFileWatcherService_StartActuallyScans(t *testing.T) {
	watchPath := t.TempDir()
	writeAgedFile(t, filepath.Join(watchPath, "report.csv"), "payload")

	pool := &fakeStoragePool{}
	coreWatcher, err := NewFileWatcher(&FileWatcherOptions{
		TenantManager:        newFakeTenantManager(),
		StoragePool:          pool,
		ConfigurationRootDir: filepath.Join(t.TempDir(), "watchers"),
		Logging:              logging.Disabled(),
	})
	if err != nil {
		t.Fatalf("NewFileWatcher() error = %v", err)
	}

	concrete := coreWatcher.(*fileWatcher)
	t.Cleanup(func() {
		concrete.awaitPendingHistoryFlush()

		if err := concrete.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	registerWatcher(t, concrete, newConfig("w1", watchPath, nil))

	// ServiceEnabled is intentionally left at its zero value: the service must
	// still scan, which is the regression this covers.
	service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
		FileWatcher:            coreWatcher,
		Logging:                logging.Disabled(),
		ConfigurationRootDir:   t.TempDir(),
		InitialDelay:           time.Millisecond,
		MinimumPollingInterval: time.Millisecond,
		DefaultPollingInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pool.writeCount() > 0 {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("background service never imported the watched file")
}

func TestBackgroundFileWatcherService_SkipsDisabledWatchers(t *testing.T) {
	watcher := newStubFileWatcher(
		&core.FileWatcherConfiguration{WatcherID: "enabled", Enabled: true},
		&core.FileWatcherConfiguration{WatcherID: "disabled", Enabled: false},
	)

	service := newBackgroundService(t, watcher, nil)
	service.executeScanCycle(context.Background())

	scanned := watcher.scannedIDs()
	if len(scanned) != 1 || scanned[0] != "enabled" {
		t.Fatalf("scanned = %v, want [enabled]", scanned)
	}
}

// --- defect 9: per-watcher polling interval --------------------------------

func TestBackgroundFileWatcherService_HonoursPerWatcherPollingInterval(t *testing.T) {
	fast := &core.FileWatcherConfiguration{WatcherID: "fast", Enabled: true, PollingInterval: 10 * time.Second}
	slow := &core.FileWatcherConfiguration{WatcherID: "slow", Enabled: true, PollingInterval: 90 * time.Second}
	watcher := newStubFileWatcher(fast, slow)

	service := newBackgroundService(t, watcher, nil)

	if interval := service.pollingIntervalFor(fast); interval != 10*time.Second {
		t.Fatalf("pollingIntervalFor(fast) = %v, want 10s", interval)
	}
	if interval := service.pollingIntervalFor(slow); interval != 90*time.Second {
		t.Fatalf("pollingIntervalFor(slow) = %v, want 90s", interval)
	}

	// The fastest watcher is due first; the slow one must not be rescanned with it.
	service.executeScanCycleAt(context.Background(), time.Unix(0, 0))
	watcher.resetScans()

	service.executeScanCycleAt(context.Background(), time.Unix(0, 0).Add(10*time.Second))

	scanned := watcher.scannedIDs()
	if len(scanned) != 1 || scanned[0] != "fast" {
		t.Fatalf("second cycle scanned = %v, want [fast] only", scanned)
	}

	// Ten seconds later only the fast watcher is scanned: the slow watcher keeps
	// its own 90s schedule instead of being dragged to the minimum interval.
	service.executeScanCycleAt(context.Background(), time.Unix(0, 0).Add(20*time.Second))

	scanned = watcher.scannedIDs()
	for _, watcherID := range scanned {
		if watcherID == "slow" {
			t.Fatalf("slow watcher was rescanned off-schedule: %v", scanned)
		}
	}

	// At 90s the slow watcher becomes due for the first time.
	service.executeScanCycleAt(context.Background(), time.Unix(0, 0).Add(90*time.Second))

	scanned = watcher.scannedIDs()
	slowScans := 0
	for _, watcherID := range scanned {
		if watcherID == "slow" {
			slowScans++
		}
	}
	if slowScans != 1 {
		t.Fatalf("slow watcher scanned %d times by t=90s, want exactly 1: %v", slowScans, scanned)
	}
}

func TestBackgroundFileWatcherService_ClampsPollingInterval(t *testing.T) {
	minimum := 5 * time.Second
	maximum := 2 * time.Minute

	tests := []struct {
		name      string
		requested time.Duration
		want      time.Duration
	}{
		{name: "below minimum", requested: time.Second, want: minimum},
		{name: "above maximum", requested: time.Hour, want: maximum},
		{name: "within range", requested: 30 * time.Second, want: 30 * time.Second},
		{name: "zero falls back to default", requested: 0, want: 30 * time.Second},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			watcher := newStubFileWatcher()
			service := newBackgroundService(t, watcher, func(opts *BackgroundFileWatcherServiceOptions) {
				opts.MinimumPollingInterval = minimum
				opts.MaximumPollingInterval = maximum
				opts.DefaultPollingInterval = 30 * time.Second
			})

			config := &core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true, PollingInterval: test.requested}
			if got := service.pollingIntervalFor(config); got != test.want {
				t.Fatalf("pollingIntervalFor(%v) = %v, want %v", test.requested, got, test.want)
			}
		})
	}
}

func TestBackgroundFileWatcherService_WaitsWhenNoWatcherIsDue(t *testing.T) {
	watcher := newStubFileWatcher(
		&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true, PollingInterval: 30 * time.Second},
	)

	service := newBackgroundService(t, watcher, nil)

	base := time.Unix(0, 0)
	service.executeScanCycleAt(context.Background(), base)
	watcher.resetScans()

	// Only 1s later: not due yet.
	service.executeScanCycleAt(context.Background(), base.Add(time.Second))

	if scanned := watcher.scannedIDs(); len(scanned) != 0 {
		t.Fatalf("scanned = %v, want nothing before the watcher is due", scanned)
	}
}

// --- defect 10: Stop must not deadlock against IsEnabled --------------------

func TestBackgroundFileWatcherService_StopDoesNotDeadlock(t *testing.T) {
	release := make(chan struct{})
	scanStarted := make(chan struct{})
	var once sync.Once

	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true})
	watcher.scanHook = func(ctx context.Context, _ string) (*core.FileWatcherScanResult, error) {
		once.Do(func() { close(scanStarted) })
		select {
		case <-release:
		case <-ctx.Done():
		}

		return &core.FileWatcherScanResult{}, nil
	}

	service := newBackgroundService(t, watcher, nil)
	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case <-scanStarted:
	case <-time.After(10 * time.Second):
		t.Fatalf("scan never started")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- service.Stop() }()

	// Stop cancels the context; the in-flight scan observes it and returns.
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("Stop() deadlocked against the running scan loop")
	}

	if service.IsRunning() {
		t.Fatalf("IsRunning() = true, want false after Stop()")
	}
}

func TestBackgroundFileWatcherService_StopRejectsWhenNotRunning(t *testing.T) {
	watcher := newStubFileWatcher()
	service := newBackgroundService(t, watcher, nil)

	if err := service.Stop(); err == nil {
		t.Fatalf("Stop() error = nil, want an error when the service is not running")
	}
}

func TestBackgroundFileWatcherService_StartStopIsRepeatable(t *testing.T) {
	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true})
	service := newBackgroundService(t, watcher, nil)

	for i := 0; i < 3; i++ {
		if err := service.Start(); err != nil {
			t.Fatalf("Start() #%d error = %v", i, err)
		}
		if err := service.Start(); err == nil {
			t.Fatalf("Start() #%d error = nil, want an error when already running", i)
		}
		if err := service.Stop(); err != nil {
			t.Fatalf("Stop() #%d error = %v", i, err)
		}
	}
}

func TestBackgroundFileWatcherService_SetEnabledToggles(t *testing.T) {
	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{WatcherID: "w1", Enabled: true})
	service := newBackgroundService(t, watcher, nil)

	service.SetEnabled(false)
	if service.IsEnabled() {
		t.Fatalf("IsEnabled() = true after SetEnabled(false)")
	}

	service.SetEnabled(true)
	if !service.IsEnabled() {
		t.Fatalf("IsEnabled() = false after SetEnabled(true)")
	}
}
