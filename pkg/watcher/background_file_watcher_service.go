package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// timeNow returns the current time. It is a package variable so background
// scheduling can be exercised deterministically in tests.
var timeNow = time.Now

const (
	// fileWatcherOptionsFileName is the persisted global watcher service
	// options document, stored under ConfigurationRootDir.
	fileWatcherOptionsFileName = "file-watcher-options.json"

	// fileWatcherOptionsTempFilePattern names the staging file for the atomic
	// options write. The "*" makes os.CreateTemp generate a unique name in the
	// same directory as the target, which is required for an atomic rename.
	fileWatcherOptionsTempFilePattern = "file-watcher-options-*.tmp"

	// fileWatcherOptionsVersion is the schema version of the persisted options
	// document. A document with any other version is treated as unreadable.
	fileWatcherOptionsVersion = 1
)

// Global service option defaults. They mirror the Locus baseline defaults.
const (
	defaultWatcherPollingInterval = 30 * time.Second
	minimumWatcherPollingInterval = 5 * time.Second
	maximumWatcherPollingInterval = time.Hour
	defaultDisabledCheckInterval  = time.Minute
	defaultMaxParallelScans       = 4
)

// BackgroundFileWatcherServiceOptions configures the background file watcher service.
type BackgroundFileWatcherServiceOptions struct {
	// FileWatcher is the underlying file watcher that performs the actual scans.
	FileWatcher core.FileWatcher

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// InitialDelay is the delay before the first scan.
	// Default: 10 seconds
	InitialDelay time.Duration

	// ServiceOptions carries the global, persistable watcher service options.
	//
	// It is the documented option surface: Options() reports exactly these
	// values (with zero durations replaced by the defaults) and UpdateOptions
	// replaces them. The direct *PollingInterval and DisabledCheckInterval
	// fields below are legacy conveniences kept for callers that only need the
	// non-persisted schedule knobs; they are folded into the effective options
	// and ServiceOptions wins whenever it sets the same value.
	ServiceOptions core.FileWatcherServiceOptions

	// MinimumPollingInterval is the minimum allowed polling interval.
	// Default: 5 seconds
	//
	// Deprecated: set ServiceOptions.MinimumPollingInterval instead.
	MinimumPollingInterval time.Duration

	// MaximumPollingInterval is the maximum allowed polling interval.
	// Default: 1 hour
	//
	// Deprecated: set ServiceOptions.MaximumPollingInterval instead.
	MaximumPollingInterval time.Duration

	// DefaultPollingInterval is the polling interval used when a watcher leaves
	// its own interval unset.
	// Default: 30 seconds
	//
	// Deprecated: set ServiceOptions.DefaultPollingInterval instead.
	DefaultPollingInterval time.Duration

	// DisabledCheckInterval is how often to check if service should be re-enabled.
	// Default: 1 minute
	//
	// Deprecated: set ServiceOptions.DisabledCheckInterval instead.
	DisabledCheckInterval time.Duration

	// ServiceEnabled controls whether the service is globally enabled.
	//
	// Default: enabled. A zero value leaves the service ENABLED so that simply
	// constructing the service cannot silently disable directory imports; use
	// SetEnabled(false) to disable it at runtime. Callers that want to start the
	// service disabled must call SetEnabled(false) explicitly or persist a
	// disabled state through UpdateOptions.
	//
	// Deprecated: the effective enabled state is exposed by ServiceOptions.
	ServiceEnabled bool

	// Enabled is the global enabled flag at construction time.
	//
	// Like ServiceEnabled, a zero value is default-safe: it never disables the
	// service on its own. An explicitly persisted disabled state (written by
	// SetEnabled(false) or UpdateOptions) always wins, so a restart keeps an
	// operator's decision.
	Enabled bool

	// ConfigurationRootDir is where the options state file lives.
	//
	// Default: "./.locus/config". The directory is created on demand; the state
	// file is loaded in the constructor and written best-effort.
	ConfigurationRootDir string
}

// fswServiceOptions is the persisted form of the global watcher service options.
//
// Only the effective options are stored: they are runtime state with a
// well-defined default, so a corrupt or unreadable document is ignored with a
// warning rather than failing construction.
type fswServiceOptions struct {
	// Version is the document schema version.
	Version int `json:"version"`

	// Enabled is the persisted global enable/disable decision.
	Enabled bool `json:"enabled"`

	// DefaultPollingInterval is the interval used when a watcher sets none.
	DefaultPollingInterval time.Duration `json:"defaultPollingInterval"`

	// MinimumPollingInterval clamps short per-watcher intervals.
	MinimumPollingInterval time.Duration `json:"minimumPollingInterval"`

	// MaximumPollingInterval clamps long per-watcher intervals.
	MaximumPollingInterval time.Duration `json:"maximumPollingInterval"`

	// DisabledCheckInterval is how often a disabled service rechecks itself.
	DisabledCheckInterval time.Duration `json:"disabledCheckInterval"`

	// MaxParallelWatcherScans bounds concurrent scans in one cycle.
	MaxParallelWatcherScans int `json:"maxParallelWatcherScans"`
}

// BackgroundFileWatcherService runs file watcher scans in the background on a scheduled interval.
//
// Each enabled watcher is scanned on its own schedule: a watcher is due when its
// next-due time has elapsed. Scans of watchers that are due in the same cycle run
// concurrently, bounded by MaxParallelWatcherScans; a non-positive value keeps
// the cycle sequential.
//
// The effective global options are immutable snapshots published atomically, so
// a reader never observes a partially applied update and UpdateOptions never
// blocks a running scan.
type BackgroundFileWatcherService struct {
	fileWatcher   core.FileWatcher
	logger        *logging.Runtime
	initialDelay  time.Duration
	configRootDir string

	// options holds the effective options snapshot. It is replaced atomically;
	// the pointer is never mutated in place.
	options atomic.Pointer[core.FileWatcherServiceOptions]

	// persistMu serializes options-file writes so two concurrent updates cannot
	// race on the staging file.
	persistMu sync.Mutex

	// lifecycleMu serializes Start/Stop transitions. It is never held while
	// waiting for the run loop, so Stop can always join a loop that is calling
	// IsEnabled (which reads the options snapshot).
	lifecycleMu sync.Mutex

	stateMu sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool

	scheduleMu   sync.Mutex
	nextScanDue  map[string]time.Time
	warnedClamps map[string]bool
}

// NewBackgroundFileWatcherService creates a new background file watcher service.
//
// The options state file under ConfigurationRootDir is loaded here, so a
// persisted enable/disable decision (and any persisted interval bounds) survive
// a process restart. A missing file is normal; a corrupt one is ignored with a
// safe warning.
//
// Precedence for the effective options:
//  1. a persisted options document (the operator's decision of record),
//  2. otherwise BackgroundFileWatcherServiceOptions.ServiceOptions,
//  3. otherwise the deprecated direct interval fields,
//  4. otherwise the documented defaults.
//
// Construction is default-safe: Enabled=false and a zero or negative
// MaxParallelWatcherScans are "unset" markers here, so the service starts enabled
// with the documented parallel bound. Use SetEnabled(false) or UpdateOptions to
// turn scanning off. A persisted document is validated; one that cannot describe
// a runnable schedule is ignored with a safe warning and the defaults apply.
func NewBackgroundFileWatcherService(opts *BackgroundFileWatcherServiceOptions) (*BackgroundFileWatcherService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.FileWatcher == nil {
		return nil, fmt.Errorf("file watcher cannot be nil: %w", core.ErrInvalidArgument)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	// Set defaults
	initialDelay := opts.InitialDelay
	if initialDelay == 0 {
		initialDelay = 10 * time.Second
	}

	configRootDir := opts.ConfigurationRootDir
	if configRootDir == "" {
		configRootDir = filepath.Join(".locus", "config")
	}

	// Fold the legacy direct fields into the global options: the explicit
	// ServiceOptions value wins, the direct field fills a gap, and the constant
	// default fills what neither set.
	options := opts.ServiceOptions
	if !isPositiveDuration(options.DefaultPollingInterval) && isPositiveDuration(opts.DefaultPollingInterval) {
		options.DefaultPollingInterval = opts.DefaultPollingInterval
	}
	if !isPositiveDuration(options.MinimumPollingInterval) && isPositiveDuration(opts.MinimumPollingInterval) {
		options.MinimumPollingInterval = opts.MinimumPollingInterval
	}
	if !isPositiveDuration(options.MaximumPollingInterval) && isPositiveDuration(opts.MaximumPollingInterval) {
		options.MaximumPollingInterval = opts.MaximumPollingInterval
	}
	if !isPositiveDuration(options.DisabledCheckInterval) && isPositiveDuration(opts.DisabledCheckInterval) {
		options.DisabledCheckInterval = opts.DisabledCheckInterval
	}

	// Default-safe: only an explicitly persisted disable turns the service off,
	// so a caller that leaves every enabled flag at its zero value still gets
	// scanning. The legacy ServiceEnabled and Enabled flags can only confirm the
	// enabled state, never disable it silently; use SetEnabled(false) or
	// UpdateOptions to disable.
	options.Enabled = true

	// A non-positive parallel-scan bound is not usable as a concurrency limit, so
	// a construction-time value that is zero or negative (Go's "unset" marker)
	// falls back to the documented default of 4. Callers that want sequential
	// scanning apply MaxParallelWatcherScans = 0 through UpdateOptions.
	if options.MaxParallelWatcherScans <= 0 {
		options.MaxParallelWatcherScans = defaultMaxParallelScans
	}

	var persisted fswServiceOptions
	loaded := false

	if ptr := loadServiceOptions(configRootDir); ptr != nil {
		persisted = *ptr
		loaded = true

		// The persisted document is the operator's decision of record: it
		// outranks the caller's construction-time defaults entirely.
		options = core.FileWatcherServiceOptions{
			Enabled:                 persisted.Enabled,
			DefaultPollingInterval:  persisted.DefaultPollingInterval,
			MinimumPollingInterval:  persisted.MinimumPollingInterval,
			MaximumPollingInterval:  persisted.MaximumPollingInterval,
			DisabledCheckInterval:   persisted.DisabledCheckInterval,
			MaxParallelWatcherScans: persisted.MaxParallelWatcherScans,
		}
	}

	normalized, err := applyServiceOptionsDefaults(options)
	if err != nil {
		// A persisted document that cannot describe a runnable schedule must not
		// take the service down; fall back to the defaults.
		logger.Emit(context.Background(), logging.Record{
			Level:     slog.LevelWarn,
			Component: "watcher.background",
			Event:     "options_invalid",
			Message:   "Ignoring invalid watcher service options; using defaults",
			Attrs:     []slog.Attr{errorTypeAttr(err)},
		})

		normalized, err = applyServiceOptionsDefaults(core.FileWatcherServiceOptions{
			Enabled:                 true,
			MaxParallelWatcherScans: defaultMaxParallelScans,
		})
		if err != nil {
			return nil, err
		}

		loaded = false
	}

	options = normalized

	service := &BackgroundFileWatcherService{
		fileWatcher:   opts.FileWatcher,
		logger:        logger,
		initialDelay:  initialDelay,
		configRootDir: configRootDir,
		nextScanDue:   make(map[string]time.Time),
		warnedClamps:  make(map[string]bool),
	}

	service.options.Store(&options)

	if loaded {
		service.emit(context.Background(), slog.LevelInfo, "options_loaded", "Loaded watcher service options",
			slog.Bool("enabled", options.Enabled),
			slog.Int("max_parallel_scans", options.MaxParallelWatcherScans))
	}

	return service, nil
}

// Options returns a copy of the effective global watcher service options.
//
// The returned value is a snapshot: mutating it never changes the service.
func (s *BackgroundFileWatcherService) Options() core.FileWatcherServiceOptions {
	if current := s.options.Load(); current != nil {
		return *current
	}

	return core.FileWatcherServiceOptions{}
}

// UpdateOptions applies and persists the global watcher service options.
//
// The call is a FULL REPLACEMENT, not a partial update: values left at their
// zero value are reset to the documented defaults rather than kept. Callers that
// want to change one knob must start from Options and modify that copy:
//
//	current := service.Options()
//	current.Enabled = false
//	_ = service.UpdateOptions(ctx, current)
//
// Validation runs before anything is applied: a rejected update leaves the
// previous options untouched and unpersisted. On success the new options are
// published atomically and written to
// <ConfigurationRootDir>/file-watcher-options.json.
//
// Persisting is best-effort by design: the options document is runtime state,
// not configuration of record, so a failed write keeps the in-memory change,
// logs a safe warning, and still returns nil.
//
// Enabled precedence differs from construction, and the difference is
// deliberate: an explicit Enabled=false here DISABLES scanning, because this
// call is an administrative decision, while Enabled=false at construction only
// means "no decision" and leaves the service enabled.
//
// Errors:
//   - ErrInvalidArgument when a duration is negative, when
//     MinimumPollingInterval is greater than MaximumPollingInterval, when a
//     DefaultPollingInterval (given or defaulted) falls outside those bounds, or
//     when MaxParallelWatcherScans is negative. A zero duration is always
//     accepted as "unset" and replaced by its default.
func (s *BackgroundFileWatcherService) UpdateOptions(ctx context.Context, options core.FileWatcherServiceOptions) error {
	normalized, err := applyServiceOptionsDefaults(options)
	if err != nil {
		return err
	}

	previous := s.Options()
	s.options.Store(&normalized)

	if err := s.persistOptions(&normalized); err != nil {
		s.emit(ctx, slog.LevelWarn, "options_save_failed", "Failed to persist watcher service options",
			slog.String("state_file", fileWatcherOptionsFileName), errorTypeAttr(err))
	}

	s.emit(ctx, slog.LevelInfo, "options_updated", "Updated watcher service options",
		slog.Bool("enabled", normalized.Enabled),
		slog.Bool("enabled_changed", previous.Enabled != normalized.Enabled),
		slog.Duration("default_polling_interval", normalized.DefaultPollingInterval),
		slog.Duration("minimum_polling_interval", normalized.MinimumPollingInterval),
		slog.Duration("maximum_polling_interval", normalized.MaximumPollingInterval),
		slog.Duration("disabled_check_interval", normalized.DisabledCheckInterval),
		slog.Int("max_parallel_scans", normalized.MaxParallelWatcherScans))

	return nil
}

// applyServiceOptionsDefaults replaces every unset (zero) duration with its
// documented default and validates the result.
//
// Zero means "unset" for the interval knobs by the project's default-safe
// convention. It does NOT mean unset for MaxParallelWatcherScans: zero is a
// meaningful runtime value there (sequential scanning), so only a negative bound
// is rejected. MaxParallelWatcherScans is left as given.
//
// The returned value is always a runnable schedule: positive intervals, a
// minimum that does not exceed the maximum, and a default interval inside those
// clamp bounds.
func applyServiceOptionsDefaults(options core.FileWatcherServiceOptions) (core.FileWatcherServiceOptions, error) {
	if options.DefaultPollingInterval == 0 {
		options.DefaultPollingInterval = defaultWatcherPollingInterval
	}
	if options.MinimumPollingInterval == 0 {
		options.MinimumPollingInterval = minimumWatcherPollingInterval
	}
	if options.MaximumPollingInterval == 0 {
		options.MaximumPollingInterval = maximumWatcherPollingInterval
	}
	if options.DisabledCheckInterval == 0 {
		options.DisabledCheckInterval = defaultDisabledCheckInterval
	}

	if options.DefaultPollingInterval < 0 {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("default polling interval cannot be negative: %w", core.ErrInvalidArgument)
	}
	if options.MinimumPollingInterval <= 0 {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("minimum polling interval must be positive: %w", core.ErrInvalidArgument)
	}
	if options.MaximumPollingInterval <= 0 {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("maximum polling interval must be positive: %w", core.ErrInvalidArgument)
	}
	if options.MinimumPollingInterval > options.MaximumPollingInterval {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("minimum polling interval cannot exceed the maximum: %w", core.ErrInvalidArgument)
	}
	if options.DefaultPollingInterval < options.MinimumPollingInterval {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("default polling interval cannot be shorter than the minimum: %w", core.ErrInvalidArgument)
	}
	if options.DefaultPollingInterval > options.MaximumPollingInterval {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("default polling interval cannot exceed the maximum: %w", core.ErrInvalidArgument)
	}
	if options.DisabledCheckInterval <= 0 {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("disabled check interval must be positive: %w", core.ErrInvalidArgument)
	}
	if options.MaxParallelWatcherScans < 0 {
		return core.FileWatcherServiceOptions{}, fmt.Errorf("max parallel watcher scans cannot be negative: %w", core.ErrInvalidArgument)
	}

	return options, nil
}

// isPositiveDuration reports whether value is set to a positive duration.
func isPositiveDuration(value time.Duration) bool {
	return value > 0
}

// parallelScanLimit returns the effective concurrent-scan bound: a non-positive
// configured value means sequential scanning.
func (s *BackgroundFileWatcherService) parallelScanLimit() int {
	limit := s.Options().MaxParallelWatcherScans
	if limit <= 0 {
		return 1
	}

	return limit
}

// Start starts the background file watcher service.
func (s *BackgroundFileWatcherService) Start() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.stateMu.Lock()
	if s.running {
		s.stateMu.Unlock()

		return fmt.Errorf("background file watcher service is already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running = true
	ctx := s.ctx
	s.stateMu.Unlock()

	s.wg.Add(1)
	go s.run(ctx)

	s.emit(ctx, slog.LevelInfo, "started", "Background file watcher service started")

	return nil
}

// Stop stops the background file watcher service gracefully.
//
// Stop never holds a lock while waiting for the run loop, so a loop that is
// checking IsEnabled (or scanning) can always finish and be joined.
func (s *BackgroundFileWatcherService) Stop() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.stateMu.Lock()
	if !s.running {
		s.stateMu.Unlock()

		return fmt.Errorf("background file watcher service is not running")
	}

	s.running = false
	ctx := s.ctx
	cancel := s.cancel
	s.stateMu.Unlock()

	s.emit(ctx, slog.LevelInfo, "stopping", "Stopping background file watcher service")

	cancel()
	s.wg.Wait()

	if ctx != nil {
		s.emit(ctx, slog.LevelInfo, "stopped", "Background file watcher service stopped")
	}

	return nil
}

// IsRunning returns whether the service is currently running.
func (s *BackgroundFileWatcherService) IsRunning() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	return s.running
}

// SetEnabled sets whether the service is enabled and persists the decision so
// it survives a process restart.
//
// Only the enabled flag changes: the call starts from Options(), so every other
// option is preserved rather than reset. A disabled service keeps running its
// loop but scans nothing, and rechecks every DisabledCheckInterval, so
// SetEnabled(true) resumes scanning without a restart.
//
// Persisting is best-effort: a failed write keeps the in-memory change, logs a
// safe warning, and does not surface an error, because the options document is
// runtime state rather than configuration of record.
func (s *BackgroundFileWatcherService) SetEnabled(enabled bool) {
	options := s.Options()
	options.Enabled = enabled

	// UpdateOptions re-validates and persists; the in-memory value is already
	// published before the write is attempted.
	if err := s.UpdateOptions(context.Background(), options); err != nil {
		s.emit(context.Background(), slog.LevelWarn, "options_update_failed", "Failed to apply watcher service options", errorTypeAttr(err))
	}
}

// IsEnabled returns whether the service is enabled.
func (s *BackgroundFileWatcherService) IsEnabled() bool {
	return s.Options().Enabled
}

// run is the main loop that executes file watcher scans on a schedule.
func (s *BackgroundFileWatcherService) run(ctx context.Context) {
	defer s.wg.Done()

	s.emit(ctx, slog.LevelInfo, "initial_delay", "Background file watcher service waiting for initial delay", slog.Duration("delay", s.initialDelay))

	select {
	case <-time.After(s.initialDelay):
		// Continue
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-ctx.Done():
			s.emit(ctx, slog.LevelInfo, "shutting_down", "File watcher service shutting down")

			return
		default:
		}

		options := s.Options()

		if !options.Enabled {
			s.emit(ctx, slog.LevelDebug, "disabled", "File watcher service is globally disabled", slog.Duration("check_interval", options.DisabledCheckInterval))

			select {
			case <-time.After(options.DisabledCheckInterval):
				continue
			case <-ctx.Done():
				return
			}
		}

		interval := s.executeScanCycle(ctx)
		s.emit(ctx, slog.LevelDebug, "next_scan", "Next scan cycle", slog.Duration("interval", interval))

		select {
		case <-time.After(interval):
			// Continue to next cycle
		case <-ctx.Done():
			return
		}
	}
}

// executeScanCycle scans every watcher that is currently due and returns the
// delay until the next watcher becomes due.
func (s *BackgroundFileWatcherService) executeScanCycle(ctx context.Context) time.Duration {
	return s.executeScanCycleAt(ctx, timeNow())
}

// executeScanCycleAt is executeScanCycle with an explicit clock for tests.
func (s *BackgroundFileWatcherService) executeScanCycleAt(ctx context.Context, now time.Time) time.Duration {
	options := s.Options()

	s.emit(ctx, slog.LevelInfo, "cycle_started", "Starting file watcher scan cycle")

	watchers, err := s.fileWatcher.GetAllWatchers(ctx)
	if err != nil {
		s.emit(ctx, slog.LevelError, "watchers_get_failed", "Failed to get watchers", errorTypeAttr(err))

		return options.DefaultPollingInterval
	}

	enabled := make([]*core.FileWatcherConfiguration, 0, len(watchers))
	for _, watcher := range watchers {
		if watcher.Enabled {
			enabled = append(enabled, watcher)
		}
	}

	if len(enabled) == 0 {
		s.emit(ctx, slog.LevelDebug, "no_enabled_watchers", "No enabled watchers found; skipping scan")
		s.pruneSchedule(nil)

		return options.DefaultPollingInterval
	}

	enabledIDs := make(map[string]bool, len(enabled))
	for _, watcher := range enabled {
		enabledIDs[watcher.WatcherID] = true
	}
	s.pruneSchedule(enabledIDs)

	due := make([]*core.FileWatcherConfiguration, 0, len(enabled))
	for _, watcher := range enabled {
		if !s.nextDue(watcher, now).After(now) {
			due = append(due, watcher)
		}
	}

	if len(due) == 0 {
		return s.delayUntilNextDue(enabled, now)
	}

	s.emit(ctx, slog.LevelInfo, "watchers_scanning", "Scanning due watchers",
		slog.Int("count", len(due)), slog.Int("enabled", len(enabled)), slog.Int("max_parallel", s.parallelScanLimit()))

	startTime := time.Now()

	totals := s.scanDueWatchers(ctx, due, now)

	s.emit(ctx, slog.LevelInfo, "cycle_completed", "File watcher scan cycle completed",
		slog.Duration("duration", time.Since(startTime)),
		slog.Int("total_imported", totals.imported),
		slog.Int("total_failed", totals.failed),
		slog.Int("total_skipped", totals.skipped),
		slog.Int64("total_bytes", totals.bytes))

	// Re-evaluate against the same clock that drove this cycle; using the wall
	// clock here would desynchronize the deterministically advanced schedule.
	nextNow := timeNow()
	if !nextNow.After(now) {
		nextNow = now
	}

	return s.delayUntilNextDue(enabled, nextNow)
}

// scanTotals accumulates the per-watcher scan results of one cycle.
type scanTotals struct {
	imported int
	failed   int
	skipped  int
	bytes    int64
}

// scanDueWatchers scans the due watchers with at most parallelScanLimit scans in
// flight and returns the merged totals.
//
// The bound is a semaphore owned by this cycle. A non-positive configured limit
// degrades to a sequential loop rather than to an unbounded fan-out.
func (s *BackgroundFileWatcherService) scanDueWatchers(ctx context.Context, due []*core.FileWatcherConfiguration, now time.Time) scanTotals {
	limit := s.parallelScanLimit()

	var total scanTotals

	if limit <= 1 || len(due) == 1 {
		for _, watcher := range due {
			if ctx.Err() != nil {
				break // Service stopping
			}

			total.add(s.scanWatcher(ctx, watcher, now))
		}

		return total
	}

	// Parallel path: the semaphore is created per cycle so a changed limit takes
	// effect on the next cycle without reconfiguring a running one.
	semaphore := make(chan struct{}, limit)

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

	for _, watcher := range due {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{}

		go func(target *core.FileWatcherConfiguration) {
			defer wg.Done()
			defer func() { <-semaphore }()

			result := s.scanWatcher(ctx, target, now)

			mu.Lock()
			defer mu.Unlock()

			total.add(result)
		}(watcher)
	}

	wg.Wait()

	return total
}

// add merges one watcher's scan result into the cycle totals.
func (t *scanTotals) add(partial scanTotals) {
	t.imported += partial.imported
	t.failed += partial.failed
	t.skipped += partial.skipped
	t.bytes += partial.bytes
}

// scanWatcher scans one watcher, schedules its next attempt and returns its
// totals. The next attempt is scheduled regardless of the outcome so a failing
// watcher is retried on its own interval instead of every cycle.
func (s *BackgroundFileWatcherService) scanWatcher(
	ctx context.Context,
	watcher *core.FileWatcherConfiguration,
	now time.Time,
) scanTotals {
	defer s.scheduleNext(watcher, now)

	result, err := s.fileWatcher.ScanNow(ctx, watcher.WatcherID)
	if err != nil {
		s.emit(ctx, slog.LevelError, "watcher_scan_failed", "Failed to scan watcher", slog.String("watcher_id", watcher.WatcherID), errorTypeAttr(err))

		return scanTotals{}
	}

	if result.FilesImported > 0 || result.FilesFailed > 0 {
		s.emit(ctx, slog.LevelInfo, "watcher_scan_completed", "Watcher scan completed",
			slog.String("watcher_id", watcher.WatcherID),
			slog.Int("discovered", result.FilesDiscovered),
			slog.Int("imported", result.FilesImported),
			slog.Int("skipped", result.FilesSkipped),
			slog.Int("failed", result.FilesFailed),
			slog.Int64("bytes", result.BytesImported),
			slog.Duration("duration", result.ScanDuration))

		if len(result.Errors) > 0 {
			s.emit(ctx, slog.LevelWarn, "watcher_scan_errors", "Watcher scan reported errors",
				slog.String("watcher_id", watcher.WatcherID), slog.Int("count", len(result.Errors)))
		}
	} else if result.FilesDiscovered > 0 {
		s.emit(ctx, slog.LevelDebug, "watcher_all_skipped", "Watcher found files but all were skipped",
			slog.String("watcher_id", watcher.WatcherID),
			slog.Int("count", result.FilesDiscovered))
	}

	return scanTotals{
		imported: result.FilesImported,
		failed:   result.FilesFailed,
		skipped:  result.FilesSkipped,
		bytes:    result.BytesImported,
	}
}

// nextDue returns the time a watcher is next due, creating the schedule entry on
// first sight so a newly registered watcher is scanned immediately.
//
// Steady state: the recorded due time is capped at now + the watcher's current
// interval. Without the cap a watcher whose configuration was updated would keep
// sleeping until its previous, longer due time, so an UpdateWatcher that shortens
// a schedule or moves a watch path would not take effect until that stale
// deadline passed.
func (s *BackgroundFileWatcherService) nextDue(watcher *core.FileWatcherConfiguration, now time.Time) time.Time {
	maxDue := now.Add(s.pollingIntervalFor(watcher))

	s.scheduleMu.Lock()
	defer s.scheduleMu.Unlock()

	due, ok := s.nextScanDue[watcher.WatcherID]
	if !ok {
		s.nextScanDue[watcher.WatcherID] = now

		return now
	}

	if due.After(maxDue) {
		due = maxDue
		s.nextScanDue[watcher.WatcherID] = due
	}

	return due
}

// scheduleNext records when a watcher should be scanned next.
func (s *BackgroundFileWatcherService) scheduleNext(watcher *core.FileWatcherConfiguration, now time.Time) {
	interval := s.pollingIntervalFor(watcher)

	s.scheduleMu.Lock()
	defer s.scheduleMu.Unlock()

	s.nextScanDue[watcher.WatcherID] = now.Add(interval)
}

// delayUntilNextDue returns the delay until the earliest watcher becomes due.
func (s *BackgroundFileWatcherService) delayUntilNextDue(watchers []*core.FileWatcherConfiguration, now time.Time) time.Duration {
	if len(watchers) == 0 {
		return s.Options().DefaultPollingInterval
	}

	due := make([]time.Time, 0, len(watchers))
	for _, watcher := range watchers {
		due = append(due, s.nextDue(watcher, now))
	}

	sort.Slice(due, func(i, j int) bool { return due[i].Before(due[j]) })

	delay := due[0].Sub(now)
	if delay <= 0 {
		// The watcher is already due; re-check promptly without busy-looping.
		return 200 * time.Millisecond
	}

	return delay
}

// pruneSchedule drops schedule entries for watchers that are no longer enabled.
func (s *BackgroundFileWatcherService) pruneSchedule(enabledIDs map[string]bool) {
	s.scheduleMu.Lock()
	defer s.scheduleMu.Unlock()

	for watcherID := range s.nextScanDue {
		if enabledIDs == nil || !enabledIDs[watcherID] {
			delete(s.nextScanDue, watcherID)
		}
	}

	for watcherID := range s.warnedClamps {
		if enabledIDs == nil || !enabledIDs[watcherID] {
			delete(s.warnedClamps, watcherID)
		}
	}
}

// pollingIntervalFor returns a watcher's effective polling interval, clamped to
// [MinimumPollingInterval, MaximumPollingInterval].
func (s *BackgroundFileWatcherService) pollingIntervalFor(watcher *core.FileWatcherConfiguration) time.Duration {
	options := s.Options()

	interval := watcher.PollingInterval
	if interval <= 0 {
		interval = options.DefaultPollingInterval
	}

	switch {
	case interval < options.MinimumPollingInterval:
		s.warnClamp(watcher.WatcherID, interval, options.MinimumPollingInterval, true)
		interval = options.MinimumPollingInterval
	case interval > options.MaximumPollingInterval:
		s.warnClamp(watcher.WatcherID, interval, options.MaximumPollingInterval, false)
		interval = options.MaximumPollingInterval
	}

	return interval
}

// warnClamp emits the interval clamp notice once per watcher and direction.
func (s *BackgroundFileWatcherService) warnClamp(watcherID string, requested, applied time.Duration, belowMinimum bool) {
	s.scheduleMu.Lock()
	alreadyWarned := s.warnedClamps[watcherID]
	if !alreadyWarned {
		s.warnedClamps[watcherID] = true
	}
	s.scheduleMu.Unlock()

	if alreadyWarned {
		return
	}

	if belowMinimum {
		s.emit(context.Background(), slog.LevelWarn, "polling_interval_clamped", "Polling interval too short; using minimum",
			slog.String("watcher_id", watcherID),
			slog.Duration("requested", requested),
			slog.Duration("minimum", applied))

		return
	}

	s.emit(context.Background(), slog.LevelWarn, "polling_interval_clamped", "Polling interval too long; using maximum",
		slog.String("watcher_id", watcherID),
		slog.Duration("requested", requested),
		slog.Duration("maximum", applied))
}

// persistOptions writes the effective options atomically.
//
// The document is staged in a unique temp file in the same directory, synced,
// closed, and then renamed over the target, so a reader never observes a
// partial document and every failure path removes the staging file.
func (s *BackgroundFileWatcherService) persistOptions(options *core.FileWatcherServiceOptions) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	if err := os.MkdirAll(s.configRootDir, 0755); err != nil {
		return fmt.Errorf("failed to create watcher options directory: %w", err)
	}

	document := fswServiceOptions{
		Version:                 fileWatcherOptionsVersion,
		Enabled:                 options.Enabled,
		DefaultPollingInterval:  options.DefaultPollingInterval,
		MinimumPollingInterval:  options.MinimumPollingInterval,
		MaximumPollingInterval:  options.MaximumPollingInterval,
		DisabledCheckInterval:   options.DisabledCheckInterval,
		MaxParallelWatcherScans: options.MaxParallelWatcherScans,
	}

	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode watcher service options: %w", err)
	}

	temp, err := os.CreateTemp(s.configRootDir, fileWatcherOptionsTempFilePattern)
	if err != nil {
		return fmt.Errorf("failed to create watcher options staging file: %w", err)
	}

	tempPath := temp.Name()

	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to write watcher options staging file: %w", err)
	}

	// Sync before publishing so a crash after the rename cannot leave an empty
	// document behind.
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to sync watcher options staging file: %w", err)
	}

	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to close watcher options staging file: %w", err)
	}

	if err := os.Rename(tempPath, filepath.Join(s.configRootDir, fileWatcherOptionsFileName)); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to publish watcher service options: %w", err)
	}

	return nil
}

// loadServiceOptions reads the persisted options document.
//
// It returns nil when the document is missing, unreadable, corrupt, or written
// by an unknown schema version: persisted runtime options must never prevent the
// service from starting, so every failure is silent here and the caller falls
// back to its defaults.
func loadServiceOptions(configRootDir string) *fswServiceOptions {
	data, err := os.ReadFile(filepath.Join(configRootDir, fileWatcherOptionsFileName))
	if err != nil {
		return nil
	}

	var document fswServiceOptions
	if err := json.Unmarshal(data, &document); err != nil {
		return nil
	}

	if document.Version != fileWatcherOptionsVersion {
		return nil
	}

	return &document
}

func (s *BackgroundFileWatcherService) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	s.logger.Emit(ctx, logging.Record{
		Level: level, Component: "watcher.background", Event: event, Message: message, Attrs: attrs,
	})
}
