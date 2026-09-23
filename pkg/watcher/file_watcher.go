package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

const (
	// importedFilesHistoryFileName is the persisted import de-duplication history.
	importedFilesHistoryFileName = "imported-files.json"

	// importedFilesHistoryTempFileName is the staging file used for the atomic
	// temp-file + rename history write.
	importedFilesHistoryTempFileName = "imported-files.json.tmp"

	// maxImportedFilesHistory caps the persisted history so it cannot grow
	// without bound. Oldest entries are dropped first.
	maxImportedFilesHistory = 10000

	// maxRecordedErrorsPerScanResult caps the errors carried by one scan result.
	maxRecordedErrorsPerScanResult = 100

	// defaultImportFingerprint is recorded when a stable fingerprint could not be
	// computed for an imported file. Such a file is still imported only once.
	defaultImportFingerprint = "-"

	// wildcardPattern matches every file name.
	wildcardPattern = "*"

	// defaultAutoCreateTenantDirectoriesCacheTTL is the runtime default of
	// FileWatcherConfiguration.AutoCreateTenantDirectoriesCacheTTL.
	defaultAutoCreateTenantDirectoriesCacheTTL = 60 * time.Second

	// defaultSkipStabilityCheckAfterAge is the runtime default of
	// FileWatcherConfiguration.SkipStabilityCheckAfterAge: a candidate at least
	// this old skips the delayed stability probe.
	defaultSkipStabilityCheckAfterAge = time.Minute

	// defaultImportedFilesPruneInterval is the runtime default of
	// FileWatcherConfiguration.ImportedFilesPruneInterval.
	defaultImportedFilesPruneInterval = 5 * time.Minute

	// defaultImportedFilesHistoryFlushInterval is the runtime default of
	// FileWatcherConfiguration.ImportedFilesHistoryFlushInterval.
	defaultImportedFilesHistoryFlushInterval = 2 * time.Second
	defaultMaxPostImportActionRetryCount     = 5
	defaultPostImportActionRetryInitialDelay = 5 * time.Second
	defaultPostImportActionRetryMaxDelay     = 5 * time.Minute

	// fingerprintSampleSize matches the latest Locus watcher: the beginning,
	// middle and end of a file are sampled without loading the whole payload.
	fingerprintSampleSize = 4 * 1024
)

// FileWatcherOptions configures the file watcher.
type FileWatcherOptions struct {
	// TenantManager is required for tenant validation and auto-creation.
	TenantManager core.TenantManager

	// StoragePool is required for file imports.
	StoragePool core.StoragePool

	// ConfigurationRootDir is the directory for storing watcher state/configuration.
	// Default: "./.locus/watchers"
	ConfigurationRootDir string

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// StatisticsRecorder optionally receives in-process scan statistics.
	// Nil means recording is disabled.
	StatisticsRecorder core.StatisticsRecorder

	// SourceCleanupStore optionally provides durable post-import cleanup.
	// Nil preserves the legacy in-memory/history behavior.
	SourceCleanupStore SourceCleanupStore

	// SourceCleanupWorkerOptions enables the durable cleanup worker. The worker
	// is started and stopped by the Venue lifecycle, not by construction.
	SourceCleanupWorkerOptions *SourceCleanupWorkerOptions
}

// importedFileRecord is the persisted de-duplication state for one source path.
type importedFileRecord struct {
	// Fingerprint identifies the exact content revision that was imported.
	Fingerprint string `json:"fingerprint"`

	// ImportedAtUnix is when the record was written; used to cap history size.
	ImportedAtUnix int64 `json:"time"`

	// PendingPostImportAction separates a durable storage success from the
	// source delete or move that still has to finish.
	PendingPostImportAction bool                  `json:"pendingPostImportAction,omitempty"`
	FileKey                 string                `json:"fileKey,omitempty"`
	WatcherID               string                `json:"watcherId,omitempty"`
	TenantID                string                `json:"tenantId,omitempty"`
	PostImportAction        core.PostImportAction `json:"postImportAction,omitempty"`
	MoveTargetPath          string                `json:"moveTargetPath,omitempty"`
	FailureCount            int                   `json:"failureCount,omitempty"`
	NextAttemptUnixNano     int64                 `json:"nextAttemptUnixNano,omitempty"`
	Quarantined             bool                  `json:"quarantined,omitempty"`

	// InFlightToken is set only in memory while an import owns the path; it is
	// never persisted.
	InFlightToken string `json:"-"`
}

// fileWatcher implements the core.FileWatcher interface.
//
// De-duplication invariant: a source path is imported once per fingerprint
// (size + modification time + sampled content hash) while it is already imported, or while an import
// of the same revision is in flight. The record is removed once a Delete or
// Move post-import action succeeds, so a refilled source path is imported
// again; Keep retains it.
type fileWatcher struct {
	tenantMgr   core.TenantManager
	storagePool core.StoragePool
	watchers    sync.Map // map[string]*watcherEntry
	configRoot  string   // Configuration root directory
	logger      *logging.Runtime

	// statistics optionally receives scan statistics. Nil disables recording.
	statistics core.StatisticsRecorder

	sourceCleanupStore  SourceCleanupStore
	sourceCleanupWorker *SourceCleanupWorker

	// now reads the current time. It is injectable so the tenant-directory cache
	// TTL, the prune throttle and the flush debounce window can be exercised
	// deterministically in tests. It must not be replaced once a scan is running.
	now func() time.Time

	// stabilityWait waits out one delayed stability probe. It is injectable for
	// the same reason as now; the default observes the scan context.
	stabilityWait func(ctx context.Context, delay time.Duration) error

	// scheduleHistoryFlush schedules a debounced history write and returns its
	// cancelable handle. It is injectable for the same reason as now; the
	// default is time.AfterFunc, which never invokes fn inline.
	scheduleHistoryFlush func(delay time.Duration, fn func()) *time.Timer

	// tenantCacheMu guards tenantCache.
	tenantCacheMu sync.Mutex
	// tenantCache holds the cached tenant enumeration per watcher ID.
	tenantCache map[string]cachedTenantList

	// pruneMu guards prunedOnce and lastPrunedAt.
	pruneMu sync.Mutex
	// prunedOnce reports whether the throttled history prune ran at least once.
	prunedOnce bool
	// lastPrunedAt is when the throttled history prune last ran.
	lastPrunedAt time.Time

	// importedFilesMu guards importedCount and the flush bookkeeping, and
	// serializes history writes.
	importedFilesMu sync.Mutex
	importedFiles   sync.Map // map[string]importedFileRecord
	importedCount   int

	// historyFlushDirty marks history changes that are not persisted yet.
	historyFlushDirty bool
	// historyFlushTimer is non-nil while one debounced write is scheduled. At
	// most one timer exists, which is what bounds writes to one per interval.
	historyFlushTimer *time.Timer
	// lastHistoryFlushAt is when the last debounced write was started.
	lastHistoryFlushAt time.Time

	// flushDone is non-nil while a history write is in flight.
	flushDone chan struct{}
	// flushAgain requests one more write pass after the in-flight one, because a
	// change arrived after that pass snapshotted the history.
	flushAgain bool

	// watcherStateMu guards watcherState and serializes state file writes.
	// watcherState holds the operator's runtime enable/disable decisions, which
	// outrank the configured Enabled flag on registration.
	watcherStateMu sync.Mutex
	watcherState   map[string]bool // map[string]bool: watcher ID -> enabled

	closed bool
}

// cachedTenantList is one watcher's cached tenant enumeration and its expiry.
type cachedTenantList struct {
	// tenants is the cached enumeration. It is owned by the cache and never
	// mutated after publication.
	tenants []core.TenantContext

	// expiresAt is the instant the entry stops being reusable.
	expiresAt time.Time
}

// watcherEntry stores one registered watcher. config is an immutable snapshot;
// mu guards its Enabled flag and any later replacement.
type watcherEntry struct {
	mu     sync.Mutex
	config *core.FileWatcherConfiguration
}

// NewFileWatcher creates a new file watcher.
func NewFileWatcher(opts *FileWatcherOptions) (core.FileWatcher, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.TenantManager == nil {
		return nil, fmt.Errorf("tenant manager cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.StoragePool == nil {
		return nil, fmt.Errorf("storage pool cannot be nil: %w", core.ErrInvalidArgument)
	}

	// Set default configuration root if not specified
	configRoot := opts.ConfigurationRootDir
	if configRoot == "" {
		configRoot = filepath.Join(".locus", "watchers")
	}

	// Ensure configuration directory exists
	if err := os.MkdirAll(configRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create configuration directory: %w", err)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	fw := &fileWatcher{
		tenantMgr:            opts.TenantManager,
		storagePool:          opts.StoragePool,
		configRoot:           configRoot,
		logger:               logger,
		statistics:           opts.StatisticsRecorder,
		sourceCleanupStore:   opts.SourceCleanupStore,
		now:                  time.Now,
		stabilityWait:        waitForStabilityDelay,
		scheduleHistoryFlush: time.AfterFunc,
		tenantCache:          make(map[string]cachedTenantList),
	}

	// Load imported files history
	if err := fw.loadImportedFilesHistory(); err != nil {
		fw.emit(context.Background(), slog.LevelWarn, "history_load_failed", "Failed to load imported files history", errorTypeAttr(err))
	}

	// Load the persisted runtime enable/disable decisions. This is best-effort
	// runtime state: an unreadable document is ignored with a safe warning
	// instead of failing construction, and the configured Enabled flag applies.
	if err := fw.loadWatcherState(); err != nil {
		fw.emit(context.Background(), slog.LevelWarn, "watcher_state_load_failed", "Failed to load watcher runtime state",
			slog.String("state_file", watcherStateFileName), errorTypeAttr(err))
	}
	if opts.SourceCleanupStore != nil && opts.SourceCleanupWorkerOptions != nil {
		workerOptions := *opts.SourceCleanupWorkerOptions
		workerOptions.Store = opts.SourceCleanupStore
		if workerOptions.Execute == nil {
			workerOptions.Execute = fw.executeSourceCleanupJob
		}
		if workerOptions.Quarantine == nil {
			workerOptions.Quarantine = fw.quarantineSourceCleanupJob
		}
		worker, err := NewSourceCleanupWorker(workerOptions)
		if err != nil {
			return nil, err
		}
		fw.sourceCleanupWorker = worker
	}
	if fw.sourceCleanupStore != nil {
		if err := fw.migrateLegacySourceCleanup(); err != nil {
			fw.emit(context.Background(), slog.LevelWarn, "source_cleanup_migration_failed", "Failed to migrate legacy source cleanup records", errorTypeAttr(err))
		}
	}

	return fw, nil
}

// migrateLegacySourceCleanup moves pending imported-files.json records into the
// durable store before scans can process them. A record is removed from the
// legacy history only after its SQLite reservation and imported state are safe.
func (w *fileWatcher) migrateLegacySourceCleanup() error {
	var migrated int
	var migrationErr error
	w.importedFiles.Range(func(key, value any) bool {
		filePath, ok := key.(string)
		record, recordOK := value.(importedFileRecord)
		if !ok || !recordOK || record.WatcherID == "" || record.TenantID == "" {
			return true
		}
		action := sourceCleanupAction(record.PostImportAction)
		if !record.PendingPostImportAction {
			action = SourceCleanupActionKeep
		}
		job := SourceCleanupJob{
			WatcherID:         record.WatcherID,
			TenantID:          record.TenantID,
			SourcePath:        filePath,
			Fingerprint:       record.Fingerprint,
			FileKey:           record.FileKey,
			Action:            action,
			MoveTargetPath:    record.MoveTargetPath,
			MaxAttempts:       defaultMaxPostImportActionRetryCount,
			RetryInitialDelay: defaultPostImportActionRetryInitialDelay,
			RetryMaxDelay:     defaultPostImportActionRetryMaxDelay,
			FailureDirectory:  filepath.Join(w.configRoot, "locus-source-failed"),
		}
		reserved, err := w.sourceCleanupStore.TryReserve(context.Background(), &job)
		if err != nil {
			migrationErr = errors.Join(migrationErr, err)
			return true
		}
		if reserved {
			if err := w.sourceCleanupStore.MarkImported(context.Background(), job.JobID, record.FileKey, "", time.Now()); err != nil {
				migrationErr = errors.Join(migrationErr, err)
				return true
			}
		} else {
			existing, loadErr := w.sourceCleanupStore.GetBySource(context.Background(), job.WatcherID, job.SourcePath)
			if loadErr != nil {
				migrationErr = errors.Join(migrationErr, loadErr)
				return true
			}
			if existing == nil || existing.State == SourceCleanupStateImporting {
				if existing == nil || existing.FileKey == "" {
					migrationErr = errors.Join(migrationErr, fmt.Errorf("legacy source cleanup reservation was not persisted for %s", filepath.Base(filePath)))
					return true
				}
			}
		}
		w.importedFiles.Delete(filePath)
		migrated++
		return true
	})
	if migrated > 0 {
		if err := w.saveImportedFilesHistory(); err != nil {
			migrationErr = errors.Join(migrationErr, err)
		}
	}
	return migrationErr
}

// Close persists the import de-duplication history.
//
// Close is safe to call multiple times and is intended to be deferred next to
// the watcher's lifetime; it does not stop the background service that drives
// scans. Every history change that is still pending is written before Close
// returns, including changes a debounced history write had not persisted yet.
func (w *fileWatcher) Close() error {
	if w.sourceCleanupWorker != nil {
		w.sourceCleanupWorker.Stop()
	}
	w.importedFilesMu.Lock()
	if w.closed {
		w.importedFilesMu.Unlock()

		return nil
	}
	w.closed = true

	// The synchronous flush below supersedes any scheduled debounced write.
	timer := w.historyFlushTimer
	w.historyFlushTimer = nil
	w.historyFlushDirty = false
	w.importedFilesMu.Unlock()

	if timer != nil {
		timer.Stop()
	}

	w.awaitPendingHistoryFlush()

	return w.saveImportedFilesHistory()
}

// StartSourceCleanup starts the durable source cleanup worker when configured.
func (w *fileWatcher) StartSourceCleanup(ctx context.Context) {
	if w.sourceCleanupWorker != nil {
		w.sourceCleanupWorker.Start(ctx)
	}
}

// StopSourceCleanup stops the durable source cleanup worker when configured.
func (w *fileWatcher) StopSourceCleanup() {
	if w.sourceCleanupWorker != nil {
		w.sourceCleanupWorker.Stop()
	}
}

// RegisterWatcher adds a new file watcher configuration.
//
// Registration normalizes the configuration into an immutable snapshot:
// defaults are applied, blank file patterns are dropped (falling back to "*"),
// invalid globs are rejected, and negative concurrency is clamped to 1. The
// watch directory is created when missing.
//
// Precedence: a persisted runtime enable/disable decision for the watcher ID
// (written by EnableWatcher/DisableWatcher and stored under
// ConfigurationRootDir) overrides the configured Enabled flag, so an operator's
// decision survives a process restart. A watcher ID without a persisted
// decision keeps the configured Enabled value. Registration never clears
// persisted state; UnregisterWatcher does.
func (w *fileWatcher) RegisterWatcher(ctx context.Context, config *core.FileWatcherConfiguration) error {
	if config == nil {
		return fmt.Errorf("configuration cannot be nil: %w", core.ErrInvalidArgument)
	}

	if config.WatcherID == "" {
		return fmt.Errorf("watcher ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	stored, err := w.prepareConfiguration(config)
	if err != nil {
		return err
	}

	w.watchers.Store(stored.WatcherID, &watcherEntry{config: stored})

	// Auto-create tenant directories if enabled
	if stored.MultiTenantMode && stored.AutoCreateTenantDirectories {
		if err := w.createTenantDirectories(ctx, stored); err != nil {
			w.emit(ctx, slog.LevelWarn, "tenant_directories_create_failed", "Failed to auto-create tenant directories", slog.String("watcher_id", stored.WatcherID), errorTypeAttr(err))
		}
	}

	return nil
}

// UpdateWatcher replaces the configuration of an existing watcher in place.
//
// The watcher ID is the identity of record: the entry keeps its slot in the
// registry, so a scan that is already running is unaffected and no caller has to
// unregister and re-register to change a setting. Only the configuration is
// replaced — the persisted import de-duplication history and the operator's
// persisted enable/disable decision are owned by the watcher ID and are
// therefore preserved, exactly as they are on registration.
//
// Validation and normalization are shared with RegisterWatcher, so an update
// cannot accept a configuration that registration would reject. A rejected
// update leaves the previous configuration untouched.
//
// Errors:
//   - ErrWatcherNotFound when no watcher has that ID
//   - ErrInvalidArgument when the configuration is invalid
func (w *fileWatcher) UpdateWatcher(ctx context.Context, config *core.FileWatcherConfiguration) error {
	if config == nil {
		return fmt.Errorf("configuration cannot be nil: %w", core.ErrInvalidArgument)
	}

	if config.WatcherID == "" {
		return fmt.Errorf("watcher ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	entry, err := w.loadEntry(config.WatcherID)
	if err != nil {
		return err
	}

	// Validate before mutating anything: a rejected update must not leave a
	// half-applied configuration behind.
	stored, err := w.prepareConfiguration(config)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	entry.config = stored
	entry.mu.Unlock()

	// Keep the schedule bookkeeping honest: a watcher that moved or changed its
	// interval must be re-evaluated by the background service on its next cycle,
	// which reads the new configuration from the registry.
	w.emit(ctx, slog.LevelInfo, "watcher_updated", "Updated file watcher configuration",
		slog.String("watcher_id", stored.WatcherID),
		slog.Bool("enabled", stored.Enabled),
		slog.Bool("multi_tenant_mode", stored.MultiTenantMode))

	if stored.MultiTenantMode && stored.AutoCreateTenantDirectories {
		if err := w.createTenantDirectories(ctx, stored); err != nil {
			w.emit(ctx, slog.LevelWarn, "tenant_directories_create_failed", "Failed to auto-create tenant directories", slog.String("watcher_id", stored.WatcherID), errorTypeAttr(err))
		}
	}

	return nil
}

// prepareConfiguration validates a caller-supplied configuration with
// RegisterWatcher's rules and returns the immutable snapshot to store.
//
// The snapshot is normalized: blank file patterns are dropped (falling back to
// "*"), invalid globs are rejected, negative concurrency is rejected, zero
// polling interval and minimum file age fall back to the runtime defaults, and
// the watch directory is created when missing. A persisted runtime
// enable/disable decision for the watcher ID outranks the configured Enabled
// flag.
//
// The returned pointer is owned by the caller and must not alias caller state.
func (w *fileWatcher) prepareConfiguration(config *core.FileWatcherConfiguration) (*core.FileWatcherConfiguration, error) {
	if config.WatchPath == "" {
		return nil, fmt.Errorf("watch path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Validate tenant in single-tenant mode
	if !config.MultiTenantMode && config.TenantID == "" {
		return nil, fmt.Errorf("tenant ID required in single-tenant mode: %w", core.ErrInvalidArgument)
	}

	if config.MaxConcurrentImports < 0 {
		return nil, fmt.Errorf("max concurrent imports cannot be negative: %w", core.ErrInvalidArgument)
	}
	if config.MaxPostImportActionRetryCount < 0 {
		return nil, fmt.Errorf("max post-import action retry count cannot be negative: %w", core.ErrInvalidArgument)
	}
	if config.PostImportActionRetryInitialDelay < 0 {
		return nil, fmt.Errorf("post-import action retry initial delay cannot be negative: %w", core.ErrInvalidArgument)
	}
	if config.PostImportActionRetryMaxDelay < 0 {
		return nil, fmt.Errorf("post-import action retry max delay cannot be negative: %w", core.ErrInvalidArgument)
	}
	if failureDirectory := strings.TrimSpace(config.SourceCleanupFailureDirectory); failureDirectory != "" {
		cleaned := filepath.Clean(failureDirectory)
		if filepath.IsAbs(failureDirectory) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(os.PathSeparator)) {
			return nil, fmt.Errorf("source cleanup failure directory must remain below the watcher configuration root: %w", core.ErrInvalidArgument)
		}
	}

	patterns, err := normalizeFilePatterns(config.FilePatterns)
	if err != nil {
		return nil, err
	}

	stored := *config
	stored.FilePatterns = patterns

	if stored.PollingInterval == 0 {
		stored.PollingInterval = 30 * time.Second
	}

	if stored.MinFileAge == 0 {
		stored.MinFileAge = 5 * time.Second
	}

	// A negative value would panic make(chan struct{}, n); clamp defensively.
	if stored.MaxConcurrentImports == 0 {
		stored.MaxConcurrentImports = 4
	}
	if stored.MaxConcurrentImports < 1 {
		stored.MaxConcurrentImports = 1
	}
	if stored.MaxPostImportActionRetryCount == 0 {
		stored.MaxPostImportActionRetryCount = defaultMaxPostImportActionRetryCount
	}
	if stored.PostImportActionRetryMaxDelay == 0 {
		stored.PostImportActionRetryMaxDelay = defaultPostImportActionRetryMaxDelay
	}

	// Locus creates the watch path during registration so a missing directory is
	// prepared once instead of erroring on every scan cycle.
	if err := os.MkdirAll(stored.WatchPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create watch path: %w", err)
	}

	// A persisted runtime decision outranks the configured Enabled flag.
	if enabled, ok := w.persistedEnabled(stored.WatcherID); ok {
		stored.Enabled = enabled
	}

	return &stored, nil
}

// UnregisterWatcher removes a file watcher and drops its persisted runtime
// enable/disable decision, so a later registration of the same watcher ID
// starts from configuration again.
//
// Dropping the persisted entry is best-effort: if the state file cannot be
// written the watcher is still removed in memory and nil is returned, because
// the state file records a runtime decision and never configuration of record.
func (w *fileWatcher) UnregisterWatcher(ctx context.Context, watcherID string) error {
	if watcherID == "" {
		return fmt.Errorf("watcher ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	w.watchers.Delete(watcherID)
	w.removeWatcherState(ctx, watcherID)

	return nil
}

// GetWatcher retrieves a watcher configuration by ID.
// The returned configuration is a copy: callers cannot mutate stored state.
func (w *fileWatcher) GetWatcher(ctx context.Context, watcherID string) (*core.FileWatcherConfiguration, error) {
	entry, err := w.loadEntry(watcherID)
	if err != nil {
		return nil, err
	}

	return snapshotConfig(entry), nil
}

// GetAllWatchers retrieves all watcher configurations, sorted by watcher ID.
// Each returned configuration is a copy.
func (w *fileWatcher) GetAllWatchers(ctx context.Context) ([]*core.FileWatcherConfiguration, error) {
	configs := make([]*core.FileWatcherConfiguration, 0)

	w.watchers.Range(func(_, value interface{}) bool {
		entry, ok := value.(*watcherEntry)
		if !ok {
			return true
		}

		configs = append(configs, snapshotConfig(entry))

		return true
	})

	sort.Slice(configs, func(i, j int) bool { return configs[i].WatcherID < configs[j].WatcherID })

	return configs, nil
}

// GetWatchersForTenant returns the watchers that import for one tenant, sorted
// by watcher ID. Each returned configuration is a copy.
//
// Only single-tenant watchers are attributed to a tenant: a multi-tenant
// watcher imports for whichever tenants its subdirectories name, so it is not
// returned for any tenant query.
//
// Errors:
//   - ErrInvalidArgument when tenantID is empty
func (w *fileWatcher) GetWatchersForTenant(ctx context.Context, tenantID string) ([]*core.FileWatcherConfiguration, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	configs := make([]*core.FileWatcherConfiguration, 0)

	w.watchers.Range(func(_, value interface{}) bool {
		entry, ok := value.(*watcherEntry)
		if !ok {
			return true
		}

		config := snapshotConfig(entry)
		if config.MultiTenantMode || config.TenantID != tenantID {
			return true
		}

		configs = append(configs, config)

		return true
	})

	sort.Slice(configs, func(i, j int) bool { return configs[i].WatcherID < configs[j].WatcherID })

	return configs, nil
}

// EnableWatcher enables a watcher and persists the runtime decision so it
// survives a process restart.
//
// Persisting is best-effort: if the state file cannot be written the in-memory
// change is kept, the failure is logged safely, and nil is returned.
func (w *fileWatcher) EnableWatcher(ctx context.Context, watcherID string) error {
	return w.setEnabled(ctx, watcherID, true)
}

// DisableWatcher disables a watcher and persists the runtime decision so it
// survives a process restart.
//
// Persisting is best-effort: if the state file cannot be written the in-memory
// change is kept, the failure is logged safely, and nil is returned.
func (w *fileWatcher) DisableWatcher(ctx context.Context, watcherID string) error {
	return w.setEnabled(ctx, watcherID, false)
}

// ScanNow manually triggers a scan for the specified watcher.
//
// A disabled watcher is rejected with an error instead of being scanned.
func (w *fileWatcher) ScanNow(ctx context.Context, watcherID string) (*core.FileWatcherScanResult, error) {
	config, err := w.GetWatcher(ctx, watcherID)
	if err != nil {
		return nil, err
	}

	if !config.Enabled {
		return nil, fmt.Errorf("watcher %s is disabled: %w", watcherID, core.ErrInvalidArgument)
	}

	return w.scanWatcher(ctx, config)
}

// ScanAllWatchers scans all enabled watchers.
func (w *fileWatcher) ScanAllWatchers(ctx context.Context) (map[string]*core.FileWatcherScanResult, error) {
	results := make(map[string]*core.FileWatcherScanResult)

	w.watchers.Range(func(key, value interface{}) bool {
		watcherID := key.(string)
		entry, ok := value.(*watcherEntry)
		if !ok {
			return true
		}

		config := snapshotConfig(entry)
		if !config.Enabled {
			return true // Skip disabled watchers
		}

		result, err := w.scanWatcher(ctx, config)
		if err != nil {
			w.emit(ctx, slog.LevelError, "scan_failed", "Failed to scan watcher", slog.String("watcher_id", watcherID), errorTypeAttr(err))
			result = &core.FileWatcherScanResult{
				Errors: []string{err.Error()},
			}
		}

		results[watcherID] = result

		return true
	})

	return results, nil
}

// scanWatcher performs the actual file scan and import for a watcher.
//
// This is the single scan-result path: ScanNow, ScanAllWatchers, and the
// background service's per-watcher scan all funnel through it, so statistics
// are recorded exactly once per completed scan and only for a scan that
// actually produced a result.
func (w *fileWatcher) scanWatcher(ctx context.Context, config *core.FileWatcherConfiguration) (*core.FileWatcherScanResult, error) {
	startTime := time.Now()

	result := &core.FileWatcherScanResult{
		Errors: make([]string, 0),
	}

	if err := ctx.Err(); err != nil {
		return result, err
	}

	// Stale history entries are pruned once per scan, bounded by the watcher's
	// throttle when it enables one.
	w.pruneStaleImportedRecords(ctx, config)

	info, err := os.Stat(config.WatchPath)
	if err != nil {
		return result, fmt.Errorf("watch path is not accessible: %w", err)
	}

	if !info.IsDir() {
		return result, fmt.Errorf("watch path is not a directory")
	}

	if config.MultiTenantMode {
		// Multi-tenant mode: scan subdirectories
		result, err = w.scanMultiTenant(ctx, config, result, startTime)
	} else {
		// Single-tenant mode: scan files directly
		result, err = w.scanSingleTenant(ctx, config, result, startTime)
	}

	if err != nil {
		return result, err
	}

	w.recordScanStatistics(config, result)

	return result, nil
}

// recordScanStatistics reports one completed scan to the statistics recorder.
//
// The tenant dimension is empty-safe: a multi-tenant watcher imports for
// whichever tenants its subdirectories name, so it carries no single tenant ID.
func (w *fileWatcher) recordScanStatistics(config *core.FileWatcherConfiguration, result *core.FileWatcherScanResult) {
	if w.statistics == nil || config == nil || result == nil {
		return
	}

	timestamp := time.Now().UTC()

	dimensions := func() map[string]string {
		return map[string]string{
			core.StatisticsDimensionTenantID:  config.TenantID,
			core.StatisticsDimensionWatcherID: config.WatcherID,
			core.StatisticsDimensionOperation: "scan",
		}
	}

	w.statistics.Record(core.StatisticWatcherScanCount, 1, timestamp, dimensions())
	w.statistics.Record(core.StatisticWatcherFilesDiscovered, int64(result.FilesDiscovered), timestamp, dimensions())
	w.statistics.Record(core.StatisticWatcherFilesImported, int64(result.FilesImported), timestamp, dimensions())
	w.statistics.Record(core.StatisticWatcherFilesSkipped, int64(result.FilesSkipped), timestamp, dimensions())
	w.statistics.Record(core.StatisticWatcherFilesFailed, int64(result.FilesFailed), timestamp, dimensions())
	w.statistics.Record(core.StatisticWatcherBytesImported, result.BytesImported, timestamp, dimensions())
}

// scanSingleTenant scans files in single-tenant mode.
func (w *fileWatcher) scanSingleTenant(ctx context.Context, config *core.FileWatcherConfiguration, result *core.FileWatcherScanResult, startTime time.Time) (*core.FileWatcherScanResult, error) {
	// Get tenant context
	tenant, err := w.tenantMgr.GetTenant(ctx, config.TenantID)
	if err != nil {
		return result, fmt.Errorf("failed to get tenant: %w", err)
	}

	// Discover files
	files, err := w.discoverFiles(ctx, config.WatchPath, config)
	if err != nil {
		return result, fmt.Errorf("failed to discover files: %w", err)
	}

	result.FilesDiscovered = len(files)

	discovered := make([]discoveredFile, 0, len(files))
	for _, filePath := range files {
		discovered = append(discovered, discoveredFile{path: filePath, tenant: tenant})
	}

	w.importDiscoveredFiles(ctx, config, result, discovered)

	result.ScanDuration = time.Since(startTime)

	return result, nil
}

// scanMultiTenant scans files in multi-tenant mode.
func (w *fileWatcher) scanMultiTenant(ctx context.Context, config *core.FileWatcherConfiguration, result *core.FileWatcherScanResult, startTime time.Time) (*core.FileWatcherScanResult, error) {
	// Auto-create tenant directories if enabled
	if config.AutoCreateTenantDirectories {
		if err := w.createTenantDirectories(ctx, config); err != nil {
			w.emit(ctx, slog.LevelWarn, "tenant_directories_create_failed", "Failed to create tenant directories", slog.String("watcher_id", config.WatcherID), errorTypeAttr(err))
		}
	}

	// List subdirectories (tenant directories)
	entries, err := os.ReadDir(config.WatchPath)
	if err != nil {
		return result, fmt.Errorf("failed to read watch path: %w", err)
	}

	discovered := make([]discoveredFile, 0)

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if !entry.IsDir() {
			continue // Skip non-directories
		}

		tenantID := entry.Name()
		tenantPath := filepath.Join(config.WatchPath, tenantID)

		// Resolve the tenant up front so a per-tenant failure is reported once
		// instead of once per file.
		tenant, err := w.tenantMgr.GetTenant(ctx, tenantID)
		if err != nil {
			w.addResultError(result, fmt.Sprintf("watcher %s tenant resolution failed: %v", config.WatcherID, err))
			continue
		}

		files, err := w.discoverFiles(ctx, tenantPath, config)
		if err != nil {
			w.addResultError(result, fmt.Sprintf("watcher %s discovery failed: %v", config.WatcherID, err))
			continue
		}

		for _, filePath := range files {
			discovered = append(discovered, discoveredFile{path: filePath, tenant: tenant})
		}
	}

	result.FilesDiscovered = len(discovered)

	w.importDiscoveredFiles(ctx, config, result, discovered)

	result.ScanDuration = time.Since(startTime)

	return result, nil
}

// discoveredFile pairs a discovered file with the tenant it belongs to, so
// nested files cannot be attributed to the wrong tenant.
type discoveredFile struct {
	path   string
	tenant core.TenantContext
}

// importDiscoveredFiles imports files with bounded concurrency and merges the
// per-file outcome into result.
func (w *fileWatcher) importDiscoveredFiles(
	ctx context.Context,
	config *core.FileWatcherConfiguration,
	result *core.FileWatcherScanResult,
	files []discoveredFile,
) {
	if len(files) == 0 {
		return
	}

	limit := config.MaxConcurrentImports
	if limit < 1 {
		limit = 1
	}

	semaphore := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, file := range files {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(entry discoveredFile) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			outcome, err := w.importFile(ctx, entry.tenant, entry.path, config)

			mu.Lock()
			defer mu.Unlock()

			result.PostImportActionsRetried += outcome.postImportActionsRetried
			if outcome.quarantined {
				result.FilesQuarantined++
			}
			if outcome.imported {
				result.FilesImported++
				result.BytesImported += outcome.bytesImported
			}
			if err != nil {
				result.FilesFailed++
				w.addResultError(result, fmt.Sprintf("watcher %s import failed: %v", config.WatcherID, err))

				return
			}

			if !outcome.imported {
				result.FilesSkipped++
			}
		}(file)
	}

	wg.Wait()
}

// addResultError records a scan error without leaking physical paths or
// original file names, capped at maxRecordedErrorsPerScanResult entries.
func (w *fileWatcher) addResultError(result *core.FileWatcherScanResult, message string) {
	if len(result.Errors) >= maxRecordedErrorsPerScanResult {
		return
	}

	result.Errors = append(result.Errors, message)
}

// discoverFiles discovers files in a directory based on configuration.
//
// IncludeSubdirectories selects recursive discovery; otherwise only the top
// directory is scanned. Errors are reported to the caller instead of being
// silently swallowed so an unreadable directory is visible in the scan result.
func (w *fileWatcher) discoverFiles(ctx context.Context, dirPath string, config *core.FileWatcherConfiguration) ([]string, error) {
	var files []string

	appendFile := func(path string, info os.FileInfo) {
		if info.IsDir() {
			return
		}

		// Check file age
		if config.MinFileAge > 0 && time.Since(info.ModTime()) < config.MinFileAge {
			return
		}

		// Check file size
		if config.MaxFileSizeBytes > 0 && info.Size() > config.MaxFileSizeBytes {
			return
		}

		// Check file patterns
		if !matchesAnyPattern(info.Name(), config.FilePatterns) {
			return
		}

		files = append(files, path)
	}

	if !config.IncludeSubdirectories {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory: %w", err)
		}

		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			if entry.IsDir() {
				continue
			}

			info, err := entry.Info()
			if err != nil {
				continue
			}

			appendFile(filepath.Join(dirPath, entry.Name()), info)
		}

		return files, nil
	}

	walkErr := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip unreadable entries; the parent walk continues.
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		appendFile(path, info)

		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("failed to walk directory: %w", walkErr)
	}

	return files, nil
}

// importFile imports a single file into the storage pool.
//
// Ordering invariant: the post-import Delete/Move action runs before the file is
// recorded as imported, so a failed action leaves the source file eligible for
// the next cycle instead of silently marking it done.
//
// A candidate that fails the delayed stability probe is reported as skipped
// rather than failed: it is still being written and is expected to be importable
// on a later scan.
type fileImportOutcome struct {
	imported                 bool
	bytesImported            int64
	postImportActionsRetried int
	quarantined              bool
}

func (w *fileWatcher) importFile(ctx context.Context, tenant core.TenantContext, filePath string, config *core.FileWatcherConfiguration) (fileImportOutcome, error) {
	var outcome fileImportOutcome
	if err := ctx.Err(); err != nil {
		return outcome, err
	}

	// Probe before the candidate is claimed or opened: a file that is still
	// growing must not be imported half-way, and the wait must not hold a lock
	// that other imports need.
	stable, err := w.confirmFileStable(ctx, filePath, config)
	if err != nil {
		return outcome, err
	}

	if !stable {
		return outcome, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return outcome, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	fileInfo, err := file.Stat()
	if err != nil {
		return outcome, fmt.Errorf("failed to stat file: %w", err)
	}

	if fileInfo.IsDir() {
		return outcome, nil
	}

	fingerprint, err := fileFingerprint(file, fileInfo)
	if err != nil {
		return outcome, fmt.Errorf("failed to fingerprint file: %w", err)
	}
	if err := file.Close(); err != nil {
		return outcome, fmt.Errorf("failed to close fingerprint source: %w", err)
	}

	if handled, pendingOutcome, pendingErr := w.tryProcessPendingPostImportAction(
		ctx, filePath, fingerprint, config); handled {
		return pendingOutcome, pendingErr
	}

	alreadyImported, inFlight := w.isFileAlreadyImported(filePath, fingerprint)
	if inFlight || alreadyImported {
		return outcome, nil
	}

	token := fmt.Sprintf("%d-%d", time.Now().UnixNano(), nextReservationSequence())
	if !w.reserveImportSlot(filePath, token, fingerprint) {
		return outcome, nil
	}

	defer w.releaseImportSlot(filePath, token)

	operationID, operationErr := createImportOperationID(tenant.ID, filePath, fingerprint)
	if operationErr != nil {
		return outcome, operationErr
	}
	var cleanupJob SourceCleanupJob
	if w.sourceCleanupStore != nil {
		failureDirectory := ""
		if config.SourceCleanupFailureDirectory != "" {
			failureDirectory = filepath.Join(w.configRoot, config.SourceCleanupFailureDirectory)
		}
		cleanupJob = SourceCleanupJob{
			WatcherID:         config.WatcherID,
			TenantID:          tenant.ID,
			SourcePath:        filePath,
			Fingerprint:       fingerprint,
			OperationID:       operationID,
			Action:            sourceCleanupAction(config.PostImportAction),
			MoveTargetPath:    moveTargetPathForConfig(filePath, config),
			FailureDirectory:  failureDirectory,
			MaxAttempts:       config.MaxPostImportActionRetryCount,
			RetryInitialDelay: config.PostImportActionRetryInitialDelay,
			RetryMaxDelay:     config.PostImportActionRetryMaxDelay,
		}
		reserved, err := w.sourceCleanupStore.TryReserve(ctx, &cleanupJob)
		if err != nil {
			return outcome, fmt.Errorf("failed to reserve source cleanup: %w", err)
		}
		if !reserved {
			return outcome, nil
		}
	}

	// Extract original filename for diagnostics only; physical paths are never
	// derived from caller-supplied names.
	originalFileName := filepath.Base(filePath)
	file, err = os.Open(filePath)
	if err != nil {
		return outcome, fmt.Errorf("failed to reopen file for import: %w", err)
	}

	var fileKey string
	if idempotent, ok := w.storagePool.(core.IdempotentStoragePool); ok {
		fileKey, err = idempotent.WriteFileIdempotently(
			ctx, tenant, file, &originalFileName, operationID)
	} else {
		fileKey, err = w.storagePool.WriteFile(ctx, tenant, file, &originalFileName)
	}
	if err != nil {
		return outcome, fmt.Errorf("failed to import file: %w", err)
	}
	outcome.imported = true
	outcome.bytesImported = fileInfo.Size()

	// Close the source before the post-import Delete/Move action: on Windows an
	// open handle blocks the rename or delete.
	if err := file.Close(); err != nil {
		return outcome, fmt.Errorf("failed to close source file: %w", err)
	}
	if w.sourceCleanupStore != nil {
		if err := w.sourceCleanupStore.MarkImported(ctx, cleanupJob.JobID, fileKey, operationID, w.now()); err != nil {
			return outcome, fmt.Errorf("failed to persist source cleanup job: %w", err)
		}
		w.emit(ctx, slog.LevelInfo, "file_imported", "Imported watched file",
			slog.String("watcher_id", config.WatcherID), slog.String("file_key", fileKey), slog.Int64("bytes", fileInfo.Size()))
		return outcome, nil
	}

	if config.PostImportAction == core.PostImportActionKeep {
		w.storeImportedRecord(filePath, fingerprint, time.Now(), config)
		w.emit(ctx, slog.LevelInfo, "file_imported", "Imported watched file",
			slog.String("watcher_id", config.WatcherID), slog.String("file_key", fileKey), slog.Int64("bytes", fileInfo.Size()))
		return outcome, nil
	}

	pending := importedFileRecord{
		Fingerprint:             fingerprint,
		ImportedAtUnix:          w.now().Unix(),
		PendingPostImportAction: true,
		FileKey:                 fileKey,
		WatcherID:               config.WatcherID,
		TenantID:                tenant.ID,
		PostImportAction:        config.PostImportAction,
	}
	if config.PostImportAction == core.PostImportActionMove {
		pending.MoveTargetPath, _ = resolveMoveTargetPath(filePath, config.MoveToDirectory)
		if pending.MoveTargetPath == "" {
			// Preserve the durable pending-action record even when the move
			// directory itself is invalid; the action retry path will report and
			// quarantine the operational filesystem error.
			pending.MoveTargetPath = filepath.Join(config.MoveToDirectory, filepath.Base(filePath))
		}
	}
	if err := w.storeImportedStateSync(filePath, pending); err != nil {
		return outcome, fmt.Errorf("failed to persist pending post-import action: %w", err)
	}

	if err := w.performPostImportAction(ctx, filePath, pending.PostImportAction, pending.MoveTargetPath); err != nil {
		w.emit(ctx, slog.LevelWarn, "post_import_action_failed", "Failed to perform post-import action",
			slog.String("watcher_id", config.WatcherID), slog.Any("action", config.PostImportAction), errorTypeAttr(err))
		outcome.quarantined, _ = w.recordPostImportActionFailure(filePath, pending, config)
		return outcome, fmt.Errorf("post-import action failed: %w", err)
	}

	if err := w.removeImportedStateSync(filePath); err != nil {
		w.emit(ctx, slog.LevelWarn, "history_save_failed", "Failed to remove completed post-import state", errorTypeAttr(err))
	}

	w.emit(ctx, slog.LevelInfo, "file_imported", "Imported watched file",
		slog.String("watcher_id", config.WatcherID), slog.String("file_key", fileKey), slog.Int64("bytes", fileInfo.Size()))

	return outcome, nil
}

func sourceCleanupAction(action core.PostImportAction) SourceCleanupAction {
	switch action {
	case core.PostImportActionMove:
		return SourceCleanupActionMove
	case core.PostImportActionKeep:
		return SourceCleanupActionKeep
	default:
		return SourceCleanupActionDelete
	}
}

func moveTargetPathForConfig(filePath string, config *core.FileWatcherConfiguration) string {
	if config == nil || config.PostImportAction != core.PostImportActionMove || config.MoveToDirectory == "" {
		return ""
	}
	target, err := resolveMoveTargetPath(filePath, config.MoveToDirectory)
	if err == nil && target != "" {
		return target
	}
	return filepath.Join(config.MoveToDirectory, filepath.Base(filePath))
}

func (w *fileWatcher) executeSourceCleanupJob(ctx context.Context, job SourceCleanupJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(job.SourcePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	file, err := os.Open(job.SourcePath)
	if err != nil {
		return err
	}
	fingerprint, fingerprintErr := fileFingerprint(file, info)
	closeErr := file.Close()
	if fingerprintErr != nil {
		return fingerprintErr
	}
	if closeErr != nil {
		return closeErr
	}
	if fingerprint != job.Fingerprint {
		return fmt.Errorf("%w: source file changed after import", ErrSourceCleanupFingerprintChanged)
	}
	return w.performPostImportAction(ctx, job.SourcePath, postImportAction(job.Action), job.MoveTargetPath)
}

func (w *fileWatcher) quarantineSourceCleanupJob(ctx context.Context, job SourceCleanupJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(job.SourcePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	file, err := os.Open(job.SourcePath)
	if err != nil {
		return err
	}
	fingerprint, fingerprintErr := fileFingerprint(file, info)
	_ = file.Close()
	if fingerprintErr != nil {
		return fingerprintErr
	}
	if fingerprint != job.Fingerprint {
		return fmt.Errorf("%w: source file changed after import", ErrSourceCleanupFingerprintChanged)
	}
	target := filepath.Join(job.FailureDirectory, job.WatcherID, filepath.Base(job.SourcePath))
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	return moveFileWithFallback(ctx, job.SourcePath, target, os.Rename)
}

func postImportAction(action SourceCleanupAction) core.PostImportAction {
	switch action {
	case SourceCleanupActionMove:
		return core.PostImportActionMove
	case SourceCleanupActionKeep:
		return core.PostImportActionKeep
	default:
		return core.PostImportActionDelete
	}
}

// performPostImportAction performs the configured action after successful import.
func (w *fileWatcher) performPostImportAction(ctx context.Context, filePath string, action core.PostImportAction, moveTargetPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch action {
	case core.PostImportActionDelete:
		err := os.Remove(filePath)
		if os.IsNotExist(err) {
			return nil
		}
		return err

	case core.PostImportActionMove:
		if moveTargetPath == "" {
			return fmt.Errorf("move directory not configured")
		}
		if err := os.MkdirAll(filepath.Dir(moveTargetPath), 0755); err != nil {
			return fmt.Errorf("failed to create move directory: %w", err)
		}
		if _, err := os.Stat(moveTargetPath); err == nil {
			equivalent, compareErr := filesHaveEquivalentContent(filePath, moveTargetPath)
			if compareErr != nil {
				return compareErr
			}
			if !equivalent {
				return fmt.Errorf("move target already exists with different content")
			}
			return os.Remove(filePath)
		} else if !os.IsNotExist(err) {
			return err
		}
		return moveFileWithFallback(ctx, filePath, moveTargetPath, os.Rename)

	case core.PostImportActionKeep:
		// Do nothing
		return nil

	default:
		return fmt.Errorf("unknown post-import action: %d", action)
	}
}

// moveFileWithFallback keeps the normal same-filesystem rename fast and atomic,
// but handles a cross-filesystem move without relying on rename semantics that
// fail with EXDEV on Unix (or ERROR_NOT_SAME_DEVICE on Windows).
//
// renameFile is injected for the focused regression test; production callers
// pass os.Rename.
func moveFileWithFallback(
	ctx context.Context,
	sourcePath string,
	destinationPath string,
	renameFile func(string, string) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := renameFile(sourcePath, destinationPath); err == nil {
		return nil
	} else if !isCrossDeviceRenameError(err) {
		return err
	}

	return copyFileAndRemoveSource(ctx, sourcePath, destinationPath, renameFile)
}

// copyFileAndRemoveSource stages a cross-filesystem move beside its destination
// so the final rename remains same-filesystem. The source is removed only after
// the destination has been fully copied and committed.
func copyFileAndRemoveSource(
	ctx context.Context,
	sourcePath string,
	destinationPath string,
	renameFile func(string, string) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to inspect move source: %w", err)
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return core.ErrFileNotFound
		}
		return fmt.Errorf("failed to open move source: %w", err)
	}

	staged, err := os.CreateTemp(filepath.Dir(destinationPath), ".venue-move-*")
	if err != nil {
		_ = source.Close()
		return fmt.Errorf("failed to stage move destination: %w", err)
	}
	stagedPath := staged.Name()
	committed := false
	defer func() {
		_ = source.Close()
		_ = staged.Close()
		if !committed {
			_ = os.Remove(stagedPath)
		}
	}()

	written, err := io.Copy(staged, source)
	if err != nil {
		return fmt.Errorf("failed to copy move source: %w", err)
	}
	if written != sourceInfo.Size() {
		return fmt.Errorf("move copy is incomplete, wrote %d of %d bytes: %w", written, sourceInfo.Size(), io.ErrUnexpectedEOF)
	}

	// Release the source before deletion; Windows rejects removing an open file.
	if err := source.Close(); err != nil {
		return fmt.Errorf("failed to close move source: %w", err)
	}
	if err := staged.Sync(); err != nil {
		return fmt.Errorf("failed to sync staged move copy: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("failed to close staged move copy: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := renameFile(stagedPath, destinationPath); err != nil {
		return fmt.Errorf("failed to commit move copy: %w", err)
	}
	committed = true

	if err := os.Remove(sourcePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove move source: %w", err)
	}
	return nil
}

// isCrossDeviceRenameError classifies only rename failures that can be
// satisfied by copying the bytes. Other errors must remain visible to the
// caller and continue through the normal retry/quarantine path.
func isCrossDeviceRenameError(err error) bool {
	if isNotSameDeviceRenameError(err) || errors.Is(err, syscall.EXDEV) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "cross-device link") ||
		strings.Contains(message, "not same device") ||
		strings.Contains(message, "different disk drive")
}

func resolveMoveTargetPath(filePath string, targetDir string) (string, error) {
	if targetInfo, err := os.Stat(targetDir); err == nil {
		if !targetInfo.IsDir() {
			return "", fmt.Errorf("move target %q is not a directory", targetDir)
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to inspect move directory %q: %w", targetDir, err)
	}

	fileName := filepath.Base(filePath)
	extension := filepath.Ext(fileName)
	nameWithoutExt := strings.TrimSuffix(fileName, extension)
	for counter := 0; ; counter++ {
		candidateName := fileName
		if counter > 0 {
			candidateName = fmt.Sprintf("%s_%d%s", nameWithoutExt, counter, extension)
		}
		candidate := filepath.Join(targetDir, candidateName)
		if _, err := os.Stat(candidate); err != nil {
			if os.IsNotExist(err) {
				return candidate, nil
			}
			return "", fmt.Errorf("failed to inspect move target %q: %w", candidate, err)
		}
	}
}

func (w *fileWatcher) tryProcessPendingPostImportAction(
	ctx context.Context,
	filePath string,
	fingerprint string,
	config *core.FileWatcherConfiguration,
) (bool, fileImportOutcome, error) {
	var outcome fileImportOutcome
	for {
		value, exists := w.importedFiles.Load(filePath)
		if !exists {
			return false, outcome, nil
		}
		record, ok := value.(importedFileRecord)
		if !ok || !record.PendingPostImportAction || record.Fingerprint != fingerprint {
			return false, outcome, nil
		}
		if record.InFlightToken != "" {
			return true, outcome, nil
		}
		if record.Quarantined {
			outcome.quarantined = true
			return true, outcome, nil
		}
		if record.NextAttemptUnixNano > w.now().UnixNano() {
			return true, outcome, nil
		}

		claim := record
		claim.InFlightToken = fmt.Sprintf("action-%d-%d", time.Now().UnixNano(), nextReservationSequence())
		if !w.importedFiles.CompareAndSwap(filePath, value, claim) {
			continue
		}

		outcome.postImportActionsRetried = 1
		err := w.performPostImportAction(ctx, filePath, record.PostImportAction, record.MoveTargetPath)
		if err == nil {
			if persistErr := w.removeImportedStateSync(filePath); persistErr != nil {
				return true, outcome, persistErr
			}
			return true, outcome, nil
		}

		record.InFlightToken = ""
		quarantined, persistErr := w.recordPostImportActionFailure(filePath, record, config)
		outcome.quarantined = quarantined
		if persistErr != nil {
			return true, outcome, errors.Join(err, persistErr)
		}
		return true, outcome, fmt.Errorf("post-import action retry failed: %w", err)
	}
}

func (w *fileWatcher) recordPostImportActionFailure(
	filePath string,
	record importedFileRecord,
	config *core.FileWatcherConfiguration,
) (bool, error) {
	record.FailureCount++
	maxAttempts := config.MaxPostImportActionRetryCount
	if maxAttempts < 1 {
		maxAttempts = defaultMaxPostImportActionRetryCount
	}
	record.Quarantined = record.FailureCount >= maxAttempts
	if record.Quarantined {
		record.NextAttemptUnixNano = 0
	} else {
		record.NextAttemptUnixNano = w.now().Add(postImportActionRetryDelay(config, record.FailureCount)).UnixNano()
	}
	return record.Quarantined, w.storeImportedStateSync(filePath, record)
}

func postImportActionRetryDelay(config *core.FileWatcherConfiguration, failureCount int) time.Duration {
	delay := config.PostImportActionRetryInitialDelay
	if delay <= 0 {
		return 0
	}
	maximum := config.PostImportActionRetryMaxDelay
	if maximum <= 0 {
		maximum = delay
	}
	for attempt := 1; attempt < failureCount; attempt++ {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return min(delay, maximum)
}

func filesHaveEquivalentContent(firstPath string, secondPath string) (bool, error) {
	first, err := os.Open(firstPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = first.Close() }()
	second, err := os.Open(secondPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = second.Close() }()

	firstInfo, err := first.Stat()
	if err != nil {
		return false, err
	}
	secondInfo, err := second.Stat()
	if err != nil {
		return false, err
	}
	if firstInfo.Size() != secondInfo.Size() {
		return false, nil
	}
	firstHash, err := contentSampleHash(first, firstInfo.Size())
	if err != nil {
		return false, err
	}
	secondHash, err := contentSampleHash(second, secondInfo.Size())
	if err != nil {
		return false, err
	}
	return firstHash == secondHash, nil
}

// createTenantDirectories creates subdirectories for all tenants.
//
// The tenant enumeration comes from tenantList, so it is reused for the
// watcher's AutoCreateTenantDirectoriesCacheTTL instead of being repeated on
// every scan.
func (w *fileWatcher) createTenantDirectories(ctx context.Context, config *core.FileWatcherConfiguration) error {
	if !config.MultiTenantMode {
		return nil
	}

	// Get all tenants
	tenants, err := w.tenantList(ctx, config)
	if err != nil {
		return err
	}

	// Create directory for each tenant
	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return err
		}

		tenantPath := filepath.Join(config.WatchPath, tenant.ID)
		if err := os.MkdirAll(tenantPath, 0755); err != nil {
			w.emit(ctx, slog.LevelWarn, "tenant_directory_create_failed", "Failed to create tenant directory", slog.String("tenant_id", tenant.ID), errorTypeAttr(err))
		} else {
			w.emit(ctx, slog.LevelInfo, "tenant_directory_created", "Created tenant directory", slog.String("tenant_id", tenant.ID))
		}
	}

	return nil
}

// tenantList returns the tenant enumeration used by AutoCreateTenantDirectories,
// reusing a per-watcher cache for AutoCreateTenantDirectoriesCacheTTL so
// repeated scans do not re-enumerate the tenant store.
//
// A zero TTL selects defaultAutoCreateTenantDirectoriesCacheTTL (60s); a
// negative TTL disables caching. The entry expires on read, so a tenant created
// after a scan becomes visible within at most one TTL. Concurrent readers are
// serialized by tenantCacheMu, and the returned slice is read-only cache state.
//
// Errors:
//   - the tenant-store error, wrapped, when enumeration fails
func (w *fileWatcher) tenantList(ctx context.Context, config *core.FileWatcherConfiguration) ([]core.TenantContext, error) {
	ttl := config.AutoCreateTenantDirectoriesCacheTTL
	if ttl == 0 {
		ttl = defaultAutoCreateTenantDirectoriesCacheTTL
	}

	if ttl > 0 {
		w.tenantCacheMu.Lock()

		cached, ok := w.tenantCache[config.WatcherID]
		if ok && w.now().Before(cached.expiresAt) {
			w.tenantCacheMu.Unlock()

			return cached.tenants, nil
		}

		w.tenantCacheMu.Unlock()
	}

	tenants, err := w.tenantMgr.GetAllTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenants: %w", err)
	}

	if ttl > 0 {
		w.tenantCacheMu.Lock()
		w.tenantCache[config.WatcherID] = cachedTenantList{
			tenants:   append([]core.TenantContext(nil), tenants...),
			expiresAt: w.now().Add(ttl),
		}
		w.tenantCacheMu.Unlock()
	}

	return tenants, nil
}

// confirmFileStable runs the delayed second stability probe for one candidate.
//
// A non-positive FileStabilityCheckDelay disables the probe. A candidate at
// least SkipStabilityCheckAfterAge old (one minute when that field is zero) is
// not re-probed: it can only be complete, so it relies on the existing
// accessibility check. A negative SkipStabilityCheckAfterAge always probes.
//
// The wait holds no lock, so concurrent imports are never blocked, and it
// observes the scan context: a cancelled scan reports the cancellation instead
// of importing. A candidate whose size or modification time changed during the
// wait is reported as unstable; a candidate that disappeared is unstable as
// well, because no stable file can be read from that path.
func (w *fileWatcher) confirmFileStable(ctx context.Context, filePath string, config *core.FileWatcherConfiguration) (bool, error) {
	delay := config.FileStabilityCheckDelay
	if delay <= 0 {
		return true, nil
	}

	before, err := os.Stat(filePath)
	if err != nil {
		// Leave the failure to the import path, which reports it with its own
		// accessibility error instead of silently skipping an unreadable file.
		return true, nil
	}

	skipAge := config.SkipStabilityCheckAfterAge
	if skipAge == 0 {
		skipAge = defaultSkipStabilityCheckAfterAge
	}

	if skipAge > 0 && w.now().Sub(before.ModTime()) >= skipAge {
		return true, nil
	}

	if err := w.stabilityWait(ctx, delay); err != nil {
		return false, err
	}

	after, err := os.Stat(filePath)
	if err != nil {
		return false, nil
	}

	return before.Size() == after.Size() && before.ModTime().Equal(after.ModTime()), nil
}

// waitForStabilityDelay waits for the stability delay and returns ctx.Err() when
// the scan is cancelled first. It holds no lock while waiting.
func waitForStabilityDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// normalizeFilePatterns trims patterns, drops blanks and duplicates, validates
// each glob and falls back to "*" when nothing usable remains.
func normalizeFilePatterns(patterns []string) ([]string, error) {
	normalized := make([]string, 0, len(patterns))

	for _, pattern := range patterns {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			continue
		}

		if _, err := filepath.Match(trimmed, "validation-probe"); err != nil {
			return nil, fmt.Errorf("invalid file pattern %q: %w", trimmed, core.ErrInvalidArgument)
		}

		duplicate := false
		for _, existing := range normalized {
			if existing == trimmed {
				duplicate = true
				break
			}
		}

		if !duplicate {
			normalized = append(normalized, trimmed)
		}
	}

	if len(normalized) == 0 {
		normalized = append(normalized, wildcardPattern)
	}

	return normalized, nil
}

// matchesAnyPattern reports whether name matches any configured pattern. An
// empty pattern list matches everything; matching is case-insensitive so the
// configured watcher behaves consistently across platforms.
func matchesAnyPattern(name string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}

	for _, pattern := range patterns {
		if matchPattern(name, pattern) {
			return true
		}
	}

	return false
}

// matchPattern matches a filename against a glob pattern.
func matchPattern(name, pattern string) bool {
	if pattern == "" || pattern == wildcardPattern {
		return true
	}

	name = strings.ToLower(name)
	pattern = strings.ToLower(pattern)

	matched, err := filepath.Match(pattern, name)
	if err != nil {
		return false
	}

	return matched
}

// fileFingerprint identifies one content revision of a file. ReadAt leaves the
// stream position at zero for the subsequent storage write.
func fileFingerprint(file *os.File, info os.FileInfo) (string, error) {
	if info == nil {
		return defaultImportFingerprint, nil
	}
	if file == nil {
		return "", fmt.Errorf("file cannot be nil")
	}

	contentHash, err := contentSampleHash(file, info.Size())
	if err != nil {
		return "", err
	}
	modified := info.ModTime().UTC().UnixNano()
	return fmt.Sprintf("fp:v3:%d:%d:%d:%s", info.Size(), modified, modified, contentHash), nil
}

func contentSampleHash(file *os.File, size int64) (string, error) {
	positions := []int64{
		0,
		max(0, (size-fingerprintSampleSize)/2),
		max(0, size-fingerprintSampleSize),
	}
	hasher := sha256.New()
	visited := make(map[int64]struct{}, len(positions))
	buffer := make([]byte, fingerprintSampleSize)
	for _, position := range positions {
		if _, ok := visited[position]; ok {
			continue
		}
		visited[position] = struct{}{}

		remaining := min(int64(fingerprintSampleSize), max(0, size-position))
		for remaining > 0 {
			readSize := min(int64(len(buffer)), remaining)
			n, err := file.ReadAt(buffer[:readSize], position)
			if n > 0 {
				_, _ = hasher.Write(buffer[:n])
				position += int64(n)
				remaining -= int64(n)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return "", err
			}
			if n == 0 {
				break
			}
		}
	}

	return base64.StdEncoding.EncodeToString(hasher.Sum(nil)), nil
}

func createImportOperationID(tenantID string, filePath string, fingerprint string) (string, error) {
	normalizedPath, err := filepath.Abs(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to normalize import path: %w", err)
	}
	payload := tenantID + "\n" + filepath.Clean(normalizedPath) + "\n" + fingerprint
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:]), nil
}

// loadEntry returns the stored entry for a watcher ID.
//
// A missing watcher is reported as core.ErrWatcherNotFound so callers can use
// errors.Is instead of matching message text.
func (w *fileWatcher) loadEntry(watcherID string) (*watcherEntry, error) {
	if watcherID == "" {
		return nil, fmt.Errorf("watcher ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	value, ok := w.watchers.Load(watcherID)
	if !ok {
		return nil, fmt.Errorf("watcher not found: %s: %w", watcherID, core.ErrWatcherNotFound)
	}

	entry, ok := value.(*watcherEntry)
	if !ok {
		return nil, fmt.Errorf("watcher not found: %s: %w", watcherID, core.ErrWatcherNotFound)
	}

	return entry, nil
}

// snapshotConfig returns a copy of a watcher's configuration.
func snapshotConfig(entry *watcherEntry) *core.FileWatcherConfiguration {
	entry.mu.Lock()
	defer entry.mu.Unlock()

	config := *entry.config
	config.FilePatterns = append([]string(nil), entry.config.FilePatterns...)

	return &config
}

// setEnabled updates a watcher's enabled flag under its own lock and persists
// the runtime decision for that watcher ID.
//
// Persisting is best-effort: the state file is runtime state, not configuration
// of record, so a failed write keeps the in-memory change, logs a warning, and
// still returns nil. Only a missing watcher ID is an error.
func (w *fileWatcher) setEnabled(ctx context.Context, watcherID string, enabled bool) error {
	entry, err := w.loadEntry(watcherID)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	entry.config.Enabled = enabled
	entry.mu.Unlock()

	// Persist outside the entry lock: the state write performs file I/O and
	// must not block readers of this watcher's configuration.
	w.recordWatcherState(ctx, watcherID, enabled)

	return nil
}

// loadImportedFilesHistory loads the imported files history from persistent
// storage, pruning records whose source file no longer exists and capping the
// in-memory history at maxImportedFilesHistory.
func (w *fileWatcher) loadImportedFilesHistory() error {
	historyPath := filepath.Join(w.configRoot, importedFilesHistoryFileName)

	if _, err := os.Stat(historyPath); os.IsNotExist(err) {
		return nil // File doesn't exist yet, no history to load
	}

	data, err := os.ReadFile(historyPath)
	if err != nil {
		return fmt.Errorf("failed to read imported files history: %w", err)
	}

	var history map[string]importedFileRecord
	if err := json.Unmarshal(data, &history); err != nil {
		return fmt.Errorf("failed to parse imported files history: %w", err)
	}

	live := make([]importedRecord, 0, len(history))
	pruned := 0

	for filePath, record := range history {
		if _, err := os.Stat(filePath); err != nil {
			pruned++
			continue
		}

		live = append(live, importedRecord{path: filePath, record: record})
	}

	trimmed := 0
	if len(live) > maxImportedFilesHistory {
		sort.Slice(live, func(i, j int) bool {
			return live[i].record.ImportedAtUnix < live[j].record.ImportedAtUnix
		})
		trimmed = len(live) - maxImportedFilesHistory
		live = live[len(live)-maxImportedFilesHistory:]
	}

	for _, record := range live {
		w.storeImportedStateLoaded(record.path, record.record)
	}

	w.emit(context.Background(), slog.LevelInfo, "history_loaded", "Loaded imported files history",
		slog.Int("count", len(live)), slog.Int("pruned", pruned), slog.Int("trimmed", trimmed))

	return nil
}

// storeImportedStateLoaded records a persisted entry without scheduling a
// history write while startup is rebuilding the in-memory view.
func (w *fileWatcher) storeImportedStateLoaded(filePath string, record importedFileRecord) {
	w.importedFilesMu.Lock()
	defer w.importedFilesMu.Unlock()

	record.InFlightToken = ""
	w.importedFiles.Store(filePath, record)
	w.importedCount++
}

// storeImportedRecordSync is retained for focused history tests and simple
// non-pending entries.
func (w *fileWatcher) storeImportedRecordSync(filePath, fingerprint string, importedAt time.Time) {
	w.storeImportedStateLoaded(filePath, importedFileRecord{
		Fingerprint:    fingerprint,
		ImportedAtUnix: importedAt.Unix(),
	})
}

// saveImportedFilesHistory persists the history atomically, dropping the oldest
// entries beyond maxImportedFilesHistory.
//
// Writes are serialized and publish through a temp file + rename, so a crash or
// concurrent caller can never leave a truncated history behind.
func (w *fileWatcher) saveImportedFilesHistory() error {
	w.importedFilesMu.Lock()
	defer w.importedFilesMu.Unlock()

	records := w.snapshotImportedRecords()
	if len(records) > maxImportedFilesHistory {
		sort.Slice(records, func(i, j int) bool {
			return records[i].record.ImportedAtUnix < records[j].record.ImportedAtUnix
		})
		records = records[len(records)-maxImportedFilesHistory:]
	}

	history := make(map[string]importedFileRecord, len(records))
	for _, record := range records {
		persisted := record.record
		persisted.InFlightToken = ""
		history[record.path] = persisted
	}

	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal imported files history: %w", err)
	}

	historyPath := filepath.Join(w.configRoot, importedFilesHistoryFileName)
	tempPath := filepath.Join(w.configRoot, importedFilesHistoryTempFileName)

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write imported files history: %w", err)
	}

	if err := replaceFile(tempPath, historyPath); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to publish imported files history: %w", err)
	}

	return nil
}

// importedRecord is one entry of an in-memory history snapshot.
type importedRecord struct {
	path   string
	record importedFileRecord
}

// snapshotImportedRecords copies the current history into a slice.
func (w *fileWatcher) snapshotImportedRecords() []importedRecord {
	records := make([]importedRecord, 0)

	w.importedFiles.Range(func(key, value interface{}) bool {
		record, ok := value.(importedFileRecord)
		if !ok {
			return true
		}

		records = append(records, importedRecord{path: key.(string), record: record})

		return true
	})

	return records
}

// isFileAlreadyImported reports whether the path is already recorded and
// whether an import of the same revision is currently in flight.
func (w *fileWatcher) isFileAlreadyImported(filePath, fingerprint string) (bool, bool) {
	value, exists := w.importedFiles.Load(filePath)
	if !exists {
		return false, false
	}

	record, ok := value.(importedFileRecord)
	if !ok {
		return false, false
	}

	if record.InFlightToken != "" {
		return false, true
	}

	return record.Fingerprint == fingerprint, false
}

// reserveImportSlot atomically claims filePath for the given fingerprint.
//
// The claim is published with LoadOrStore and carries a unique token, so two
// concurrent scans can never both believe they own the same path: only the
// goroutine whose token is still stored may proceed.
func (w *fileWatcher) reserveImportSlot(filePath, token, fingerprint string) bool {
	claim := importedFileRecord{InFlightToken: token, ImportedAtUnix: time.Now().Unix()}

	if _, loaded := w.importedFiles.LoadOrStore(filePath, claim); loaded {
		value, exists := w.importedFiles.Load(filePath)
		if !exists {
			return false
		}

		record, ok := value.(importedFileRecord)
		if !ok || record.InFlightToken != "" {
			return false
		}

		if record.Fingerprint == fingerprint {
			return false
		}

		// Claim the slot for this revision only if the stale record is still the
		// exact value we inspected.
		if !w.importedFiles.CompareAndSwap(filePath, value, claim) {
			return false
		}
	} else {
		w.incrementImportedCount()
	}

	// Ownership re-check: another goroutine may have replaced the entry between
	// our CAS and this load.
	current, exists := w.importedFiles.Load(filePath)
	if !exists {
		return false
	}

	stored, ok := current.(importedFileRecord)
	if !ok || stored.InFlightToken != token {
		return false
	}

	return true
}

// releaseImportSlot removes an owned in-flight claim, leaving committed records.
func (w *fileWatcher) releaseImportSlot(filePath, token string) {
	if token == "" {
		return
	}

	value, exists := w.importedFiles.Load(filePath)
	if !exists {
		return
	}

	record, ok := value.(importedFileRecord)
	if !ok || record.InFlightToken != token {
		return
	}

	if w.importedFiles.CompareAndDelete(filePath, value) {
		w.importedFilesMu.Lock()
		w.importedCount--
		w.importedFilesMu.Unlock()
	}
}

// reservationSequence disambiguates claims made within the same clock tick.
var reservationSequence atomic.Uint64

// nextReservationSequence returns the next claim sequence number.
func nextReservationSequence() uint64 {
	return reservationSequence.Add(1)
}

// storeImportedRecord writes an entry, replacing any previous revision, and
// schedules the history persistence its configuration asks for.
//
// The in-memory entry is published before persistence is scheduled, so a
// deferred (debounced) write never widens the de-duplication window: a second
// scan already sees the record.
func (w *fileWatcher) storeImportedRecord(filePath, fingerprint string, importedAt time.Time, config *core.FileWatcherConfiguration) {
	if importedAt.IsZero() {
		importedAt = time.Now()
	}

	w.importedFilesMu.Lock()
	if _, loaded := w.importedFiles.LoadAndDelete(filePath); !loaded {
		w.importedCount++
	}
	w.importedFiles.Store(filePath, importedFileRecord{
		Fingerprint:    fingerprint,
		ImportedAtUnix: importedAt.Unix(),
	})
	w.importedFilesMu.Unlock()

	w.persistHistory(config)
}

// storeImportedStateSync persists pending post-import work before the action is
// attempted, closing the crash window between storage and source cleanup.
func (w *fileWatcher) storeImportedStateSync(filePath string, record importedFileRecord) error {
	if record.ImportedAtUnix == 0 {
		record.ImportedAtUnix = w.now().Unix()
	}
	record.InFlightToken = ""
	w.importedFilesMu.Lock()
	if _, loaded := w.importedFiles.LoadAndDelete(filePath); !loaded {
		w.importedCount++
	}
	w.importedFiles.Store(filePath, record)
	w.importedFilesMu.Unlock()
	return w.saveImportedFilesHistory()
}

func (w *fileWatcher) removeImportedStateSync(filePath string) error {
	w.importedFilesMu.Lock()
	if _, loaded := w.importedFiles.LoadAndDelete(filePath); loaded {
		w.importedCount--
	}
	w.importedFilesMu.Unlock()
	return w.saveImportedFilesHistory()
}

// incrementImportedCount tracks history size without scanning the sync.Map.
func (w *fileWatcher) incrementImportedCount() {
	w.importedFilesMu.Lock()
	w.importedCount++
	w.importedFilesMu.Unlock()
}

// importedEntryCount returns the current number of history entries.
func (w *fileWatcher) importedEntryCount() int {
	w.importedFilesMu.Lock()
	defer w.importedFilesMu.Unlock()

	return w.importedCount
}

// pruneStaleImportedRecords drops history entries whose source path no longer
// exists and persists the history when something was dropped.
//
// A watcher that enables EnableImportedFilesPruneThrottle prunes at most once
// per ImportedFilesPruneInterval (default five minutes); the throttled prune
// always runs on the first opportunity after the interval elapsed, so it is
// never skipped forever. A watcher without the throttle keeps the un-throttled
// baseline the switch exists to bound: it prunes on every scan.
//
// In-flight claims are never evicted: reserveImportSlot publishes a claim before
// its source file is imported, so dropping one would let a concurrent scan
// import the same revision twice.
func (w *fileWatcher) pruneStaleImportedRecords(ctx context.Context, config *core.FileWatcherConfiguration) int {
	if !w.pruneDue(config) {
		return 0
	}

	pruned := 0

	w.importedFiles.Range(func(key, value interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		filePath, ok := key.(string)
		if !ok {
			return true
		}

		record, ok := value.(importedFileRecord)
		if !ok || record.InFlightToken != "" {
			return true
		}

		if _, err := os.Stat(filePath); err == nil {
			return true
		}

		if w.importedFiles.CompareAndDelete(key, value) {
			pruned++
		}

		return true
	})

	if pruned == 0 {
		return 0
	}

	w.importedFilesMu.Lock()
	w.importedCount -= pruned
	w.importedFilesMu.Unlock()

	w.persistHistory(config)

	return pruned
}

// pruneDue reports whether the history prune may run now, recording the run so
// the next throttled prune waits for the full interval.
func (w *fileWatcher) pruneDue(config *core.FileWatcherConfiguration) bool {
	if config == nil || !config.EnableImportedFilesPruneThrottle {
		return true
	}

	interval := config.ImportedFilesPruneInterval
	if interval <= 0 {
		interval = defaultImportedFilesPruneInterval
	}

	now := w.now()

	w.pruneMu.Lock()
	defer w.pruneMu.Unlock()

	if w.prunedOnce && now.Sub(w.lastPrunedAt) < interval {
		return false
	}

	w.prunedOnce = true
	w.lastPrunedAt = now

	return true
}

// persistHistory schedules the history persistence a configuration asks for: a
// debounced write when the watcher enables the debounce, an immediate one
// otherwise.
func (w *fileWatcher) persistHistory(config *core.FileWatcherConfiguration) {
	if config != nil && config.EnableImportedFilesHistoryFlushDebounce {
		w.scheduleDebouncedHistoryFlush(config)

		return
	}

	w.persistHistoryAsync()
}

// historyFlushInterval returns the effective debounce window. A non-positive
// value means "unset" and selects defaultImportedFilesHistoryFlushInterval.
func historyFlushInterval(config *core.FileWatcherConfiguration) time.Duration {
	if config.ImportedFilesHistoryFlushInterval <= 0 {
		return defaultImportedFilesHistoryFlushInterval
	}

	return config.ImportedFilesHistoryFlushInterval
}

// scheduleDebouncedHistoryFlush marks the history dirty and guarantees that
// exactly one debounced write is scheduled.
//
// Writes are spaced by at least ImportedFilesHistoryFlushInterval: the first
// pending change is written as soon as possible, and a change recorded while a
// write is in flight schedules the next one one full interval after that write
// started. A change is therefore always persisted within one interval of being
// recorded, and Close forces a final synchronous write.
//
// The scheduled function must not run inline; time.AfterFunc does not, and the
// injectable test scheduler is expected to record the callback instead.
func (w *fileWatcher) scheduleDebouncedHistoryFlush(config *core.FileWatcherConfiguration) {
	interval := historyFlushInterval(config)

	w.importedFilesMu.Lock()
	if w.closed {
		w.importedFilesMu.Unlock()

		return
	}

	w.historyFlushDirty = true
	if w.historyFlushTimer != nil {
		// A write is already scheduled and will pick up this change.
		w.importedFilesMu.Unlock()

		return
	}

	var delay time.Duration

	if !w.lastHistoryFlushAt.IsZero() {
		if remaining := interval - w.now().Sub(w.lastHistoryFlushAt); remaining > 0 {
			delay = remaining
		}
	}

	w.historyFlushTimer = w.scheduleHistoryFlush(delay, w.flushDebouncedHistory)
	w.importedFilesMu.Unlock()
}

// flushDebouncedHistory runs one scheduled debounced history write.
func (w *fileWatcher) flushDebouncedHistory() {
	w.importedFilesMu.Lock()

	// Close already flushed everything in memory, so a callback that fires while
	// Close is in progress must not write a second, redundant document.
	if w.closed {
		w.historyFlushTimer = nil
		w.historyFlushDirty = false
		w.importedFilesMu.Unlock()

		return
	}

	w.historyFlushTimer = nil
	dirty := w.historyFlushDirty
	w.historyFlushDirty = false
	w.lastHistoryFlushAt = w.now()
	w.importedFilesMu.Unlock()

	if !dirty {
		return
	}

	w.persistHistoryAsync()
}

// persistHistoryAsync writes the history off the import hot path.
//
// Exactly one flusher goroutine is active at a time; callers that arrive during
// a flush are coalesced instead of racing on the temp file. A caller whose
// change arrived after the running flusher snapshotted the history requests one
// more pass, so a change is never dropped by the coalescing. The caller can
// observe completion through flushDone.
func (w *fileWatcher) persistHistoryAsync() {
	w.importedFilesMu.Lock()
	if w.flushDone != nil {
		w.flushAgain = true
		w.importedFilesMu.Unlock()

		return
	}

	done := make(chan struct{})
	w.flushDone = done
	w.flushAgain = false
	w.importedFilesMu.Unlock()

	go func() {
		defer close(done)

		for {
			if err := w.saveImportedFilesHistory(); err != nil {
				w.emit(context.Background(), slog.LevelError, "history_save_failed", "Failed to save imported files history", errorTypeAttr(err))
			}

			w.importedFilesMu.Lock()
			again := w.flushAgain
			w.flushAgain = false

			if again {
				w.importedFilesMu.Unlock()

				continue
			}

			w.flushDone = nil
			w.importedFilesMu.Unlock()

			return
		}
	}()
}

// awaitPendingHistoryFlush blocks until any in-flight history write completes.
// It is used by shutdown and tests so the state directory is quiescent.
func (w *fileWatcher) awaitPendingHistoryFlush() {
	for {
		w.importedFilesMu.Lock()
		done := w.flushDone
		w.importedFilesMu.Unlock()

		if done == nil {
			return
		}

		<-done
	}
}

func (w *fileWatcher) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	w.logger.Emit(ctx, logging.Record{
		Level: level, Component: "watcher.files", Event: event, Message: message, Attrs: attrs,
	})
}

func errorTypeAttr(err error) slog.Attr {
	return slog.String("error_type", fmt.Sprintf("%T", err))
}
