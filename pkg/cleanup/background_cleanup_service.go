package cleanup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// BackgroundCleanupServiceOptions configures the background cleanup service.
type BackgroundCleanupServiceOptions struct {
	// CleanupService is the underlying cleanup service that performs the actual cleanup operations.
	CleanupService core.CleanupService

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// CleanupInterval is the interval between cleanup runs.
	// Default: 1 hour
	CleanupInterval time.Duration

	// InitialDelay is the delay before the first cleanup run.
	// Default: 1 minute
	InitialDelay time.Duration

	// CleanupEmptyDirectories enables empty directory cleanup.
	// Default: true
	CleanupEmptyDirectories bool

	// CleanupTimedOutFiles enables timed-out file cleanup.
	// Default: true
	CleanupTimedOutFiles bool

	// ProcessingTimeout is the timeout for processing files.
	// Files in Processing status longer than this will be reset.
	// Default: 30 minutes
	ProcessingTimeout time.Duration

	// CleanupPermanentlyFailedFiles enables permanently failed file cleanup.
	// Default: true
	CleanupPermanentlyFailedFiles bool

	// FailedFileRetentionPeriod is how long to keep permanently failed files before cleanup.
	// Default: 3 days
	FailedFileRetentionPeriod time.Duration

	// CleanupCompletedRecords enables completed-file cleanup.
	// Default: true
	CleanupCompletedRecords bool

	// CompletedRecordRetentionPeriod is how long completed files remain before cleanup.
	// Default: 0 (remove during the next cleanup cycle)
	CompletedRecordRetentionPeriod time.Duration

	// CleanupOrphanedMetadata enables orphaned-metadata cleanup: metadata records
	// whose physical file is gone (for example after out-of-band deletion or a
	// lost volume). It costs one file-existence check per tracked record, so it
	// is opt-in.
	// Default: false
	CleanupOrphanedMetadata bool

	// CleanupJunkFiles enables the recursive OS junk-file sweep (Thumbs.db,
	// .DS_Store, desktop.ini).
	// Default: true (callers that want the documented defaults start from
	// config.DefaultConfig()).
	CleanupJunkFiles bool

	// JunkFileCleanupInterval is the minimum interval between recursive junk-file
	// sweeps. Zero selects the default of 20 minutes; a non-positive value runs
	// the sweep on every cleanup cycle.
	JunkFileCleanupInterval time.Duration

	// CleanupInvalidDatabaseBackups enables removal of expired quarantined
	// database directories ("<dbDir>.corrupted.<stamp>").
	// Default: true (callers that want the documented defaults start from
	// config.DefaultConfig()).
	CleanupInvalidDatabaseBackups bool

	// OptimizeDatabases enables database optimization.
	// Default: true
	OptimizeDatabases bool

	// DatabaseOptimizationInterval is the interval between database optimization runs.
	// Default: 24 hours
	DatabaseOptimizationInterval time.Duration
}

// BackgroundCleanupService runs cleanup operations in the background on a scheduled interval.
type BackgroundCleanupService struct {
	cleanupService                core.CleanupService
	logger                        *logging.Runtime
	cleanupInterval               time.Duration
	initialDelay                  time.Duration
	cleanupEmptyDirectories       bool
	cleanupTimedOutFiles          bool
	processingTimeout             time.Duration
	cleanupPermanentlyFailedFiles bool
	failedFileRetention           time.Duration
	cleanupCompletedRecords       bool
	completedRecordRetention      time.Duration
	cleanupOrphanedMetadata       bool
	cleanupJunkFiles              bool
	junkFileCleanupInterval       time.Duration
	cleanupInvalidDatabaseBackups bool
	optimizeDatabases             bool
	databaseOptimizationInterval  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	lastOptimizationTime time.Time
	lastJunkFileCleanup  time.Time
	running              bool
}

// NewBackgroundCleanupService creates a new background cleanup service.
func NewBackgroundCleanupService(opts *BackgroundCleanupServiceOptions) (*BackgroundCleanupService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.CleanupService == nil {
		return nil, fmt.Errorf("cleanup service cannot be nil: %w", core.ErrInvalidArgument)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	// Set defaults
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = 1 * time.Hour
	}

	initialDelay := opts.InitialDelay
	if initialDelay == 0 {
		initialDelay = 1 * time.Minute
	}

	processingTimeout := opts.ProcessingTimeout
	if processingTimeout == 0 {
		processingTimeout = 30 * time.Minute
	}

	databaseOptimizationInterval := opts.DatabaseOptimizationInterval
	if databaseOptimizationInterval == 0 {
		databaseOptimizationInterval = 24 * time.Hour
	}

	failedFileRetention := opts.FailedFileRetentionPeriod
	if failedFileRetention == 0 {
		failedFileRetention = 3 * 24 * time.Hour
	}

	junkFileCleanupInterval := opts.JunkFileCleanupInterval
	if junkFileCleanupInterval == 0 {
		junkFileCleanupInterval = 20 * time.Minute
	}

	// Default enable all cleanup operations
	cleanupEmptyDirectories := opts.CleanupEmptyDirectories
	cleanupTimedOutFiles := opts.CleanupTimedOutFiles
	cleanupPermanentlyFailedFiles := opts.CleanupPermanentlyFailedFiles
	cleanupCompletedRecords := opts.CleanupCompletedRecords
	cleanupOrphanedMetadata := opts.CleanupOrphanedMetadata
	optimizeDatabases := opts.OptimizeDatabases

	// Every flag is honoured exactly as configured: an explicit false disables
	// that category for callers that want to disable cleanup entirely. Callers
	// that want the documented defaults start from config.DefaultConfig().

	return &BackgroundCleanupService{
		cleanupService:                opts.CleanupService,
		logger:                        logger,
		cleanupInterval:               cleanupInterval,
		initialDelay:                  initialDelay,
		cleanupEmptyDirectories:       cleanupEmptyDirectories,
		cleanupTimedOutFiles:          cleanupTimedOutFiles,
		processingTimeout:             processingTimeout,
		cleanupPermanentlyFailedFiles: cleanupPermanentlyFailedFiles,
		failedFileRetention:           failedFileRetention,
		cleanupCompletedRecords:       cleanupCompletedRecords,
		completedRecordRetention:      opts.CompletedRecordRetentionPeriod,
		cleanupOrphanedMetadata:       cleanupOrphanedMetadata,
		cleanupJunkFiles:              opts.CleanupJunkFiles,
		junkFileCleanupInterval:       junkFileCleanupInterval,
		cleanupInvalidDatabaseBackups: opts.CleanupInvalidDatabaseBackups,
		optimizeDatabases:             optimizeDatabases,
		databaseOptimizationInterval:  databaseOptimizationInterval,
		lastOptimizationTime:          time.Time{},
		running:                       false,
	}, nil
}

// Start starts the background cleanup service.
func (s *BackgroundCleanupService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("background cleanup service is already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running = true

	s.wg.Add(1)
	go s.run()

	s.emit(s.ctx, slog.LevelInfo, "started", "Background cleanup service started")

	return nil
}

// Stop stops the background cleanup service gracefully.
//
// Stop never holds s.mu while it waits for the run goroutine: the run goroutine
// takes s.mu.RLock() through shouldOptimizeDatabases(), so holding the write lock
// across s.wg.Wait() would deadlock against an in-flight cleanup cycle.
func (s *BackgroundCleanupService) Stop() error {
	s.mu.Lock()

	if !s.running {
		s.mu.Unlock()
		return fmt.Errorf("background cleanup service is not running")
	}

	ctx := s.ctx
	s.emit(ctx, slog.LevelInfo, "stopping", "Stopping background cleanup service")

	// Snapshot, cancel and toggle state while holding the lock, then release it
	// before waiting so the run goroutine can still observe service state.
	s.running = false
	s.cancel()
	s.mu.Unlock()

	// Wait outside the lock: the run goroutine must be able to take s.mu.RLock().
	s.wg.Wait()

	s.emit(ctx, slog.LevelInfo, "stopped", "Background cleanup service stopped")

	return nil
}

// IsRunning returns whether the service is currently running.
func (s *BackgroundCleanupService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// run is the main loop that executes cleanup operations on a schedule.
//
// Every cleanup pass is guarded by cleanupPass, so a panic inside one pass is
// contained and logged: Locus treats a failed cleanup cycle as non-fatal, and a
// background defect must never bring down the host process or stop the schedule.
func (s *BackgroundCleanupService) run() {
	defer s.wg.Done()

	// Initial delay before first cleanup
	s.emit(s.ctx, slog.LevelInfo, "initial_delay", "Background cleanup service waiting for initial delay", slog.Duration("delay", s.initialDelay))
	select {
	case <-time.After(s.initialDelay):
		// Continue
	case <-s.ctx.Done():
		return
	}

	// Create ticker for periodic cleanup
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	// Execute first cleanup immediately after initial delay
	s.cleanupPass()

	// Main loop
	for {
		select {
		case <-ticker.C:
			s.cleanupPass()
		case <-s.ctx.Done():
			s.emit(s.ctx, slog.LevelInfo, "shutting_down", "Cleanup service shutting down")
			return
		}
	}
}

// cleanupPass runs one cleanup cycle and contains a panic raised anywhere inside
// it. The deferred recover keeps the calling goroutine and its schedule intact.
func (s *BackgroundCleanupService) cleanupPass() {
	defer func() {
		if r := recover(); r != nil {
			s.emit(s.ctx, slog.LevelError, "cleanup_cycle_panicked",
				"Background cleanup cycle panicked and was contained",
				slog.String("panic_type", fmt.Sprintf("%T", r)))
		}
	}()

	s.executeCleanup()
}

// executeCleanup performs all configured cleanup operations.
func (s *BackgroundCleanupService) executeCleanup() {
	s.emit(s.ctx, slog.LevelInfo, "cycle_started", "Starting cleanup cycle")
	startTime := time.Now()

	totalStats := &core.CleanupStatistics{}

	// 1. Cleanup empty directories
	if s.cleanupEmptyDirectories {
		stats, err := s.cleanupService.CleanupEmptyDirectories(s.ctx)
		if err != nil {
			s.emitStepError(s.ctx, "empty_directories_failed", "Failed to cleanup empty directories", err)
		} else {
			totalStats.EmptyDirectoriesRemoved += stats.EmptyDirectoriesRemoved
			if stats.EmptyDirectoriesRemoved > 0 {
				s.emit(s.ctx, slog.LevelInfo, "empty_directories_removed", "Cleaned up empty directories", slog.Int("count", stats.EmptyDirectoriesRemoved))
			}
		}
	}

	// 2. Cleanup OS junk files on their own cadence: this is a recursive volume
	// scan, so it is throttled independently of the lighter status cleanup.
	if s.cleanupJunkFiles && s.shouldCleanupJunkFiles() {
		stats, err := s.cleanupService.CleanupJunkFiles(s.ctx)
		if err != nil {
			s.emitStepError(s.ctx, "junk_files_failed", "Failed to cleanup junk files", err)
		} else {
			totalStats.JunkFilesRemoved += stats.JunkFilesRemoved
			totalStats.SpaceFreed += stats.SpaceFreed
			s.recordJunkSweep()
			if stats.JunkFilesRemoved > 0 {
				s.emit(s.ctx, slog.LevelInfo, "junk_files_removed", "Removed OS junk files", slog.Int("count", stats.JunkFilesRemoved))
			}
		}
	}

	// 3. Cleanup timed-out processing files
	if s.cleanupTimedOutFiles {
		stats, err := s.cleanupService.CleanupTimedOutProcessingFiles(s.ctx, s.processingTimeout)
		if err != nil {
			s.emitStepError(s.ctx, "timed_out_files_failed", "Failed to cleanup timed-out files", err)
		} else {
			totalStats.TimedOutFilesReset += stats.TimedOutFilesReset
			if stats.TimedOutFilesReset > 0 {
				s.emit(s.ctx, slog.LevelInfo, "timed_out_files_reset", "Reset timed-out files", slog.Int("count", stats.TimedOutFilesReset))
			}
		}
	}

	// 4. Cleanup completed files
	if s.cleanupCompletedRecords {
		stats, err := s.cleanupService.CleanupCompletedFiles(s.ctx, s.completedRecordRetention)
		if err != nil {
			s.emitStepError(s.ctx, "completed_files_failed", "Failed to cleanup completed files", err)
		} else {
			totalStats.CompletedRecordsRemoved += stats.CompletedRecordsRemoved
			totalStats.SpaceFreed += stats.SpaceFreed
			if stats.CompletedRecordsRemoved > 0 {
				s.emit(s.ctx, slog.LevelInfo, "completed_files_removed", "Cleaned up completed files",
					slog.Int("count", stats.CompletedRecordsRemoved),
					slog.Int64("freed_bytes", stats.SpaceFreed))
			}
		}
	}

	// 5. Cleanup permanently failed files
	if s.cleanupPermanentlyFailedFiles {
		stats, err := s.cleanupService.CleanupPermanentlyFailedFiles(s.ctx, s.failedFileRetention)
		if err != nil {
			s.emitStepError(s.ctx, "permanent_files_failed", "Failed to cleanup permanently failed files", err)
		} else {
			totalStats.PermanentlyFailedFilesRemoved += stats.PermanentlyFailedFilesRemoved
			totalStats.DeadLetteredFiles += stats.DeadLetteredFiles
			totalStats.SpaceFreed += stats.SpaceFreed
			if stats.PermanentlyFailedFilesRemoved > 0 || stats.DeadLetteredFiles > 0 {
				s.emit(s.ctx, slog.LevelInfo, "permanent_files_removed", "Applied the permanent-failure disposition",
					slog.Int("count", stats.PermanentlyFailedFilesRemoved),
					slog.Int("dead_lettered", stats.DeadLetteredFiles),
					slog.Int64("freed_bytes", stats.SpaceFreed))
			}
		}
	}

	// 6. Cleanup orphaned metadata (records whose physical file is gone)
	if s.cleanupOrphanedMetadata {
		stats, err := s.cleanupService.CleanupOrphanedMetadata(s.ctx)
		if err != nil {
			s.emitStepError(s.ctx, "orphaned_metadata_failed", "Failed to cleanup orphaned metadata", err)
		} else {
			totalStats.OrphanedMetadataRemoved += stats.OrphanedMetadataRemoved
			if stats.OrphanedMetadataRemoved > 0 {
				s.emit(s.ctx, slog.LevelInfo, "orphaned_metadata_removed", "Removed orphaned metadata records",
					slog.Int("count", stats.OrphanedMetadataRemoved))
			}
		}
	}

	// 7. Remove expired quarantined database directories (<dbDir>.corrupted.<stamp>)
	if s.cleanupInvalidDatabaseBackups {
		stats, err := s.cleanupService.CleanupInvalidDatabaseBackups(s.ctx)
		if err != nil {
			s.emitStepError(s.ctx, "invalid_database_backups_failed", "Failed to cleanup invalid database backups", err)
		} else {
			totalStats.InvalidDatabaseBackupsRemoved += stats.InvalidDatabaseBackupsRemoved
			totalStats.SpaceFreed += stats.SpaceFreed
			if stats.InvalidDatabaseBackupsRemoved > 0 {
				s.emit(s.ctx, slog.LevelInfo, "invalid_database_backups_removed", "Removed expired quarantined databases",
					slog.Int("count", stats.InvalidDatabaseBackupsRemoved))
			}
		}
	}

	// 8. Optimize databases (if enough time has passed)
	if s.optimizeDatabases && s.shouldOptimizeDatabases() {
		s.emit(s.ctx, slog.LevelInfo, "database_optimization_started", "Starting database optimization")
		stats, err := s.cleanupService.OptimizeDatabases(s.ctx)
		if err != nil {
			s.emitStepError(s.ctx, "database_optimization_failed", "Failed to optimize databases", err)
		} else {
			totalStats.MetadataDatabasesOptimized += stats.MetadataDatabasesOptimized
			totalStats.QuotaDatabasesOptimized += stats.QuotaDatabasesOptimized
			s.mu.Lock()
			s.lastOptimizationTime = time.Now()
			s.mu.Unlock()
			s.emit(s.ctx, slog.LevelInfo, "database_optimization_completed", "Database optimization completed",
				slog.Int("metadata_databases", stats.MetadataDatabasesOptimized),
				slog.Int("quota_databases", stats.QuotaDatabasesOptimized))
		}
	}

	duration := time.Since(startTime)
	s.emit(s.ctx, slog.LevelInfo, "cycle_completed", "Cleanup cycle completed",
		slog.Duration("duration", duration),
		slog.Int("empty_dirs_removed", totalStats.EmptyDirectoriesRemoved),
		slog.Int("junk_files_removed", totalStats.JunkFilesRemoved),
		slog.Int("timed_out_reset", totalStats.TimedOutFilesReset),
		slog.Int("completed_removed", totalStats.CompletedRecordsRemoved),
		slog.Int("failed_removed", totalStats.PermanentlyFailedFilesRemoved),
		slog.Int("dead_lettered", totalStats.DeadLetteredFiles),
		slog.Int("invalid_database_backups_removed", totalStats.InvalidDatabaseBackupsRemoved),
		slog.Int("metadata_databases_optimized", totalStats.MetadataDatabasesOptimized),
		slog.Int("quota_databases_optimized", totalStats.QuotaDatabasesOptimized),
		slog.Int64("space_freed_bytes", totalStats.SpaceFreed))
}

func (s *BackgroundCleanupService) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	s.logger.Emit(ctx, logging.Record{
		Level: level, Component: "cleanup.background", Event: event, Message: message, Attrs: attrs,
	})
}

func errorTypeAttr(err error) slog.Attr {
	return slog.String("error_type", fmt.Sprintf("%T", err))
}

// emitStepError reports a failed cleanup step. Cancellation is a normal shutdown
// outcome, not an error.
func (s *BackgroundCleanupService) emitStepError(ctx context.Context, event, message string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		s.emit(ctx, slog.LevelDebug, event+"_cancelled", message+" was cancelled")
		return
	}
	s.emit(ctx, slog.LevelError, event, message, errorTypeAttr(err))
}

// shouldOptimizeDatabases returns true if enough time has passed since last optimization.
func (s *BackgroundCleanupService) shouldOptimizeDatabases() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastOptimizationTime.IsZero() {
		return true
	}
	return time.Since(s.lastOptimizationTime) >= s.databaseOptimizationInterval
}

// shouldCleanupJunkFiles returns true if enough time has passed since the last
// recursive junk-file sweep. A non-positive interval sweeps on every cycle.
func (s *BackgroundCleanupService) shouldCleanupJunkFiles() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.junkFileCleanupInterval <= 0 {
		return true
	}
	if s.lastJunkFileCleanup.IsZero() {
		return true
	}
	return time.Since(s.lastJunkFileCleanup) >= s.junkFileCleanupInterval
}

// recordJunkSweep marks a successful junk-file sweep, which starts the throttle
// window for the next one.
func (s *BackgroundCleanupService) recordJunkSweep() {
	s.mu.Lock()
	s.lastJunkFileCleanup = time.Now()
	s.mu.Unlock()
}
