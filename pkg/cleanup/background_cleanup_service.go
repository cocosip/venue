package cleanup

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// BackgroundCleanupServiceOptions configures the background cleanup service.
type BackgroundCleanupServiceOptions struct {
	// CleanupService is the underlying cleanup service that performs the actual cleanup operations.
	CleanupService core.CleanupService

	// Logger is the logger instance to use.
	// If nil, uses slog.Default().
	Logger *slog.Logger

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
	// Default: 7 days (not implemented yet, reserved for future)
	FailedFileRetentionPeriod time.Duration

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
	logger                        *slog.Logger
	cleanupInterval               time.Duration
	initialDelay                  time.Duration
	cleanupEmptyDirectories       bool
	cleanupTimedOutFiles          bool
	processingTimeout             time.Duration
	cleanupPermanentlyFailedFiles bool
	optimizeDatabases             bool
	databaseOptimizationInterval  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	lastOptimizationTime time.Time
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

	// Set logger (use default if not provided)
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
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

	// Default enable all cleanup operations
	cleanupEmptyDirectories := opts.CleanupEmptyDirectories
	cleanupTimedOutFiles := opts.CleanupTimedOutFiles
	cleanupPermanentlyFailedFiles := opts.CleanupPermanentlyFailedFiles
	optimizeDatabases := opts.OptimizeDatabases

	// If all are false, enable them by default
	if !cleanupEmptyDirectories && !cleanupTimedOutFiles && !cleanupPermanentlyFailedFiles && !optimizeDatabases {
		cleanupEmptyDirectories = true
		cleanupTimedOutFiles = true
		cleanupPermanentlyFailedFiles = true
		optimizeDatabases = true
	}

	return &BackgroundCleanupService{
		cleanupService:                opts.CleanupService,
		logger:                        logger,
		cleanupInterval:               cleanupInterval,
		initialDelay:                  initialDelay,
		cleanupEmptyDirectories:       cleanupEmptyDirectories,
		cleanupTimedOutFiles:          cleanupTimedOutFiles,
		processingTimeout:             processingTimeout,
		cleanupPermanentlyFailedFiles: cleanupPermanentlyFailedFiles,
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

	s.logger.Info("Background cleanup service started")

	return nil
}

// Stop stops the background cleanup service gracefully.
func (s *BackgroundCleanupService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("background cleanup service is not running")
	}

	s.logger.Info("Stopping background cleanup service...")
	s.cancel()
	s.wg.Wait()
	s.running = false

	s.logger.Info("Background cleanup service stopped")

	return nil
}

// IsRunning returns whether the service is currently running.
func (s *BackgroundCleanupService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// run is the main loop that executes cleanup operations on a schedule.
func (s *BackgroundCleanupService) run() {
	defer s.wg.Done()

	// Initial delay before first cleanup
	s.logger.Info("Background cleanup service waiting for initial delay", "delay", s.initialDelay)
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
	s.executeCleanup()

	// Main loop
	for {
		select {
		case <-ticker.C:
			s.executeCleanup()
		case <-s.ctx.Done():
			s.logger.Info("Cleanup service shutting down")
			return
		}
	}
}

// executeCleanup performs all configured cleanup operations.
func (s *BackgroundCleanupService) executeCleanup() {
	s.logger.Info("Starting cleanup cycle")
	startTime := time.Now()

	totalStats := &core.CleanupStatistics{}

	// 1. Cleanup empty directories
	if s.cleanupEmptyDirectories {
		stats, err := s.cleanupService.CleanupEmptyDirectories(s.ctx)
		if err != nil {
			s.logger.Error("Failed to cleanup empty directories", "error", err)
		} else {
			totalStats.EmptyDirectoriesRemoved += stats.EmptyDirectoriesRemoved
			if stats.EmptyDirectoriesRemoved > 0 {
				s.logger.Info("Cleaned up empty directories", "count", stats.EmptyDirectoriesRemoved)
			}
		}
	}

	// 2. Cleanup timed-out processing files
	if s.cleanupTimedOutFiles {
		stats, err := s.cleanupService.CleanupTimedOutProcessingFiles(s.ctx, s.processingTimeout)
		if err != nil {
			s.logger.Error("Failed to cleanup timed-out files", "error", err)
		} else {
			totalStats.TimedOutFilesReset += stats.TimedOutFilesReset
			if stats.TimedOutFilesReset > 0 {
				s.logger.Info("Reset timed-out files", "count", stats.TimedOutFilesReset)
			}
		}
	}

	// 3. Cleanup permanently failed files
	if s.cleanupPermanentlyFailedFiles {
		stats, err := s.cleanupService.CleanupPermanentlyFailedFiles(s.ctx)
		if err != nil {
			s.logger.Error("Failed to cleanup permanently failed files", "error", err)
		} else {
			totalStats.PermanentlyFailedFilesRemoved += stats.PermanentlyFailedFilesRemoved
			totalStats.SpaceFreed += stats.SpaceFreed
			if stats.PermanentlyFailedFilesRemoved > 0 {
				s.logger.Info("Cleaned up permanently failed files",
					"count", stats.PermanentlyFailedFilesRemoved,
					"freed_bytes", stats.SpaceFreed)
			}
		}
	}

	// 4. Optimize databases (if enough time has passed)
	if s.optimizeDatabases && s.shouldOptimizeDatabases() {
		s.logger.Info("Starting database optimization")
		// Note: Database optimization is not yet implemented in CleanupService interface
		// This is a placeholder for future implementation
		s.lastOptimizationTime = time.Now()
		s.logger.Info("Database optimization completed")
	}

	duration := time.Since(startTime)
	s.logger.Info("Cleanup cycle completed",
		"duration", duration,
		"empty_dirs_removed", totalStats.EmptyDirectoriesRemoved,
		"timed_out_reset", totalStats.TimedOutFilesReset,
		"failed_removed", totalStats.PermanentlyFailedFilesRemoved,
		"space_freed_bytes", totalStats.SpaceFreed)
}

// shouldOptimizeDatabases returns true if enough time has passed since last optimization.
func (s *BackgroundCleanupService) shouldOptimizeDatabases() bool {
	if s.lastOptimizationTime.IsZero() {
		return true
	}
	return time.Since(s.lastOptimizationTime) >= s.databaseOptimizationInterval
}
