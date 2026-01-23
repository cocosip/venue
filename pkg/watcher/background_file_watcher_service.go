package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// BackgroundFileWatcherServiceOptions configures the background file watcher service.
type BackgroundFileWatcherServiceOptions struct {
	// FileWatcher is the underlying file watcher that performs the actual scans.
	FileWatcher core.FileWatcher

	// Logger is the logger instance to use.
	// If nil, uses slog.Default().
	Logger *slog.Logger

	// InitialDelay is the delay before the first scan.
	// Default: 10 seconds
	InitialDelay time.Duration

	// MinimumPollingInterval is the minimum allowed polling interval.
	// Default: 5 seconds
	MinimumPollingInterval time.Duration

	// DefaultPollingInterval is the default polling interval if no watchers are configured.
	// Default: 30 seconds
	DefaultPollingInterval time.Duration

	// DisabledCheckInterval is how often to check if service should be re-enabled.
	// Default: 1 minute
	DisabledCheckInterval time.Duration

	// ServiceEnabled controls whether the service is globally enabled.
	// Default: true
	ServiceEnabled bool
}

// BackgroundFileWatcherService runs file watcher scans in the background on a scheduled interval.
type BackgroundFileWatcherService struct {
	fileWatcher            core.FileWatcher
	logger                 *slog.Logger
	initialDelay           time.Duration
	minimumPollingInterval time.Duration
	defaultPollingInterval time.Duration
	disabledCheckInterval  time.Duration
	serviceEnabled         bool

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// NewBackgroundFileWatcherService creates a new background file watcher service.
func NewBackgroundFileWatcherService(opts *BackgroundFileWatcherServiceOptions) (*BackgroundFileWatcherService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.FileWatcher == nil {
		return nil, fmt.Errorf("file watcher cannot be nil: %w", core.ErrInvalidArgument)
	}

	// Set logger (use default if not provided)
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Set defaults
	initialDelay := opts.InitialDelay
	if initialDelay == 0 {
		initialDelay = 10 * time.Second
	}

	minimumPollingInterval := opts.MinimumPollingInterval
	if minimumPollingInterval == 0 {
		minimumPollingInterval = 5 * time.Second
	}

	defaultPollingInterval := opts.DefaultPollingInterval
	if defaultPollingInterval == 0 {
		defaultPollingInterval = 30 * time.Second
	}

	disabledCheckInterval := opts.DisabledCheckInterval
	if disabledCheckInterval == 0 {
		disabledCheckInterval = 1 * time.Minute
	}

	return &BackgroundFileWatcherService{
		fileWatcher:            opts.FileWatcher,
		logger:                 logger,
		initialDelay:           initialDelay,
		minimumPollingInterval: minimumPollingInterval,
		defaultPollingInterval: defaultPollingInterval,
		disabledCheckInterval:  disabledCheckInterval,
		serviceEnabled:         opts.ServiceEnabled,
		running:                false,
	}, nil
}

// Start starts the background file watcher service.
func (s *BackgroundFileWatcherService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("background file watcher service is already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running = true

	s.wg.Add(1)
	go s.run()

	s.logger.Info("Background file watcher service started")

	return nil
}

// Stop stops the background file watcher service gracefully.
func (s *BackgroundFileWatcherService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("background file watcher service is not running")
	}

	s.logger.Info("Stopping background file watcher service...")
	s.cancel()
	s.wg.Wait()
	s.running = false

	s.logger.Info("Background file watcher service stopped")

	return nil
}

// IsRunning returns whether the service is currently running.
func (s *BackgroundFileWatcherService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// SetEnabled sets whether the service is enabled.
func (s *BackgroundFileWatcherService) SetEnabled(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serviceEnabled = enabled
}

// IsEnabled returns whether the service is enabled.
func (s *BackgroundFileWatcherService) IsEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.serviceEnabled
}

// run is the main loop that executes file watcher scans on a schedule.
func (s *BackgroundFileWatcherService) run() {
	defer s.wg.Done()

	// Initial delay before first scan
	s.logger.Info("Background file watcher service waiting for initial delay", "delay", s.initialDelay)
	select {
	case <-time.After(s.initialDelay):
		// Continue
	case <-s.ctx.Done():
		return
	}

	// Main loop
	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("File watcher service shutting down")
			return
		default:
			// Check if service is enabled
			if !s.IsEnabled() {
				s.logger.Debug("File watcher service is globally disabled", "checkInterval", s.disabledCheckInterval)

				select {
				case <-time.After(s.disabledCheckInterval):
					continue
				case <-s.ctx.Done():
					return
				}
			}

			// Execute scan cycle
			s.executeScanCycle()

			// Calculate next scan interval
			interval := s.calculateNextInterval()
			s.logger.Debug("Next scan cycle", "interval", interval)

			// Wait for next cycle
			select {
			case <-time.After(interval):
				// Continue to next cycle
			case <-s.ctx.Done():
				return
			}
		}
	}
}

// executeScanCycle performs a scan of all enabled watchers.
func (s *BackgroundFileWatcherService) executeScanCycle() {
	s.logger.Info("Starting file watcher scan cycle")
	startTime := time.Now()

	// Get all watchers
	watchers, err := s.fileWatcher.GetAllWatchers(s.ctx)
	if err != nil {
		s.logger.Error("Failed to get watchers", "error", err)
		return
	}

	// Count enabled watchers
	enabledCount := 0
	for _, watcher := range watchers {
		if watcher.Enabled {
			enabledCount++
		}
	}

	if enabledCount == 0 {
		s.logger.Debug("No enabled watchers found, skipping scan")
		return
	}

	s.logger.Info("Scanning enabled watchers", "count", enabledCount)

	// Scan all watchers
	totalImported := 0
	totalFailed := 0
	totalSkipped := 0
	totalBytes := int64(0)

	for _, watcher := range watchers {
		if !watcher.Enabled {
			continue
		}

		if s.ctx.Err() != nil {
			break // Service stopping
		}

		result, err := s.fileWatcher.ScanNow(s.ctx, watcher.WatcherID)
		if err != nil {
			s.logger.Error("Failed to scan watcher", "watcherID", watcher.WatcherID, "error", err)
			continue
		}

		if result.FilesImported > 0 || result.FilesFailed > 0 {
			s.logger.Info("Watcher scan completed",
				"watcherID", watcher.WatcherID,
				"discovered", result.FilesDiscovered,
				"imported", result.FilesImported,
				"skipped", result.FilesSkipped,
				"failed", result.FilesFailed,
				"bytes", result.BytesImported,
				"duration", result.ScanDuration)

			if len(result.Errors) > 0 {
				// Log first few errors
				for i, errMsg := range result.Errors {
					if i >= 5 {
						s.logger.Warn("Additional errors", "watcherID", watcher.WatcherID, "count", len(result.Errors)-5)
						break
					}
					s.logger.Warn("Watcher error", "watcherID", watcher.WatcherID, "error", errMsg)
				}
			}
		} else if result.FilesDiscovered > 0 {
			s.logger.Debug("Watcher found files but all skipped",
				"watcherID", watcher.WatcherID,
				"count", result.FilesDiscovered)
		}

		totalImported += result.FilesImported
		totalFailed += result.FilesFailed
		totalSkipped += result.FilesSkipped
		totalBytes += result.BytesImported
	}

	duration := time.Since(startTime)
	s.logger.Info("File watcher scan cycle completed",
		"duration", duration,
		"total_imported", totalImported,
		"total_failed", totalFailed,
		"total_skipped", totalSkipped,
		"total_bytes", totalBytes)
}

// calculateNextInterval calculates the next scan interval based on watcher configurations.
func (s *BackgroundFileWatcherService) calculateNextInterval() time.Duration {
	watchers, err := s.fileWatcher.GetAllWatchers(s.ctx)
	if err != nil {
		return s.defaultPollingInterval
	}

	// Find the minimum polling interval among enabled watchers
	minInterval := s.defaultPollingInterval
	found := false

	for _, watcher := range watchers {
		if !watcher.Enabled {
			continue
		}

		found = true
		if watcher.PollingInterval < minInterval {
			minInterval = watcher.PollingInterval
		}
	}

	if !found {
		return s.defaultPollingInterval
	}

	// Enforce minimum interval limit
	if minInterval < s.minimumPollingInterval {
		s.logger.Warn("Polling interval too short, using minimum",
			"requested", minInterval,
			"minimum", s.minimumPollingInterval)
		minInterval = s.minimumPollingInterval
	}

	return minInterval
}
