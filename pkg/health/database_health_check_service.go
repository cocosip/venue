package health

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// DatabaseHealthCheckServiceOptions configures the database health check service.
type DatabaseHealthCheckServiceOptions struct {
	// DatabaseHealthChecker performs the actual health checks.
	DatabaseHealthChecker core.DatabaseHealthChecker

	// Logger is the logger instance to use.
	// If nil, uses slog.Default().
	Logger *slog.Logger

	// InitialDelay is the delay before performing the health check.
	// Default: 2 seconds (to allow other services to initialize)
	InitialDelay time.Duration

	// MaxRetries is the maximum number of retries for health checks.
	// Default: 3
	MaxRetries int

	// RetryDelay is the delay between retries.
	// Default: 1 second
	RetryDelay time.Duration

	// CheckOnStartupOnly if true, only checks on startup. Otherwise checks periodically.
	// Default: true
	CheckOnStartupOnly bool

	// PeriodicCheckInterval is the interval for periodic checks (if not startup-only).
	// Default: 1 hour
	PeriodicCheckInterval time.Duration
}

// DatabaseHealthCheckService performs database health checks on startup.
type DatabaseHealthCheckService struct {
	healthChecker         core.DatabaseHealthChecker
	logger                *slog.Logger
	initialDelay          time.Duration
	maxRetries            int
	retryDelay            time.Duration
	checkOnStartupOnly    bool
	periodicCheckInterval time.Duration

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// NewDatabaseHealthCheckService creates a new database health check service.
func NewDatabaseHealthCheckService(opts *DatabaseHealthCheckServiceOptions) (*DatabaseHealthCheckService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.DatabaseHealthChecker == nil {
		return nil, fmt.Errorf("database health checker cannot be nil: %w", core.ErrInvalidArgument)
	}

	// Set logger (use default if not provided)
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Set defaults
	initialDelay := opts.InitialDelay
	if initialDelay == 0 {
		initialDelay = 2 * time.Second
	}

	maxRetries := opts.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	retryDelay := opts.RetryDelay
	if retryDelay == 0 {
		retryDelay = 1 * time.Second
	}

	periodicCheckInterval := opts.PeriodicCheckInterval
	if periodicCheckInterval == 0 {
		periodicCheckInterval = 1 * time.Hour
	}

	return &DatabaseHealthCheckService{
		healthChecker:         opts.DatabaseHealthChecker,
		logger:                logger,
		initialDelay:          initialDelay,
		maxRetries:            maxRetries,
		retryDelay:            retryDelay,
		checkOnStartupOnly:    opts.CheckOnStartupOnly,
		periodicCheckInterval: periodicCheckInterval,
		running:               false,
	}, nil
}

// Start starts the database health check service.
func (s *DatabaseHealthCheckService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("database health check service is already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running = true

	s.wg.Add(1)
	go s.run()

	s.logger.Info("Database health check service started")

	return nil
}

// Stop stops the database health check service gracefully.
func (s *DatabaseHealthCheckService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("database health check service is not running")
	}

	s.logger.Info("Stopping database health check service...")
	s.cancel()
	s.wg.Wait()
	s.running = false

	s.logger.Info("Database health check service stopped")

	return nil
}

// IsRunning returns whether the service is currently running.
func (s *DatabaseHealthCheckService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// run is the main loop that performs health checks.
func (s *DatabaseHealthCheckService) run() {
	defer s.wg.Done()

	// Initial delay to allow other services to initialize
	s.logger.Info("Waiting for other services to initialize", "delay", s.initialDelay)
	select {
	case <-time.After(s.initialDelay):
		// Continue
	case <-s.ctx.Done():
		return
	}

	// Perform health check with retry
	s.logger.Info("Starting database health check")
	s.performHealthCheckWithRetry()

	// If startup-only mode, stop here
	if s.checkOnStartupOnly {
		s.logger.Info("Database health check completed (startup-only mode)")
		return
	}

	// Periodic checks
	ticker := time.NewTicker(s.periodicCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.logger.Info("Performing periodic database health check")
			s.performHealthCheckWithRetry()
		case <-s.ctx.Done():
			s.logger.Info("Database health check service shutting down")
			return
		}
	}
}

// performHealthCheckWithRetry performs health check with retry logic.
func (s *DatabaseHealthCheckService) performHealthCheckWithRetry() {
	var lastReport *core.DatabaseHealthReport
	var lastErr error

	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		report, err := s.healthChecker.CheckAllDatabases(s.ctx)
		if err != nil {
			lastErr = err
			s.logger.Warn("Health check failed",
				"attempt", attempt,
				"maxRetries", s.maxRetries,
				"error", err)

			if attempt < s.maxRetries {
				select {
				case <-time.After(s.retryDelay):
					continue
				case <-s.ctx.Done():
					return
				}
			}
			continue
		}

		lastReport = report
		lastErr = nil

		// If all healthy, no need to retry
		if report.AllHealthy {
			if attempt > 1 {
				s.logger.Info("Database health check succeeded after retries",
					"attempt", attempt,
					"maxRetries", s.maxRetries)
			}
			break
		}

		// If not healthy and not last attempt, retry
		if attempt < s.maxRetries {
			s.logger.Debug("Health check found issues, retrying to rule out timing conflicts",
				"attempt", attempt,
				"maxRetries", s.maxRetries,
				"corrupted", len(report.CorruptedDatabases))

			select {
			case <-time.After(s.retryDelay):
				continue
			case <-s.ctx.Done():
				return
			}
		}
	}

	// Report results
	if lastErr != nil {
		s.logger.Error("Database health check failed after all retries", "error", lastErr)
		return
	}

	if lastReport == nil {
		s.logger.Warn("Database health check completed with no report")
		return
	}

	s.reportHealthStatus(lastReport)
}

// reportHealthStatus reports the health check results.
func (s *DatabaseHealthCheckService) reportHealthStatus(report *core.DatabaseHealthReport) {
	// Check for no databases
	if report.HealthyDatabases == 0 && len(report.CorruptedDatabases) == 0 {
		if len(report.OrphanedTenants) > 0 {
			s.logger.Warn("METADATA LOSS DETECTED",
				"orphanedTenants", len(report.OrphanedTenants),
				"tenants", report.OrphanedTenants)
			s.logger.Warn("Orphaned tenants have physical files but no metadata database")
			s.logger.Warn("Consider using recovery procedures to rebuild metadata")
		} else {
			s.logger.Info("No database files found. This is normal for first startup.")
		}
		return
	}

	// All healthy
	if report.AllHealthy {
		s.logger.Info("Database health check completed. All databases are healthy.",
			"count", report.HealthyDatabases)
		return
	}

	// Some corrupted databases
	s.logger.Warn("Database health check completed with issues",
		"healthy", report.HealthyDatabases,
		"corrupted", len(report.CorruptedDatabases))

	for _, corrupted := range report.CorruptedDatabases {
		s.logger.Error("CORRUPTED DATABASE DETECTED",
			"type", corrupted.DatabaseType,
			"tenant", corrupted.TenantID,
			"path", corrupted.DatabasePath,
			"error", corrupted.Error)
	}

	s.logger.Error("Manual intervention required to repair corrupted databases")
}

// CheckNow manually triggers a health check.
func (s *DatabaseHealthCheckService) CheckNow() (*core.DatabaseHealthReport, error) {
	return s.healthChecker.CheckAllDatabases(context.Background())
}
