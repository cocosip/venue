package health

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// DatabaseHealthCheckServiceOptions configures the database health check service.
type DatabaseHealthCheckServiceOptions struct {
	// DatabaseHealthChecker performs the actual health checks.
	DatabaseHealthChecker core.DatabaseHealthChecker

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

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
	logger                *logging.Runtime
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

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
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

	s.emit(s.ctx, slog.LevelInfo, "started", "Database health check service started")

	return nil
}

// Stop stops the database health check service gracefully.
func (s *DatabaseHealthCheckService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("database health check service is not running")
	}

	s.emit(s.ctx, slog.LevelInfo, "stopping", "Stopping database health check service")
	s.cancel()
	s.wg.Wait()
	s.running = false

	s.emit(s.ctx, slog.LevelInfo, "stopped", "Database health check service stopped")

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
	s.emit(s.ctx, slog.LevelInfo, "initial_delay", "Waiting for other services to initialize", slog.Duration("delay", s.initialDelay))
	select {
	case <-time.After(s.initialDelay):
		// Continue
	case <-s.ctx.Done():
		return
	}

	// Perform health check with retry
	s.emit(s.ctx, slog.LevelInfo, "check_started", "Starting database health check")
	s.performHealthCheckWithRetry()

	// If startup-only mode, stop here
	if s.checkOnStartupOnly {
		s.emit(s.ctx, slog.LevelInfo, "startup_check_completed", "Database health check completed")
		return
	}

	// Periodic checks
	ticker := time.NewTicker(s.periodicCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.emit(s.ctx, slog.LevelInfo, "periodic_check_started", "Performing periodic database health check")
			s.performHealthCheckWithRetry()
		case <-s.ctx.Done():
			s.emit(s.ctx, slog.LevelInfo, "shutting_down", "Database health check service shutting down")
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
			s.emit(s.ctx, slog.LevelWarn, "check_failed", "Health check failed",
				slog.Int("attempt", attempt),
				slog.Int("max_retries", s.maxRetries),
				errorTypeAttr(err))

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
				s.emit(s.ctx, slog.LevelInfo, "check_recovered", "Database health check succeeded after retries",
					slog.Int("attempt", attempt),
					slog.Int("max_retries", s.maxRetries))
			}
			break
		}

		// If not healthy and not last attempt, retry
		if attempt < s.maxRetries {
			s.emit(s.ctx, slog.LevelDebug, "check_retrying", "Health check found issues, retrying",
				slog.Int("attempt", attempt),
				slog.Int("max_retries", s.maxRetries),
				slog.Int("corrupted", len(report.CorruptedDatabases)))

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
		s.emit(s.ctx, slog.LevelError, "check_exhausted", "Database health check failed after all retries", errorTypeAttr(lastErr))
		return
	}

	if lastReport == nil {
		s.emit(s.ctx, slog.LevelWarn, "report_missing", "Database health check completed with no report")
		return
	}

	s.reportHealthStatus(lastReport)
}

// reportHealthStatus reports the health check results.
//
// Reporting is log-only by design: Start never fails because a database is
// unhealthy, so the caller decides whether to repair, ignore, or shut down.
func (s *DatabaseHealthCheckService) reportHealthStatus(report *core.DatabaseHealthReport) {
	// Check for no databases
	if report.HealthyDatabases == 0 && len(report.CorruptedDatabases) == 0 {
		if len(report.OrphanedTenants) > 0 {
			s.emit(s.ctx, slog.LevelWarn, "metadata_loss_detected", "Orphaned tenants have physical files but no metadata database",
				slog.Int("orphaned_tenants", len(report.OrphanedTenants)))
		} else {
			s.emit(s.ctx, slog.LevelInfo, "no_databases", "No database files found")
		}
		return
	}

	// All healthy
	if report.AllHealthy {
		s.emit(s.ctx, slog.LevelInfo, "check_healthy", "Database health check completed; all databases are healthy",
			slog.Int("count", report.HealthyDatabases))
		return
	}

	// Some corrupted databases
	s.emit(s.ctx, slog.LevelWarn, "check_unhealthy", "Database health check completed with issues",
		slog.Int("healthy", report.HealthyDatabases),
		slog.Int("corrupted", len(report.CorruptedDatabases)))

	for _, corrupted := range report.CorruptedDatabases {
		s.emit(s.ctx, slog.LevelError, "database_corrupted", "Corrupted database detected",
			slog.Any("database_type", corrupted.DatabaseType),
			slog.String("tenant_id", corrupted.TenantID))
	}

	s.emit(s.ctx, slog.LevelError, "manual_intervention_required", "Manual intervention required to repair corrupted databases")
}

func (s *DatabaseHealthCheckService) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	s.logger.Emit(ctx, logging.Record{
		Level: level, Component: "health.database_service", Event: event, Message: message, Attrs: attrs,
	})
}

// CheckNow manually triggers a health check.
func (s *DatabaseHealthCheckService) CheckNow() (*core.DatabaseHealthReport, error) {
	return s.healthChecker.CheckAllDatabases(context.Background())
}
