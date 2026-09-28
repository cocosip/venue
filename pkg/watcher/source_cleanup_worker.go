package watcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/logging"
)

// SourceCleanupWorkerOptions configures the cancellation-aware cleanup worker.
// Execute must perform one source action and return an error for retryable
// failures. Quarantine is called after the retry budget is exhausted.
type SourceCleanupWorkerOptions struct {
	Store                        SourceCleanupStore
	PollingInterval              time.Duration
	MaxConcurrentActions         int
	ImportReservationTimeout     time.Duration
	TerminalRetentionPeriod      time.Duration
	TerminalPruneBatchSize       int
	DatabaseOptimization         bool
	DatabaseOptimizationInterval time.Duration
	RetryInitialDelay            time.Duration
	RetryMaxDelay                time.Duration
	Execute                      func(context.Context, SourceCleanupJob) error
	Quarantine                   func(context.Context, SourceCleanupJob) error
	// RuntimeEnabled gates maintenance and cleanup processing. It is used by
	// the Venue integration to follow the persisted global and per-watcher
	// enablement state; nil means enabled.
	RuntimeEnabled func(context.Context) bool
	// ImportInFlight protects a live import from the stale-reservation
	// recovery. It receives every reservation whose import has outlived the
	// ImportReservationTimeout and reports whether that import is still running
	// in this process; such a reservation must not be recovered, or a successful
	// import would be turned into a spurious failure and a duplicate. nil
	// recovers every stale reservation.
	ImportInFlight func(SourceCleanupJob) bool
	// Logging is the injected logging runtime. The worker's whole store surface
	// is best-effort, so this logger is the only place where a dying cleanup
	// pipeline (a locked, corrupted, or full cleanup database) remains visible;
	// nil disables logging.
	Logging *logging.Runtime
	Now     func() time.Time
}

// SourceCleanupWorker drains durable source cleanup jobs until its context is
// cancelled. It does not own or close the store; the caller owns store lifetime.
//
// Start and Stop are idempotent and restartable: Stop joins the run loop before
// returning, and a Start after a completed Stop launches a fresh loop. They are
// serialized with each other, so a Stop in progress delays a concurrent Start
// instead of racing it.
type SourceCleanupWorker struct {
	options SourceCleanupWorkerOptions

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	lastOptimize time.Time
}

// NewSourceCleanupWorker validates options and constructs a worker.
func NewSourceCleanupWorker(options SourceCleanupWorkerOptions) (*SourceCleanupWorker, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("source cleanup store is required")
	}
	if options.Execute == nil {
		return nil, fmt.Errorf("source cleanup execute function is required")
	}
	if options.PollingInterval <= 0 {
		return nil, fmt.Errorf("source cleanup polling interval must be positive")
	}
	if options.MaxConcurrentActions <= 0 {
		return nil, fmt.Errorf("source cleanup concurrency must be positive")
	}
	if options.ImportReservationTimeout <= 0 {
		return nil, fmt.Errorf("source cleanup reservation timeout must be positive")
	}
	if options.TerminalRetentionPeriod <= 0 {
		return nil, fmt.Errorf("source cleanup terminal retention must be positive")
	}
	if options.TerminalPruneBatchSize <= 0 {
		return nil, fmt.Errorf("source cleanup prune batch size must be positive")
	}
	if options.RetryInitialDelay < 0 || options.RetryMaxDelay < 0 {
		return nil, fmt.Errorf("source cleanup retry delays cannot be negative")
	}
	if options.RetryMaxDelay == 0 {
		options.RetryMaxDelay = options.RetryInitialDelay
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.DatabaseOptimization && options.DatabaseOptimizationInterval <= 0 {
		return nil, fmt.Errorf("source cleanup optimization interval must be positive")
	}
	return &SourceCleanupWorker{options: options, lastOptimize: options.Now()}, nil
}

// Start launches the worker loop. It is harmless while the loop is already
// running, and it relaunches after a completed Stop.
func (w *SourceCleanupWorker) Start(parent context.Context) {
	if parent == nil {
		parent = context.Background()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	w.running = true
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(ctx)
	}()
}

// Stop cancels the worker and waits for all in-flight actions. It is safe to
// call without a prior Start and safe to repeat.
func (w *SourceCleanupWorker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.running = false
	// The mutex is released before the join: the run loop never takes it, so
	// holding it across Wait would only delay the caller, but Start must not
	// run while Wait is in flight, which the same lock serializes.
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
}

func (w *SourceCleanupWorker) run(ctx context.Context) {
	ticker := time.NewTicker(w.options.PollingInterval)
	defer ticker.Stop()
	for {
		w.processPass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *SourceCleanupWorker) processPass(ctx context.Context) {
	if w.options.RuntimeEnabled != nil && !w.options.RuntimeEnabled(ctx) {
		return
	}
	now := w.options.Now()
	if _, err := w.options.Store.RecoverStaleImports(ctx, now, w.options.ImportReservationTimeout, w.options.ImportInFlight); err != nil {
		w.warn(ctx, "source_cleanup_recover_failed", err)
	}
	if _, err := w.options.Store.PruneTerminal(ctx, now.Add(-w.options.TerminalRetentionPeriod), w.options.TerminalPruneBatchSize); err != nil {
		w.warn(ctx, "source_cleanup_prune_failed", err)
	}
	if w.options.DatabaseOptimization && (w.lastOptimize.IsZero() || now.Sub(w.lastOptimize) >= w.options.DatabaseOptimizationInterval) {
		if err := w.options.Store.Optimize(ctx); err == nil {
			w.lastOptimize = now
		} else {
			w.warn(ctx, "source_cleanup_optimize_failed", err)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		jobs, err := w.options.Store.ClaimDue(ctx, now, w.options.ImportReservationTimeout, w.options.MaxConcurrentActions)
		if err != nil {
			w.warn(ctx, "source_cleanup_claim_failed", err)
			return
		}
		if len(jobs) == 0 {
			return
		}
		sem := make(chan struct{}, w.options.MaxConcurrentActions)
		var actions sync.WaitGroup
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			actions.Add(1)
			go func(job SourceCleanupJob) {
				defer actions.Done()
				defer func() { <-sem }()
				w.processJob(ctx, job)
			}(job)
		}
		actions.Wait()
	}
}

func (w *SourceCleanupWorker) processJob(ctx context.Context, job SourceCleanupJob) {
	if job.State == SourceCleanupStateMovePending {
		w.processMovePendingJob(ctx, job)
		return
	}
	err := w.options.Execute(ctx, job)
	if err == nil {
		if markErr := w.options.Store.MarkSucceeded(ctx, job); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	}
	if errors.Is(err, ErrSourceCleanupFingerprintChanged) {
		// The source path has been reused for a different revision. The old
		// job is obsolete and must not retry or quarantine the replacement.
		if markErr := w.options.Store.MarkSucceeded(ctx, job); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	}
	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	attempt := job.AttemptCount + 1
	if attempt >= maxAttempts {
		if w.options.Quarantine != nil && job.FailureDirectory != "" {
			if quarantineErr := w.options.Quarantine(ctx, job); quarantineErr != nil {
				err = fmt.Errorf("cleanup failed: %w; quarantine failed: %v", err, quarantineErr)
				if markErr := w.options.Store.MarkMovePending(ctx, job, w.options.Now().Add(w.retryDelay(job, attempt)), err); markErr != nil {
					w.warn(ctx, "source_cleanup_mark_failed", markErr)
				}
				return
			}
			if markErr := w.options.Store.MarkSucceeded(ctx, job); markErr != nil {
				w.warn(ctx, "source_cleanup_mark_failed", markErr)
			}
			return
		}
		if markErr := w.options.Store.MarkFailed(ctx, job, err); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	}
	delay := w.retryDelay(job, attempt)
	if markErr := w.options.Store.MarkRetry(ctx, job, w.options.Now().Add(delay), err); markErr != nil {
		w.warn(ctx, "source_cleanup_mark_failed", markErr)
	}
}

// processMovePendingJob retries a quarantine that failed after the retry budget
// was already exhausted.
//
// The move-pending state has its own attempt budget: without one, a persistently
// failing quarantine (a failure directory on a full or offline volume) retries
// forever, never becomes terminal, and permanently consumes the store's active
// job budget. An exhausted budget retires the job as Failed, which the prune
// cycle can then clean up.
func (w *SourceCleanupWorker) processMovePendingJob(ctx context.Context, job SourceCleanupJob) {
	if w.options.Quarantine == nil || job.FailureDirectory == "" {
		if markErr := w.options.Store.MarkFailed(ctx, job, errors.New("source cleanup failure directory is unavailable")); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	}
	if err := w.options.Quarantine(ctx, job); err == nil {
		if markErr := w.options.Store.MarkSucceeded(ctx, job); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	} else if errors.Is(err, ErrSourceCleanupFingerprintChanged) {
		if markErr := w.options.Store.MarkSucceeded(ctx, job); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
		return
	} else {
		maxAttempts := job.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 5
		}
		attempt := job.AttemptCount + 1
		if attempt >= maxAttempts {
			if markErr := w.options.Store.MarkFailed(ctx, job, fmt.Errorf("quarantine failed after retry budget: %w", err)); markErr != nil {
				w.warn(ctx, "source_cleanup_mark_failed", markErr)
			}
			return
		}
		if markErr := w.options.Store.MarkMovePending(ctx, job, w.options.Now().Add(w.retryDelay(job, attempt)), err); markErr != nil {
			w.warn(ctx, "source_cleanup_mark_failed", markErr)
		}
	}
}

// warn emits one degraded-pipeline event through the injected logging runtime.
// The worker has no other error surface: every store operation is best-effort,
// so without this the death of the cleanup pipeline would be silent.
func (w *SourceCleanupWorker) warn(ctx context.Context, event string, err error) {
	if w.options.Logging == nil || err == nil {
		return
	}
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	if !w.options.Logging.Enabled(ctx, slog.LevelWarn) {
		return
	}
	w.options.Logging.Emit(ctx, logging.Record{
		Level:     slog.LevelWarn,
		Component: "watcher.source_cleanup",
		Event:     event,
		Message:   "source cleanup pipeline degraded",
		Attrs: []slog.Attr{
			slog.String("error_type", fmt.Sprintf("%T", err)),
		},
	})
}

func (w *SourceCleanupWorker) retryDelay(job SourceCleanupJob, attempt int) time.Duration {
	initial, maximum := job.RetryInitialDelay, job.RetryMaxDelay
	if initial <= 0 {
		initial = w.options.RetryInitialDelay
	}
	if maximum <= 0 {
		maximum = w.options.RetryMaxDelay
	}
	return retryDelay(initial, maximum, attempt)
}

func retryDelay(initial, maximum time.Duration, attempt int) time.Duration {
	if initial <= 0 {
		return 0
	}
	if maximum <= 0 {
		maximum = initial
	}
	delay := initial
	for i := 1; i < attempt; i++ {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
