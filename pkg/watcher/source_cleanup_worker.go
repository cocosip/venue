package watcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
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
	Now            func() time.Time
}

// SourceCleanupWorker drains durable source cleanup jobs until its context is
// cancelled. It does not own or close the store; the caller owns store lifetime.
type SourceCleanupWorker struct {
	options      SourceCleanupWorkerOptions
	startOnce    sync.Once
	stopOnce     sync.Once
	mu           sync.Mutex
	cancel       context.CancelFunc
	wg           sync.WaitGroup
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

// Start launches the worker loop once. Repeated calls are harmless.
func (w *SourceCleanupWorker) Start(parent context.Context) {
	if parent == nil {
		parent = context.Background()
	}
	w.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		w.mu.Lock()
		w.cancel = cancel
		w.mu.Unlock()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.run(ctx)
		}()
	})
}

// Stop cancels the worker and waits for all in-flight actions.
func (w *SourceCleanupWorker) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		cancel := w.cancel
		w.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})
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
	_, _ = w.options.Store.RecoverStaleImports(ctx, now, w.options.ImportReservationTimeout)
	_, _ = w.options.Store.PruneTerminal(ctx, now.Add(-w.options.TerminalRetentionPeriod), w.options.TerminalPruneBatchSize)
	if w.options.DatabaseOptimization && (w.lastOptimize.IsZero() || now.Sub(w.lastOptimize) >= w.options.DatabaseOptimizationInterval) {
		if err := w.options.Store.Optimize(ctx); err == nil {
			w.lastOptimize = now
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		jobs, err := w.options.Store.ClaimDue(ctx, now, w.options.ImportReservationTimeout, w.options.MaxConcurrentActions)
		if err != nil || len(jobs) == 0 {
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
		_ = w.options.Store.MarkSucceeded(ctx, job)
		return
	}
	if errors.Is(err, ErrSourceCleanupFingerprintChanged) {
		// The source path has been reused for a different revision. The old
		// job is obsolete and must not retry or quarantine the replacement.
		_ = w.options.Store.MarkSucceeded(ctx, job)
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
				_ = w.options.Store.MarkMovePending(ctx, job, w.options.Now().Add(w.retryDelay(job, attempt)), err)
				return
			}
			_ = w.options.Store.MarkSucceeded(ctx, job)
			return
		}
		_ = w.options.Store.MarkFailed(ctx, job, err)
		return
	}
	delay := w.retryDelay(job, attempt)
	_ = w.options.Store.MarkRetry(ctx, job, w.options.Now().Add(delay), err)
}

func (w *SourceCleanupWorker) processMovePendingJob(ctx context.Context, job SourceCleanupJob) {
	if w.options.Quarantine == nil || job.FailureDirectory == "" {
		_ = w.options.Store.MarkFailed(ctx, job, errors.New("source cleanup failure directory is unavailable"))
		return
	}
	if err := w.options.Quarantine(ctx, job); err == nil {
		_ = w.options.Store.MarkSucceeded(ctx, job)
		return
	} else if errors.Is(err, ErrSourceCleanupFingerprintChanged) {
		_ = w.options.Store.MarkSucceeded(ctx, job)
		return
	} else {
		attempt := job.AttemptCount + 1
		_ = w.options.Store.MarkMovePending(ctx, job, w.options.Now().Add(w.retryDelay(job, attempt)), err)
	}
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
