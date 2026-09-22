package scheduler

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

// FileSchedulerOptions configures the file scheduler.
type FileSchedulerOptions struct {
	// RetryPolicy defines the retry behavior for failed files.
	RetryPolicy *core.FileRetryPolicy

	// ProcessingTimeout is the duration after which a Processing file is considered timed out.
	// Default: 30 minutes
	ProcessingTimeout time.Duration

	// RecoverTimedOutOnEmptyQueue reclaims timed-out Processing files when a claim
	// finds no available candidate, so a crashed worker's file is not stuck until
	// the next cleanup cycle.
	RecoverTimedOutOnEmptyQueue bool

	// TimedOutReclaimCooldown is the minimum interval between immediate reclaim
	// attempts for one tenant. Default: 30 seconds; a negative value disables the
	// cooldown (every empty result may reclaim). The cooldown is shared by the
	// synchronous empty-queue reclaim and the opportunistic background pass, so the
	// two cannot both run for one tenant inside one cooldown window.
	TimedOutReclaimCooldown time.Duration

	// EmptyQueueReclaimBatchSize bounds the synchronous reclaim performed when a
	// claim finds an empty queue. Zero selects DefaultEmptyQueueReclaimBatchSize;
	// a negative value disables the synchronous pass.
	EmptyQueueReclaimBatchSize int

	// BackgroundTimedOutReclaimEnabled turns on the opportunistic background
	// pass that runs after a successful claim.
	BackgroundTimedOutReclaimEnabled bool

	// BackgroundTimedOutReclaimBatchSize bounds one background pass. A
	// non-positive value disables the background pass.
	BackgroundTimedOutReclaimBatchSize int

	// Logging is the instance-scoped logging runtime that receives the contained
	// warning when a background pass fails. Nil disables logging.
	Logging *logging.Runtime
}

// DefaultEmptyQueueReclaimBatchSize is the default number of timed-out files one
// immediate reclaim attempt resets when a claim finds no available work.
const DefaultEmptyQueueReclaimBatchSize = 32

// DefaultBackgroundTimedOutReclaimBatchSize is the default number of timed-out
// files one opportunistic background pass resets after a successful claim.
const DefaultBackgroundTimedOutReclaimBatchSize = 8

// DefaultFileSchedulerOptions returns default scheduler options.
func DefaultFileSchedulerOptions() *FileSchedulerOptions {
	return &FileSchedulerOptions{
		RetryPolicy:                        core.DefaultFileRetryPolicy(),
		ProcessingTimeout:                  30 * time.Minute,
		RecoverTimedOutOnEmptyQueue:        true,
		TimedOutReclaimCooldown:            30 * time.Second,
		EmptyQueueReclaimBatchSize:         DefaultEmptyQueueReclaimBatchSize,
		BackgroundTimedOutReclaimEnabled:   true,
		BackgroundTimedOutReclaimBatchSize: DefaultBackgroundTimedOutReclaimBatchSize,
	}
}

// claimCandidateLimit bounds how many pending candidates one single-file claim
// inspects before giving up, so a contended queue cannot turn one call into a
// full scan.
const claimCandidateLimit = 10

// batchCandidateLimit caps how many pending candidates one batch claim inspects.
const batchCandidateLimit = 100

// defaultTimedOutReclaimCooldown is applied when
// FileSchedulerOptions.TimedOutReclaimCooldown is left at its zero value, which
// means "unset"; a negative value explicitly disables the cooldown.
const defaultTimedOutReclaimCooldown = 30 * time.Second

// claimRetryable reports whether err describes a lost claim rather than a real
// infrastructure failure. Callers use it to decide between trying the next
// candidate and returning err.
//
// core.ErrFileNotClaimable classifies contention: pkg/metadata wraps it when the
// candidate is no longer Pending or is not yet available, which happens when a
// competing worker won the race. core.ErrFileNotFound covers a candidate that
// disappeared before it could be claimed. Any other failure, such as
// core.ErrDatabaseError, describes the repository itself and must not be
// swallowed as an empty queue.
func claimRetryable(err error) bool {
	return errors.Is(err, core.ErrFileNotClaimable) || errors.Is(err, core.ErrFileNotFound)
}

// fileScheduler implements the FileScheduler interface.
//
// The scheduler works exclusively through the metadata repository, so it does not
// retain the volume map that NewFileScheduler validates: volume lookup and
// physical I/O belong to the storage pool.
type fileScheduler struct {
	metadataRepo      core.MetadataRepository
	retryPolicy       *core.FileRetryPolicy
	processingTimeout time.Duration

	recoverTimedOutOnEmptyQueue bool
	timedOutReclaimCooldown     time.Duration
	emptyQueueReclaimBatchSize  int

	backgroundTimedOutReclaimEnabled   bool
	backgroundTimedOutReclaimBatchSize int

	// logger receives the contained warning of a failed background pass. It is
	// copied from the options and may be nil, which logging.Runtime treats as
	// silent.
	logger *logging.Runtime

	// reclaimMu guards reclaimDeadlines so the cooldown check and the reservation
	// that follows it are one atomic operation.
	reclaimMu sync.Mutex
	// reclaimDeadlines records, per tenant, the earliest instant at which another
	// immediate reclaim may run. It holds one entry per tenant that has hit an
	// empty queue, so memory stays proportional to the tenant count.
	//
	// The background pass keeps its own deadlines (backgroundReclaimDeadlines)
	// on purpose: an emergency reclaim must never be suppressed by a recent
	// opportunistic pass, because the caller blocked on an empty queue is the one
	// that needs the timed-out records back now. Both maps use the same
	// TimedOutReclaimCooldown, so neither path can run more often than configured.
	reclaimDeadlines map[string]time.Time
	// backgroundReclaimDeadlines is the cooldown state of the opportunistic
	// background pass, keyed by tenant.
	backgroundReclaimDeadlines map[string]time.Time

	// backgroundReclaims holds one marker per tenant with an in-flight background
	// pass, so a burst of successful claims starts one goroutine, not one per
	// claim. Entries are removed by the pass itself.
	backgroundReclaims sync.Map

	// backgroundCtx is cancelled by Close so an in-flight pass stops before the
	// caller closes the metadata repository. The scheduler owns the context and
	// its cancel function; callers never see either.
	backgroundCtx    context.Context
	cancelBackground context.CancelFunc

	// lifecycleMu orders "start a pass" against Close: a pass may only register
	// while closed is false, and Close sets closed before it waits.
	lifecycleMu sync.Mutex
	// closed reports whether Close has started. Once set, no new background pass
	// is scheduled, so Close's wait covers every registered pass.
	closed bool
	// backgroundWG tracks in-flight background passes; Close waits on it before
	// returning so no pass can write to a repository the caller closes next.
	backgroundWG sync.WaitGroup
}

// reclaimReservation records what a successful cooldown reservation replaced so
// a caller that never started its reclaim can restore the previous deadline.
type reclaimReservation struct {
	tenantID    string
	previous    time.Time
	hadPrevious bool
}

// NewFileScheduler creates a new file scheduler.
//
// volumes provides the mounted volumes the scheduler serves. At least one volume
// is required; the map itself is not retained because queue transitions are
// driven by the metadata repository alone.
//
// The returned scheduler owns an internal lifecycle context and one background
// goroutine per tenant with an in-flight opportunistic reclaim pass. Close stops
// scheduling those passes and waits for the in-flight ones, so a caller that
// closes the metadata repository next never races a pass; see CloseFileScheduler
// for a scheduler held through the core.FileScheduler interface.
func NewFileScheduler(
	metadataRepo core.MetadataRepository,
	volumes map[string]core.StorageVolume,
	opts *FileSchedulerOptions,
) (core.FileScheduler, error) {
	if metadataRepo == nil {
		return nil, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	if len(volumes) == 0 {
		return nil, fmt.Errorf("at least one storage volume is required: %w", core.ErrInvalidArgument)
	}

	if opts == nil {
		opts = DefaultFileSchedulerOptions()
	}

	if opts.RetryPolicy == nil {
		opts.RetryPolicy = core.DefaultFileRetryPolicy()
	}

	if opts.ProcessingTimeout == 0 {
		opts.ProcessingTimeout = 30 * time.Minute
	}

	// A zero cooldown means the option was left unset; only a negative value
	// disables the cooldown gate.
	if opts.TimedOutReclaimCooldown == 0 {
		opts.TimedOutReclaimCooldown = defaultTimedOutReclaimCooldown
	}

	// A zero empty-queue batch size means the option was left unset, matching the
	// documented configuration semantics; only a negative value disables the
	// synchronous pass. The background batch size deliberately has no such
	// substitution: any non-positive value disables the background pass.
	if opts.EmptyQueueReclaimBatchSize == 0 {
		opts.EmptyQueueReclaimBatchSize = DefaultEmptyQueueReclaimBatchSize
	}

	backgroundCtx, cancelBackground := context.WithCancel(context.Background())

	return &fileScheduler{
		metadataRepo:                       metadataRepo,
		retryPolicy:                        opts.RetryPolicy,
		processingTimeout:                  opts.ProcessingTimeout,
		recoverTimedOutOnEmptyQueue:        opts.RecoverTimedOutOnEmptyQueue,
		timedOutReclaimCooldown:            opts.TimedOutReclaimCooldown,
		emptyQueueReclaimBatchSize:         opts.EmptyQueueReclaimBatchSize,
		backgroundTimedOutReclaimEnabled:   opts.BackgroundTimedOutReclaimEnabled,
		backgroundTimedOutReclaimBatchSize: opts.BackgroundTimedOutReclaimBatchSize,
		logger:                             opts.Logging,
		reclaimDeadlines:                   make(map[string]time.Time),
		backgroundReclaimDeadlines:         make(map[string]time.Time),
		backgroundCtx:                      backgroundCtx,
		cancelBackground:                   cancelBackground,
	}, nil
}

// GetNextFileForProcessing retrieves the next pending file atomically.
// Status is transitioned from Pending to Processing within a transaction.
//
// It returns (nil, nil) when no file is currently claimable. A claim that was
// lost to a competing worker is treated as "not claimable"; any other failure is
// returned to the caller instead of being masked as an empty queue.
//
// At most claimCandidateLimit candidates are inspected per call. Under heavy
// contention a single call can therefore return (nil, nil) while claimable files
// still exist; a caller that polls again makes progress on the next attempt.
//
// When nothing is claimable and FileSchedulerOptions.RecoverTimedOutOnEmptyQueue
// is enabled, one immediate reclaim of timed-out Processing files runs (subject
// to the per-tenant cooldown) and the pending scan is retried once. A failure of
// the reclaim or of the retry scan reaches the caller and is never reported as
// an empty queue.
//
// A successful claim also schedules at most one opportunistic background reclaim
// for the tenant when FileSchedulerOptions.BackgroundTimedOutReclaimEnabled is
// set, the background batch size is positive, and ProcessingTimeout is positive.
// The background pass keeps its own per-tenant cooldown window, so an emergency
// reclaim is never suppressed by a recent pass; it never delays or fails this
// call and is bounded by Close.
func (s *fileScheduler) GetNextFileForProcessing(ctx context.Context, tenant core.TenantContext) (*core.FileLocation, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	location, claimed, err := s.claimNextPendingFile(ctx, tenant.ID)
	if err != nil {
		return nil, err
	}
	if claimed {
		s.scheduleBackgroundTimedOutReclaim(tenant.ID)
		return location, nil
	}

	reclaimed, err := s.reclaimTimedOutOnEmptyQueue(ctx, tenant.ID)
	if err != nil {
		return nil, err
	}
	if !reclaimed {
		return nil, nil
	}

	// The reclaim made timed-out files claimable again; retry the pending scan
	// exactly once so the caller sees the recovered work in this call.
	location, _, err = s.claimNextPendingFile(ctx, tenant.ID)
	if err != nil {
		return nil, err
	}
	if location != nil {
		s.scheduleBackgroundTimedOutReclaim(tenant.ID)
	}
	return location, nil
}

// GetNextBatchForProcessing retrieves multiple files atomically.
//
// It returns an empty slice when no file is currently claimable. A claim that was
// lost to a competing worker is treated as "not claimable"; any other failure is
// returned to the caller instead of being masked as an empty queue.
//
// Like GetNextFileForProcessing, an empty result triggers one immediate reclaim
// of timed-out Processing files when immediate reclaim is enabled, followed by
// exactly one retry of the pending scan. Reclaim and retry failures reach the
// caller.
//
// A batch that returned files schedules the opportunistic background reclaim on
// the same terms as GetNextFileForProcessing: one pass per tenant and cooldown
// window, bounded by the background batch size, and stopped by Close.
func (s *fileScheduler) GetNextBatchForProcessing(ctx context.Context, tenant core.TenantContext, batchSize int) ([]*core.FileLocation, error) {
	// Validate tenant is enabled
	if !tenant.IsEnabled() {
		return nil, core.ErrTenantDisabled
	}

	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive: %w", core.ErrInvalidArgument)
	}

	locations, err := s.claimPendingBatch(ctx, tenant.ID, batchSize)
	if err != nil {
		return nil, err
	}
	if len(locations) > 0 {
		s.scheduleBackgroundTimedOutReclaim(tenant.ID)
		return locations, nil
	}

	reclaimed, err := s.reclaimTimedOutOnEmptyQueue(ctx, tenant.ID)
	if err != nil {
		return nil, err
	}
	if !reclaimed {
		return locations, nil
	}

	locations, err = s.claimPendingBatch(ctx, tenant.ID, batchSize)
	if err != nil {
		return nil, err
	}
	if len(locations) > 0 {
		s.scheduleBackgroundTimedOutReclaim(tenant.ID)
	}
	return locations, nil
}

// claimNextPendingFile inspects one bounded window of pending candidates and
// tries to claim the first one that is still claimable.
//
// claimed reports whether a file was transitioned to Processing. A false result
// with a nil error means nothing in the window was claimable, either because the
// window was empty or because every candidate was lost to a competing worker.
func (s *fileScheduler) claimNextPendingFile(ctx context.Context, tenantID string) (*core.FileLocation, bool, error) {
	// A bounded candidate window keeps one call from scanning the whole queue.
	pendingFiles, err := s.metadataRepo.GetPendingFiles(ctx, tenantID, claimCandidateLimit)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get pending files: %w", err)
	}

	// Try to claim the first available file. Use optimistic locking: try to
	// transition the status, and if it fails because another worker won the race,
	// move on to the next candidate.
	for _, file := range pendingFiles {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		err := s.transitionToProcessing(ctx, file)
		if err == nil {
			// Successfully claimed the file.
			return file.ToFileLocation(), true, nil
		}
		if !claimRetryable(err) {
			return nil, false, fmt.Errorf("failed to claim file %q: %w", file.FileKey, err)
		}
	}

	return nil, false, nil
}

// claimPendingBatch claims up to batchSize pending files from one bounded
// candidate window. It returns the claimed files, which may be empty when
// nothing was claimable, or nil when the pending scan failed.
func (s *fileScheduler) claimPendingBatch(ctx context.Context, tenantID string, batchSize int) ([]*core.FileLocation, error) {
	// Get pending files (fetch more than needed to account for race conditions)
	fetchSize := batchSize * 2
	if fetchSize > batchCandidateLimit {
		fetchSize = batchCandidateLimit
	}

	pendingFiles, err := s.metadataRepo.GetPendingFiles(ctx, tenantID, fetchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	if len(pendingFiles) == 0 {
		return []*core.FileLocation{}, nil
	}

	// Try to claim files up to batchSize
	var claimedFiles []*core.FileLocation
	for _, file := range pendingFiles {
		if len(claimedFiles) >= batchSize {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		err := s.transitionToProcessing(ctx, file)
		if err == nil {
			// Successfully claimed the file.
			claimedFiles = append(claimedFiles, file.ToFileLocation())
			continue
		}
		if !claimRetryable(err) {
			return nil, fmt.Errorf("failed to claim file %q: %w", file.FileKey, err)
		}
	}

	return claimedFiles, nil
}

// reclaimTimedOutOnEmptyQueue resets timed-out Processing files after a claim
// found no candidate.
//
// It reports whether a reclaim ran. A false result with a nil error means
// immediate reclaim is disabled or the per-tenant cooldown suppressed this
// attempt, and the caller must report an empty queue. Any error means the
// reclaim could not run or failed and must reach the caller.
func (s *fileScheduler) reclaimTimedOutOnEmptyQueue(ctx context.Context, tenantID string) (bool, error) {
	if !s.recoverTimedOutOnEmptyQueue || s.emptyQueueReclaimBatchSize <= 0 {
		return false, nil
	}

	// The reservation happens before the work so concurrent workers cannot both
	// run a full reclaim for one tenant.
	reservation, reserved := s.reserveTimedOutReclaim(s.reclaimDeadlines, tenantID, time.Now())
	if !reserved {
		return false, nil
	}

	// A call cancelled before the reclaim started must not consume the tenant's
	// cooldown slot.
	if err := ctx.Err(); err != nil {
		s.rollbackTimedOutReclaim(s.reclaimDeadlines, reservation)
		return false, err
	}

	if _, err := s.resetTimedOutProcessingFiles(ctx, tenantID, s.processingTimeout, s.emptyQueueReclaimBatchSize); err != nil {
		s.rollbackTimedOutReclaim(s.reclaimDeadlines, reservation)
		return false, err
	}

	return true, nil
}

// reserveTimedOutReclaim atomically reserves the reclaim slot of one cooldown
// map for one tenant and reports whether the caller may run the reclaim.
//
// deadlines is the cooldown state of the calling path: the immediate empty-queue
// reclaim and the opportunistic background pass each own a map, so neither path
// can suppress the other. The returned reservation token lets a caller that never
// started its reclaim restore the previous deadline through rollbackTimedOutReclaim.
func (s *fileScheduler) reserveTimedOutReclaim(deadlines map[string]time.Time, tenantID string, now time.Time) (reclaimReservation, bool) {
	s.reclaimMu.Lock()
	defer s.reclaimMu.Unlock()

	reservation := reclaimReservation{tenantID: tenantID}
	if previous, ok := deadlines[tenantID]; ok {
		reservation.previous = previous
		reservation.hadPrevious = true
		// A non-positive cooldown disables the gate: every empty result may
		// reclaim.
		if s.timedOutReclaimCooldown > 0 && now.Before(previous) {
			return reclaimReservation{}, false
		}
	}

	deadlines[tenantID] = now.Add(s.timedOutReclaimCooldown)
	return reservation, true
}

// rollbackTimedOutReclaim releases a reservation whose reclaim never started, so
// a cancelled or failed call does not hold the tenant's cooldown slot.
func (s *fileScheduler) rollbackTimedOutReclaim(deadlines map[string]time.Time, reservation reclaimReservation) {
	s.reclaimMu.Lock()
	defer s.reclaimMu.Unlock()

	if reservation.hadPrevious {
		deadlines[reservation.tenantID] = reservation.previous
		return
	}
	delete(deadlines, reservation.tenantID)
}

// scheduleBackgroundTimedOutReclaim starts at most one opportunistic background
// reclaim pass for tenantID after a successful claim.
//
// The bookkeeping is synchronous and the work is not: the in-flight marker, the
// shared per-tenant cooldown reservation, and the lifecycle registration all
// happen before the pass goroutine starts, so concurrent successful claims cannot
// start more than one pass per tenant and cooldown window. Any failure to register
// leaves the claim result untouched, and a scheduler that is closing simply does
// not schedule.
func (s *fileScheduler) scheduleBackgroundTimedOutReclaim(tenantID string) {
	if !s.backgroundTimedOutReclaimEnabled ||
		s.backgroundTimedOutReclaimBatchSize <= 0 ||
		s.processingTimeout <= 0 {
		return
	}

	// The marker is taken before the cooldown so a claim that arrives while a pass
	// is already running neither starts a second goroutine nor consumes a cooldown
	// window of its own.
	if _, inFlight := s.backgroundReclaims.LoadOrStore(tenantID, struct{}{}); inFlight {
		return
	}

	// The reservation is atomic and belongs to the background path alone, so a
	// recent emergency reclaim cannot keep timed-out records locked away while a
	// long cleanup interval runs.
	reservation, reserved := s.reserveTimedOutReclaim(s.backgroundReclaimDeadlines, tenantID, time.Now())
	if !reserved {
		s.backgroundReclaims.Delete(tenantID)
		return
	}

	if !s.beginBackgroundReclaim() {
		// Close already started: start no pass that could outlive the scheduler,
		// and hand the cooldown slot back exactly as it was found.
		s.backgroundReclaims.Delete(tenantID)
		s.rollbackTimedOutReclaim(s.backgroundReclaimDeadlines, reservation)
		return
	}

	go s.runBackgroundTimedOutReclaim(tenantID)
}

// beginBackgroundReclaim registers one in-flight background pass and reports
// whether it may start.
//
// It is the only place that adds to backgroundWG, and it adds only while Close has
// not started, so Close's wait always covers every pass that was allowed to run.
func (s *fileScheduler) beginBackgroundReclaim() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	if s.closed {
		return false
	}
	s.backgroundWG.Add(1)
	return true
}

// runBackgroundTimedOutReclaim performs one bounded background reclaim pass for
// one tenant.
//
// The pass resets at most backgroundTimedOutReclaimBatchSize timed-out records
// through resetTimedOutProcessingFiles, the same lease-checked transition the
// empty-queue reclaim uses, so a timed-out snapshot whose lease was already
// replaced can never overwrite a newer claim. The pass observes the scheduler's
// lifecycle context, so Close makes it stop instead of writing to a repository the
// caller closes next.
//
// Every failure is contained: it is logged through the injected runtime and never
// reaches a claim caller or panics the process. The in-flight marker and the
// lifecycle registration are released in defer on every path.
func (s *fileScheduler) runBackgroundTimedOutReclaim(tenantID string) {
	defer s.backgroundWG.Done()
	defer s.backgroundReclaims.Delete(tenantID)
	defer func() {
		if recovered := recover(); recovered != nil {
			// Only the panic type is logged: a panic value may carry a physical
			// path or other tenant-sensitive data.
			s.warnBackgroundReclaim(tenantID, "background_timed_out_reclaim_panic",
				"Background timed-out Processing reclaim panicked",
				slog.String("panic_type", fmt.Sprintf("%T", recovered)))
		}
	}()

	if s.backgroundCtx.Err() != nil {
		// The scheduler is closing: stop before touching the repository, and do not
		// report a shutdown cancellation as a reclaim failure.
		return
	}

	if _, err := s.resetTimedOutProcessingFiles(
		s.backgroundCtx,
		tenantID,
		s.processingTimeout,
		s.backgroundTimedOutReclaimBatchSize,
	); err != nil {
		if s.backgroundCtx.Err() != nil {
			// Close raced the pass; the reclaim is best-effort and its cancellation
			// is not an infrastructure failure.
			return
		}
		s.warnBackgroundReclaim(tenantID, "background_timed_out_reclaim_failed",
			"Background timed-out Processing reclaim failed",
			slog.String("error_type", fmt.Sprintf("%T", err)))
	}
}

// warnBackgroundReclaim emits the one contained warning of a failed background
// pass. Only the tenant ID and the failure type are logged: a raw error may carry
// a physical path or other tenant-sensitive data.
func (s *fileScheduler) warnBackgroundReclaim(tenantID string, event string, message string, attrs ...slog.Attr) {
	if s.logger == nil {
		return
	}

	s.logger.Emit(s.backgroundCtx, logging.Record{
		Level:     slog.LevelWarn,
		Component: "scheduler.files",
		Event:     event,
		Message:   message,
		Attrs:     append([]slog.Attr{slog.String("tenant_id", tenantID)}, attrs...),
	})
}

// Close stops the scheduler's opportunistic background reclaim work and waits for
// the in-flight passes to finish.
//
// Close is idempotent and safe in a deferred shutdown path. After it returns, no
// background pass is running and none can start, so the caller may close the
// metadata repository without racing a pass; explicit operations that callers
// already invoked are unaffected. Close does not close the metadata repository,
// which the caller owns.
func (s *fileScheduler) Close() error {
	s.lifecycleMu.Lock()
	if !s.closed {
		s.closed = true
		s.cancelBackground()
	}
	s.lifecycleMu.Unlock()

	s.backgroundWG.Wait()
	return nil
}

// CloseFileScheduler closes a scheduler created by NewFileScheduler when only the
// core.FileScheduler interface is available, and waits for its in-flight
// background reclaim passes.
//
// A nil scheduler, or one that does not expose the optional Close capability, is a
// no-op, mirroring how the public entry point treats other optional closers.
func CloseFileScheduler(scheduler core.FileScheduler) error {
	closer, ok := scheduler.(interface{ Close() error })
	if !ok {
		return nil
	}
	return closer.Close()
}

// transitionToProcessing atomically transitions a file from Pending to Processing.
// Returns error if the file is no longer in Pending status (already claimed).
func (s *fileScheduler) transitionToProcessing(ctx context.Context, file *core.FileMetadata) error {
	// Use atomic compare-and-swap operation
	updated, err := s.metadataRepo.CompareAndTransitionToProcessing(ctx, file.TenantID, file.FileKey)
	if err != nil {
		return err
	}

	// Update the input file metadata with the updated values
	*file = *updated

	return nil
}

// MarkAsCompleted marks a file as completed and schedules it for deletion.
func (s *fileScheduler) MarkAsCompleted(ctx context.Context, lease core.FileProcessingLease) error {
	if lease.TenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if lease.FileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	completedAt := time.Now()
	_, err := s.metadataRepo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
		current.Status = core.FileStatusCompleted
		current.ProcessingStartTime = nil
		current.CompletedAt = &completedAt
		current.AvailableForProcessingAt = nil
		current.LastError = ""
		current.UpdatedAt = completedAt
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to complete processing lease: %w", err)
	}

	return nil
}

// MarkAsFailed marks a file as failed and schedules retry or permanent failure.
func (s *fileScheduler) MarkAsFailed(ctx context.Context, lease core.FileProcessingLease, errorMessage string) error {
	if lease.TenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if lease.FileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	now := time.Now()
	_, err := s.metadataRepo.CompareAndUpdateProcessing(ctx, lease, func(metadata *core.FileMetadata) error {
		metadata.RetryCount++
		metadata.LastFailedAt = &now
		metadata.LastError = errorMessage
		metadata.UpdatedAt = now
		metadata.ProcessingStartTime = nil

		if metadata.RetryCount >= s.retryPolicy.MaxRetryCount {
			metadata.Status = core.FileStatusPermanentlyFailed
			metadata.AvailableForProcessingAt = nil
			return nil
		}

		metadata.Status = core.FileStatusPending
		retryDelay := s.retryPolicy.CalculateRetryDelay(metadata.RetryCount)
		availableAt := now.Add(retryDelay)
		metadata.AvailableForProcessingAt = &availableAt
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to fail processing lease: %w", err)
	}

	return nil
}

// GetFileStatus returns the current status of a file.
//
// On failure it returns the zero FileProcessingStatus and the repository error,
// which preserves core.ErrFileNotFound; the returned status is only meaningful
// when err is nil.
func (s *fileScheduler) GetFileStatus(ctx context.Context, tenant core.TenantContext, fileKey string) (core.FileProcessingStatus, error) {
	if tenant.ID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return 0, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	metadata, err := s.metadataRepo.Get(ctx, tenant.ID, fileKey)
	if err != nil {
		// Return the repository error unchanged so errors.Is(err, core.ErrFileNotFound)
		// keeps working for callers, and avoid pairing it with a default status.
		return 0, err
	}

	return metadata.Status, nil
}

// ResetTimedOutFiles finds files in Processing status that exceed timeout
// and resets them to Pending status for retry.
//
// A recovered file becomes available immediately, mirroring Locus
// (ProcessingTimeoutRecoveryService.cs:86-91 sets AvailableForProcessingAt to
// the recovery instant). Availability is recorded as exactly the recovery
// instant, which is already claimable: pkg/metadata compares inclusively
// (available_for_processing_at <= now), so a stored availability that equals the
// current instant is claimable and coarse platform clock granularity cannot leave
// the recovered file invisible. Files whose processing lease was replaced
// concurrently are skipped, and a request that is canceled mid-loop returns
// ctx.Err() with the count reached so far.
func (s *fileScheduler) ResetTimedOutFiles(ctx context.Context, tenant core.TenantContext, timeout time.Duration) (int, error) {
	if tenant.ID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	// Use configured timeout if not specified
	if timeout == 0 {
		timeout = s.processingTimeout
	}

	// Unbounded scan: the cleanup cycle is expected to drain the whole set.
	return s.resetTimedOutProcessingFiles(ctx, tenant.ID, timeout, 0)
}

// resetTimedOutProcessingFiles resets timed-out Processing files to Pending
// through the lease-checked CompareAndUpdateProcessing path.
//
// limit > 0 bounds how many timed-out candidates are examined, which the
// empty-queue reclaim and the opportunistic background pass use to cap one
// attempt; limit <= 0 examines them all. The reset is intentionally best-effort
// per file so one lease mismatch does not abort the rest of the scan.
func (s *fileScheduler) resetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration, limit int) (int, error) {
	// Get timed out files
	timedOutFiles, err := s.metadataRepo.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
	if err != nil {
		return 0, fmt.Errorf("failed to get timed out files: %w", err)
	}

	resetCount := 0
	for index, file := range timedOutFiles {
		if limit > 0 && index >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return resetCount, err
		}
		if file.ProcessingStartTime == nil {
			continue
		}
		lease := core.FileProcessingLease{
			TenantID:               file.TenantID,
			FileKey:                file.FileKey,
			ProcessingStartTimeUTC: *file.ProcessingStartTime,
		}
		now := time.Now()
		// A recovered file is claimable immediately. The metadata readiness
		// check is inclusive, so the recovery instant itself is enough; the
		// timestamp is stored explicitly so the invariant does not depend on
		// whatever availability value the file carried before the timeout.
		//
		// The lease match makes the reset safe: a stale worker can never unseat a
		// newer claim, because the update is rejected unless the stored
		// ProcessingStartTime still identifies this exact processing attempt.
		availableAt := now
		_, err := s.metadataRepo.CompareAndUpdateProcessing(ctx, lease, func(current *core.FileMetadata) error {
			current.Status = core.FileStatusPending
			current.ProcessingStartTime = nil
			current.AvailableForProcessingAt = &availableAt
			current.UpdatedAt = now
			return nil
		})
		if err != nil {
			continue
		}

		resetCount++
	}

	return resetCount, nil
}
