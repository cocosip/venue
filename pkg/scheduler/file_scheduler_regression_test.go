package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestResetTimedOutFilesRejectsNegativeTimeout is the regression test for a
// negative timeout inverting the cutoff: now-(-timeout) is in the future, so
// every actively processing file would be treated as timed out and flipped back
// to Pending while a live worker still holds it. The lease check cannot help,
// because the lease of a live claim matches.
func TestResetTimedOutFilesRejectsNegativeTimeout(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	activeStart := time.Now().UTC().Add(-time.Minute)
	file := createTestFileMetadata("active-worker", core.FileStatusProcessing)
	file.ProcessingStartTime = &activeStart
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), -time.Hour)
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument for a negative timeout", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 for a rejected timeout", count)
	}

	current, err := repo.Get(ctx, "test-tenant", file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing {
		t.Errorf("status = %v, want Processing: a live claim was reset", current.Status)
	}
	if current.ProcessingStartTime == nil || !current.ProcessingStartTime.Equal(activeStart) {
		t.Errorf("ProcessingStartTime = %v, want the live lease %v", current.ProcessingStartTime, activeStart)
	}
}

// TestResetTimedOutProcessingFilesRejectsNegativeTimeout pins the same rule on
// the internal reclaim entry point, so neither the synchronous empty-queue pass
// nor the background pass can reach the repository with an inverted cutoff.
func TestResetTimedOutProcessingFilesRejectsNegativeTimeout(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	concrete, ok := scheduler.(*fileScheduler)
	if !ok {
		t.Fatalf("scheduler type = %T, want *fileScheduler", scheduler)
	}

	count, err := concrete.resetTimedOutProcessingFiles(ctx, "test-tenant", -time.Second, 0)
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument for a negative timeout", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 for a rejected timeout", count)
	}
}

// TestResetTimedOutFilesSurfacesRepositoryFailure is the regression test for a
// swallowed infrastructure error: only a lease mismatch is an expected
// per-file outcome, and anything else must reach the caller instead of being
// reported as a successful partial reset.
func TestResetTimedOutFilesSurfacesRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	longAgo := time.Now().UTC().Add(-2 * time.Hour)

	newRepo := func(updateErr error) *stubMetadataRepository {
		return &stubMetadataRepository{
			timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
				file := createTestFileMetadata("timed-out", core.FileStatusProcessing)
				file.ProcessingStartTime = &longAgo
				return []*core.FileMetadata{file}, nil
			},
			processingUpdate: func(
				_ context.Context,
				_ core.FileProcessingLease,
				_ func(*core.FileMetadata) error,
			) (*core.FileMetadata, error) {
				return nil, updateErr
			},
		}
	}

	t.Run("database failure is returned", func(t *testing.T) {
		scheduler, err := NewFileScheduler(
			newRepo(fmt.Errorf("metadata repository is closed: %w", core.ErrDatabaseError)),
			volumes,
			nil,
		)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), time.Hour)
		if !errors.Is(err, core.ErrDatabaseError) {
			t.Fatalf("error = %v, want ErrDatabaseError to reach the caller", err)
		}
		if count != 0 {
			t.Errorf("count = %d, want 0 for a failed reset", count)
		}
	})

	t.Run("lease mismatch is still skipped", func(t *testing.T) {
		scheduler, err := NewFileScheduler(
			newRepo(fmt.Errorf("stale lease: %w", core.ErrProcessingLeaseMismatch)),
			volumes,
			nil,
		)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		count, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), time.Hour)
		if err != nil {
			t.Fatalf("error = %v, want nil for an expected lease mismatch", err)
		}
		if count != 0 {
			t.Errorf("count = %d, want 0 for a skipped file", count)
		}
	})
}

// TestNewFileSchedulerClampsInvalidRetryPolicyDelays is the regression test for
// a hand-built retry policy: a zero MaxRetryDelay turns exponential backoff into
// a tight loop that never waits, and a negative InitialRetryDelay yields an
// availableAt in the past.
func TestNewFileSchedulerClampsInvalidRetryPolicyDelays(t *testing.T) {
	defaults := core.DefaultFileRetryPolicy()

	testCases := []struct {
		name           string
		policy         *core.FileRetryPolicy
		wantInitial    time.Duration
		wantMax        time.Duration
		wantMaxRetries int
	}{
		{
			name: "zero delays select the defaults",
			policy: &core.FileRetryPolicy{
				MaxRetryCount:         3,
				UseExponentialBackoff: true,
			},
			wantInitial:    defaults.InitialRetryDelay,
			wantMax:        defaults.MaxRetryDelay,
			wantMaxRetries: 3,
		},
		{
			name: "negative delays select the defaults",
			policy: &core.FileRetryPolicy{
				MaxRetryCount:         3,
				InitialRetryDelay:     -time.Second,
				UseExponentialBackoff: true,
				MaxRetryDelay:         -time.Hour,
			},
			wantInitial:    defaults.InitialRetryDelay,
			wantMax:        defaults.MaxRetryDelay,
			wantMaxRetries: 3,
		},
		{
			name: "explicit delays are preserved",
			policy: &core.FileRetryPolicy{
				MaxRetryCount:         7,
				InitialRetryDelay:     time.Second,
				UseExponentialBackoff: true,
				MaxRetryDelay:         time.Minute,
			},
			wantInitial:    time.Second,
			wantMax:        time.Minute,
			wantMaxRetries: 7,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volumes := createTestVolumes(t)
			defer cleanupVolumes(volumes)
			repo, _ := createTestRepository(t)

			created, err := NewFileScheduler(repo, volumes, &FileSchedulerOptions{RetryPolicy: tc.policy})
			if err != nil {
				t.Fatalf("new scheduler: %v", err)
			}
			scheduler, ok := created.(*fileScheduler)
			if !ok {
				t.Fatalf("scheduler type = %T, want *fileScheduler", created)
			}

			if got := scheduler.retryPolicy.InitialRetryDelay; got != tc.wantInitial {
				t.Errorf("InitialRetryDelay = %v, want %v", got, tc.wantInitial)
			}
			if got := scheduler.retryPolicy.MaxRetryDelay; got != tc.wantMax {
				t.Errorf("MaxRetryDelay = %v, want %v", got, tc.wantMax)
			}
			if got := scheduler.retryPolicy.MaxRetryCount; got != tc.wantMaxRetries {
				t.Errorf("MaxRetryCount = %d, want %d", got, tc.wantMaxRetries)
			}

			// The clamped policy must produce a real delay for every retry.
			for retry := 1; retry <= tc.wantMaxRetries; retry++ {
				if delay := scheduler.retryPolicy.CalculateRetryDelay(retry); delay <= 0 {
					t.Errorf("CalculateRetryDelay(%d) = %v, want a positive delay", retry, delay)
				}
			}
		})
	}
}

// TestMarkAsFailedMaxRetryCountCountsRetries pins the documented semantics of
// core.FileRetryPolicy.MaxRetryCount: it is the number of retries, so a file
// with MaxRetryCount=3 is retried three times and becomes PermanentlyFailed on
// the fourth failure.
func TestMarkAsFailedMaxRetryCountCountsRetries(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	const maxRetries = 3

	scheduler, err := NewFileScheduler(repo, volumes, &FileSchedulerOptions{
		RetryPolicy: &core.FileRetryPolicy{
			MaxRetryCount:         maxRetries,
			InitialRetryDelay:     time.Millisecond,
			UseExponentialBackoff: true,
			MaxRetryDelay:         time.Millisecond,
		},
		ProcessingTimeout: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	leaseStart := time.Now().UTC()
	file := createTestFileMetadata("retry-semantics", core.FileStatusProcessing)
	file.ProcessingStartTime = &leaseStart
	if err := repo.AddOrUpdate(ctx, file); err != nil {
		t.Fatalf("add metadata: %v", err)
	}

	lease := core.FileProcessingLease{
		TenantID:               "test-tenant",
		FileKey:                file.FileKey,
		ProcessingStartTimeUTC: leaseStart,
	}

	for retry := 1; retry <= maxRetries; retry++ {
		if err := scheduler.MarkAsFailed(ctx, lease, "transient failure"); err != nil {
			t.Fatalf("retry %d: mark as failed: %v", retry, err)
		}

		current, err := repo.Get(ctx, "test-tenant", file.FileKey)
		if err != nil {
			t.Fatalf("retry %d: get metadata: %v", retry, err)
		}
		if current.RetryCount != retry {
			t.Fatalf("retry %d: RetryCount = %d, want %d", retry, current.RetryCount, retry)
		}
		if current.Status != core.FileStatusPending {
			t.Fatalf("retry %d: status = %v, want Pending (MaxRetryCount retries must be used)", retry, current.Status)
		}
		if current.AvailableForProcessingAt == nil {
			t.Fatalf("retry %d: AvailableForProcessingAt = nil, want the backoff instant", retry)
		}

		// Re-claim for the next attempt, as a worker would after waiting out the
		// backoff. The wait is shortened by making the record available now
		// instead of sleeping through the real delay.
		availableNow := time.Now().Add(-time.Second)
		current.AvailableForProcessingAt = &availableNow
		if err := repo.AddOrUpdate(ctx, current); err != nil {
			t.Fatalf("retry %d: make available: %v", retry, err)
		}
		claimed, err := repo.CompareAndTransitionToProcessing(ctx, "test-tenant", file.FileKey)
		if err != nil {
			t.Fatalf("retry %d: reclaim: %v", retry, err)
		}
		if claimed.ProcessingStartTime == nil {
			t.Fatalf("retry %d: reclaim has no processing start", retry)
		}
		lease.ProcessingStartTimeUTC = *claimed.ProcessingStartTime
	}

	// One failure beyond the configured retries is permanent.
	if err := scheduler.MarkAsFailed(ctx, lease, "final failure"); err != nil {
		t.Fatalf("final failure: %v", err)
	}

	current, err := repo.Get(ctx, "test-tenant", file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("status = %v, want PermanentlyFailed after %d retries", current.Status, maxRetries)
	}
	if current.RetryCount != maxRetries+1 {
		t.Errorf("RetryCount = %d, want %d", current.RetryCount, maxRetries+1)
	}
	if current.AvailableForProcessingAt != nil {
		t.Errorf("AvailableForProcessingAt = %v, want nil for a permanently failed file", current.AvailableForProcessingAt)
	}
}

// TestMarkAsCompletedRejectsReleasedLeaseNoOp pins the completion contract: a
// repository that answers a stale completion with the stored, non-completed
// record and a nil error must not be reported as a successful completion, or a
// caller would count work that did not happen.
func TestMarkAsCompletedRejectsReleasedLeaseNoOp(t *testing.T) {
	ctx := context.Background()
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	repo := &stubMetadataRepository{
		processingUpdate: func(
			_ context.Context,
			_ core.FileProcessingLease,
			_ func(*core.FileMetadata) error,
		) (*core.FileMetadata, error) {
			// The released-lease no-op: the repository returns the stored record
			// without applying the update.
			return createTestFileMetadata("released", core.FileStatusPending), nil
		},
	}

	scheduler, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	metadata, err := scheduler.MarkAsCompleted(ctx, core.FileProcessingLease{
		TenantID:               "test-tenant",
		FileKey:                "released",
		ProcessingStartTimeUTC: time.Now().UTC(),
	})
	if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
		t.Fatalf("error = %v, want ErrProcessingLeaseMismatch for a no-op completion", err)
	}
	if metadata != nil {
		t.Errorf("metadata = %v, want nil alongside the error", metadata)
	}
}

// TestMarkAsFailedRejectsReleasedLeaseNoOp is the regression test for a failure
// report that the released-lease no-op answered with success: the worker whose
// lease the timeout path already reclaimed was told its failure was recorded,
// while RetryCount never moved and the file could be reclaimed forever instead of
// reaching PermanentlyFailed.
func TestMarkAsFailedRejectsReleasedLeaseNoOp(t *testing.T) {
	ctx := context.Background()
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	newScheduler := func(t *testing.T, repo core.MetadataRepository) core.FileScheduler {
		t.Helper()
		scheduler, err := NewFileScheduler(repo, volumes, &FileSchedulerOptions{
			RetryPolicy: &core.FileRetryPolicy{
				MaxRetryCount:         3,
				InitialRetryDelay:     time.Millisecond,
				UseExponentialBackoff: true,
				MaxRetryDelay:         time.Millisecond,
			},
			ProcessingTimeout: 30 * time.Minute,
		})
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}
		return scheduler
	}

	t.Run("reclaimed lease is reported", func(t *testing.T) {
		repo, _ := createTestRepository(t)
		scheduler := newScheduler(t, repo)

		leaseStart := time.Now().UTC().Add(-time.Hour)
		file := createTestFileMetadata("reclaimed", core.FileStatusProcessing)
		file.ProcessingStartTime = &leaseStart
		file.RetryCount = 1
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata: %v", err)
		}

		// The timeout path reclaims the lease before the worker reports.
		reset, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), 30*time.Minute)
		if err != nil {
			t.Fatalf("reset timed out files: %v", err)
		}
		if reset != 1 {
			t.Fatalf("reset count = %d, want 1", reset)
		}

		err = scheduler.MarkAsFailed(ctx, core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                file.FileKey,
			ProcessingStartTimeUTC: leaseStart,
		}, "worker died")
		if !errors.Is(err, core.ErrProcessingLeaseMismatch) {
			t.Fatalf("MarkAsFailed() error = %v, want ErrProcessingLeaseMismatch for a released lease", err)
		}

		current, err := repo.Get(ctx, "test-tenant", file.FileKey)
		if err != nil {
			t.Fatalf("get metadata: %v", err)
		}
		if current.RetryCount != 1 {
			t.Errorf("RetryCount = %d, want the untouched 1 from the released lease", current.RetryCount)
		}
		if current.LastError != "" {
			t.Errorf("LastError = %q, want no recorded failure", current.LastError)
		}
	})

	t.Run("live lease records the retry", func(t *testing.T) {
		repo, _ := createTestRepository(t)
		scheduler := newScheduler(t, repo)

		leaseStart := time.Now().UTC()
		file := createTestFileMetadata("live", core.FileStatusProcessing)
		file.ProcessingStartTime = &leaseStart
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata: %v", err)
		}

		lease := core.FileProcessingLease{
			TenantID:               "test-tenant",
			FileKey:                file.FileKey,
			ProcessingStartTimeUTC: leaseStart,
		}
		if err := scheduler.MarkAsFailed(ctx, lease, "transient failure"); err != nil {
			t.Fatalf("MarkAsFailed() error = %v, want the retry to be recorded", err)
		}

		current, err := repo.Get(ctx, "test-tenant", file.FileKey)
		if err != nil {
			t.Fatalf("get metadata: %v", err)
		}
		if current.RetryCount != 1 {
			t.Errorf("RetryCount = %d, want 1", current.RetryCount)
		}
		if current.Status != core.FileStatusPending {
			t.Errorf("status = %v, want Pending", current.Status)
		}
		if current.LastError != "transient failure" {
			t.Errorf("LastError = %q, want the reported failure", current.LastError)
		}
	})
}

// TestNewFileSchedulerRejectsNegativeProcessingTimeout is the regression test for
// a negative timeout that passed construction and then inverted the cutoff at the
// point of use: with the default empty-queue reclaim enabled, every empty claim
// returned an error while the background pass was silently disabled.
func TestNewFileSchedulerRejectsNegativeProcessingTimeout(t *testing.T) {
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	if _, err := NewFileScheduler(repo, volumes, &FileSchedulerOptions{ProcessingTimeout: -time.Minute}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("NewFileScheduler(ProcessingTimeout: -1m) error = %v, want ErrInvalidArgument", err)
	}

	// The zero value keeps selecting the default rather than failing.
	scheduler, err := NewFileScheduler(repo, volumes, &FileSchedulerOptions{ProcessingTimeout: 0})
	if err != nil {
		t.Fatalf("NewFileScheduler(ProcessingTimeout: 0) error = %v", err)
	}
	defer func() { _ = CloseFileScheduler(scheduler) }()

	concrete, ok := scheduler.(*fileScheduler)
	if !ok {
		t.Fatalf("scheduler type = %T, want *fileScheduler", scheduler)
	}
	if concrete.processingTimeout != 30*time.Minute {
		t.Errorf("processingTimeout = %v, want the 30m default", concrete.processingTimeout)
	}
}

// timedOutScanRecorder records the limit of every timed-out scan and delegates to
// the wrapped repository, which is what makes a paged drain observable.
type timedOutScanRecorder struct {
	core.MetadataRepository

	mu     sync.Mutex
	limits []int
}

func (r *timedOutScanRecorder) GetTimedOutProcessingFiles(
	ctx context.Context,
	tenantID string,
	timeout time.Duration,
	limit int,
) ([]*core.FileMetadata, error) {
	r.mu.Lock()
	r.limits = append(r.limits, limit)
	r.mu.Unlock()

	return r.MetadataRepository.GetTimedOutProcessingFiles(ctx, tenantID, timeout, limit)
}

func (r *timedOutScanRecorder) recordedLimits() []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]int(nil), r.limits...)
}

// TestResetTimedOutFilesDrainsInBoundedPages is the regression test for the
// unbounded scan: the maintenance drain passed limit 0, so a large timed-out set
// was materialized in one query. It now walks the set in bounded pages.
func TestResetTimedOutFilesDrainsInBoundedPages(t *testing.T) {
	ctx := context.Background()
	repo, _ := createTestRepository(t)
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	recorder := &timedOutScanRecorder{MetadataRepository: repo}
	scheduler, err := NewFileScheduler(recorder, volumes, &FileSchedulerOptions{ProcessingTimeout: 30 * time.Minute})
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	defer func() { _ = CloseFileScheduler(scheduler) }()

	start := time.Now().UTC().Add(-time.Hour)
	const totalFiles = resetTimedOutFilesPageSize + 40
	for index := 0; index < totalFiles; index++ {
		file := createTestFileMetadata(fmt.Sprintf("timed-out-%04d", index), core.FileStatusProcessing)
		file.ProcessingStartTime = &start
		if err := repo.AddOrUpdate(ctx, file); err != nil {
			t.Fatalf("add metadata %d: %v", index, err)
		}
	}

	reset, err := scheduler.ResetTimedOutFiles(ctx, createTestTenant(), 30*time.Minute)
	if err != nil {
		t.Fatalf("ResetTimedOutFiles() error = %v", err)
	}
	if reset != totalFiles {
		t.Errorf("reset count = %d, want every one of the %d timed-out files", reset, totalFiles)
	}

	limits := recorder.recordedLimits()
	if len(limits) < 2 {
		t.Fatalf("timed-out scans = %d, want the set drained in bounded pages", len(limits))
	}
	for _, limit := range limits {
		if limit <= 0 || limit > resetTimedOutFilesPageSize {
			t.Errorf("timed-out scan limit = %d, want a positive limit of at most %d", limit, resetTimedOutFilesPageSize)
		}
	}
}
