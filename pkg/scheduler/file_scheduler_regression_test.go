package scheduler

import (
	"context"
	"errors"
	"fmt"
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
