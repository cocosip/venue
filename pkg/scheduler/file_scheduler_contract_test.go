package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// stubMetadataRepository overrides the few MetadataRepository operations the
// scheduler contract tests need. Every other method panics through the embedded
// nil interface, so an unexpected call fails loudly instead of silently passing.
type stubMetadataRepository struct {
	core.MetadataRepository
	pendingFiles     func(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error)
	timedOutFiles    func(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error)
	transitionResult func(ctx context.Context, tenantID string, fileKey string) (*core.FileMetadata, error)
}

func (s *stubMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	if s.pendingFiles == nil {
		return nil, nil
	}
	return s.pendingFiles(ctx, tenantID, limit)
}

func (s *stubMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error) {
	if s.timedOutFiles == nil {
		return nil, nil
	}
	return s.timedOutFiles(ctx, tenantID, timeout)
}

func (s *stubMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, tenantID string, fileKey string) (*core.FileMetadata, error) {
	if s.transitionResult == nil {
		return nil, errors.New("stubMetadataRepository: unexpected CompareAndTransitionToProcessing call")
	}
	return s.transitionResult(ctx, tenantID, fileKey)
}

// TestGetNextFileForProcessingEmptyQueueReturnsNil pins the Locus v2.0.0 contract:
// an exhausted queue is a normal result, not an error. Locus
// FileScheduler.GetNextFileForProcessingAsync returns null
// (src/Locus.Storage/FileScheduler.cs:183-191) and never throws
// NoFilesAvailableException.
func TestGetNextFileForProcessingEmptyQueueReturnsNil(t *testing.T) {
	ctx := context.Background()

	t.Run("repository has no pending files", func(t *testing.T) {
		repo := &stubMetadataRepository{}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("empty queue returned error %v, want nil", err)
		}
		if location != nil {
			t.Fatalf("empty queue returned location %v, want nil", location)
		}
	})

	t.Run("all pending candidates were claimed concurrently", func(t *testing.T) {
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{
					createTestFileMetadata("a", core.FileStatusPending),
					createTestFileMetadata("b", core.FileStatusPending),
				}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, core.ErrFileNotFound
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("lost claim race returned error %v, want nil", err)
		}
		if location != nil {
			t.Fatalf("lost claim race returned location %v, want nil", location)
		}
	})

	t.Run("real repository empty queue", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()
		defer func() { _ = repo.Close() }()

		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("empty queue returned error %v, want nil", err)
		}
		if location != nil {
			t.Fatalf("empty queue returned location %v, want nil", location)
		}
	})
}

// TestGetNextBatchForProcessingEmptyQueueReturnsEmptySlice pins the batch variant:
// an exhausted queue returns an empty slice and no error.
func TestGetNextBatchForProcessingEmptyQueueReturnsEmptySlice(t *testing.T) {
	ctx := context.Background()

	t.Run("repository has no pending files", func(t *testing.T) {
		repo := &stubMetadataRepository{}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		locations, err := scheduler.GetNextBatchForProcessing(ctx, createTestTenant(), 10)
		if err != nil {
			t.Fatalf("empty queue returned error %v, want nil", err)
		}
		if len(locations) != 0 {
			t.Fatalf("empty queue returned %d locations, want 0", len(locations))
		}
	})

	t.Run("all pending candidates were claimed concurrently", func(t *testing.T) {
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{
					createTestFileMetadata("a", core.FileStatusPending),
					createTestFileMetadata("b", core.FileStatusPending),
					createTestFileMetadata("c", core.FileStatusPending),
				}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, core.ErrFileNotFound
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		locations, err := scheduler.GetNextBatchForProcessing(ctx, createTestTenant(), 10)
		if err != nil {
			t.Fatalf("lost claim races returned error %v, want nil", err)
		}
		if len(locations) != 0 {
			t.Fatalf("lost claim races returned %d locations, want 0", len(locations))
		}
	})
}

// TestGetNextFileForProcessingPropagatesClaimFailures pins defect 2.15: only
// contention ("this candidate is already gone") may be skipped. Any other
// failure must reach the caller instead of being masked as an empty queue.
func TestGetNextFileForProcessingPropagatesClaimFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("enumeration failure is returned", func(t *testing.T) {
		enumerationErr := errors.New("badger: value log truncate required")
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return nil, enumerationErr
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if !errors.Is(err, enumerationErr) {
			t.Fatalf("error = %v, want wrapped %v", err, enumerationErr)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})

	t.Run("infrastructure claim failure is returned", func(t *testing.T) {
		claimErr := errors.New("repository is closed")
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{createTestFileMetadata("a", core.FileStatusPending)}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, claimErr
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if !errors.Is(err, claimErr) {
			t.Fatalf("error = %v, want wrapped %v", err, claimErr)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})

	t.Run("claim failure falls through to the next candidate", func(t *testing.T) {
		first := createTestFileMetadata("a", core.FileStatusPending)
		second := createTestFileMetadata("b", core.FileStatusPending)
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{first, second}, nil
			},
			transitionResult: func(_ context.Context, _ string, fileKey string) (*core.FileMetadata, error) {
				if fileKey == first.FileKey {
					return nil, core.ErrFileNotFound
				}
				claimed := *second
				now := time.Now().UTC()
				claimed.Status = core.FileStatusProcessing
				claimed.ProcessingStartTime = &now
				return &claimed, nil
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("claim next candidate: %v", err)
		}
		if location == nil || location.FileKey != second.FileKey {
			t.Fatalf("location = %v, want file %q", location, second.FileKey)
		}
	})

	t.Run("contention sentinel is skipped", func(t *testing.T) {
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{createTestFileMetadata("a", core.FileStatusPending)}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, fmt.Errorf("file is not in pending status: %w", core.ErrFileNotClaimable)
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("contention sentinel returned error %v, want nil", err)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})

	t.Run("database failure is not masked as contention", func(t *testing.T) {
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{createTestFileMetadata("a", core.FileStatusPending)}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, fmt.Errorf("metadata repository is closed: %w", core.ErrDatabaseError)
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if !errors.Is(err, core.ErrDatabaseError) {
			t.Fatalf("error = %v, want ErrDatabaseError", err)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})

	t.Run("wrapped contention is still skipped", func(t *testing.T) {
		contentionErr := fmt.Errorf("candidate is gone: %w", core.ErrFileNotFound)
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return []*core.FileMetadata{createTestFileMetadata("a", core.FileStatusPending)}, nil
			},
			transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
				return nil, contentionErr
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		location, err := scheduler.GetNextFileForProcessing(ctx, createTestTenant())
		if err != nil {
			t.Fatalf("wrapped contention returned error %v, want nil", err)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})
}

// TestGetNextBatchForProcessingPropagatesClaimFailures is the batch counterpart
// of TestGetNextFileForProcessingPropagatesClaimFailures.
func TestGetNextBatchForProcessingPropagatesClaimFailures(t *testing.T) {
	ctx := context.Background()

	claimErr := errors.New("repository is closed")
	repo := &stubMetadataRepository{
		pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
			return []*core.FileMetadata{createTestFileMetadata("a", core.FileStatusPending)}, nil
		},
		transitionResult: func(context.Context, string, string) (*core.FileMetadata, error) {
			return nil, claimErr
		},
	}
	scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	locations, err := scheduler.GetNextBatchForProcessing(ctx, createTestTenant(), 5)
	if !errors.Is(err, claimErr) {
		t.Fatalf("error = %v, want wrapped %v", err, claimErr)
	}
	if len(locations) != 0 {
		t.Fatalf("locations = %v, want empty on failure", locations)
	}
}

// TestClaimLoopsHonorContextCancellation pins the AGENTS.md rule
// "Check context.Context cancellation in loops".
func TestClaimLoopsHonorContextCancellation(t *testing.T) {
	tenant := createTestTenant()

	newSchedulerWithCandidates := func(t *testing.T, files int) core.FileScheduler {
		t.Helper()
		candidates := make([]*core.FileMetadata, 0, files)
		for i := 0; i < files; i++ {
			candidates = append(candidates, createTestFileMetadata(string(rune('a'+i)), core.FileStatusPending))
		}
		transitions := 0
		repo := &stubMetadataRepository{
			pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
				return candidates, nil
			},
			transitionResult: func(_ context.Context, _ string, fileKey string) (*core.FileMetadata, error) {
				transitions++
				if transitions > 1 {
					return nil, core.ErrFileNotFound
				}
				for _, candidate := range candidates {
					if candidate.FileKey == fileKey {
						claimed := *candidate
						now := time.Now().UTC()
						claimed.Status = core.FileStatusProcessing
						claimed.ProcessingStartTime = &now
						return &claimed, nil
					}
				}
				return nil, core.ErrFileNotFound
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}
		return scheduler
	}

	t.Run("single claim returns context error", func(t *testing.T) {
		scheduler := newSchedulerWithCandidates(t, 3)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if location != nil {
			t.Fatalf("location = %v, want nil", location)
		}
	})

	t.Run("batch claim returns context error", func(t *testing.T) {
		scheduler := newSchedulerWithCandidates(t, 3)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 3)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if len(locations) != 0 {
			t.Fatalf("locations = %v, want empty", locations)
		}
	})

	t.Run("reset timed out files returns context error", func(t *testing.T) {
		longAgo := time.Now().Add(-2 * time.Hour)
		repo := &stubMetadataRepository{
			timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
				files := make([]*core.FileMetadata, 0, 3)
				for i := 0; i < 3; i++ {
					file := createTestFileMetadata(string(rune('a'+i)), core.FileStatusProcessing)
					start := longAgo
					file.ProcessingStartTime = &start
					files = append(files, file)
				}
				return files, nil
			},
		}
		scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
		if err != nil {
			t.Fatalf("new scheduler: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		count, err := scheduler.ResetTimedOutFiles(ctx, tenant, time.Hour)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if count != 0 {
			t.Fatalf("count = %d, want 0", count)
		}
	})
}

// TestResetTimedOutFilesRefreshesAvailability pins the Locus invariant
// (ProcessingTimeoutRecoveryService.cs:86-91 ->
// MetadataRepository.TryResetTimedOutFileAsync): a recovered file becomes
// available at the recovery instant, so a stale or future
// AvailableForProcessingAt can never keep the recovered file invisible. Before
// the fix the stale value survived the reset and the file stayed unclaimable.
func TestResetTimedOutFilesRefreshesAvailability(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	stalePast := time.Now().Add(-3 * time.Hour)
	future := time.Now().Add(2 * time.Hour)

	cases := []struct {
		name        string
		availableAt *time.Time
	}{
		{name: "stale availability in the past", availableAt: &stalePast},
		{name: "future availability", availableAt: &future},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo, tmpDir := createTestRepository(t)
			defer func() { _ = os.RemoveAll(tmpDir) }()

			longAgo := time.Now().Add(-2 * time.Hour)
			file := createTestFileMetadata("stale-availability", core.FileStatusProcessing)
			file.ProcessingStartTime = &longAgo
			file.AvailableForProcessingAt = tc.availableAt
			if err := repo.AddOrUpdate(ctx, file); err != nil {
				t.Fatalf("add metadata: %v", err)
			}

			scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
			if err != nil {
				t.Fatalf("new scheduler: %v", err)
			}

			before := time.Now()
			count, err := scheduler.ResetTimedOutFiles(ctx, tenant, time.Hour)
			if err != nil {
				t.Fatalf("reset timed out files: %v", err)
			}
			if count != 1 {
				t.Fatalf("reset count = %d, want 1", count)
			}

			current, err := repo.Get(ctx, tenant.ID, file.FileKey)
			if err != nil {
				t.Fatalf("get metadata: %v", err)
			}
			if current.Status != core.FileStatusPending {
				t.Fatalf("status = %v, want Pending", current.Status)
			}
			if current.AvailableForProcessingAt == nil {
				t.Fatal("AvailableForProcessingAt = nil, want the recovery instant")
			}
			if current.AvailableForProcessingAt.Before(before.Add(-time.Second)) {
				t.Errorf("AvailableForProcessingAt = %v, want the recovery instant", current.AvailableForProcessingAt)
			}
			// The recovery instant must not be in the future. Equality with
			// "now" is claimable: the metadata readiness check is inclusive
			// because coarse platform clock granularity can hand the claim the
			// same timestamp.
			if current.AvailableForProcessingAt.After(time.Now()) {
				t.Errorf("AvailableForProcessingAt = %v, want a value that is already claimable", current.AvailableForProcessingAt)
			}

			// The invariant must hold for the claim path too.
			location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("recovered file is not claimable: %v", err)
			}
			if location == nil || location.FileKey != file.FileKey {
				t.Fatalf("location = %v, want recovered file %q", location, file.FileKey)
			}
		})
	}
}

// TestGetFileStatusWrapsNotFound keeps errors.Is working while returning the
// zero status instead of a misleading default.
func TestGetFileStatusWrapsNotFound(t *testing.T) {
	ctx := context.Background()
	repo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	status, err := scheduler.GetFileStatus(ctx, createTestTenant(), "missing-file")
	if !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("error = %v, want ErrFileNotFound", err)
	}
	if status != 0 {
		t.Fatalf("status = %v, want the zero FileProcessingStatus on error", status)
	}
}
