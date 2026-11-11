package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// claimingFakeRepository serves claims through the optional batch capability and
// panics on the legacy paths, so a test proves the scheduler takes the new path.
type claimingFakeRepository struct {
	core.MetadataRepository

	claimCalls []int
	pending    []*core.FileMetadata
	scanCalls  int
}

func (r *claimingFakeRepository) ClaimPendingFiles(_ context.Context, _ string, limit int) ([]*core.FileMetadata, error) {
	r.claimCalls = append(r.claimCalls, limit)
	if len(r.pending) == 0 {
		return nil, nil
	}
	count := limit
	if count > len(r.pending) {
		count = len(r.pending)
	}
	claimed := make([]*core.FileMetadata, 0, count)
	for _, record := range r.pending[:count] {
		now := time.Now().UTC()
		transitioned := *record
		transitioned.Status = core.FileStatusProcessing
		transitioned.ProcessingStartTime = &now
		claimed = append(claimed, &transitioned)
	}
	r.pending = r.pending[count:]
	return claimed, nil
}

func (r *claimingFakeRepository) GetPendingFiles(_ context.Context, _ string, _ int) ([]*core.FileMetadata, error) {
	r.scanCalls++
	return nil, nil
}

// TestGetNextFileForProcessingUsesBatchClaimCapability proves a repository that
// claims atomically is used instead of the candidate-window scan.
func TestGetNextFileForProcessingUsesBatchClaimCapability(t *testing.T) {
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	repo := &claimingFakeRepository{pending: []*core.FileMetadata{createTestFileMetadata("file-1", core.FileStatusPending)}}
	sched, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	location, err := sched.GetNextFileForProcessing(context.Background(), createTestTenant())
	if err != nil {
		t.Fatalf("GetNextFileForProcessing() error = %v", err)
	}
	if location == nil || location.FileKey != "file-1" {
		t.Fatalf("GetNextFileForProcessing() = %v, want file-1", location)
	}
	if len(repo.claimCalls) != 1 || repo.claimCalls[0] != 1 {
		t.Fatalf("claim limits = %v, want one single-file claim", repo.claimCalls)
	}
	if repo.scanCalls != 0 {
		t.Fatalf("candidate scans = %d, want 0 when the repository claims atomically", repo.scanCalls)
	}
}

// TestGetNextBatchForProcessingUsesBatchClaimCapability proves one atomic claim
// serves the whole batch, including a batch larger than the candidate window.
func TestGetNextBatchForProcessingUsesBatchClaimCapability(t *testing.T) {
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	repo := &claimingFakeRepository{pending: []*core.FileMetadata{
		createTestFileMetadata("file-1", core.FileStatusPending),
		createTestFileMetadata("file-2", core.FileStatusPending),
	}}
	sched, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	locations, err := sched.GetNextBatchForProcessing(context.Background(), createTestTenant(), 150)
	if err != nil {
		t.Fatalf("GetNextBatchForProcessing() error = %v", err)
	}
	if len(locations) != 2 {
		t.Fatalf("claimed = %d files, want 2", len(locations))
	}
	if len(repo.claimCalls) != 1 || repo.claimCalls[0] != 150 {
		t.Fatalf("claim limits = %v, want one claim of the full batch size", repo.claimCalls)
	}
	if repo.scanCalls != 0 {
		t.Fatalf("candidate scans = %d, want 0 when the repository claims atomically", repo.scanCalls)
	}
}

// TestGetNextFileForProcessingSkipsRetryWhenReclaimResetNothing pins the reclaim
// return value: an empty queue whose reclaim found no timed-out file does not pay
// a second pending scan.
func TestGetNextFileForProcessingSkipsRetryWhenReclaimResetNothing(t *testing.T) {
	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	pendingScans := 0
	repo := &stubMetadataRepository{
		pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
			pendingScans++
			return nil, nil
		},
		timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
			return nil, nil
		},
	}
	sched, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	if _, err := sched.GetNextFileForProcessing(context.Background(), createTestTenant()); err != nil {
		t.Fatalf("GetNextFileForProcessing() error = %v", err)
	}
	if pendingScans != 1 {
		t.Fatalf("pending scans = %d, want 1: a reclaim that reset nothing must not trigger a retry scan", pendingScans)
	}
}
