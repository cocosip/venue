package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// pagingFakeRepository serves the pending queue in fixed windows and implements
// the core.PendingFilesPager capability, so a batch claim can walk windows.
type pagingFakeRepository struct {
	core.MetadataRepository

	windows   [][]*core.FileMetadata
	claimed   map[string]bool
	pagesRead int
}

func (r *pagingFakeRepository) windowAfter(after *core.FileMetadata) []*core.FileMetadata {
	if after == nil {
		r.pagesRead++
		if len(r.windows) == 0 {
			return nil
		}
		return r.windows[0]
	}
	for i, window := range r.windows {
		if len(window) > 0 && window[len(window)-1].FileKey == after.FileKey {
			r.pagesRead++
			if i+1 >= len(r.windows) {
				return nil
			}
			return r.windows[i+1]
		}
	}
	return nil
}

func (r *pagingFakeRepository) GetPendingFiles(_ context.Context, _ string, limit int) ([]*core.FileMetadata, error) {
	window := r.windowAfter(nil)
	if len(window) > limit {
		return window[:limit], nil
	}
	return window, nil
}

func (r *pagingFakeRepository) GetPendingFilesAfter(_ context.Context, _ string, after *core.FileMetadata, limit int) ([]*core.FileMetadata, error) {
	window := r.windowAfter(after)
	if len(window) > limit {
		return window[:limit], nil
	}
	return window, nil
}

func (r *pagingFakeRepository) CompareAndTransitionToProcessing(_ context.Context, _ string, fileKey string) (*core.FileMetadata, error) {
	for _, window := range r.windows {
		for _, candidate := range window {
			if candidate.FileKey == fileKey {
				if r.claimed[fileKey] {
					return nil, core.ErrFileNotClaimable
				}
				r.claimed[fileKey] = true
				claimed := *candidate
				now := time.Now().UTC()
				claimed.Status = core.FileStatusProcessing
				claimed.ProcessingStartTime = &now
				return &claimed, nil
			}
		}
	}
	return nil, core.ErrFileNotFound
}

// TestGetNextBatchForProcessingClaimsWithinWindows verifies that a batch larger
// than the bounded candidate window still claims up to batchSize files: the
// claim walks keyset-paged windows while it makes progress, instead of
// silently capping at the window size. It also pins the overflow guard: the
// candidate fetch stays bounded even for an absurd batchSize.
func TestGetNextBatchForProcessingClaimsWithinWindows(t *testing.T) {
	tenant := createTestTenant()
	const windows = 3
	const perWindow = batchCandidateLimit
	const batch = 150

	makeKey := func(window, index int) string {
		// Zero-padded and unique per (window, index): the fake dedups claims by
		// key, so colliding keys would undercount.
		return fmt.Sprintf("%04d%028d", window, index)
	}

	repo := &pagingFakeRepository{claimed: map[string]bool{}}
	for w := 0; w < windows; w++ {
		files := make([]*core.FileMetadata, 0, perWindow)
		for i := 0; i < perWindow; i++ {
			files = append(files, createTestFileMetadata(makeKey(w, i), core.FileStatusPending))
		}
		repo.windows = append(repo.windows, files)
	}

	volumes := createTestVolumes(t)
	sched, err := NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	locations, err := sched.GetNextBatchForProcessing(context.Background(), tenant, batch)
	if err != nil {
		t.Fatalf("GetNextBatchForProcessing() error = %v", err)
	}
	if len(locations) != batch {
		t.Fatalf("claimed = %d files, want %d: a batch above the candidate window must page on", len(locations), batch)
	}
	if repo.pagesRead < 2 {
		t.Fatalf("candidate windows read = %d, want at least 2", repo.pagesRead)
	}
	if got := batchFetchSize(batch); got != batchCandidateLimit {
		t.Fatalf("batchFetchSize(%d) = %d, want the bounded window cap", batch, got)
	}
	if got := batchFetchSize(int(^uint(0) >> 1)); got != batchCandidateLimit {
		t.Fatalf("batchFetchSize(max int) = %d, want the bounded window cap without overflow", got)
	}
}
