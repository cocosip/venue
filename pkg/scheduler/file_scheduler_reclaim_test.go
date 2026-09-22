package scheduler

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// reclaimCountingRepository counts timed-out scans so a test can pin how many
// immediate reclaim attempts actually reached the repository.
type reclaimCountingRepository struct {
	core.MetadataRepository
	timedOutCalls atomic.Int64
}

func (r *reclaimCountingRepository) GetTimedOutProcessingFiles(
	ctx context.Context,
	tenantID string,
	timeout time.Duration,
) ([]*core.FileMetadata, error) {
	r.timedOutCalls.Add(1)
	return r.MetadataRepository.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
}

// reclaimTestScheduler builds a scheduler over a real repository with immediate
// reclaim configured by mutate, which receives the defaults.
//
// The opportunistic background pass is switched off so these tests measure the
// synchronous empty-queue path in isolation; the background pass has its own
// suite in file_scheduler_background_reclaim_test.go.
func reclaimTestScheduler(
	t *testing.T,
	repo core.MetadataRepository,
	volumes map[string]core.StorageVolume,
	mutate func(*FileSchedulerOptions),
) core.FileScheduler {
	t.Helper()

	opts := DefaultFileSchedulerOptions()
	opts.ProcessingTimeout = time.Hour
	opts.BackgroundTimedOutReclaimEnabled = false
	if mutate != nil {
		mutate(opts)
	}

	scheduler, err := NewFileScheduler(repo, volumes, opts)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	return scheduler
}

// addTimedOutProcessingFile stores a Processing file whose lease started long
// before the scheduler's processing timeout, simulating a worker that died.
func addTimedOutProcessingFile(t *testing.T, repo core.MetadataRepository, fileKey string) *core.FileMetadata {
	t.Helper()

	longAgo := time.Now().Add(-2 * time.Hour)
	file := createTestFileMetadata(fileKey, core.FileStatusProcessing)
	file.ProcessingStartTime = &longAgo
	if err := repo.AddOrUpdate(context.Background(), file); err != nil {
		t.Fatalf("add metadata %q: %v", fileKey, err)
	}
	return file
}

// TestGetNextFileForProcessingRecoversTimedOutFileOnEmptyQueue pins Locus
// v2.0.0 behavior: a worker whose process died leaves its file in Processing,
// and another worker that finds an empty queue reclaims that timed-out file
// immediately instead of waiting for the periodic cleanup cycle.
func TestGetNextFileForProcessingRecoversTimedOutFileOnEmptyQueue(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, repo, "crashed-worker")

	scheduler := reclaimTestScheduler(t, repo, volumes, nil)

	location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("reclaim timed-out file: %v", err)
	}
	if location == nil {
		t.Fatal("empty queue returned no file, want the timed-out file recovered and claimed")
	}
	if location.FileKey != crashed.FileKey {
		t.Fatalf("location.FileKey = %q, want %q", location.FileKey, crashed.FileKey)
	}
	if location.Status != core.FileStatusProcessing {
		t.Fatalf("location.Status = %v, want Processing", location.Status)
	}
	if location.Lease == nil {
		t.Fatal("location.Lease = nil, want the fresh claim lease")
	}
	if !location.Lease.ProcessingStartTimeUTC.After(*crashed.ProcessingStartTime) {
		t.Fatalf("lease start = %v, want a fresh claim after the stale lease",
			location.Lease.ProcessingStartTimeUTC)
	}

	current, err := repo.Get(ctx, tenant.ID, crashed.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing {
		t.Fatalf("status = %v, want Processing", current.Status)
	}
	if current.ProcessingStartTime == nil ||
		!current.ProcessingStartTime.Equal(location.Lease.ProcessingStartTimeUTC) {
		t.Fatalf("ProcessingStartTime = %v, want the reclaim claim lease %v",
			current.ProcessingStartTime, location.Lease.ProcessingStartTimeUTC)
	}
	if current.AvailableForProcessingAt != nil && current.AvailableForProcessingAt.After(time.Now()) {
		t.Fatalf("AvailableForProcessingAt = %v, want the recovery instant",
			current.AvailableForProcessingAt)
	}

	// The recovered file is gone from the queue again: it is Processing under a
	// fresh lease that has not timed out yet.
	again, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if again != nil {
		t.Fatalf("second claim = %v, want nil while the fresh lease is active", again.FileKey)
	}
}

// TestGetNextFileForProcessingLeavesTimedOutFileWhenReclaimDisabled pins the off
// switch: with recovery disabled or with a negative reclaim batch size the empty
// queue stays empty and the crashed worker's lease is untouched.
func TestGetNextFileForProcessingLeavesTimedOutFileWhenReclaimDisabled(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	cases := []struct {
		name   string
		mutate func(*FileSchedulerOptions)
	}{
		{
			name:   "recover timed out on empty queue disabled",
			mutate: func(o *FileSchedulerOptions) { o.RecoverTimedOutOnEmptyQueue = false },
		},
		{
			name:   "empty queue reclaim batch size negative",
			mutate: func(o *FileSchedulerOptions) { o.EmptyQueueReclaimBatchSize = -1 },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo, tmpDir := createTestRepository(t)
			defer func() { _ = os.RemoveAll(tmpDir) }()

			volumes := createTestVolumes(t)
			defer cleanupVolumes(volumes)

			crashed := addTimedOutProcessingFile(t, repo, "crashed-worker")
			scheduler := reclaimTestScheduler(t, repo, volumes, tc.mutate)

			location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("disabled reclaim returned error %v, want nil", err)
			}
			if location != nil {
				t.Fatalf("disabled reclaim returned %v, want nil", location.FileKey)
			}

			current, err := repo.Get(ctx, tenant.ID, crashed.FileKey)
			if err != nil {
				t.Fatalf("get metadata: %v", err)
			}
			if current.Status != core.FileStatusProcessing {
				t.Fatalf("status = %v, want the file left in Processing", current.Status)
			}
			if current.ProcessingStartTime == nil ||
				!current.ProcessingStartTime.Equal(*crashed.ProcessingStartTime) {
				t.Fatalf("ProcessingStartTime = %v, want the original lease %v",
					current.ProcessingStartTime, crashed.ProcessingStartTime)
			}

			locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
			if err != nil {
				t.Fatalf("disabled batch reclaim returned error %v, want nil", err)
			}
			if len(locations) != 0 {
				t.Fatalf("disabled batch reclaim returned %d files, want 0", len(locations))
			}
		})
	}
}

// TestGetNextBatchForProcessingRecoversTimedOutFilesOnEmptyQueue pins the batch
// variant of the immediate reclaim.
func TestGetNextBatchForProcessingRecoversTimedOutFilesOnEmptyQueue(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	addTimedOutProcessingFile(t, repo, "crashed-a")
	addTimedOutProcessingFile(t, repo, "crashed-b")

	scheduler := reclaimTestScheduler(t, repo, volumes, nil)

	locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 10)
	if err != nil {
		t.Fatalf("batch reclaim: %v", err)
	}
	if len(locations) != 2 {
		t.Fatalf("batch reclaim returned %d files, want 2", len(locations))
	}

	for _, location := range locations {
		if location.Status != core.FileStatusProcessing {
			t.Errorf("%s status = %v, want Processing", location.FileKey, location.Status)
		}
		if location.Lease == nil {
			t.Fatalf("%s lease = nil, want the fresh claim lease", location.FileKey)
		}
		current, err := repo.Get(ctx, tenant.ID, location.FileKey)
		if err != nil {
			t.Fatalf("get metadata %s: %v", location.FileKey, err)
		}
		if current.ProcessingStartTime == nil ||
			!current.ProcessingStartTime.Equal(location.Lease.ProcessingStartTimeUTC) {
			t.Errorf("%s ProcessingStartTime = %v, want the reclaim claim lease %v",
				location.FileKey, current.ProcessingStartTime, location.Lease.ProcessingStartTimeUTC)
		}
		if current.AvailableForProcessingAt != nil && current.AvailableForProcessingAt.After(time.Now()) {
			t.Errorf("%s AvailableForProcessingAt = %v, want the recovery instant",
				location.FileKey, current.AvailableForProcessingAt)
		}
	}
}

// TestEmptyQueueReclaimHonoursCooldown pins the per-tenant cooldown: a second
// empty-queue call in quick succession must not run another reclaim, and a call
// after the cooldown elapses must reclaim again.
func TestEmptyQueueReclaimHonoursCooldown(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	t.Run("cooldown suppresses a second reclaim", func(t *testing.T) {
		baseRepo, tmpDir := createTestRepository(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		addTimedOutProcessingFile(t, baseRepo, "crashed-a")

		repo := &reclaimCountingRepository{MetadataRepository: baseRepo}
		scheduler := reclaimTestScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
			o.TimedOutReclaimCooldown = time.Hour
		})

		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("first reclaim: %v", err)
		}
		if location == nil || location.FileKey != "crashed-a" {
			t.Fatalf("first reclaim = %v, want crashed-a", location)
		}
		if got := repo.timedOutCalls.Load(); got != 1 {
			t.Fatalf("timed-out scans = %d, want 1", got)
		}

		second := addTimedOutProcessingFile(t, baseRepo, "crashed-b")
		location, err = scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("second reclaim: %v", err)
		}
		if location != nil {
			t.Fatalf("second call returned %v, want nil while the cooldown holds", location.FileKey)
		}
		if got := repo.timedOutCalls.Load(); got != 1 {
			t.Fatalf("timed-out scans = %d, want 1 while the cooldown holds", got)
		}

		current, err := baseRepo.Get(ctx, tenant.ID, second.FileKey)
		if err != nil {
			t.Fatalf("get metadata: %v", err)
		}
		if current.Status != core.FileStatusProcessing ||
			current.ProcessingStartTime == nil ||
			!current.ProcessingStartTime.Equal(*second.ProcessingStartTime) {
			t.Fatalf("cooldown did not leave the second file untouched: status %v start %v",
				current.Status, current.ProcessingStartTime)
		}
	})

	t.Run("reclaim resumes after the cooldown elapses", func(t *testing.T) {
		const cooldown = 50 * time.Millisecond

		baseRepo, tmpDir := createTestRepository(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		repo := &reclaimCountingRepository{MetadataRepository: baseRepo}
		scheduler := reclaimTestScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
			o.TimedOutReclaimCooldown = cooldown
		})

		// The first empty-queue call runs a reclaim that finds nothing, which
		// still consumes the cooldown slot.
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("first reclaim: %v", err)
		}
		if location != nil {
			t.Fatalf("first call returned %v, want nil", location.FileKey)
		}
		if got := repo.timedOutCalls.Load(); got != 1 {
			t.Fatalf("timed-out scans = %d, want 1", got)
		}

		// time.Sleep guarantees at least the requested duration, so this wait
		// always outlives the 50ms cooldown.
		time.Sleep(cooldown + 30*time.Millisecond)

		addTimedOutProcessingFile(t, baseRepo, "crashed-later")

		location, err = scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("reclaim after cooldown: %v", err)
		}
		if location == nil || location.FileKey != "crashed-later" {
			t.Fatalf("reclaim after cooldown = %v, want crashed-later", location)
		}
		if got := repo.timedOutCalls.Load(); got != 2 {
			t.Fatalf("timed-out scans = %d, want 2 after the cooldown elapsed", got)
		}
	})

	t.Run("non-positive cooldown disables the gate", func(t *testing.T) {
		baseRepo, tmpDir := createTestRepository(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		repo := &reclaimCountingRepository{MetadataRepository: baseRepo}
		scheduler := reclaimTestScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
			o.TimedOutReclaimCooldown = -1
		})

		for i := 0; i < 2; i++ {
			if _, err := scheduler.GetNextFileForProcessing(ctx, tenant); err != nil {
				t.Fatalf("empty call %d: %v", i, err)
			}
		}
		if got := repo.timedOutCalls.Load(); got != 2 {
			t.Fatalf("timed-out scans = %d, want 2 with the cooldown disabled", got)
		}
	})
}

// TestEmptyQueueReclaimBoundedByBatchSize pins that one immediate reclaim resets
// at most EmptyQueueReclaimBatchSize timed-out files.
func TestEmptyQueueReclaimBoundedByBatchSize(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	files := []*core.FileMetadata{
		addTimedOutProcessingFile(t, repo, "crashed-a"),
		addTimedOutProcessingFile(t, repo, "crashed-b"),
		addTimedOutProcessingFile(t, repo, "crashed-c"),
	}

	scheduler := reclaimTestScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.EmptyQueueReclaimBatchSize = 1
		o.TimedOutReclaimCooldown = -1
	})

	claimed := make(map[string]bool)
	for call := 0; call < len(files); call++ {
		location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("call %d: %v", call, err)
		}
		if location == nil {
			t.Fatalf("call %d returned nil, want one recovered file per reclaim", call)
		}
		if claimed[location.FileKey] {
			t.Fatalf("call %d returned %s twice", call, location.FileKey)
		}
		claimed[location.FileKey] = true
	}

	// Each call could only reach one timed-out file, so three calls are needed to
	// drain three files: a single reclaim never resets more than the batch size.
	if len(claimed) != len(files) {
		t.Fatalf("claimed %d files, want %d", len(claimed), len(files))
	}

	survivingStaleLeases := 0
	for _, file := range files {
		current, err := repo.Get(ctx, tenant.ID, file.FileKey)
		if err != nil {
			t.Fatalf("get metadata %s: %v", file.FileKey, err)
		}
		if current.ProcessingStartTime != nil && current.ProcessingStartTime.Equal(*file.ProcessingStartTime) {
			survivingStaleLeases++
		}
	}
	if survivingStaleLeases != 0 {
		t.Fatalf("%d files still carry their stale lease, want none after three reclaims",
			survivingStaleLeases)
	}

	final, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("final call: %v", err)
	}
	if final != nil {
		t.Fatalf("final call = %v, want nil", final.FileKey)
	}
}

// TestEmptyQueueReclaimIsExclusiveUnderConcurrency pins that the atomic
// reservation keeps concurrent workers from running a full reclaim for the same
// tenant at the same time, and that only one of them claims the recovered file.
func TestEmptyQueueReclaimIsExclusiveUnderConcurrency(t *testing.T) {
	const workers = 8

	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")

	repo := &reclaimCountingRepository{MetadataRepository: baseRepo}
	scheduler := reclaimTestScheduler(t, repo, volumes, nil)

	results := make([]*core.FileLocation, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] = scheduler.GetNextFileForProcessing(ctx, tenant)
		}(i)
	}
	close(start)
	wg.Wait()

	claimed := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Errorf("worker %d: unexpected error %v", i, errs[i])
			continue
		}
		if results[i] == nil {
			continue
		}
		claimed++
		if results[i].FileKey != crashed.FileKey {
			t.Errorf("worker %d claimed %q, want %q", i, results[i].FileKey, crashed.FileKey)
		}
	}
	if claimed != 1 {
		t.Fatalf("workers claimed %d files, want exactly 1", claimed)
	}
	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("immediate reclaim attempts = %d, want exactly 1", got)
	}

	current, err := baseRepo.Get(ctx, tenant.ID, crashed.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing || current.ProcessingStartTime == nil {
		t.Fatalf("status = %v lease = %v, want a live Processing claim", current.Status, current.ProcessingStartTime)
	}
	if !current.ProcessingStartTime.After(*crashed.ProcessingStartTime) {
		t.Fatalf("ProcessingStartTime = %v, want a fresh claim", current.ProcessingStartTime)
	}
}

// TestEmptyQueueReclaimPropagatesRepositoryFailure pins that a failed reclaim is
// returned as an error instead of being masked as an empty queue.
func TestEmptyQueueReclaimPropagatesRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	reclaimErr := errors.New("badger: value log truncate required")
	repo := &stubMetadataRepository{
		timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
			return nil, reclaimErr
		},
	}
	scheduler, err := NewFileScheduler(repo, createTestVolumes(t), nil)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if !errors.Is(err, reclaimErr) {
		t.Fatalf("error = %v, want wrapped %v", err, reclaimErr)
	}
	if location != nil {
		t.Fatalf("location = %v, want nil on reclaim failure", location)
	}

	locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
	if !errors.Is(err, reclaimErr) {
		t.Fatalf("batch error = %v, want wrapped %v", err, reclaimErr)
	}
	if len(locations) != 0 {
		t.Fatalf("batch locations = %v, want empty on reclaim failure", locations)
	}
}

// TestEmptyQueueReclaimPreservesReplacementLease pins the lease invariant on the
// immediate reclaim path: a timed-out snapshot whose lease was already replaced
// must not unseat the newer claim.
func TestEmptyQueueReclaimPreservesReplacementLease(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	file := addTimedOutProcessingFile(t, baseRepo, "replacement-lease")

	newStart := time.Now().UTC()
	repo := &replacementLeaseRepository{
		MetadataRepository: baseRepo,
		tenantID:           tenant.ID,
		fileKey:            file.FileKey,
		replacementStart:   newStart,
	}
	scheduler := reclaimTestScheduler(t, repo, volumes, nil)

	location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if location != nil {
		t.Fatalf("location = %v, want nil because the snapshot lease was stale", location.FileKey)
	}

	current, err := baseRepo.Get(ctx, tenant.ID, file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing ||
		current.ProcessingStartTime == nil ||
		!current.ProcessingStartTime.Equal(newStart) {
		t.Fatalf("replacement lease overwritten: status %v start %v", current.Status, current.ProcessingStartTime)
	}
}

// TestCanceledEmptyQueueCallDoesNotConsumeReclaimSlot pins that a call cancelled
// before the reclaim starts rolls its cooldown reservation back instead of
// blocking the tenant for the whole cooldown.
func TestCanceledEmptyQueueCallDoesNotConsumeReclaimSlot(t *testing.T) {
	tenant := createTestTenant()

	var timedOutCalls atomic.Int64
	repo := &stubMetadataRepository{
		timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
			timedOutCalls.Add(1)
			return nil, nil
		},
	}
	scheduler, err := NewFileScheduler(repo, createTestVolumes(t), &FileSchedulerOptions{
		RecoverTimedOutOnEmptyQueue: true,
		TimedOutReclaimCooldown:     time.Hour,
		EmptyQueueReclaimBatchSize:  32,
	})
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	location, err := scheduler.GetNextFileForProcessing(canceledCtx, tenant)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if location != nil {
		t.Fatalf("location = %v, want nil", location)
	}
	if got := timedOutCalls.Load(); got != 0 {
		t.Fatalf("timed-out scans = %d, want 0: a cancelled call must not start a reclaim", got)
	}

	// The cancelled call must not have held the tenant's cooldown slot.
	if _, err := scheduler.GetNextFileForProcessing(context.Background(), tenant); err != nil {
		t.Fatalf("call after cancellation: %v", err)
	}
	if got := timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want 1 after the slot was rolled back", got)
	}
}

// TestDefaultFileSchedulerOptionsEnableEmptyQueueReclaim pins the documented
// defaults the Lead wires from config.CleanupConfig.
func TestDefaultFileSchedulerOptionsEnableEmptyQueueReclaim(t *testing.T) {
	opts := DefaultFileSchedulerOptions()

	if !opts.RecoverTimedOutOnEmptyQueue {
		t.Error("RecoverTimedOutOnEmptyQueue = false, want true")
	}
	if opts.TimedOutReclaimCooldown != 30*time.Second {
		t.Errorf("TimedOutReclaimCooldown = %v, want 30s", opts.TimedOutReclaimCooldown)
	}
	if opts.EmptyQueueReclaimBatchSize != 32 {
		t.Errorf("EmptyQueueReclaimBatchSize = %d, want 32", opts.EmptyQueueReclaimBatchSize)
	}
}

// TestDefaultFileSchedulerOptionsPinLocusReclaimDefaults pins the Locus v2.0.0
// defaults (FileScheduler.cs:52-62,79-82) for the opportunistic background pass
// that config.CleanupConfig mirrors.
func TestDefaultFileSchedulerOptionsPinLocusReclaimDefaults(t *testing.T) {
	opts := DefaultFileSchedulerOptions()

	if !opts.BackgroundTimedOutReclaimEnabled {
		t.Error("BackgroundTimedOutReclaimEnabled = false, want true")
	}
	if opts.BackgroundTimedOutReclaimBatchSize != 8 {
		t.Errorf("BackgroundTimedOutReclaimBatchSize = %d, want 8", opts.BackgroundTimedOutReclaimBatchSize)
	}
	if DefaultBackgroundTimedOutReclaimBatchSize != 8 {
		t.Errorf("DefaultBackgroundTimedOutReclaimBatchSize = %d, want 8",
			DefaultBackgroundTimedOutReclaimBatchSize)
	}
}

// TestEmptyQueueReclaimBatchSizeZeroSelectsDefault pins the configuration
// semantics documented on FileSchedulerOptions.EmptyQueueReclaimBatchSize: a zero
// value is "unset" and selects DefaultEmptyQueueReclaimBatchSize, so one
// empty-queue reclaim resets every timed-out file the default ceiling covers.
// Only a negative value disables the synchronous pass, which
// TestGetNextFileForProcessingLeavesTimedOutFileWhenReclaimDisabled covers.
func TestEmptyQueueReclaimBatchSizeZeroSelectsDefault(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	repo, tmpDir := createTestRepository(t)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	files := []*core.FileMetadata{
		addTimedOutProcessingFile(t, repo, "crashed-a"),
		addTimedOutProcessingFile(t, repo, "crashed-b"),
		addTimedOutProcessingFile(t, repo, "crashed-c"),
	}

	// The default ceiling (32) is far above the three files, so a single
	// empty-queue reclaim must reset all of them. A disabled pass would leave all
	// three leases stale; a ceiling of one would leave two.
	scheduler := reclaimTestScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.EmptyQueueReclaimBatchSize = 0
	})

	location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if location == nil {
		t.Fatal("empty queue returned no file, want the timed-out files recovered")
	}

	for _, file := range files {
		current, err := repo.Get(ctx, tenant.ID, file.FileKey)
		if err != nil {
			t.Fatalf("get metadata %s: %v", file.FileKey, err)
		}
		if current.ProcessingStartTime != nil && current.ProcessingStartTime.Equal(*file.ProcessingStartTime) {
			t.Errorf("%s still carries its stale lease %v, want the default ceiling to reset it",
				file.FileKey, current.ProcessingStartTime)
		}
	}
}
