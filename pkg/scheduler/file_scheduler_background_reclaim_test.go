package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// capturedRecord is one structured record a logging.Runtime emitted.
type capturedRecord struct {
	level     slog.Level
	message   string
	component string
	event     string
	attrs     map[string]string
}

// recordCapturingHandler captures every emitted record so a test can assert on the
// structured warning without a real log writer.
type recordCapturingHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (h *recordCapturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordCapturingHandler) Handle(_ context.Context, record slog.Record) error {
	captured := capturedRecord{
		level:   record.Level,
		message: record.Message,
		attrs:   make(map[string]string),
	}
	record.Attrs(func(attr slog.Attr) bool {
		switch attr.Key {
		case "component":
			captured.component = attr.Value.String()
		case "event":
			captured.event = attr.Value.String()
		default:
			captured.attrs[attr.Key] = attr.Value.String()
		}
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, captured)
	return nil
}

func (h *recordCapturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordCapturingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordCapturingHandler) snapshot() []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]capturedRecord(nil), h.records...)
}

// backgroundTrackingRepository observes the opportunistic background reclaim
// path: it counts timed-out scans and reports every lease-checked transition that
// succeeded, so a test can await the pass it just triggered instead of polling.
type backgroundTrackingRepository struct {
	core.MetadataRepository
	timedOutCalls atomic.Int64
	transitions   atomic.Int64
	resets        chan string
}

func newBackgroundTrackingRepository(repo core.MetadataRepository) *backgroundTrackingRepository {
	return &backgroundTrackingRepository{
		MetadataRepository: repo,
		resets:             make(chan string, 64),
	}
}

func (r *backgroundTrackingRepository) GetTimedOutProcessingFiles(
	ctx context.Context,
	tenantID string,
	timeout time.Duration,
) ([]*core.FileMetadata, error) {
	r.timedOutCalls.Add(1)
	return r.MetadataRepository.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
}

func (r *backgroundTrackingRepository) CompareAndUpdateProcessing(
	ctx context.Context,
	lease core.FileProcessingLease,
	update func(*core.FileMetadata) error,
) (*core.FileMetadata, error) {
	updated, err := r.MetadataRepository.CompareAndUpdateProcessing(ctx, lease, update)
	r.transitions.Add(1)
	if err == nil {
		select {
		case r.resets <- lease.FileKey:
		default:
		}
	}
	return updated, err
}

// gatedReclaimRepository holds the first timed-out scan until the test releases
// it, so one background pass can be kept in flight deterministically.
type gatedReclaimRepository struct {
	*backgroundTrackingRepository
	gate        atomic.Bool
	scanStarted chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func newGatedReclaimRepository(repo core.MetadataRepository) *gatedReclaimRepository {
	return &gatedReclaimRepository{
		backgroundTrackingRepository: newBackgroundTrackingRepository(repo),
		scanStarted:                  make(chan struct{}),
		release:                      make(chan struct{}),
	}
}

func (r *gatedReclaimRepository) GetTimedOutProcessingFiles(
	ctx context.Context,
	tenantID string,
	timeout time.Duration,
) ([]*core.FileMetadata, error) {
	if r.gate.CompareAndSwap(false, true) {
		close(r.scanStarted)
		<-r.release
	}
	return r.backgroundTrackingRepository.GetTimedOutProcessingFiles(ctx, tenantID, timeout)
}

// releaseGate lets a gated background pass continue. It is safe to call more
// than once so a failed wait can release the pass before the test aborts.
func (r *gatedReclaimRepository) releaseGate() {
	r.releaseOnce.Do(func() { close(r.release) })
}

// backgroundReclaimScheduler builds a scheduler over repo with Locus-compatible
// defaults, immediate reclaim switched off and a one-hour processing timeout, so
// each test isolates the opportunistic background pass. mutate may adjust the
// options. The concrete scheduler is returned so a test can await and close the
// passes it triggers.
func backgroundReclaimScheduler(
	t *testing.T,
	repo core.MetadataRepository,
	volumes map[string]core.StorageVolume,
	mutate func(*FileSchedulerOptions),
) *fileScheduler {
	t.Helper()

	opts := DefaultFileSchedulerOptions()
	opts.RecoverTimedOutOnEmptyQueue = false
	opts.ProcessingTimeout = time.Hour
	// The background pass is switched on explicitly so each test does not depend
	// on the package default; TestDefaultFileSchedulerOptionsPinLocusDefaults pins
	// that default separately.
	opts.BackgroundTimedOutReclaimEnabled = true
	if mutate != nil {
		mutate(opts)
	}

	scheduler, err := NewFileScheduler(repo, volumes, opts)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	concrete, ok := scheduler.(*fileScheduler)
	if !ok {
		t.Fatalf("NewFileScheduler returned %T, want *fileScheduler", scheduler)
	}
	return concrete
}

// addPendingTestFile stores one immediately claimable Pending file.
func addPendingTestFile(t *testing.T, repo core.MetadataRepository, fileKey string) *core.FileMetadata {
	t.Helper()

	file := createTestFileMetadata(fileKey, core.FileStatusPending)
	if err := repo.AddOrUpdate(context.Background(), file); err != nil {
		t.Fatalf("add pending metadata %q: %v", fileKey, err)
	}
	return file
}

// claimPendingFile claims exactly one file and requires the claim to succeed.
func claimPendingFile(t *testing.T, scheduler core.FileScheduler, tenant core.TenantContext) *core.FileLocation {
	t.Helper()

	location, err := scheduler.GetNextFileForProcessing(context.Background(), tenant)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if location == nil {
		t.Fatal("claim returned no file")
	}
	return location
}

// awaitBackgroundReset waits for one lease-checked reset performed by a background
// pass, so the test never races the pass it just triggered.
func awaitBackgroundReset(t *testing.T, repo *backgroundTrackingRepository) string {
	t.Helper()

	select {
	case fileKey := <-repo.resets:
		return fileKey
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the background reclaim to reset a timed-out file")
		return ""
	}
}

// awaitBackgroundScanStart waits until a gated background pass entered its
// timed-out scan, releasing the gate before failing so a blocked pass cannot keep
// the scheduler from closing.
func awaitBackgroundScanStart(t *testing.T, repo *gatedReclaimRepository) {
	t.Helper()

	select {
	case <-repo.scanStarted:
	case <-time.After(10 * time.Second):
		repo.releaseGate()
		t.Fatal("timed out waiting for the background reclaim to start")
	}
}

// requireStaleLease asserts the stored file still carries the Processing state and
// lease it had before the background pass ran.
func requireStaleLease(t *testing.T, repo core.MetadataRepository, file *core.FileMetadata) {
	t.Helper()

	current, err := repo.Get(context.Background(), file.TenantID, file.FileKey)
	if err != nil {
		t.Fatalf("get metadata %q: %v", file.FileKey, err)
	}
	if current.Status != core.FileStatusProcessing {
		t.Fatalf("%s status = %v, want the file left in Processing", file.FileKey, current.Status)
	}
	if current.ProcessingStartTime == nil ||
		!current.ProcessingStartTime.Equal(*file.ProcessingStartTime) {
		t.Fatalf("%s ProcessingStartTime = %v, want the original lease %v",
			file.FileKey, current.ProcessingStartTime, file.ProcessingStartTime)
	}
}

// awaitGoroutineBaseline waits until the running goroutine count drops back to
// want, which catches a background pass goroutine that outlived Close.
func awaitGoroutineBaseline(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d: a background reclaim leaked past Close", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSuccessfulClaimRunsBackgroundTimedOutReclaim pins Locus v2.0.0 behavior
// (FileScheduler.cs:621-663): a successful claim schedules one opportunistic
// reclaim of timed-out Processing files, so a crashed worker's file is recovered
// even though no empty-queue claim and no cleanup cycle ever runs.
func TestSuccessfulClaimRunsBackgroundTimedOutReclaim(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")
	claimable := addPendingTestFile(t, baseRepo, "fresh-work")

	repo := newBackgroundTrackingRepository(baseRepo)
	// Immediate reclaim is off, so the only path that may reset crashed-worker is
	// the background pass scheduled by the successful claim below.
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	location := claimPendingFile(t, scheduler, tenant)
	if location.FileKey != claimable.FileKey {
		t.Fatalf("claim = %q, want the pending file %q", location.FileKey, claimable.FileKey)
	}

	resetKey := awaitBackgroundReset(t, repo)
	scheduler.backgroundWG.Wait()

	if resetKey != crashed.FileKey {
		t.Fatalf("background reset %q, want %q", resetKey, crashed.FileKey)
	}
	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want exactly the one background pass", got)
	}

	current, err := baseRepo.Get(ctx, tenant.ID, crashed.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusPending {
		t.Fatalf("status = %v, want Pending after the background reclaim", current.Status)
	}
	if current.ProcessingStartTime != nil {
		t.Errorf("ProcessingStartTime = %v, want the stale lease cleared", current.ProcessingStartTime)
	}
	if current.AvailableForProcessingAt == nil || current.AvailableForProcessingAt.After(time.Now()) {
		t.Errorf("AvailableForProcessingAt = %v, want the recovery instant", current.AvailableForProcessingAt)
	}
}

// TestGetNextBatchForProcessingRunsBackgroundTimedOutReclaim pins that the batch
// claim path schedules the same opportunistic pass after it returns files.
func TestGetNextBatchForProcessingRunsBackgroundTimedOutReclaim(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")
	addPendingTestFile(t, baseRepo, "fresh-work")

	repo := newBackgroundTrackingRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	locations, err := scheduler.GetNextBatchForProcessing(ctx, tenant, 5)
	if err != nil {
		t.Fatalf("batch claim: %v", err)
	}
	if len(locations) != 1 || locations[0].FileKey != "fresh-work" {
		t.Fatalf("batch claim = %v, want the single pending file", locations)
	}

	if resetKey := awaitBackgroundReset(t, repo); resetKey != crashed.FileKey {
		t.Fatalf("background reset %q, want %q", resetKey, crashed.FileKey)
	}
	scheduler.backgroundWG.Wait()

	current, err := baseRepo.Get(ctx, tenant.ID, crashed.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusPending {
		t.Fatalf("status = %v, want Pending after the background reclaim", current.Status)
	}
}

// TestBackgroundTimedOutReclaimHonoursBatchSize pins that one background pass
// resets at most BackgroundTimedOutReclaimBatchSize timed-out files.
func TestBackgroundTimedOutReclaimHonoursBatchSize(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	files := []*core.FileMetadata{
		addTimedOutProcessingFile(t, baseRepo, "crashed-a"),
		addTimedOutProcessingFile(t, baseRepo, "crashed-b"),
		addTimedOutProcessingFile(t, baseRepo, "crashed-c"),
	}
	addPendingTestFile(t, baseRepo, "fresh-work")

	repo := newBackgroundTrackingRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.BackgroundTimedOutReclaimBatchSize = 1
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	claimPendingFile(t, scheduler, tenant)

	awaitBackgroundReset(t, repo)
	// Waiting for the pass to finish makes the "only one file was reset" assertion
	// deterministic: the marker and goroutine were registered before the claim
	// returned.
	scheduler.backgroundWG.Wait()

	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want exactly 1", got)
	}
	select {
	case extra := <-repo.resets:
		t.Fatalf("background pass reset %q as well, want at most one file", extra)
	default:
	}

	pending := 0
	for _, file := range files {
		current, err := baseRepo.Get(ctx, tenant.ID, file.FileKey)
		if err != nil {
			t.Fatalf("get metadata %s: %v", file.FileKey, err)
		}
		switch current.Status {
		case core.FileStatusPending:
			pending++
		case core.FileStatusProcessing:
			if current.ProcessingStartTime == nil ||
				!current.ProcessingStartTime.Equal(*file.ProcessingStartTime) {
				t.Errorf("%s lease = %v, want the original stale lease %v",
					file.FileKey, current.ProcessingStartTime, file.ProcessingStartTime)
			}
		default:
			t.Errorf("%s status = %v, want Pending or Processing", file.FileKey, current.Status)
		}
	}
	if pending != 1 {
		t.Fatalf("pending timed-out files = %d, want exactly the batch size 1", pending)
	}
}

// TestBackgroundTimedOutReclaimDoesNotSuppressEmptyQueueReclaim pins that the two
// reclaim paths keep independent cooldown state: a worker that finds an empty
// queue must still get its timed-out records back even though a successful claim
// just ran an opportunistic background pass.
//
// A shared cooldown would let the background pass lock the emergency path out for
// a whole cooldown window, which delays recovery of a crashed worker by up to
// TimedOutReclaimCooldown when no other claim succeeds in between.
func TestBackgroundTimedOutReclaimDoesNotSuppressEmptyQueueReclaim(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashedA := addTimedOutProcessingFile(t, baseRepo, "crashed-a")
	addPendingTestFile(t, baseRepo, "fresh-work")

	repo := newBackgroundTrackingRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.RecoverTimedOutOnEmptyQueue = true
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	// A successful claim starts the opportunistic pass, which reserves its own
	// cooldown window for a full hour.
	claimPendingFile(t, scheduler, tenant)
	if resetKey := awaitBackgroundReset(t, repo); resetKey != crashedA.FileKey {
		t.Fatalf("background reset %q, want %q", resetKey, crashedA.FileKey)
	}
	scheduler.backgroundWG.Wait()

	// The recovered record is claimed normally, leaving the queue empty again.
	if location := claimPendingFile(t, scheduler, tenant); location.FileKey != crashedA.FileKey {
		t.Fatalf("claim = %q, want the recovered file %q", location.FileKey, crashedA.FileKey)
	}
	scheduler.backgroundWG.Wait()

	// A second crashed worker appears while the background cooldown is still
	// active and the queue is now empty: the emergency path must still recover it.
	crashedB := addTimedOutProcessingFile(t, baseRepo, "crashed-b")

	location, err := scheduler.GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("empty-queue claim: %v", err)
	}
	if location == nil || location.FileKey != crashedB.FileKey {
		t.Fatalf("empty-queue claim = %#v, want the timed-out file %q to be recovered", location, crashedB.FileKey)
	}
	if got := repo.timedOutCalls.Load(); got != 2 {
		t.Fatalf("timed-out scans = %d, want 2: the emergency reclaim must not be suppressed", got)
	}
}

// TestReclaimPathsKeepIndependentCooldownState pins the cooldown invariant of the
// two reclaim paths without racing a background goroutine: each path owns its own
// per-tenant deadline map, so neither can suppress the other, while each still
// honours its own window and a zero cooldown keeps both ungated.
func TestReclaimPathsKeepIndependentCooldownState(t *testing.T) {
	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	scheduler := backgroundReclaimScheduler(t, baseRepo, volumes, func(o *FileSchedulerOptions) {
		o.RecoverTimedOutOnEmptyQueue = true
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	now := time.Now()

	// Direction 1: an emergency reclaim reserves its own window and must not
	// suppress the opportunistic pass inside that window.
	reservation, reserved := scheduler.reserveTimedOutReclaim(scheduler.reclaimDeadlines, "cooldown-a", now)
	if !reserved {
		t.Fatal("the first emergency reclaim was not allowed")
	}
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.backgroundReclaimDeadlines, "cooldown-a", now); !reserved {
		t.Fatal("the background pass was suppressed by the emergency reclaim cooldown")
	}

	// Each path still honours its own window.
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.reclaimDeadlines, "cooldown-a", now); reserved {
		t.Fatal("the emergency reclaim ran twice inside its own cooldown window")
	}
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.backgroundReclaimDeadlines, "cooldown-a", now); reserved {
		t.Fatal("the background pass ran twice inside its own cooldown window")
	}

	// Direction 2: a background reservation must not suppress the emergency path.
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.backgroundReclaimDeadlines, "cooldown-b", now); !reserved {
		t.Fatal("the first background pass was not allowed")
	}
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.reclaimDeadlines, "cooldown-b", now); !reserved {
		t.Fatal("the emergency reclaim was suppressed by the background pass cooldown")
	}

	// A non-positive cooldown keeps both paths ungated.
	scheduler.timedOutReclaimCooldown = 0
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.reclaimDeadlines, "cooldown-a", now); !reserved {
		t.Fatal("a zero cooldown must not gate the emergency reclaim")
	}
	if _, reserved := scheduler.reserveTimedOutReclaim(scheduler.backgroundReclaimDeadlines, "cooldown-a", now); !reserved {
		t.Fatal("a zero cooldown must not gate the background pass")
	}

	scheduler.rollbackTimedOutReclaim(scheduler.reclaimDeadlines, reservation)
}

// TestBackgroundTimedOutReclaimCooldownSuppressesSecondPass pins the
// background-to-background direction of the shared cooldown: a second successful
// claim inside the window must not start another pass, so a new timed-out record
// stays Processing.
func TestBackgroundTimedOutReclaimCooldownSuppressesSecondPass(t *testing.T) {
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashedA := addTimedOutProcessingFile(t, baseRepo, "crashed-a")
	addPendingTestFile(t, baseRepo, "fresh-work-a")

	repo := newBackgroundTrackingRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	claimPendingFile(t, scheduler, tenant)
	if resetKey := awaitBackgroundReset(t, repo); resetKey != crashedA.FileKey {
		t.Fatalf("background reset %q, want %q", resetKey, crashedA.FileKey)
	}
	scheduler.backgroundWG.Wait()

	crashedB := addTimedOutProcessingFile(t, baseRepo, "crashed-b")
	addPendingTestFile(t, baseRepo, "fresh-work-b")

	claimPendingFile(t, scheduler, tenant)
	// If a second pass had been scheduled, its registration happened before the
	// claim returned, so this wait observes it and the scan count would increase.
	scheduler.backgroundWG.Wait()

	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want 1 while the cooldown holds", got)
	}
	select {
	case extra := <-repo.resets:
		t.Fatalf("the second claim reset %q, want no reset inside the cooldown", extra)
	default:
	}
	requireStaleLease(t, baseRepo, crashedB)
}

// TestBackgroundTimedOutReclaimDisabledPerformsNoWork pins every off switch: a
// disabled flag, a non-positive batch size, or a non-positive processing timeout
// leaves the crashed worker's file untouched after a successful claim.
func TestBackgroundTimedOutReclaimDisabledPerformsNoWork(t *testing.T) {
	tenant := createTestTenant()

	cases := []struct {
		name   string
		mutate func(*FileSchedulerOptions)
	}{
		{
			name:   "background reclaim flag disabled",
			mutate: func(o *FileSchedulerOptions) { o.BackgroundTimedOutReclaimEnabled = false },
		},
		{
			name:   "background batch size zero",
			mutate: func(o *FileSchedulerOptions) { o.BackgroundTimedOutReclaimBatchSize = 0 },
		},
		{
			name:   "background batch size negative",
			mutate: func(o *FileSchedulerOptions) { o.BackgroundTimedOutReclaimBatchSize = -1 },
		},
		{
			name:   "processing timeout negative",
			mutate: func(o *FileSchedulerOptions) { o.ProcessingTimeout = -time.Second },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseRepo, _ := createTestRepository(t)

			volumes := createTestVolumes(t)
			defer cleanupVolumes(volumes)

			crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")
			addPendingTestFile(t, baseRepo, "fresh-work")

			repo := newBackgroundTrackingRepository(baseRepo)
			scheduler := backgroundReclaimScheduler(t, repo, volumes, tc.mutate)
			defer func() { _ = scheduler.Close() }()

			claimPendingFile(t, scheduler, tenant)
			scheduler.backgroundWG.Wait()

			if got := repo.timedOutCalls.Load(); got != 0 {
				t.Fatalf("timed-out scans = %d, want 0 with the pass disabled", got)
			}
			if got := repo.transitions.Load(); got != 0 {
				t.Fatalf("lease-checked transitions = %d, want 0 with the pass disabled", got)
			}
			requireStaleLease(t, baseRepo, crashed)
		})
	}
}

// TestBackgroundTimedOutReclaimPreservesReplacementLease pins the lease invariant
// on the background path: a timed-out snapshot whose lease was replaced between
// the scan and the update must not unseat the newer claim.
func TestBackgroundTimedOutReclaimPreservesReplacementLease(t *testing.T) {
	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	file := addTimedOutProcessingFile(t, baseRepo, "replacement-lease")

	newStart := time.Now().UTC()
	replacement := &replacementLeaseRepository{
		MetadataRepository: baseRepo,
		tenantID:           tenant.ID,
		fileKey:            file.FileKey,
		replacementStart:   newStart,
	}
	repo := newBackgroundTrackingRepository(replacement)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

	addPendingTestFile(t, baseRepo, "fresh-work")
	claimPendingFile(t, scheduler, tenant)

	// The pass ran to completion; its stale-lease update was rejected instead of
	// overwriting the replacement claim.
	scheduler.backgroundWG.Wait()

	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want the one background pass", got)
	}
	if got := repo.transitions.Load(); got != 1 {
		t.Fatalf("lease-checked transitions = %d, want the rejected update attempt", got)
	}
	if got := len(repo.resets); got != 0 {
		t.Fatalf("successful resets = %d, want 0 for a stale lease", got)
	}

	current, err := baseRepo.Get(ctx, tenant.ID, file.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusProcessing ||
		current.ProcessingStartTime == nil ||
		!current.ProcessingStartTime.Equal(newStart) {
		t.Fatalf("replacement lease overwritten: status %v start %v",
			current.Status, current.ProcessingStartTime)
	}
}

// TestBackgroundTimedOutReclaimConcurrentClaimsStartOnePass pins that concurrent
// successful claims for one tenant are race-free and start at most one background
// pass per cooldown window.
func TestBackgroundTimedOutReclaimConcurrentClaimsStartOnePass(t *testing.T) {
	const workers = 8

	ctx := context.Background()
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")
	for i := 0; i < workers; i++ {
		addPendingTestFile(t, baseRepo, "work-"+string(rune('a'+i)))
	}

	repo := newGatedReclaimRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	defer func() { _ = scheduler.Close() }()

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

	// The first successful claim schedules the only pass; it is held inside its
	// scan until every worker has claimed, so the reclaimed file cannot be claimed
	// in the middle of this test.
	awaitBackgroundScanStart(t, repo)
	wg.Wait()
	repo.releaseGate()
	scheduler.backgroundWG.Wait()

	claimed := make(map[string]bool, workers)
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Errorf("worker %d: unexpected error %v", i, errs[i])
			continue
		}
		if results[i] == nil {
			t.Errorf("worker %d claimed no file", i)
			continue
		}
		if claimed[results[i].FileKey] {
			t.Errorf("file %q was claimed twice", results[i].FileKey)
		}
		claimed[results[i].FileKey] = true
	}
	if len(claimed) != workers {
		t.Fatalf("workers claimed %d files, want %d", len(claimed), workers)
	}
	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want exactly one pass per cooldown window", got)
	}
	if resetKey := awaitBackgroundReset(t, repo.backgroundTrackingRepository); resetKey != crashed.FileKey {
		t.Fatalf("background reset %q, want %q", resetKey, crashed.FileKey)
	}
	if _, ok := scheduler.backgroundReclaims.Load(tenant.ID); ok {
		t.Error("in-flight marker left set after the pass finished")
	}

	current, err := baseRepo.Get(ctx, tenant.ID, crashed.FileKey)
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}
	if current.Status != core.FileStatusPending {
		t.Fatalf("status = %v, want the reclaimed file back in Pending", current.Status)
	}
}

// TestCloseWaitsForInFlightBackgroundReclaim pins the scheduler lifecycle: Close
// makes the background pass observe cancellation, waits for the in-flight pass,
// releases the in-flight marker, and never lets a later claim start a pass that
// would write to a repository the caller closes next.
func TestCloseWaitsForInFlightBackgroundReclaim(t *testing.T) {
	tenant := createTestTenant()

	baseRepo, _ := createTestRepository(t)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	crashed := addTimedOutProcessingFile(t, baseRepo, "crashed-worker")
	addPendingTestFile(t, baseRepo, "fresh-work")

	repo := newGatedReclaimRepository(baseRepo)
	scheduler := backgroundReclaimScheduler(t, repo, volumes, func(o *FileSchedulerOptions) {
		o.TimedOutReclaimCooldown = time.Hour
	})
	// The close defer is registered first so the gate is released before the
	// deferred Close waits for the pass.
	defer func() { _ = scheduler.Close() }()
	defer repo.releaseGate()

	before := runtime.NumGoroutine()

	claimPendingFile(t, scheduler, tenant)
	awaitBackgroundScanStart(t, repo)

	closed := make(chan error, 1)
	go func() { closed <- scheduler.Close() }()

	select {
	case err := <-closed:
		t.Fatalf("Close returned (err %v) while a background pass was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	repo.releaseGate()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the in-flight background pass finished")
	}

	if _, ok := scheduler.backgroundReclaims.Load(tenant.ID); ok {
		t.Error("in-flight marker left set after Close")
	}
	// The cancelled pass must not have written after Close.
	if got := len(repo.resets); got != 0 {
		t.Errorf("successful resets = %d, want 0: the cancelled pass must not write", got)
	}
	requireStaleLease(t, baseRepo, crashed)

	// A claim after Close still works, but it must not schedule a new pass.
	addPendingTestFile(t, baseRepo, "after-close-work")
	claimPendingFile(t, scheduler, tenant)
	scheduler.backgroundWG.Wait()
	if got := repo.timedOutCalls.Load(); got != 1 {
		t.Fatalf("timed-out scans = %d, want no new pass after Close", got)
	}

	// Close is idempotent and the pass goroutine is gone.
	if err := scheduler.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	awaitGoroutineBaseline(t, before)
}

// TestBackgroundTimedOutReclaimContainsAndLogsFailure pins that a failing
// background pass never reaches the claim caller, emits exactly one structured
// warning through the injected runtime, and never logs the raw error, which may
// carry a physical path.
func TestBackgroundTimedOutReclaimContainsAndLogsFailure(t *testing.T) {
	tenant := createTestTenant()

	// The message deliberately carries a physical path and a database suffix: the
	// scheduler must log only the failure type.
	scanFailure := errors.New(`sqlite: database disk image is malformed for C:\venue-data\tenant.db`)
	repo := &stubMetadataRepository{
		pendingFiles: func(context.Context, string, int) ([]*core.FileMetadata, error) {
			return []*core.FileMetadata{createTestFileMetadata("fresh-work", core.FileStatusPending)}, nil
		},
		transitionResult: func(_ context.Context, _ string, fileKey string) (*core.FileMetadata, error) {
			claimed := createTestFileMetadata(fileKey, core.FileStatusProcessing)
			now := time.Now().UTC()
			claimed.ProcessingStartTime = &now
			return claimed, nil
		},
		timedOutFiles: func(context.Context, string, time.Duration) ([]*core.FileMetadata, error) {
			return nil, scanFailure
		},
	}

	handler := &recordCapturingHandler{}
	runtime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging runtime: %v", err)
	}

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	opts := DefaultFileSchedulerOptions()
	opts.RecoverTimedOutOnEmptyQueue = false
	opts.ProcessingTimeout = time.Hour
	opts.Logging = runtime

	scheduler, err := NewFileScheduler(repo, volumes, opts)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	concrete, ok := scheduler.(*fileScheduler)
	if !ok {
		t.Fatalf("NewFileScheduler returned %T, want *fileScheduler", scheduler)
	}
	defer func() { _ = concrete.Close() }()

	location := claimPendingFile(t, concrete, tenant)
	if location.FileKey != "fresh-work" {
		t.Fatalf("claim = %q, want fresh-work", location.FileKey)
	}
	concrete.backgroundWG.Wait()

	records := handler.snapshot()
	if len(records) != 1 {
		t.Fatalf("emitted records = %d, want exactly one contained warning", len(records))
	}
	warning := records[0]
	if warning.level != slog.LevelWarn {
		t.Errorf("level = %v, want Warn", warning.level)
	}
	if warning.component != "scheduler.files" {
		t.Errorf("component = %q, want scheduler.files", warning.component)
	}
	if warning.event != "background_timed_out_reclaim_failed" {
		t.Errorf("event = %q, want background_timed_out_reclaim_failed", warning.event)
	}
	if got := warning.attrs["tenant_id"]; got != tenant.ID {
		t.Errorf("tenant_id = %q, want %q", got, tenant.ID)
	}
	if got := warning.attrs["error_type"]; got == "" {
		t.Error("error_type = empty, want the failure type")
	} else if got == scanFailure.Error() {
		t.Errorf("error_type = %q, want the type, not the raw error", got)
	}

	logged := warning.message
	for _, value := range warning.attrs {
		logged += " " + value
	}
	for _, leak := range []string{"truncate", ".db", `C:\`, "venue-data"} {
		if strings.Contains(logged, leak) {
			t.Errorf("logged warning contains %q, want only the failure type: %q", leak, logged)
		}
	}
}
