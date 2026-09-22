package cleanup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// deadLetterTestKey is a managed file key shape: 32 lowercase hexadecimal
// characters.
const deadLetterTestKey = "00112233445566778899aabbccddeeff"

// moveRecordingVolume adds the optional core.FileMover capability to a real
// volume, so a test can assert the dead-letter path prefers it and can inject
// move failures. Successful moves are performed for real through the embedded
// volume.
type moveRecordingVolume struct {
	core.StorageVolume
	mu         sync.Mutex
	moves      [][2]string
	moveErrFor func(ctx context.Context, from string, to string) error
}

func (v *moveRecordingVolume) MoveFile(ctx context.Context, from string, to string) error {
	v.mu.Lock()
	hook := v.moveErrFor
	v.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, from, to); err != nil {
			return err
		}
	}

	reader, err := v.ReadFile(ctx, from)
	if err != nil {
		return err
	}
	_, writeErr := v.WriteFile(ctx, to, reader)
	closeErr := reader.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := v.DeleteFile(ctx, from); err != nil {
		return err
	}

	v.mu.Lock()
	v.moves = append(v.moves, [2]string{from, to})
	v.mu.Unlock()
	return nil
}

func (v *moveRecordingVolume) moveCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.moves)
}

// readFailVolume fails the fallback copy path, which is the read+write+delete
// path used when a volume does not implement core.FileMover.
type readFailVolume struct {
	core.StorageVolume
	readErr error
}

func (v *readFailVolume) ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	return nil, v.readErr
}

// noMoverVolume hides the volume's optional core.FileMover capability, so a test
// can exercise the documented read+write+delete fallback.
type noMoverVolume struct {
	core.StorageVolume
}

// newDeadLetterRecord builds an eligible permanently failed record.
func newDeadLetterRecord(fileKey string, physicalPath string) *core.FileMetadata {
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	return &core.FileMetadata{
		FileKey:       fileKey,
		TenantID:      "test-tenant",
		VolumeID:      "test-volume",
		PhysicalPath:  physicalPath,
		DirectoryPath: "/dir",
		FileSize:      7,
		FileExtension: ".txt",
		Status:        core.FileStatusPermanentlyFailed,
		LastFailedAt:  &old,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// TestCleanupPermanentlyFailedFiles_MoveToDeadLetterDefaultLayout is the
// regression test for the Locus MoveToDeadLetter disposition: the payload moves
// inside the same volume to
// <root>/<tenantID>/<yyyyMMdd>/<xx>/<yy>/<fileKey><ext>, the record is marked
// DeadLettered and persisted, and the tenant and directory counts are released
// exactly once (idempotent on a second run).
func TestCleanupPermanentlyFailedFiles_MoveToDeadLetterDefaultLayout(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
		// DeadLetter is left zero: the Locus defaults apply.
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount(tenant) error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 1 {
		t.Fatalf("DeadLetteredFiles = %d, want 1", stats.DeadLetteredFiles)
	}
	if stats.PermanentlyFailedFilesRemoved != 0 {
		t.Errorf("PermanentlyFailedFilesRemoved = %d, want 0", stats.PermanentlyFailedFilesRemoved)
	}
	if stats.SpaceFreed != 0 {
		t.Errorf("SpaceFreed = %d, want 0 (the payload is retained in the dead-letter area)", stats.SpaceFreed)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusDeadLettered {
		t.Errorf("status = %s, want DeadLettered", stored.Status)
	}
	if stored.DeadLetteredAt == nil {
		t.Fatal("DeadLetteredAt = nil, want the transition timestamp")
	}

	// Stored physical paths use forward slashes on every platform.
	wantPath := filepath.ToSlash(filepath.Join(
		".deadletter",
		"test-tenant",
		stored.DeadLetteredAt.UTC().Format("20060102"),
		deadLetterTestKey[0:2],
		deadLetterTestKey[2:4],
		deadLetterTestKey+".txt",
	))
	if stored.PhysicalPath != wantPath {
		t.Errorf("PhysicalPath = %q, want %q", stored.PhysicalPath, wantPath)
	}

	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || exists {
		t.Errorf("source payload exists = %v, error = %v; want it moved", exists, existsErr)
	}
	if exists, existsErr := base.FileExists(ctx, wantPath); existsErr != nil || !exists {
		t.Errorf("dead-letter payload exists = %v, error = %v; want it present", exists, existsErr)
	}
	if got := mover.moveCount(); got != 1 {
		t.Errorf("FileMover.MoveFile calls = %d, want 1", got)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
	if dirDecrements, _ := dirQuotaMgr.Snapshot(); dirDecrements != 1 {
		t.Errorf("directory quota decrements = %d, want 1", dirDecrements)
	}

	// Idempotent: a DeadLettered record is skipped.
	second, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("second CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if second.DeadLetteredFiles != 0 {
		t.Errorf("second run DeadLetteredFiles = %d, want 0", second.DeadLetteredFiles)
	}
	if got := mover.moveCount(); got != 1 {
		t.Errorf("second run FileMover.MoveFile calls = %d, want 1", got)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 1 {
		t.Errorf("second run tenant quota decrements = %d, want 1", decrements)
	}
	if dirDecrements, _ := dirQuotaMgr.Snapshot(); dirDecrements != 1 {
		t.Errorf("second run directory quota decrements = %d, want 1", dirDecrements)
	}
}

// TestCleanupPermanentlyFailedFiles_MoveToDeadLetterWithoutFileMover verifies the
// documented fallback: a volume that does not implement core.FileMover is served
// by a read + write + delete copy that preserves the payload bytes.
func TestCleanupPermanentlyFailedFiles_MoveToDeadLetterWithoutFileMover(t *testing.T) {
	ctx := context.Background()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
	})

	base := volumes["test-volume"]
	if _, isMover := base.(core.FileMover); !isMover {
		t.Fatal("this test needs a volume that does implement core.FileMover, to prove the wrapper hides it")
	}
	fallback := &noMoverVolume{StorageVolume: base}
	if _, isMover := any(fallback).(core.FileMover); isMover {
		t.Fatal("the fallback wrapper unexpectedly implements core.FileMover")
	}
	volumes["test-volume"] = fallback

	const payload = "dead-letter payload bytes"
	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	writeTestFile(t, base.MountPath(), record.PhysicalPath, payload)
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 1 {
		t.Fatalf("DeadLetteredFiles = %d, want 1", stats.DeadLetteredFiles)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusDeadLettered {
		t.Errorf("status = %s, want DeadLettered", stored.Status)
	}

	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || exists {
		t.Errorf("source payload exists = %v, error = %v; want it moved", exists, existsErr)
	}
	reader, err := base.ReadFile(ctx, stored.PhysicalPath)
	if err != nil {
		t.Fatalf("ReadFile(dead letter) error = %v", err)
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("reading the dead-letter payload failed: read=%v close=%v", readErr, closeErr)
	}
	if got := string(content); got != payload {
		t.Errorf("dead-letter payload = %q, want %q", got, payload)
	}
}

// TestCleanupPermanentlyFailedFiles_MoveToDeadLetterCustomLayout pins the
// configured layout knobs, including the literal reading of a non-zero
// DeadLetterOptions value.
func TestCleanupPermanentlyFailedFiles_MoveToDeadLetterCustomLayout(t *testing.T) {
	ctx := context.Background()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
		opts.DeadLetter = DeadLetterOptions{RootPath: "dead", ShardingDepth: 1}
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	if _, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour); err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	wantPath := filepath.ToSlash(filepath.Join("dead", deadLetterTestKey[0:2], deadLetterTestKey+".txt"))
	if stored.PhysicalPath != wantPath {
		t.Errorf("PhysicalPath = %q, want %q", stored.PhysicalPath, wantPath)
	}
}

// TestDeadLetterOptions_DefaultResolution documents the resolution contract: a
// completely unset option selects the Locus defaults, and a non-zero value is
// honoured literally.
func TestDeadLetterOptions_DefaultResolution(t *testing.T) {
	if got, want := (DeadLetterOptions{}).withDefaults(), DefaultDeadLetterOptions(); got != want {
		t.Errorf("zero DeadLetterOptions resolved to %+v, want %+v", got, want)
	}
	if got, want := DefaultDeadLetterOptions(), (DeadLetterOptions{
		RootPath:             ".deadletter",
		IncludeTenantInPath:  true,
		IncludeDatePartition: true,
		ShardingDepth:        2,
	}); got != want {
		t.Errorf("DefaultDeadLetterOptions() = %+v, want %+v", got, want)
	}

	literal := DeadLetterOptions{RootPath: "dead", ShardingDepth: 1}.withDefaults()
	if literal.IncludeTenantInPath || literal.IncludeDatePartition {
		t.Errorf("explicit non-zero options must be honoured literally, got %+v", literal)
	}
	if literal.RootPath != "dead" || literal.ShardingDepth != 1 {
		t.Errorf("explicit non-zero options were rewritten: %+v", literal)
	}
}

// TestBuildDeadLetterPath_RejectsUnsafeTargets verifies the dead-letter target is
// always a volume-relative path that stays inside the volume.
func TestBuildDeadLetterPath_RejectsUnsafeTargets(t *testing.T) {
	valid := &core.FileMetadata{
		FileKey:       deadLetterTestKey,
		TenantID:      "test-tenant",
		FileExtension: ".txt",
	}

	absoluteRoot := filepath.Join(string(filepath.Separator), "var", "dead")
	if volumeName := filepath.VolumeName(os.TempDir()); volumeName != "" {
		absoluteRoot = filepath.Join(volumeName+string(filepath.Separator), "dead")
	}

	tests := []struct {
		name    string
		options DeadLetterOptions
		record  *core.FileMetadata
		wantErr bool
	}{
		{
			name:    "absolute root",
			options: DeadLetterOptions{RootPath: absoluteRoot},
			record:  valid,
			wantErr: true,
		},
		{
			name:    "escaping root",
			options: DeadLetterOptions{RootPath: filepath.Join("..", "outside")},
			record:  valid,
			wantErr: true,
		},
		{
			name:    "invalid tenant identifier",
			options: DeadLetterOptions{RootPath: "dead", IncludeTenantInPath: true},
			record:  &core.FileMetadata{FileKey: deadLetterTestKey, TenantID: "..", FileExtension: ".txt"},
			wantErr: true,
		},
		{
			name:    "path separator in the file key",
			options: DeadLetterOptions{RootPath: "dead"},
			record:  &core.FileMetadata{FileKey: ".." + string(filepath.Separator) + "escape", TenantID: "test-tenant"},
			wantErr: true,
		},
		{
			name:    "empty file key",
			options: DeadLetterOptions{RootPath: "dead"},
			record:  &core.FileMetadata{FileKey: "", TenantID: "test-tenant"},
			wantErr: true,
		},
		{
			name:    "valid target",
			options: DeadLetterOptions{RootPath: "dead", IncludeTenantInPath: true, ShardingDepth: 2},
			record:  valid,
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, _, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
				opts.DeadLetter = test.options
			})
			concrete, ok := service.(*cleanupService)
			if !ok {
				t.Fatalf("unexpected cleanup service type %T", service)
			}

			path, err := concrete.buildDeadLetterPath(test.record, time.Now())
			if test.wantErr {
				if err == nil {
					t.Fatalf("buildDeadLetterPath() = %q, want an error", path)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildDeadLetterPath() error = %v", err)
			}
			if filepath.IsAbs(path) {
				t.Errorf("buildDeadLetterPath() = %q, want a volume-relative path", path)
			}
			if rel, relErr := filepath.Rel("dead", path); relErr != nil || rel == "" {
				t.Errorf("buildDeadLetterPath() = %q, want it rooted below %q", path, "dead")
			}
		})
	}
}

// TestCleanupPermanentlyFailedFiles_UnsafeDeadLetterTargetLeavesRecordUntouched
// proves a rejected target skips the record without touching the payload, the
// record or the quota counts.
func TestCleanupPermanentlyFailedFiles_UnsafeDeadLetterTargetLeavesRecordUntouched(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
		opts.DeadLetter = DeadLetterOptions{RootPath: filepath.Join("..", "outside")}
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 0 {
		t.Errorf("DeadLetteredFiles = %d, want 0", stats.DeadLetteredFiles)
	}
	if got := mover.moveCount(); got != 0 {
		t.Errorf("FileMover.MoveFile calls = %d, want 0", got)
	}
	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("status = %s, want PermanentlyFailed", stored.Status)
	}
	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || !exists {
		t.Errorf("payload exists = %v, error = %v; want it retained", exists, existsErr)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0", decrements)
	}
}

// TestCleanupPermanentlyFailedFiles_KeepLeavesEverythingUntouched verifies the
// Keep disposition leaves the payload, the metadata and the quota counts exactly
// as they were.
func TestCleanupPermanentlyFailedFiles_KeepLeavesEverythingUntouched(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedKeep
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount(tenant) error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if *stats != (core.CleanupStatistics{}) {
		t.Errorf("Keep statistics = %+v, want an empty statistic", stats)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("status = %s, want PermanentlyFailed (untouched)", stored.Status)
	}
	if stored.DeadLetteredAt != nil {
		t.Errorf("DeadLetteredAt = %v, want nil", stored.DeadLetteredAt)
	}
	if stored.PhysicalPath != record.PhysicalPath {
		t.Errorf("PhysicalPath = %q, want %q", stored.PhysicalPath, record.PhysicalPath)
	}
	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || !exists {
		t.Errorf("payload exists = %v, error = %v; want it retained", exists, existsErr)
	}
	if got := mover.moveCount(); got != 0 {
		t.Errorf("FileMover.MoveFile calls = %d, want 0", got)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0", decrements)
	}
	if dirDecrements, _ := dirQuotaMgr.Snapshot(); dirDecrements != 0 {
		t.Errorf("directory quota decrements = %d, want 0", dirDecrements)
	}
}

// TestCleanupPermanentlyFailedFiles_MoveFailureKeepsRecordAndContinues verifies
// that a failed move leaves the record, the payload and the quota counts
// untouched, and that the sweep continues with the next eligible record.
func TestCleanupPermanentlyFailedFiles_MoveFailureKeepsRecordAndContinues(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
	})

	base := volumes["test-volume"]

	const failingKey = "aaaabbbbccccddddeeeeffff00001111"
	const succeedingKey = "11110000aaaabbbbccccddddeeeeffff"

	failingRecord := newDeadLetterRecord(failingKey, filepath.Join("tenant-001", failingKey+".txt"))
	succeedingRecord := newDeadLetterRecord(succeedingKey, filepath.Join("tenant-001", succeedingKey+".txt"))
	failingPath := failingRecord.PhysicalPath

	mover := &moveRecordingVolume{
		StorageVolume: base,
		moveErrFor: func(_ context.Context, from string, _ string) error {
			if from == failingPath {
				return errors.New("move refused")
			}
			return nil
		},
	}
	volumes["test-volume"] = mover

	for _, record := range []*core.FileMetadata{failingRecord, succeedingRecord} {
		if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", record.PhysicalPath, err)
		}
		if err := repo.AddOrUpdate(ctx, record); err != nil {
			t.Fatalf("AddOrUpdate(%s) error = %v", record.FileKey, err)
		}
		if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
			t.Fatalf("IncrementFileCount(dir) error = %v", err)
		}
		if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
			t.Fatalf("IncrementFileCount(tenant) error = %v", err)
		}
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 1 {
		t.Fatalf("DeadLetteredFiles = %d, want 1 (the second record must still be processed)", stats.DeadLetteredFiles)
	}

	failed, err := repo.Get(ctx, "test-tenant", failingKey)
	if err != nil {
		t.Fatalf("Get(failing) error = %v", err)
	}
	if failed.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("failing record status = %s, want PermanentlyFailed", failed.Status)
	}
	if failed.PhysicalPath != failingPath {
		t.Errorf("failing record PhysicalPath = %q, want %q", failed.PhysicalPath, failingPath)
	}
	if exists, existsErr := base.FileExists(ctx, failingPath); existsErr != nil || !exists {
		t.Errorf("failing payload exists = %v, error = %v; want it retained", exists, existsErr)
	}

	succeeded, err := repo.Get(ctx, "test-tenant", succeedingKey)
	if err != nil {
		t.Fatalf("Get(succeeding) error = %v", err)
	}
	if succeeded.Status != core.FileStatusDeadLettered {
		t.Errorf("succeeding record status = %s, want DeadLettered", succeeded.Status)
	}

	// Only the successful transition releases a directory and tenant count.
	if dirDecrements, _ := dirQuotaMgr.Snapshot(); dirDecrements != 1 {
		t.Errorf("directory quota decrements = %d, want 1", dirDecrements)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
}

// TestCleanupPermanentlyFailedFiles_QuotaFailureRollsBackTransition verifies that
// a failed directory-count release restores the record and the payload, so a
// later cycle can retry and the counts cannot drift.
func TestCleanupPermanentlyFailedFiles_QuotaFailureRollsBackTransition(t *testing.T) {
	ctx := context.Background()

	tenantQuotaMgr := newStubTenantQuotaManager()
	dirQuotaMgr := &stubDirectoryQuotaManager{decrementErr: errors.New("directory quota unavailable")}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenantQuotaMgr
		opts.DirectoryQuotaManager = dirQuotaMgr
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenantQuotaMgr.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount(tenant) error = %v", err)
	}
	if err := dirQuotaMgr.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	dirDecrementsBefore, dirIncrementsBefore := dirQuotaMgr.Snapshot()

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 0 {
		t.Errorf("DeadLetteredFiles = %d, want 0 after a quota failure", stats.DeadLetteredFiles)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("status = %s, want PermanentlyFailed (rolled back)", stored.Status)
	}
	if stored.PhysicalPath != record.PhysicalPath {
		t.Errorf("PhysicalPath = %q, want the original %q (rolled back)", stored.PhysicalPath, record.PhysicalPath)
	}
	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || !exists {
		t.Errorf("payload exists = %v, error = %v; want it restored", exists, existsErr)
	}
	if decrements, _ := tenantQuotaMgr.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0", decrements)
	}
	dirDecrements, dirIncrements := dirQuotaMgr.Snapshot()
	if dirDecrements != dirDecrementsBefore || dirIncrements != dirIncrementsBefore {
		t.Errorf("directory quota changes = %d/%d, want no change from %d/%d",
			dirDecrements, dirIncrements, dirDecrementsBefore, dirIncrementsBefore)
	}
}

// TestCleanupPermanentlyFailedFiles_DeadLetterMoveFailureFromReadFallback
// verifies the fallback copy path fails safe: the record keeps pointing at the
// payload that is still in place.
func TestCleanupPermanentlyFailedFiles_DeadLetterMoveFailureFromReadFallback(t *testing.T) {
	ctx := context.Background()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
	})

	base := volumes["test-volume"]
	volumes["test-volume"] = &readFailVolume{StorageVolume: base, readErr: errors.New("volume read failed")}

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 0 {
		t.Errorf("DeadLetteredFiles = %d, want 0", stats.DeadLetteredFiles)
	}

	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusPermanentlyFailed {
		t.Errorf("status = %s, want PermanentlyFailed", stored.Status)
	}
	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || !exists {
		t.Errorf("payload exists = %v, error = %v; want it retained", exists, existsErr)
	}
}

// TestCleanupPermanentlyFailedFiles_DeadLetterTransitionsMissingPayload pins the
// Locus behaviour for a record without a readable payload: the record is
// transitioned without a move, so its quota no longer leaks.
func TestCleanupPermanentlyFailedFiles_DeadLetterTransitionsMissingPayload(t *testing.T) {
	ctx := context.Background()

	tenants := newStubTenantQuotaManager()

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenants
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, "")
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := tenants.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount() error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.DeadLetteredFiles != 1 {
		t.Fatalf("DeadLetteredFiles = %d, want 1", stats.DeadLetteredFiles)
	}
	if got := mover.moveCount(); got != 0 {
		t.Errorf("FileMover.MoveFile calls = %d, want 0", got)
	}
	stored, err := repo.Get(ctx, "test-tenant", deadLetterTestKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stored.Status != core.FileStatusDeadLettered {
		t.Errorf("status = %s, want DeadLettered", stored.Status)
	}
	if decrements, _ := tenants.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
}

// TestCleanupPermanentlyFailedFiles_DeleteRemovesPayloadAndMetadata is a guard
// test: the Delete disposition still removes the payload and its metadata in the
// documented order.
func TestCleanupPermanentlyFailedFiles_DeleteRemovesPayloadAndMetadata(t *testing.T) {
	ctx := context.Background()

	tenants := newStubTenantQuotaManager()
	dirs := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenants
		opts.DirectoryQuotaManager = dirs
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedDelete
	})

	base := volumes["test-volume"]
	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	if err := writeVolumeFile(t, base, record.PhysicalPath); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := dirs.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	if err := tenants.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount(tenant) error = %v", err)
	}

	stats, err := service.CleanupPermanentlyFailedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupPermanentlyFailedFiles() error = %v", err)
	}
	if stats.PermanentlyFailedFilesRemoved != 1 {
		t.Fatalf("PermanentlyFailedFilesRemoved = %d, want 1", stats.PermanentlyFailedFilesRemoved)
	}
	if stats.DeadLetteredFiles != 0 {
		t.Errorf("DeadLetteredFiles = %d, want 0", stats.DeadLetteredFiles)
	}
	if stats.SpaceFreed != record.FileSize {
		t.Errorf("SpaceFreed = %d, want %d", stats.SpaceFreed, record.FileSize)
	}
	if _, err := repo.Get(ctx, "test-tenant", deadLetterTestKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Errorf("metadata Get() error = %v, want ErrFileNotFound", err)
	}
	if exists, existsErr := base.FileExists(ctx, record.PhysicalPath); existsErr != nil || exists {
		t.Errorf("payload exists = %v, error = %v; want it deleted", exists, existsErr)
	}
	if decrements, _ := tenants.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
	if dirDecrements, _ := dirs.Snapshot(); dirDecrements != 1 {
		t.Errorf("directory quota decrements = %d, want 1", dirDecrements)
	}
}
