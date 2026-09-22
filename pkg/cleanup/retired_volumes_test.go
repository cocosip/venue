package cleanup

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// failingDeleteRepository fails metadata deletion on demand, so a test can prove
// quota counts are only released for a metadata row that is really gone.
type failingDeleteRepository struct {
	core.MetadataRepository
	deleteErr error
}

func (r *failingDeleteRepository) Delete(ctx context.Context, tenantID, fileKey string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	return r.MetadataRepository.Delete(ctx, tenantID, fileKey)
}

// TestCleanupPermanentlyFailedFiles_RetiredVolumePurgeMetadataOnly verifies the
// PurgeMetadataOnly retired-volume policy: metadata and both quota counts are
// released, physical storage is not touched, and the retired policy takes
// precedence over the permanently-failed disposition.
func TestCleanupPermanentlyFailedFiles_RetiredVolumePurgeMetadataOnly(t *testing.T) {
	ctx := context.Background()

	tenants := newStubTenantQuotaManager()
	dirs := &stubDirectoryQuotaManager{}

	service, repo, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenants
		opts.DirectoryQuotaManager = dirs
		opts.PermanentlyFailedDisposition = core.PermanentlyFailedMoveToDeadLetter
		opts.RetiredVolumes = map[string]core.RetiredVolumeDisposition{
			"retired-volume": core.RetiredVolumePurgeMetadataOnly,
		}
	})

	base := volumes["test-volume"]
	mover := &moveRecordingVolume{StorageVolume: base}
	volumes["test-volume"] = mover

	record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
	record.VolumeID = "retired-volume"
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
		t.Errorf("PermanentlyFailedFilesRemoved = %d, want 1", stats.PermanentlyFailedFilesRemoved)
	}
	if stats.DeadLetteredFiles != 0 {
		t.Errorf("DeadLetteredFiles = %d, want 0", stats.DeadLetteredFiles)
	}
	if stats.SpaceFreed != 0 {
		t.Errorf("SpaceFreed = %d, want 0 (no physical storage was touched)", stats.SpaceFreed)
	}

	if _, getErr := repo.Get(ctx, "test-tenant", deadLetterTestKey); !errors.Is(getErr, core.ErrFileNotFound) {
		t.Errorf("metadata Get() error = %v, want ErrFileNotFound", getErr)
	}
	if got := mover.moveCount(); got != 0 {
		t.Errorf("FileMover.MoveFile calls = %d, want 0", got)
	}
	if decrements, _ := tenants.snapshot(); decrements != 1 {
		t.Errorf("tenant quota decrements = %d, want 1", decrements)
	}
	if dirDecrements, _ := dirs.Snapshot(); dirDecrements != 1 {
		t.Errorf("directory quota decrements = %d, want 1", dirDecrements)
	}
}

// TestCleanupPermanentlyFailedFiles_RetiredVolumeKeepRetainsRecord verifies the
// default retired-volume policy keeps today's behaviour: the record is retained
// and quota accounting is untouched.
func TestCleanupPermanentlyFailedFiles_RetiredVolumeKeepRetainsRecord(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		dispositions map[string]core.RetiredVolumeDisposition
	}{
		{name: "explicit Keep", dispositions: map[string]core.RetiredVolumeDisposition{"retired-volume": core.RetiredVolumeKeep}},
		{name: "unlisted volume", dispositions: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tenants := newStubTenantQuotaManager()
			dirs := &stubDirectoryQuotaManager{}

			service, repo, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
				opts.TenantQuotaManager = tenants
				opts.DirectoryQuotaManager = dirs
				opts.PermanentlyFailedDisposition = core.PermanentlyFailedDelete
				opts.RetiredVolumes = test.dispositions
			})

			record := newDeadLetterRecord(deadLetterTestKey, filepath.Join("tenant-001", deadLetterTestKey+".txt"))
			record.VolumeID = "retired-volume"
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
			if *stats != (core.CleanupStatistics{}) {
				t.Errorf("statistics = %+v, want an empty statistic", stats)
			}
			if _, getErr := repo.Get(ctx, "test-tenant", deadLetterTestKey); getErr != nil {
				t.Errorf("metadata must be retained, Get() error = %v", getErr)
			}
			if decrements, _ := tenants.snapshot(); decrements != 0 {
				t.Errorf("tenant quota decrements = %d, want 0", decrements)
			}
			if dirDecrements, _ := dirs.Snapshot(); dirDecrements != 0 {
				t.Errorf("directory quota decrements = %d, want 0", dirDecrements)
			}
		})
	}
}

// TestCleanupCompletedFiles_RetiredVolumePolicy verifies the retired-volume
// policy is applied wherever cleanup meets an unregistered volume, including the
// completed-file sweep.
func TestCleanupCompletedFiles_RetiredVolumePolicy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		dispositions map[string]core.RetiredVolumeDisposition
		wantRemoved  int
	}{
		{
			name:         "purge metadata only",
			dispositions: map[string]core.RetiredVolumeDisposition{"retired-volume": core.RetiredVolumePurgeMetadataOnly},
			wantRemoved:  1,
		},
		{
			name:         "keep",
			dispositions: map[string]core.RetiredVolumeDisposition{"retired-volume": core.RetiredVolumeKeep},
			wantRemoved:  0,
		},
		{
			name:         "unlisted",
			dispositions: nil,
			wantRemoved:  0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tenants := newStubTenantQuotaManager()
			dirs := &stubDirectoryQuotaManager{}

			service, repo, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
				opts.TenantQuotaManager = tenants
				opts.DirectoryQuotaManager = dirs
				opts.RetiredVolumes = test.dispositions
			})

			old := time.Now().Add(-48 * time.Hour)
			record := createTestFileMetadata("completed-retired", core.FileStatusCompleted)
			record.VolumeID = "retired-volume"
			record.CompletedAt = &old
			record.DirectoryPath = "/dir"
			if err := repo.AddOrUpdate(ctx, record); err != nil {
				t.Fatalf("AddOrUpdate() error = %v", err)
			}
			if err := dirs.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
				t.Fatalf("IncrementFileCount(dir) error = %v", err)
			}
			if err := tenants.IncrementFileCount(ctx, "test-tenant"); err != nil {
				t.Fatalf("IncrementFileCount(tenant) error = %v", err)
			}

			stats, err := service.CleanupCompletedFiles(ctx, time.Hour)
			if err != nil {
				t.Fatalf("CleanupCompletedFiles() error = %v", err)
			}
			if stats.CompletedRecordsRemoved != test.wantRemoved {
				t.Errorf("CompletedRecordsRemoved = %d, want %d", stats.CompletedRecordsRemoved, test.wantRemoved)
			}
			if _, getErr := repo.Get(ctx, "test-tenant", record.FileKey); errors.Is(getErr, core.ErrFileNotFound) == (test.wantRemoved == 0) {
				t.Errorf("metadata Get() error = %v, wantRemoved = %d", getErr, test.wantRemoved)
			}
			wantDecrements := test.wantRemoved
			if decrements, _ := tenants.snapshot(); decrements != wantDecrements {
				t.Errorf("tenant quota decrements = %d, want %d", decrements, wantDecrements)
			}
			if dirDecrements, _ := dirs.Snapshot(); dirDecrements != wantDecrements {
				t.Errorf("directory quota decrements = %d, want %d", dirDecrements, wantDecrements)
			}
		})
	}
}

// TestCleanupOrphanedMetadata_RetiredVolumePolicy verifies the retired-volume
// policy in the orphan sweep, where the physical file can no longer be checked
// because the volume is gone.
func TestCleanupOrphanedMetadata_RetiredVolumePolicy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		dispositions map[string]core.RetiredVolumeDisposition
		wantRemoved  int
	}{
		{
			name:         "purge metadata only",
			dispositions: map[string]core.RetiredVolumeDisposition{"retired-volume": core.RetiredVolumePurgeMetadataOnly},
			wantRemoved:  1,
		},
		{
			name:         "keep",
			dispositions: map[string]core.RetiredVolumeDisposition{"retired-volume": core.RetiredVolumeKeep},
			wantRemoved:  0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tenants := newStubTenantQuotaManager()
			dirs := &stubDirectoryQuotaManager{}

			service, repo, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
				opts.TenantQuotaManager = tenants
				opts.DirectoryQuotaManager = dirs
				opts.RetiredVolumes = test.dispositions
			})

			record := createTestFileMetadata("orphan-retired", core.FileStatusPending)
			record.VolumeID = "retired-volume"
			record.PhysicalPath = "tenant-001/orphan-retired.txt"
			record.DirectoryPath = "/dir"
			if err := repo.AddOrUpdate(ctx, record); err != nil {
				t.Fatalf("AddOrUpdate() error = %v", err)
			}
			if err := dirs.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
				t.Fatalf("IncrementFileCount(dir) error = %v", err)
			}
			if err := tenants.IncrementFileCount(ctx, "test-tenant"); err != nil {
				t.Fatalf("IncrementFileCount(tenant) error = %v", err)
			}

			stats, err := service.CleanupOrphanedMetadata(ctx)
			if err != nil {
				t.Fatalf("CleanupOrphanedMetadata() error = %v", err)
			}
			if stats.OrphanedMetadataRemoved != test.wantRemoved {
				t.Errorf("OrphanedMetadataRemoved = %d, want %d", stats.OrphanedMetadataRemoved, test.wantRemoved)
			}
			_, getErr := repo.Get(ctx, "test-tenant", record.FileKey)
			if test.wantRemoved == 1 && !errors.Is(getErr, core.ErrFileNotFound) {
				t.Errorf("metadata Get() error = %v, want ErrFileNotFound", getErr)
			}
			if test.wantRemoved == 0 && getErr != nil {
				t.Errorf("metadata must be retained, Get() error = %v", getErr)
			}
			if decrements, _ := tenants.snapshot(); decrements != test.wantRemoved {
				t.Errorf("tenant quota decrements = %d, want %d", decrements, test.wantRemoved)
			}
			if dirDecrements, _ := dirs.Snapshot(); dirDecrements != test.wantRemoved {
				t.Errorf("directory quota decrements = %d, want %d", dirDecrements, test.wantRemoved)
			}
		})
	}
}

// TestCleanupCompletedFiles_RetiredVolumeDeleteFailureKeepsCounting verifies the
// accounting stays paired with a metadata row that is really gone: a failed
// delete releases no quota count.
func TestCleanupCompletedFiles_RetiredVolumeDeleteFailureKeepsCounting(t *testing.T) {
	ctx := context.Background()

	tenants := newStubTenantQuotaManager()
	dirs := &stubDirectoryQuotaManager{}

	service, repo, _ := newRegressionCleanupService(t, []string{"test-tenant"}, func(opts *CleanupServiceOptions) {
		opts.TenantQuotaManager = tenants
		opts.DirectoryQuotaManager = dirs
		opts.RetiredVolumes = map[string]core.RetiredVolumeDisposition{
			"retired-volume": core.RetiredVolumePurgeMetadataOnly,
		}
	})

	old := time.Now().Add(-48 * time.Hour)
	record := createTestFileMetadata("completed-failing-delete", core.FileStatusCompleted)
	record.VolumeID = "retired-volume"
	record.CompletedAt = &old
	record.DirectoryPath = "/dir"
	if err := repo.AddOrUpdate(ctx, record); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}
	if err := dirs.IncrementFileCount(ctx, "test-tenant", "/dir"); err != nil {
		t.Fatalf("IncrementFileCount(dir) error = %v", err)
	}
	if err := tenants.IncrementFileCount(ctx, "test-tenant"); err != nil {
		t.Fatalf("IncrementFileCount(tenant) error = %v", err)
	}

	replaceMetadataRepository(t, service, &failingDeleteRepository{
		MetadataRepository: repo,
		deleteErr:          errors.New("metadata store unavailable"),
	})

	stats, err := service.CleanupCompletedFiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("CleanupCompletedFiles() error = %v", err)
	}
	if stats.CompletedRecordsRemoved != 0 {
		t.Errorf("CompletedRecordsRemoved = %d, want 0", stats.CompletedRecordsRemoved)
	}
	if decrements, _ := tenants.snapshot(); decrements != 0 {
		t.Errorf("tenant quota decrements = %d, want 0 (no metadata row was removed)", decrements)
	}
	if dirDecrements, _ := dirs.Snapshot(); dirDecrements != 0 {
		t.Errorf("directory quota decrements = %d, want 0 (no metadata row was removed)", dirDecrements)
	}
}
