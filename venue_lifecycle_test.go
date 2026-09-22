package venue_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
)

func newLifecycleConfig(t *testing.T) *config.Config {
	t.Helper()
	root := t.TempDir()
	return config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2))
}

// TestStopWithoutStartReleasesRepositories is the regression test for the
// resource leak: Venue opens its repositories in NewVenue, so Stop must release
// them even when Start was never called.
func TestStopWithoutStartReleasesRepositories(t *testing.T) {
	cfg := newLifecycleConfig(t)

	first, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("first NewVenue() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop() without Start() error = %v, want nil", err)
	}

	// If the metadata database lock had leaked, this open would fail.
	second, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("second NewVenue() error = %v (repositories were not released)", err)
	}
	if err := second.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
}

// TestStopIsIdempotent guards the documented deferred-shutdown contract.
func TestStopIsIdempotent(t *testing.T) {
	cfg := newLifecycleConfig(t)

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.Stop(); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}
	if runtime.IsRunning() {
		t.Fatal("IsRunning() = true after Stop")
	}
	if err := runtime.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v, want nil", err)
	}
	if err := runtime.Start(); err == nil {
		t.Fatal("Start() after Stop() error = nil, want error")
	}
}

// TestNewVenueFailureReleasesOpenedRepositories covers the construction-failure
// path: the metadata repository is opened before volume validation fails, and it
// must not strand the database lock.
func TestNewVenueFailureReleasesOpenedRepositories(t *testing.T) {
	cfg := newLifecycleConfig(t)
	cfg.Volumes = []config.VolumeConfig{{
		VolumeID:   "primary",
		MountPath:  filepath.Join(t.TempDir(), "storage"),
		VolumeType: "UnsupportedBackend",
	}}

	if _, err := venue.NewVenue(cfg); err == nil {
		t.Fatal("NewVenue() error = nil, want unsupported volume type error")
	}

	// The failed construction must have released the metadata database.
	cfg.Volumes[0].VolumeType = "LocalFileSystem"
	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() after failed construction error = %v (database lock leaked)", err)
	}
	if err := runtime.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

// TestFailedFileRetentionPeriodIsWiredToBackgroundCleanup proves the documented
// Cleanup.FailedFileRetentionPeriod and Cleanup.PermanentlyFailedDisposition
// options reach the background cleanup service: with Delete and a 1h retention
// the record is reaped, with 24h it survives, and with MoveToDeadLetter the
// payload moves under the volume's dead-letter root.
func TestFailedFileRetentionPeriodIsWiredToBackgroundCleanup(t *testing.T) {
	tests := []struct {
		name             string
		retention        time.Duration
		disposition      string
		wantGone         bool
		wantDeadLettered bool
	}{
		{name: "one hour retention with Delete reaps two hour old failure", retention: time.Hour, disposition: "Delete", wantGone: true},
		{name: "one day retention keeps two hour old failure", retention: 24 * time.Hour, disposition: "Delete", wantGone: false},
		{name: "one hour retention dead-letters two hour old failure", retention: time.Hour, disposition: "MoveToDeadLetter", wantDeadLettered: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			volumeRoot := filepath.Join(root, "storage")
			cfg := config.New().
				WithMetadataDirectory(filepath.Join(root, "metadata")).
				WithQuotaDirectory(filepath.Join(root, "quota")).
				WithDatabaseHealthCheckEnabled(false).
				WithBackgroundCleanupEnabled(true).
				WithVolumes(config.NewVolumeConfig().
					WithVolumeID("primary").
					WithMountPath(volumeRoot).
					WithShardingDepth(2))
			cfg.Cleanup.CleanupInterval = 20 * time.Millisecond
			cfg.Cleanup.InitialDelay = time.Millisecond
			cfg.Cleanup.CleanupEmptyDirectories = false
			cfg.Cleanup.CleanupTimedOutFiles = false
			cfg.Cleanup.CleanupCompletedRecords = false
			cfg.Cleanup.OptimizeDatabases = false
			cfg.Cleanup.CleanupPermanentlyFailedFiles = true
			cfg.Cleanup.FailedFileRetentionPeriod = test.retention
			cfg.Cleanup.PermanentlyFailedDisposition = test.disposition

			runtime, err := venue.NewVenue(cfg)
			if err != nil {
				t.Fatalf("NewVenue() error = %v", err)
			}
			if err := runtime.Start(); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() { _ = runtime.Stop() })

			ctx := context.Background()
			// Cleanup enumerates known tenants, so the tenant must exist.
			if err := runtime.TenantManager().CreateTenant(ctx, "tenant-1"); err != nil {
				t.Fatalf("CreateTenant() error = %v", err)
			}
			failedAt := time.Now().Add(-2 * time.Hour)
			const fileKey = "ffffffffffffffffffffffffffffffff"
			relativePath := "tenant-1/ff/ff/" + fileKey + ".txt"
			// The payload must exist for the dead-letter disposition to move it.
			payloadPath := filepath.Join(volumeRoot, filepath.FromSlash(relativePath))
			if err := writeFileForTest(payloadPath, "failed payload"); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := runtime.MetadataRepository().AddOrUpdate(ctx, &core.FileMetadata{
				FileKey:      fileKey,
				TenantID:     "tenant-1",
				VolumeID:     "primary",
				PhysicalPath: relativePath,
				FileSize:     int64(len("failed payload")),
				Status:       core.FileStatusPermanentlyFailed,
				RetryCount:   3,
				LastFailedAt: &failedAt,
				CreatedAt:    failedAt,
				UpdatedAt:    failedAt,
			}); err != nil {
				t.Fatalf("AddOrUpdate() error = %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				record, err := runtime.MetadataRepository().Get(ctx, "tenant-1", fileKey)
				gone := errors.Is(err, core.ErrFileNotFound)
				if !test.wantGone && !test.wantDeadLettered && gone {
					t.Fatalf("record was removed although the disposition kept it (err = %v)", err)
				}
				switch {
				case test.wantGone && gone:
					if _, statErr := os.Stat(payloadPath); statErr == nil {
						t.Fatal("payload still exists after Delete disposition")
					}
					return
				case test.wantDeadLettered && record != nil && record.Status == core.FileStatusDeadLettered:
					if record.DeadLetteredAt == nil {
						t.Fatal("DeadLetteredAt is nil for a dead-lettered record")
					}
					// Stored paths use forward slashes on every platform.
					if !strings.HasPrefix(record.PhysicalPath, ".deadletter/") {
						t.Fatalf("PhysicalPath = %q, want a path under .deadletter", record.PhysicalPath)
					}
					deadLetterPath := filepath.Join(volumeRoot, filepath.FromSlash(record.PhysicalPath))
					if _, statErr := os.Stat(deadLetterPath); statErr != nil {
						t.Fatalf("dead-letter payload missing at %s: %v", deadLetterPath, statErr)
					}
					if _, statErr := os.Stat(payloadPath); statErr == nil {
						t.Fatal("payload still exists at its original path after dead-lettering")
					}
					return
				case !test.wantGone && !test.wantDeadLettered && record != nil:
					// The disposition kept the record; it stays PermanentlyFailed.
					if record.Status != core.FileStatusPermanentlyFailed {
						t.Fatalf("status = %v, want PermanentlyFailed", record.Status)
					}
					if _, statErr := os.Stat(payloadPath); statErr != nil {
						t.Fatalf("payload missing although the disposition kept it: %v", statErr)
					}
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("after cleanup cycles: gone = %v, record = %+v (err = %v)", gone, record, err)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// TestTenantTraversalRejectedThroughPublicAPI asserts the P0 path-traversal fix
// through the public entry point.
func TestTenantTraversalRejectedThroughPublicAPI(t *testing.T) {
	runtime, err := venue.NewVenue(newLifecycleConfig(t))
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	ctx := context.Background()
	for _, id := range []string{"../../evil", `..\..\evil`, "sub/nested", "..", ".locus"} {
		if _, err := runtime.TenantManager().GetTenant(ctx, id); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("GetTenant(%q) error = %v, want ErrInvalidArgument", id, err)
		}
		if err := runtime.TenantManager().CreateTenant(ctx, id); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("CreateTenant(%q) error = %v, want ErrInvalidArgument", id, err)
		}
	}

	// A valid tenant still works end to end.
	tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetTenant(tenant-1) error = %v", err)
	}
	if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

// TestOrphanRecoveryIsOptIn checks the new recovery service is absent unless
// explicitly enabled.
func TestOrphanRecoveryIsOptIn(t *testing.T) {
	cfg := newLifecycleConfig(t)
	disabled, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if disabled.OrphanRecovery() != nil {
		t.Error("OrphanRecovery() != nil while disabled")
	}
	if err := disabled.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	cfg.OrphanRecovery.Enabled = true
	cfg.OrphanRecovery.RunOnStartup = true
	cfg.OrphanRecovery.InitialDelay = time.Millisecond

	enabled, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() with orphan recovery error = %v", err)
	}
	if enabled.OrphanRecovery() == nil {
		t.Fatal("OrphanRecovery() = nil while enabled")
	}
	if err := enabled.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !enabled.OrphanRecovery().IsRunning() {
		t.Error("OrphanRecovery().IsRunning() = false after Start")
	}
	if err := enabled.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

// TestOrphanRecoveryRequeuesStrayFileThroughVenue covers the end-to-end recovery
// path: a physical file with no metadata re-enters the queue.
func TestOrphanRecoveryRequeuesStrayFileThroughVenue(t *testing.T) {
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "storage")
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(volumeRoot).
			WithShardingDepth(2)).
		WithOrphanRecovery(config.NewOrphanRecoveryConfig().
			WithEnabled(true).
			WithRecoveryInterval(time.Hour))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	const fileKey = "0123456789abcdef0123456789abcdef"
	strayRelative := filepath.Join("tenant-1", "01", "23", fileKey+".txt")
	strayPath := filepath.Join(volumeRoot, strayRelative)
	if err := writeFileForTest(strayPath, "orphaned payload"); err != nil {
		t.Fatalf("write stray file: %v", err)
	}

	ctx := context.Background()
	report, err := runtime.OrphanRecovery().RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 1 {
		t.Fatalf("FilesRecovered = %d, want 1 (%#v)", report.FilesRecovered, report)
	}

	location, err := runtime.StoragePool().GetNextFileForProcessing(ctx, mustTenant(t, runtime, "tenant-1"))
	if err != nil {
		t.Fatalf("GetNextFileForProcessing() error = %v", err)
	}
	if location == nil || location.FileKey != fileKey {
		t.Fatalf("claimed file = %#v, want recovered file %s", location, fileKey)
	}
}

// TestReconcileQuotaCountsRepairsDrift covers the runtime repair API: counters
// that drifted away from persisted metadata are restored without a restart.
func TestReconcileQuotaCountsRepairsDrift(t *testing.T) {
	runtime, err := venue.NewVenue(newLifecycleConfig(t))
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	ctx := context.Background()
	if err := runtime.TenantManager().CreateTenant(ctx, "tenant-1"); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	tenant := mustTenant(t, runtime, "tenant-1")

	for _, name := range []string{"a.txt", "b.txt"} {
		originalName := name
		if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader(name), &originalName); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	if _, err := runtime.StoragePool().WriteFileToDirectory(
		ctx, tenant, strings.NewReader("c"), nil, "docs/2026",
	); err != nil {
		t.Fatalf("WriteFileToDirectory() error = %v", err)
	}

	// Simulate drift: both counters are wrong.
	if err := runtime.TenantQuotaManager().SetFileCount(ctx, "tenant-1", 42); err != nil {
		t.Fatalf("SetFileCount(tenant) error = %v", err)
	}
	if err := runtime.DirectoryQuotaManager().SetFileCount(ctx, "tenant-1", "/", 42); err != nil {
		t.Fatalf("SetFileCount(directory) error = %v", err)
	}
	if err := runtime.DirectoryQuotaManager().SetFileCount(ctx, "tenant-1", "/docs/2026", 42); err != nil {
		t.Fatalf("SetFileCount(docs) error = %v", err)
	}

	if err := runtime.ReconcileQuotaCounts(ctx); err != nil {
		t.Fatalf("ReconcileQuotaCounts() error = %v", err)
	}

	tenantCount, err := runtime.TenantQuotaManager().GetFileCount(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetFileCount(tenant) error = %v", err)
	}
	if tenantCount != 3 {
		t.Errorf("tenant count = %d, want 3", tenantCount)
	}

	rootCount, err := runtime.DirectoryQuotaManager().GetFileCount(ctx, "tenant-1", "/")
	if err != nil {
		t.Fatalf("GetFileCount(/) error = %v", err)
	}
	if rootCount != 2 {
		t.Errorf("root directory count = %d, want 2", rootCount)
	}

	docsCount, err := runtime.DirectoryQuotaManager().GetFileCount(ctx, "tenant-1", "/docs/2026")
	if err != nil {
		t.Fatalf("GetFileCount(/docs/2026) error = %v", err)
	}
	if docsCount != 1 {
		t.Errorf("docs directory count = %d, want 1", docsCount)
	}

	// Reconciliation is idempotent.
	if err := runtime.ReconcileQuotaCounts(ctx); err != nil {
		t.Fatalf("second ReconcileQuotaCounts() error = %v", err)
	}
	if tenantCount, err = runtime.TenantQuotaManager().GetFileCount(ctx, "tenant-1"); err != nil || tenantCount != 3 {
		t.Errorf("tenant count after second reconcile = %d (err = %v), want 3", tenantCount, err)
	}
}

// TestCorruptedDatabaseRecoveryIsOptIn covers the destructive repair path end to
// end: a database that cannot be opened fails construction by default, and with
// the option enabled it is quarantined, recreated, and usable.
func TestCorruptedDatabaseRecoveryIsOptIn(t *testing.T) {
	root := t.TempDir()
	baseConfig := func() *config.Config {
		return config.New().
			WithMetadataDirectory(filepath.Join(root, "metadata")).
			WithQuotaDirectory(filepath.Join(root, "quota")).
			WithDatabaseHealthCheckEnabled(false).
			WithBackgroundCleanupEnabled(false).
			WithVolumes(config.NewVolumeConfig().
				WithVolumeID("primary").
				WithMountPath(filepath.Join(root, "storage")).
				WithShardingDepth(2))
	}

	// Create a healthy instance, then close it and damage its metadata database.
	first, err := venue.NewVenue(baseConfig())
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	metadataDatabaseDir := filepath.Join(root, "metadata", "shared", "metadata")
	if _, err := os.Stat(metadataDatabaseDir); err != nil {
		t.Fatalf("expected a metadata database at %s: %v", metadataDatabaseDir, err)
	}
	if err := os.WriteFile(filepath.Join(metadataDatabaseDir, "MANIFEST"), []byte("not a manifest"), 0o600); err != nil {
		t.Fatalf("corrupt MANIFEST: %v", err)
	}

	// Default: recovery is off, so construction must fail loudly.
	if _, err := venue.NewVenue(baseConfig()); err == nil {
		t.Fatal("NewVenue() error = nil for a corrupted database with recovery disabled")
	}

	// Opt in: the database is quarantined and recreated.
	recoveryConfig := baseConfig()
	recoveryConfig.BadgerDB.RecoverCorruptedDatabase = true
	runtime, err := venue.NewVenue(recoveryConfig)
	if err != nil {
		t.Fatalf("NewVenue() with recovery enabled error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	quarantined, err := filepath.Glob(metadataDatabaseDir + ".corrupted.*")
	if err != nil {
		t.Fatalf("glob quarantine directories: %v", err)
	}
	if len(quarantined) == 0 {
		t.Fatal("no quarantined database directory was created")
	}

	// The recreated database must be fully usable.
	ctx := context.Background()
	if err := runtime.TenantManager().CreateTenant(ctx, "tenant-1"); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	tenant := mustTenant(t, runtime, "tenant-1")
	if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil); err != nil {
		t.Fatalf("WriteFile() after recovery error = %v", err)
	}
}

// TestEmptyQueueReclaimWiredThroughVenue guards the configuration wiring for
// immediate timed-out reclaim, including the batch size that has no config
// field: a zero value there would silently disable the feature.
func TestEmptyQueueReclaimWiredThroughVenue(t *testing.T) {
	tests := []struct {
		name      string
		enabled   bool
		wantClaim bool
	}{
		{name: "reclaim enabled", enabled: true, wantClaim: true},
		{name: "reclaim disabled", enabled: false, wantClaim: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := config.New().
				WithMetadataDirectory(filepath.Join(root, "metadata")).
				WithQuotaDirectory(filepath.Join(root, "quota")).
				WithDatabaseHealthCheckEnabled(false).
				WithBackgroundCleanupEnabled(false).
				WithVolumes(config.NewVolumeConfig().
					WithVolumeID("primary").
					WithMountPath(filepath.Join(root, "storage")).
					WithShardingDepth(2))
			// A 1ms processing timeout makes a claimed file immediately eligible
			// for timed-out recovery without waiting for the cleanup cycle.
			cfg.Cleanup.ProcessingTimeout = time.Millisecond
			cfg.Cleanup.RecoverTimedOutOnEmptyQueue = test.enabled

			runtime, err := venue.NewVenue(cfg)
			if err != nil {
				t.Fatalf("NewVenue() error = %v", err)
			}
			t.Cleanup(func() { _ = runtime.Stop() })

			ctx := context.Background()
			if err := runtime.TenantManager().CreateTenant(ctx, "tenant-1"); err != nil {
				t.Fatalf("CreateTenant() error = %v", err)
			}
			tenant := mustTenant(t, runtime, "tenant-1")

			originalName := "a.txt"
			fileKey, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), &originalName)
			if err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			// A worker claims the file and then dies without releasing it.
			claimed, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("first claim error = %v", err)
			}
			if claimed == nil || claimed.FileKey != fileKey {
				t.Fatalf("first claim = %#v, want file %s", claimed, fileKey)
			}

			time.Sleep(20 * time.Millisecond)

			recovered, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("second claim error = %v", err)
			}
			switch {
			case test.wantClaim:
				if recovered == nil || recovered.FileKey != fileKey {
					t.Fatalf("second claim = %#v, want the timed-out file %s to be reclaimed immediately", recovered, fileKey)
				}
			default:
				if recovered != nil {
					t.Fatalf("second claim = %#v, want nil while reclaim is disabled", recovered)
				}
			}
		})
	}
}

// TestWatcherRootsDeriveWatchersThroughVenue covers the W2 wiring: configured
// roots derive one watcher per tenant directory, the per-tenant query works, and
// the global watcher service options reach the service.
func TestWatcherRootsDeriveWatchersThroughVenue(t *testing.T) {
	root := t.TempDir()
	watchRoot := filepath.Join(root, "incoming")
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := os.MkdirAll(filepath.Join(watchRoot, tenant), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", tenant, err)
		}
	}

	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithFileWatcherConfigurationDirectory(filepath.Join(root, "watcher-state")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithFileWatcherRoots(config.NewFileWatcherRootConfig(watchRoot))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	if runtime.FileWatcherAutoManager() == nil {
		t.Fatal("FileWatcherAutoManager() = nil with a configured root")
	}
	if runtime.FileWatcherService() == nil {
		t.Fatal("FileWatcherService() = nil with a configured root")
	}

	ctx := context.Background()
	watchers, err := runtime.FileWatcher().GetAllWatchers(ctx)
	if err != nil {
		t.Fatalf("GetAllWatchers() error = %v", err)
	}
	if len(watchers) != 2 {
		t.Fatalf("watchers = %d, want one per tenant directory (%#v)", len(watchers), watchers)
	}
	for _, watcher := range watchers {
		if watcher.WatchPath != filepath.Join(watchRoot, watcher.TenantID) {
			t.Errorf("watcher %s watch path = %q, want %q", watcher.WatcherID, watcher.WatchPath, filepath.Join(watchRoot, watcher.TenantID))
		}
		if watcher.MultiTenantMode {
			t.Errorf("derived watcher %s must be single-tenant", watcher.WatcherID)
		}
	}

	forTenant, err := runtime.FileWatcher().GetWatchersForTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("GetWatchersForTenant() error = %v", err)
	}
	if len(forTenant) != 1 || forTenant[0].TenantID != "tenant-a" {
		t.Fatalf("GetWatchersForTenant(tenant-a) = %#v, want exactly that tenant's watcher", forTenant)
	}

	// The global service options come from configuration.
	options := runtime.FileWatcherService().Options()
	if !options.Enabled {
		t.Error("watcher service Options().Enabled = false, want true")
	}
	if options.MaxParallelWatcherScans != 4 {
		t.Errorf("MaxParallelWatcherScans = %d, want 4", options.MaxParallelWatcherScans)
	}
	if options.MinimumPollingInterval != 5*time.Second || options.MaximumPollingInterval != time.Hour {
		t.Errorf("polling bounds = %v/%v, want 5s/1h", options.MinimumPollingInterval, options.MaximumPollingInterval)
	}
}

// TestWatcherOperationsThroughVenue covers the public watcher surface added in
// W2: updating a watcher in place, and a globally disabled watcher service that
// stays disabled across a restart.
func TestWatcherOperationsThroughVenue(t *testing.T) {
	root := t.TempDir()
	watchRoot := filepath.Join(root, "incoming")
	if err := os.MkdirAll(watchRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithFileWatcherConfigurationDirectory(filepath.Join(root, "watcher-state")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithFileWatchers(config.NewFileWatcherConfig().
			WithWatcherID("w1").
			WithTenantID("tenant-a").
			WithWatchPath(watchRoot))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	ctx := context.Background()

	updated := &core.FileWatcherConfiguration{
		WatcherID:             "w1",
		TenantID:              "tenant-a",
		WatchPath:             watchRoot,
		IncludeSubdirectories: true,
		FilePatterns:          []string{"*.csv"},
		PostImportAction:      core.PostImportActionKeep,
		PollingInterval:       10 * time.Second,
		MinFileAge:            time.Second,
		MaxConcurrentImports:  2,
		Enabled:               true,
	}
	if err := runtime.FileWatcher().UpdateWatcher(ctx, updated); err != nil {
		t.Fatalf("UpdateWatcher() error = %v", err)
	}
	reloaded, err := runtime.FileWatcher().GetWatcher(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWatcher() error = %v", err)
	}
	if reloaded.PollingInterval != 10*time.Second || reloaded.PostImportAction != core.PostImportActionKeep {
		t.Fatalf("updated watcher = %+v, want the new schedule and action", reloaded)
	}

	unknown := *updated
	unknown.WatcherID = "missing"
	if err := runtime.FileWatcher().UpdateWatcher(ctx, &unknown); !errors.Is(err, core.ErrWatcherNotFound) {
		t.Fatalf("UpdateWatcher(unknown) error = %v, want ErrWatcherNotFound", err)
	}

	// A globally disabled watcher service must stay disabled after a restart.
	options := runtime.FileWatcherService().Options()
	options.Enabled = false
	if err := runtime.FileWatcherService().UpdateOptions(ctx, options); err != nil {
		t.Fatalf("UpdateOptions() error = %v", err)
	}
	if err := runtime.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	restarted, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("second NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = restarted.Stop() })
	if restarted.FileWatcherService().IsEnabled() {
		t.Fatal("global watcher disable did not persist across a restart")
	}
}

func mustTenant(t *testing.T, runtime *venue.Venue, tenantID string) core.TenantContext {
	t.Helper()
	tenant, err := runtime.TenantManager().GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("GetTenant(%q) error = %v", tenantID, err)
	}
	return tenant
}

func writeFileForTest(path string, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
