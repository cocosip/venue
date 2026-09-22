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

// newStartupTestConfig builds a lifecycle configuration rooted at root so a test
// can damage the metadata database between two constructions.
func newStartupTestConfig(root string) *config.Config {
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

// corruptMetadataDatabase damages the manifest of the shared metadata database so
// the next open must quarantine it.
func corruptMetadataDatabase(t *testing.T, root string) {
	t.Helper()

	metadataDatabaseDir := filepath.Join(root, "metadata", "shared", "metadata")
	if _, err := os.Stat(metadataDatabaseDir); err != nil {
		t.Fatalf("expected a metadata database at %s: %v", metadataDatabaseDir, err)
	}
	if err := os.WriteFile(filepath.Join(metadataDatabaseDir, "MANIFEST"), []byte("not a manifest"), 0o600); err != nil {
		t.Fatalf("corrupt MANIFEST: %v", err)
	}
}

// TestFailFastOnStartupRecoveryFailureThroughVenue covers the W5.10 wiring: a
// quarantine that no backup can repair fails startup only when the operator asked
// for fail-fast behaviour, and is otherwise reported as a degraded runtime.
func TestFailFastOnStartupRecoveryFailureThroughVenue(t *testing.T) {
	root := t.TempDir()

	first, err := venue.NewVenue(newStartupTestConfig(root))
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	corruptMetadataDatabase(t, root)

	degradedConfig := newStartupTestConfig(root)
	degradedConfig.BadgerDB.RecoverCorruptedDatabase = true
	degradedConfig.FailFastOnStartupRecoveryFailure = false

	degraded, err := venue.NewVenue(degradedConfig)
	if err != nil {
		t.Fatalf("NewVenue() with recovery enabled error = %v, want a degraded but running runtime", err)
	}
	if err := degraded.Stop(); err != nil {
		t.Fatalf("degraded Stop() error = %v", err)
	}
	if degraded.IsRunning() {
		t.Error("IsRunning() = true after Stop")
	}

	// A second corruption is still present in the quarantined copy, so the
	// fail-fast configuration must refuse to start on the same fixture.
	corruptMetadataDatabase(t, root)

	failFastConfig := newStartupTestConfig(root)
	failFastConfig.BadgerDB.RecoverCorruptedDatabase = true
	failFastConfig.FailFastOnStartupRecoveryFailure = true

	if _, err := venue.NewVenue(failFastConfig); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("NewVenue() with fail-fast error = %v, want core.ErrDatabaseError", err)
	}
}

// TestFailFastDoesNotTriggerWhenABackupWasRestored proves the two recovery
// features compose: a quarantine repaired from a backup is a healthy start, not
// a failure, even with fail-fast enabled.
func TestFailFastDoesNotTriggerWhenABackupWasRestored(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backupDir := filepath.Join(root, "backups")

	backupConfig := newStartupTestConfig(root)
	backupConfig.BadgerDB.BackupDirectory = backupDir
	backupConfig.BadgerDB.BackupInterval = time.Hour
	backupConfig.BadgerDB.BackupRetention = 24 * time.Hour

	first, err := venue.NewVenue(backupConfig)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	tenant := mustTenant(t, first, "recovery-tenant")
	fileKey, err := first.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	backupService := first.MetadataBackupService()
	if backupService == nil {
		t.Fatal("MetadataBackupService() = nil, want a configured runner")
	}
	if err := backupService.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	corruptMetadataDatabase(t, root)

	restoreConfig := newStartupTestConfig(root)
	restoreConfig.BadgerDB.BackupDirectory = backupDir
	restoreConfig.BadgerDB.BackupInterval = time.Hour
	restoreConfig.BadgerDB.BackupRetention = 24 * time.Hour
	restoreConfig.BadgerDB.RecoverCorruptedDatabase = true
	restoreConfig.BadgerDB.AutoRestoreFromBackup = true
	restoreConfig.FailFastOnStartupRecoveryFailure = true

	restored, err := venue.NewVenue(restoreConfig)
	if err != nil {
		t.Fatalf("NewVenue() with a restorable backup error = %v, want a successful recovery", err)
	}
	t.Cleanup(func() { _ = restored.Stop() })

	restoredTenant := mustTenant(t, restored, "recovery-tenant")
	info, err := restored.StoragePool().GetFileInfo(ctx, restoredTenant, fileKey)
	if err != nil {
		t.Fatalf("GetFileInfo() after backup restore error = %v", err)
	}
	if info.Status != core.FileStatusPending {
		t.Errorf("restored status = %v, want %v", info.Status, core.FileStatusPending)
	}
}

// TestEmptyQueueReclaimBatchSizeIsConfigurableThroughVenue covers the W5.2
// wiring gap: the configured batch size must reach the scheduler, including the
// documented "negative disables the synchronous pass" behaviour.
func TestEmptyQueueReclaimBatchSizeIsConfigurableThroughVenue(t *testing.T) {
	tests := []struct {
		name        string
		batchSize   int
		wantReclaim bool
	}{
		{name: "negative disables immediate reclaim", batchSize: -1, wantReclaim: false},
		{name: "positive batch size keeps reclaim", batchSize: 4, wantReclaim: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := newStartupTestConfig(root)
			// A 1ms processing timeout makes a claimed file immediately eligible
			// for timed-out recovery without waiting for the cleanup cycle. The
			// background pass is switched off so the configured batch size is the
			// only path that can recover the record; the background path has its
			// own test.
			cfg.Cleanup.ProcessingTimeout = time.Millisecond
			cfg.Cleanup.RecoverTimedOutOnEmptyQueue = true
			cfg.Cleanup.EnableBackgroundTimedOutReclaim = false
			cfg.Cleanup.EmptyQueueReclaimBatchSize = test.batchSize

			runtime, err := venue.NewVenue(cfg)
			if err != nil {
				t.Fatalf("NewVenue() error = %v", err)
			}
			t.Cleanup(func() { _ = runtime.Stop() })

			ctx := context.Background()
			tenant := mustTenant(t, runtime, "tenant-1")
			fileKey, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil)
			if err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			// A worker claims the file and dies without releasing it.
			claimed, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("first claim error = %v", err)
			}
			if claimed == nil || claimed.FileKey != fileKey {
				t.Fatalf("first claim = %#v, want file %s", claimed, fileKey)
			}

			time.Sleep(20 * time.Millisecond)

			second, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
			if err != nil {
				t.Fatalf("second claim error = %v", err)
			}
			if test.wantReclaim {
				if second == nil || second.FileKey != fileKey {
					t.Fatalf("second claim = %#v, want the timed-out file to be reclaimed", second)
				}
				return
			}
			if second != nil {
				t.Fatalf("second claim = %#v, want nil while the batch size disables immediate reclaim", second)
			}
		})
	}
}

// TestBackgroundTimedOutReclaimThroughVenue covers the configuration wiring of
// the opportunistic background pass: with the synchronous empty-queue path
// switched off, a successful claim must still recover an abandoned record, so a
// deployment with a long cleanup interval keeps its queue moving.
func TestBackgroundTimedOutReclaimThroughVenue(t *testing.T) {
	root := t.TempDir()
	cfg := newStartupTestConfig(root)
	cfg.Cleanup.ProcessingTimeout = time.Millisecond
	cfg.Cleanup.RecoverTimedOutOnEmptyQueue = false
	cfg.Cleanup.EnableBackgroundTimedOutReclaim = true
	cfg.Cleanup.BackgroundTimedOutReclaimBatchSize = 8
	// A short cooldown keeps the test fast while still exercising the gate: the
	// pass triggered by the first claim runs before the record times out, so the
	// recovery must come from a later pass.
	cfg.Cleanup.TimedOutReclaimCooldown = 30 * time.Millisecond

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Stop() })

	ctx := context.Background()
	tenant := mustTenant(t, runtime, "tenant-1")

	abandoned, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("abandoned"), nil)
	if err != nil {
		t.Fatalf("WriteFile(abandoned) error = %v", err)
	}
	claimed, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("first claim error = %v", err)
	}
	if claimed == nil || claimed.FileKey != abandoned {
		t.Fatalf("first claim = %#v, want the abandoned file %s", claimed, abandoned)
	}

	// Wait until the abandoned record exceeds the processing timeout and the
	// cooldown of the pass triggered above has expired, then claim a second file:
	// that successful claim starts the pass which must recover the record.
	time.Sleep(60 * time.Millisecond)
	if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("fresh"), nil); err != nil {
		t.Fatalf("WriteFile(fresh) error = %v", err)
	}
	if fresh, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant); err != nil {
		t.Fatalf("fresh claim error = %v", err)
	} else if fresh == nil {
		t.Fatal("fresh claim returned no file")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		recovered, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
		if err != nil {
			t.Fatalf("recovery claim error = %v", err)
		}
		if recovered != nil && recovered.FileKey == abandoned {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("the abandoned file %s was not recovered by the background pass within 5s", abandoned)
}
