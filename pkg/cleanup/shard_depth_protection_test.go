package cleanup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/volume"
)

// shardChainSegments is a chain of five two-character lowercase hexadecimal
// segments, the exact shape a sharded file key produces. Five levels cover every
// supported sharding depth (0-3) plus one level below the deepest chain.
var shardChainSegments = []string{"aa", "bb", "cc", "dd", "ee"}

// plainVolume hides an optional capability of the wrapped volume. Embedding the
// core.StorageVolume interface promotes only that interface's methods, so a
// *plainVolume no longer satisfies core.ShardingDepthProvider even when the
// wrapped volume does. It models a third-party volume implementation.
type plainVolume struct {
	core.StorageVolume
}

// newShardDepthCleanupService builds a cleanup service over real volumes whose
// configured sharding depth is fixed, plus a fixed tenant set.
//
// Every mount is created with t.TempDir() when the caller does not provide one.
// The returned map is keyed "vol-1", "vol-2", ... in mount order.
func newShardDepthCleanupService(
	t *testing.T,
	shardDepth int,
	manager core.TenantManager,
	mounts ...string,
) (core.CleanupService, map[string]core.StorageVolume) {
	t.Helper()

	if len(mounts) == 0 {
		mounts = []string{t.TempDir()}
	}

	volumes := make(map[string]core.StorageVolume, len(mounts))
	for index, mount := range mounts {
		volumeID := "vol-" + string(rune('1'+index))
		vol, err := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
			VolumeID:   volumeID,
			MountPath:  mount,
			ShardDepth: shardDepth,
		})
		if err != nil {
			t.Fatalf("NewLocalFileSystemVolume(%s) error = %v", volumeID, err)
		}
		volumes[volumeID] = vol
	}

	repo := &stubMetadataRepository{}
	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}

	service, err := NewCleanupService(&CleanupServiceOptions{
		TenantManager:      manager,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	})
	if err != nil {
		t.Fatalf("NewCleanupService() error = %v", err)
	}

	return service, volumes
}

// TestCleanupEmptyDirectories_UsesReportedShardingDepth verifies that a volume
// which reports its configured sharding depth has exactly that many shard
// levels protected below the tenant directory, and that everything below the
// shard chain is still removable. The structural heuristic must not widen the
// protected set beyond the reported depth.
func TestCleanupEmptyDirectories_UsesReportedShardingDepth(t *testing.T) {
	const tenantID = "tenant-001"

	for depth := 0; depth <= 3; depth++ {
		t.Run("depth "+string(rune('0'+depth)), func(t *testing.T) {
			service, volumes := newShardDepthCleanupService(t, depth, newMultiTenantManager(tenantID))
			mount := volumes["vol-1"].MountPath()

			chainRoot := filepath.Join(append([]string{mount, tenantID}, shardChainSegments...)...)
			if err := os.MkdirAll(chainRoot, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s) error = %v", chainRoot, err)
			}

			if _, err := service.CleanupEmptyDirectories(context.Background()); err != nil {
				t.Fatalf("CleanupEmptyDirectories() error = %v", err)
			}

			if _, err := os.Stat(filepath.Join(mount, tenantID)); err != nil {
				t.Fatalf("tenant directory must remain, stat error = %v", err)
			}

			for level := 1; level <= len(shardChainSegments); level++ {
				path := filepath.Join(append([]string{mount, tenantID}, shardChainSegments[:level]...)...)
				_, statErr := os.Stat(path)
				switch {
				case level <= depth:
					if statErr != nil {
						t.Errorf("depth %d: expected shard directory %s to be protected, stat error = %v", depth, path, statErr)
					}
				default:
					if !os.IsNotExist(statErr) {
						t.Errorf("depth %d: expected non-shard directory %s to be removed, stat error = %v", depth, path, statErr)
					}
				}
			}
		})
	}
}

// TestCleanupEmptyDirectories_FallsBackToStructuralShardProtection verifies
// that a volume which does not report its sharding depth keeps the structural
// heuristic, so shard-looking directories stay protected.
func TestCleanupEmptyDirectories_FallsBackToStructuralShardDetection(t *testing.T) {
	const tenantID = "tenant-001"

	manager := newMultiTenantManager(tenantID)
	mount := t.TempDir()

	realVolume, err := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
		VolumeID:  "vol-1",
		MountPath: mount,
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume() error = %v", err)
	}

	// The wrapper hides core.ShardingDepthProvider, so cleanup must fall back to
	// the structural heuristic.
	volumes := map[string]core.StorageVolume{"vol-1": &plainVolume{StorageVolume: realVolume}}

	repo := &stubMetadataRepository{}
	sched, err := scheduler.NewFileScheduler(repo, volumes, nil)
	if err != nil {
		t.Fatalf("NewFileScheduler() error = %v", err)
	}
	fallbackService, err := NewCleanupService(&CleanupServiceOptions{
		TenantManager:      manager,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	})
	if err != nil {
		t.Fatalf("NewCleanupService() error = %v", err)
	}

	shardRoot := filepath.Join(mount, tenantID, "aa", "bb")
	customEmpty := filepath.Join(mount, tenantID, "custom-empty")
	for _, path := range []string{shardRoot, customEmpty} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", path, err)
		}
	}

	if _, err := fallbackService.CleanupEmptyDirectories(context.Background()); err != nil {
		t.Fatalf("CleanupEmptyDirectories() error = %v", err)
	}

	if _, err := os.Stat(shardRoot); err != nil {
		t.Errorf("expected structural shard hierarchy %s to be protected, stat error = %v", shardRoot, err)
	}
	if _, err := os.Stat(customEmpty); !os.IsNotExist(err) {
		t.Errorf("expected non-shard empty directory %s to be removed, stat error = %v", customEmpty, err)
	}
}
