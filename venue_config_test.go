package venue_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
)

func TestNewVenueAcceptsSharedConfig(t *testing.T) {
	acceptSharedConfigConstructor(venue.NewVenue)
}

func acceptSharedConfigConstructor(func(*config.Config) (*venue.Venue, error)) {}

func TestConfiguredVolumeShardingControlsWritePath(t *testing.T) {
	root := t.TempDir()
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig("tenant-1"))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	name := "report.txt"
	fileKey, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("content"), &name)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if len(fileKey) != 32 || strings.Contains(fileKey, "-") {
		t.Fatalf("fileKey = %q, want 32 lowercase hexadecimal characters", fileKey)
	}

	location, err := runtime.StoragePool().GetFileLocation(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("GetFileLocation() error = %v", err)
	}
	wantPath := filepath.Join("tenant-1", fileKey[:2], fileKey[2:4], fileKey+".txt")
	if location.PhysicalPath != wantPath {
		t.Fatalf("PhysicalPath = %q, want %q", location.PhysicalPath, wantPath)
	}
}

func TestWriteFileToDirectoryTracksLogicalDirectoryQuota(t *testing.T) {
	root := t.TempDir()
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig("tenant-1"))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	name := "report.txt"
	fileKey, err := runtime.StoragePool().WriteFileToDirectory(
		ctx,
		tenant,
		strings.NewReader("content"),
		&name,
		`reports\2026//daily/./`,
	)
	if err != nil {
		t.Fatalf("WriteFileToDirectory() error = %v", err)
	}

	location, err := runtime.StoragePool().GetFileLocation(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("GetFileLocation() error = %v", err)
	}
	if location.DirectoryPath != "/reports/2026/daily" {
		t.Fatalf("DirectoryPath = %q, want %q", location.DirectoryPath, "/reports/2026/daily")
	}
	count, err := runtime.DirectoryQuotaManager().GetFileCount(ctx, tenant.ID, "/reports/2026/daily")
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("logical directory count = %d, want 1", count)
	}
}

func TestTenantQuotaCountSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	quotaLimit := int64(1)
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig("tenant-1").WithQuota(quotaLimit))

	ctx := context.Background()
	first, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("first NewVenue() error = %v", err)
	}
	if err := first.Start(); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	tenant, err := first.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("first GetTenant() error = %v", err)
	}
	if _, err := first.StoragePool().WriteFile(ctx, tenant, strings.NewReader("first"), nil); err != nil {
		t.Fatalf("first WriteFile() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("second NewVenue() error = %v", err)
	}
	if err := second.Start(); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := second.Stop(); err != nil {
			t.Errorf("second Stop() error = %v", err)
		}
	})
	tenant, err = second.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("second GetTenant() error = %v", err)
	}
	if _, err := second.StoragePool().WriteFile(ctx, tenant, strings.NewReader("second"), nil); !errors.Is(err, core.ErrTenantQuotaExceeded) {
		t.Fatalf("second WriteFile() error = %v, want %v", err, core.ErrTenantQuotaExceeded)
	}
}

func TestExplicitUnlimitedTenantQuotaOverridesDefault(t *testing.T) {
	root := t.TempDir()
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDefaultTenantQuota(1).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig("tenant-1").WithQuota(0))

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("content"), nil); err != nil {
			t.Fatalf("WriteFile() #%d error = %v", i+1, err)
		}
	}
}

func TestDirectoryQuotaCountIsRebuiltFromMetadata(t *testing.T) {
	root := t.TempDir()
	cfg := config.New().
		WithMetadataDirectory(filepath.Join(root, "metadata")).
		WithQuotaDirectory(filepath.Join(root, "quota")).
		WithDatabaseHealthCheckEnabled(false).
		WithBackgroundCleanupEnabled(false).
		WithVolumes(config.NewVolumeConfig().
			WithVolumeID("primary").
			WithMountPath(filepath.Join(root, "storage")).
			WithShardingDepth(2)).
		WithTenants(config.NewTenantConfig("tenant-1"))

	ctx := context.Background()
	first, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("first NewVenue() error = %v", err)
	}
	if err := first.Start(); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	tenant, err := first.TenantManager().GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("first GetTenant() error = %v", err)
	}
	if _, err := first.StoragePool().WriteFileToDirectory(ctx, tenant, strings.NewReader("content"), nil, "/reports"); err != nil {
		t.Fatalf("WriteFileToDirectory() error = %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}
	if err := os.RemoveAll(cfg.QuotaDirectory); err != nil {
		t.Fatalf("RemoveAll(quota) error = %v", err)
	}

	second, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("second NewVenue() error = %v", err)
	}
	if err := second.Start(); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := second.Stop(); err != nil {
			t.Errorf("second Stop() error = %v", err)
		}
	})
	count, err := second.DirectoryQuotaManager().GetFileCount(ctx, "tenant-1", "/reports")
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("directory count = %d, want 1", count)
	}
}
