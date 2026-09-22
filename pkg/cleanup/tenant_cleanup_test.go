package cleanup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// fixedTenantManager is a TenantManager backed by a fixed tenant set. Its
// read-only lookup never materializes a tenant, so a test can prove that a
// tenant-scoped sweep does not create its tenant as a side effect.
type fixedTenantManager struct {
	tenants map[string]core.TenantContext
}

func newFixedTenantManager(tenantIDs ...string) *fixedTenantManager {
	tenants := make(map[string]core.TenantContext, len(tenantIDs))
	for _, id := range tenantIDs {
		tenants[id] = core.TenantContext{ID: id, Status: core.TenantStatusEnabled}
	}
	return &fixedTenantManager{tenants: tenants}
}

func (m *fixedTenantManager) GetTenant(_ context.Context, tenantID string) (core.TenantContext, error) {
	tenant, ok := m.tenants[tenantID]
	if !ok {
		return core.TenantContext{}, core.ErrTenantNotFound
	}
	return tenant, nil
}

func (m *fixedTenantManager) TryGetTenant(_ context.Context, tenantID string) (core.TenantContext, bool, error) {
	tenant, ok := m.tenants[tenantID]
	return tenant, ok, nil
}

func (m *fixedTenantManager) IsTenantEnabled(_ context.Context, tenantID string) (bool, error) {
	_, ok := m.tenants[tenantID]
	return ok, nil
}

func (m *fixedTenantManager) CreateTenant(_ context.Context, tenantID string) error {
	m.tenants[tenantID] = core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled}
	return nil
}

func (m *fixedTenantManager) EnableTenant(_ context.Context, tenantID string) error {
	if _, ok := m.tenants[tenantID]; !ok {
		return core.ErrTenantNotFound
	}
	return nil
}

func (m *fixedTenantManager) DisableTenant(_ context.Context, tenantID string) error {
	if _, ok := m.tenants[tenantID]; !ok {
		return core.ErrTenantNotFound
	}
	return nil
}

func (m *fixedTenantManager) GetAllTenants(_ context.Context) ([]core.TenantContext, error) {
	tenants := make([]core.TenantContext, 0, len(m.tenants))
	for _, tenant := range m.tenants {
		tenants = append(tenants, tenant)
	}
	return tenants, nil
}

// TestCleanupEmptyDirectoriesForTenant_RemovesOnlyThatTenant verifies that the
// tenant-scoped sweep reclaims one tenant's empty directories on every volume
// and never inspects or removes another tenant's directories.
func TestCleanupEmptyDirectoriesForTenant_RemovesOnlyThatTenant(t *testing.T) {
	ctx := context.Background()

	manager := newFixedTenantManager("tenant-001", "tenant-002")
	service, volumes := newShardDepthCleanupService(t, 0, manager, t.TempDir(), t.TempDir())

	scoped, ok := service.(core.TenantCleanupService)
	if !ok {
		t.Fatal("cleanup service does not implement core.TenantCleanupService")
	}

	targetRemoved := []string{
		filepath.Join(volumes["vol-1"].MountPath(), "tenant-001", "a", "b"),
		filepath.Join(volumes["vol-1"].MountPath(), "tenant-001", "aa"),
		filepath.Join(volumes["vol-2"].MountPath(), "tenant-001", "nested"),
	}
	otherTenantKept := []string{
		filepath.Join(volumes["vol-1"].MountPath(), "tenant-002", "keep"),
		filepath.Join(volumes["vol-1"].MountPath(), "tenant-002", "aa"),
		filepath.Join(volumes["vol-2"].MountPath(), "tenant-002", "keep"),
	}
	for _, path := range append(append([]string{}, targetRemoved...), otherTenantKept...) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", path, err)
		}
	}

	stats, err := scoped.CleanupEmptyDirectoriesForTenant(ctx, "tenant-001")
	if err != nil {
		t.Fatalf("CleanupEmptyDirectoriesForTenant() error = %v", err)
	}

	// a/b, aa and nested are removed on the first pass; the now-empty parent
	// "a" follows on the next one.
	if stats.EmptyDirectoriesRemoved != 4 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 4", stats.EmptyDirectoriesRemoved)
	}

	for _, path := range targetRemoved {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Errorf("expected %s to be removed, stat error = %v", path, statErr)
		}
	}

	// The tenant directory itself is the minimum protection depth.
	for _, volumeID := range []string{"vol-1", "vol-2"} {
		tenantRoot := filepath.Join(volumes[volumeID].MountPath(), "tenant-001")
		if _, statErr := os.Stat(tenantRoot); statErr != nil {
			t.Errorf("expected tenant directory %s to remain, stat error = %v", tenantRoot, statErr)
		}
	}

	for _, path := range otherTenantKept {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Errorf("expected another tenant's directory %s to be untouched, stat error = %v", path, statErr)
		}
	}
}

// TestCleanupEmptyDirectoriesForTenant_DoesNotCreateUnknownTenant verifies that
// an unknown tenant is rejected and never materialized as a side effect.
func TestCleanupEmptyDirectoriesForTenant_DoesNotCreateUnknownTenant(t *testing.T) {
	ctx := context.Background()

	manager := newFixedTenantManager("tenant-001")
	service, volumes := newShardDepthCleanupService(t, 0, manager)
	scoped := service.(core.TenantCleanupService)

	unknownDir := filepath.Join(volumes["vol-1"].MountPath(), "ghost-tenant", "empty")
	if err := os.MkdirAll(unknownDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", unknownDir, err)
	}

	stats, err := scoped.CleanupEmptyDirectoriesForTenant(ctx, "ghost-tenant")
	if !errors.Is(err, core.ErrTenantNotFound) {
		t.Fatalf("CleanupEmptyDirectoriesForTenant() error = %v, want ErrTenantNotFound", err)
	}
	if stats == nil {
		t.Fatal("CleanupEmptyDirectoriesForTenant() statistics = nil, want an empty statistics value")
	}
	if stats.EmptyDirectoriesRemoved != 0 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 0", stats.EmptyDirectoriesRemoved)
	}
	if _, statErr := os.Stat(unknownDir); statErr != nil {
		t.Errorf("unknown tenant's directory must not be touched, stat error = %v", statErr)
	}
	if _, ok := manager.tenants["ghost-tenant"]; ok {
		t.Error("cleanup must not create the unknown tenant")
	}
}

// TestCleanupEmptyDirectoriesForTenant_RejectsInvalidTenantID verifies that an
// empty or path-unsafe tenant identifier is rejected before any path is built.
func TestCleanupEmptyDirectoriesForTenant_RejectsInvalidTenantID(t *testing.T) {
	tests := []struct {
		name     string
		tenantID string
	}{
		{name: "empty", tenantID: ""},
		{name: "path traversal", tenantID: "../evil"},
		{name: "forward slash", tenantID: "tenant/child"},
		{name: "backslash", tenantID: `tenant\child`},
		{name: "dot prefix", tenantID: ".hidden"},
		{name: "reserved device name", tenantID: "CON"},
		{name: "too long", tenantID: strings.Repeat("t", core.MaxTenantIDLength+1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := newFixedTenantManager("tenant-001")
			service, _ := newShardDepthCleanupService(t, 0, manager)
			scoped := service.(core.TenantCleanupService)

			_, err := scoped.CleanupEmptyDirectoriesForTenant(context.Background(), test.tenantID)
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("CleanupEmptyDirectoriesForTenant(%q) error = %v, want ErrInvalidArgument", test.tenantID, err)
			}
		})
	}
}

// TestCleanupEmptyDirectoriesForTenant_SkipsVolumeWithoutTenantDirectory
// verifies that a known tenant with no materialized directory on a volume is a
// successful no-op rather than an error.
func TestCleanupEmptyDirectoriesForTenant_SkipsVolumeWithoutTenantDirectory(t *testing.T) {
	manager := newFixedTenantManager("tenant-001")
	service, _ := newShardDepthCleanupService(t, 0, manager)
	scoped := service.(core.TenantCleanupService)

	stats, err := scoped.CleanupEmptyDirectoriesForTenant(context.Background(), "tenant-001")
	if err != nil {
		t.Fatalf("CleanupEmptyDirectoriesForTenant() error = %v", err)
	}
	if stats.EmptyDirectoriesRemoved != 0 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 0", stats.EmptyDirectoriesRemoved)
	}
}

// TestCleanupEmptyDirectoriesForTenant_ProtectsShardDirectories verifies that
// the tenant-scoped sweep keeps the volume's reported shard chain intact.
func TestCleanupEmptyDirectoriesForTenant_ProtectsShardDirectories(t *testing.T) {
	manager := newFixedTenantManager("tenant-001")
	service, volumes := newShardDepthCleanupService(t, 2, manager)
	scoped := service.(core.TenantCleanupService)

	tenantRoot := filepath.Join(volumes["vol-1"].MountPath(), "tenant-001")
	shardDir := filepath.Join(tenantRoot, "ab", "cd")
	removable := filepath.Join(shardDir, "unused")
	for _, path := range []string{shardDir, removable} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) error = %v", path, err)
		}
	}

	stats, err := scoped.CleanupEmptyDirectoriesForTenant(context.Background(), "tenant-001")
	if err != nil {
		t.Fatalf("CleanupEmptyDirectoriesForTenant() error = %v", err)
	}
	if stats.EmptyDirectoriesRemoved != 1 {
		t.Errorf("EmptyDirectoriesRemoved = %d, want 1", stats.EmptyDirectoriesRemoved)
	}
	if _, statErr := os.Stat(shardDir); statErr != nil {
		t.Errorf("expected shard directory %s to be protected, stat error = %v", shardDir, statErr)
	}
	if _, statErr := os.Stat(removable); !os.IsNotExist(statErr) {
		t.Errorf("expected below-chain directory %s to be removed, stat error = %v", removable, statErr)
	}
}
