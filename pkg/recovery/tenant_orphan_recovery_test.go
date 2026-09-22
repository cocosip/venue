package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// stubTenantManager is a read-only TenantManager over a fixed tenant set. Its
// lookup never materializes a tenant, so a test can prove that a tenant-scoped
// scan does not create its tenant as a side effect.
type stubTenantManager struct {
	mu      sync.Mutex
	tenants map[string]core.TenantContext
}

func newStubTenantManager(tenantIDs ...string) *stubTenantManager {
	tenants := make(map[string]core.TenantContext, len(tenantIDs))
	for _, id := range tenantIDs {
		tenants[id] = core.TenantContext{ID: id, Status: core.TenantStatusEnabled}
	}
	return &stubTenantManager{tenants: tenants}
}

func (m *stubTenantManager) GetTenant(_ context.Context, tenantID string) (core.TenantContext, error) {
	tenant, ok := m.lookup(tenantID)
	if !ok {
		return core.TenantContext{}, core.ErrTenantNotFound
	}
	return tenant, nil
}

func (m *stubTenantManager) TryGetTenant(_ context.Context, tenantID string) (core.TenantContext, bool, error) {
	tenant, ok := m.lookup(tenantID)
	return tenant, ok, nil
}

func (m *stubTenantManager) IsTenantEnabled(_ context.Context, tenantID string) (bool, error) {
	_, ok := m.lookup(tenantID)
	return ok, nil
}

func (m *stubTenantManager) CreateTenant(_ context.Context, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenants[tenantID] = core.TenantContext{ID: tenantID, Status: core.TenantStatusEnabled}
	return nil
}

func (m *stubTenantManager) EnableTenant(_ context.Context, tenantID string) error {
	if _, ok := m.lookup(tenantID); !ok {
		return core.ErrTenantNotFound
	}
	return nil
}

func (m *stubTenantManager) DisableTenant(_ context.Context, tenantID string) error {
	if _, ok := m.lookup(tenantID); !ok {
		return core.ErrTenantNotFound
	}
	return nil
}

func (m *stubTenantManager) GetAllTenants(_ context.Context) ([]core.TenantContext, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tenants := make([]core.TenantContext, 0, len(m.tenants))
	for _, tenant := range m.tenants {
		tenants = append(tenants, tenant)
	}
	return tenants, nil
}

func (m *stubTenantManager) lookup(tenantID string) (core.TenantContext, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tenant, ok := m.tenants[tenantID]
	return tenant, ok
}

// newTenantScopedTestService builds a recovery service that can resolve tenants
// through the supplied manager.
func newTenantScopedTestService(
	t *testing.T,
	repo core.MetadataRepository,
	volumeRoot string,
	manager core.TenantManager,
	tenantQuota core.TenantQuotaManager,
	dirQuota core.DirectoryQuotaManager,
) *OrphanRecoveryService {
	t.Helper()

	service, err := NewOrphanRecoveryService(&OrphanRecoveryServiceOptions{
		MetadataRepository:    repo,
		Volumes:               map[string]core.StorageVolume{"vol-1": &stubVolume{id: "vol-1", mount: volumeRoot}},
		TenantManager:         manager,
		TenantQuotaManager:    tenantQuota,
		DirectoryQuotaManager: dirQuota,
	})
	if err != nil {
		t.Fatalf("NewOrphanRecoveryService() error = %v", err)
	}
	return service
}

// TestRecoverOrphanedFilesForTenant_RecoversOnlyThatTenant verifies that the
// tenant-scoped scan rebuilds only the named tenant's orphaned metadata, leaves
// another tenant's files alone, and stays idempotent.
func TestRecoverOrphanedFilesForTenant_RecoversOnlyThatTenant(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")

	const (
		otherKey = "fedcba9876543210fedcba9876543210"
		payload  = "target payload"
	)
	targetRelative := filepath.Join("tenant-001", "aa", "bb", testFileKey+".txt")
	otherRelative := filepath.Join("tenant-002", "aa", "bb", otherKey+".txt")
	writeFile(t, filepath.Join(volumeRoot, targetRelative), payload)
	writeFile(t, filepath.Join(volumeRoot, otherRelative), "other tenant payload")

	repo := newTestRepository(t, root)
	tenantQuota := newStubTenantQuota()
	dirQuota := newStubDirQuota()
	service := newTenantScopedTestService(
		t, repo, volumeRoot, newStubTenantManager("tenant-001", "tenant-002"), tenantQuota, dirQuota,
	)

	scoped, ok := any(service).(core.TenantOrphanRecoveryService)
	if !ok {
		t.Fatal("orphan recovery service does not implement core.TenantOrphanRecoveryService")
	}

	report, err := scoped.RecoverOrphanedFilesForTenant(ctx, "tenant-001")
	if err != nil {
		t.Fatalf("RecoverOrphanedFilesForTenant() error = %v", err)
	}
	if report.FilesRecovered != 1 {
		t.Fatalf("FilesRecovered = %d, want 1 (%#v)", report.FilesRecovered, report)
	}
	// Only the named tenant's subtree is inspected at all.
	if report.FilesScanned != 1 {
		t.Errorf("FilesScanned = %d, want 1 (another tenant must not be inspected)", report.FilesScanned)
	}
	if report.BytesRecovered != int64(len(payload)) {
		t.Errorf("BytesRecovered = %d, want %d", report.BytesRecovered, len(payload))
	}
	if report.FilesFailed != 0 {
		t.Errorf("FilesFailed = %d, want 0 (%#v)", report.FilesFailed, report)
	}

	restored, err := repo.Get(ctx, "tenant-001", testFileKey)
	if err != nil {
		t.Fatalf("Get(tenant-001) error = %v", err)
	}
	if restored.Status != core.FileStatusPending {
		t.Errorf("recovered status = %v, want Pending", restored.Status)
	}
	if filepath.ToSlash(restored.PhysicalPath) != filepath.ToSlash(targetRelative) {
		t.Errorf("recovered PhysicalPath = %q, want %q", restored.PhysicalPath, targetRelative)
	}

	if _, err := repo.Get(ctx, "tenant-002", otherKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Errorf("another tenant's file was recovered: Get() error = %v, want ErrFileNotFound", err)
	}
	if tenantQuota.increments() != 1 || dirQuota.increments() != 1 {
		t.Errorf("quota increments = %d/%d, want 1/1 (only the recovered file may be charged)",
			tenantQuota.increments(), dirQuota.increments())
	}

	// A second scoped scan must not duplicate the record or the quota charge.
	second, err := scoped.RecoverOrphanedFilesForTenant(ctx, "tenant-001")
	if err != nil {
		t.Fatalf("second RecoverOrphanedFilesForTenant() error = %v", err)
	}
	if second.FilesRecovered != 0 {
		t.Errorf("second FilesRecovered = %d, want 0", second.FilesRecovered)
	}
	if tenantQuota.increments() != 1 || dirQuota.increments() != 1 {
		t.Errorf("quota increments after second scan = %d/%d, want 1/1",
			tenantQuota.increments(), dirQuota.increments())
	}
}

// TestRecoverOrphanedFilesForTenant_RejectsUnknownTenant verifies that an
// unknown tenant is rejected without being created or scanned.
func TestRecoverOrphanedFilesForTenant_RejectsUnknownTenant(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "ghost-tenant", "aa", "bb", testFileKey+".txt"), "payload")

	repo := newTestRepository(t, root)
	manager := newStubTenantManager("tenant-001")
	service := newTenantScopedTestService(t, repo, volumeRoot, manager, nil, nil)

	_, err := service.RecoverOrphanedFilesForTenant(ctx, "ghost-tenant")
	if !errors.Is(err, core.ErrTenantNotFound) {
		t.Fatalf("RecoverOrphanedFilesForTenant() error = %v, want ErrTenantNotFound", err)
	}

	if _, err := repo.Get(ctx, "ghost-tenant", testFileKey); !errors.Is(err, core.ErrFileNotFound) {
		t.Errorf("unknown tenant's file must not be recovered, Get() error = %v", err)
	}
	if _, ok := manager.lookup("ghost-tenant"); ok {
		t.Error("orphan recovery must not create the unknown tenant")
	}
}

// TestRecoverOrphanedFilesForTenant_RejectsInvalidTenantID verifies that an
// empty or path-unsafe tenant identifier is rejected before any path is built.
func TestRecoverOrphanedFilesForTenant_RejectsInvalidTenantID(t *testing.T) {
	tests := []struct {
		name     string
		tenantID string
	}{
		{name: "empty", tenantID: ""},
		{name: "path traversal", tenantID: "../evil"},
		{name: "forward slash", tenantID: "tenant/child"},
		{name: "backslash", tenantID: `tenant\child`},
		{name: "dot prefix", tenantID: ".hidden"},
		{name: "reserved device name", tenantID: "NUL"},
		{name: "too long", tenantID: strings.Repeat("t", core.MaxTenantIDLength+1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			repo := newTestRepository(t, root)
			service := newTenantScopedTestService(
				t, repo, filepath.Join(root, "volume"), newStubTenantManager("tenant-001"), nil, nil,
			)

			_, err := service.RecoverOrphanedFilesForTenant(context.Background(), test.tenantID)
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("RecoverOrphanedFilesForTenant(%q) error = %v, want ErrInvalidArgument", test.tenantID, err)
			}
		})
	}
}

// TestRecoverOrphanedFilesForTenant_FailsClosedWithoutTenantManager verifies
// that the tenant-scoped entry point refuses to scan a tenant it cannot verify,
// while the all-tenant RecoverNow keeps working.
func TestRecoverOrphanedFilesForTenant_FailsClosedWithoutTenantManager(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "payload")

	repo := newTestRepository(t, root)
	service := newTestService(t, repo, volumeRoot, nil, nil)

	if _, err := service.RecoverOrphanedFilesForTenant(ctx, "tenant-001"); err == nil {
		t.Fatal("RecoverOrphanedFilesForTenant() error = nil, want a configuration error")
	}

	// RecoverNow does not need a tenant manager.
	report, err := service.RecoverNow(ctx)
	if err != nil {
		t.Fatalf("RecoverNow() error = %v", err)
	}
	if report.FilesRecovered != 1 {
		t.Fatalf("FilesRecovered = %d, want 1", report.FilesRecovered)
	}
}

// TestRecoverOrphanedFilesForTenant_SkipsVolumeWithoutTenantDirectory verifies
// that a known tenant with no directory on a volume is a successful no-op.
func TestRecoverOrphanedFilesForTenant_SkipsVolumeWithoutTenantDirectory(t *testing.T) {
	root := t.TempDir()
	repo := newTestRepository(t, root)
	service := newTenantScopedTestService(
		t, repo, filepath.Join(root, "volume"), newStubTenantManager("tenant-001"), nil, nil,
	)

	report, err := service.RecoverOrphanedFilesForTenant(context.Background(), "tenant-001")
	if err != nil {
		t.Fatalf("RecoverOrphanedFilesForTenant() error = %v", err)
	}
	if report.FilesScanned != 0 || report.FilesRecovered != 0 || report.FilesFailed != 0 {
		t.Errorf("report = %#v, want an empty report", report)
	}
}

// TestRecoverOrphanedFilesForTenant_ConcurrentWithRecoverNow verifies that a
// tenant-scoped scan and the all-tenant scan are safe to run concurrently: each
// orphan is registered exactly once and charged to quota exactly once.
func TestRecoverOrphanedFilesForTenant_ConcurrentWithRecoverNow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeRoot := filepath.Join(root, "volume")

	const otherKey = "00112233445566778899aabbccddeeff"
	writeFile(t, filepath.Join(volumeRoot, "tenant-001", "aa", "bb", testFileKey+".txt"), "first")
	writeFile(t, filepath.Join(volumeRoot, "tenant-002", "aa", "bb", otherKey+".txt"), "second")

	repo := newTestRepository(t, root)
	tenantQuota := newStubTenantQuota()
	service := newTenantScopedTestService(
		t, repo, volumeRoot, newStubTenantManager("tenant-001", "tenant-002"), tenantQuota, nil,
	)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		recovered int
		failed    int
	)

	run := func(report *core.OrphanRecoveryReport, err error) {
		if err != nil {
			t.Errorf("scan error = %v", err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		recovered += report.FilesRecovered
		failed += report.FilesFailed
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		report, err := service.RecoverNow(ctx)
		run(report, err)
	}()
	go func() {
		defer wg.Done()
		report, err := service.RecoverOrphanedFilesForTenant(ctx, "tenant-001")
		run(report, err)
	}()
	wg.Wait()

	if recovered != 2 {
		t.Errorf("recovered across concurrent scans = %d, want 2", recovered)
	}
	if failed != 0 {
		t.Errorf("failed = %d, want 0 (scans must be serialized, not conflicting)", failed)
	}
	if tenantQuota.increments() != 2 {
		t.Errorf("tenant quota increments = %d, want 2", tenantQuota.increments())
	}
}
