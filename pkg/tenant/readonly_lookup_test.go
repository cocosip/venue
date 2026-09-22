package tenant

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// newAutoCreateManager builds a manager rooted in a temporary directory with
// auto-create enabled, which is the configuration that used to make read-only
// probes materialize tenant state.
func newAutoCreateManager(t *testing.T) (core.TenantManager, string) {
	t.Helper()

	root := t.TempDir()
	opts := DefaultTenantManagerOptions(root)
	opts.EnableAutoCreate = true

	manager, err := NewTenantManager(opts)
	if err != nil {
		t.Fatalf("NewTenantManager() error = %v", err)
	}

	return manager, root
}

// tenantMetadataDir returns the metadata directory for a manager root.
func tenantMetadataDir(root string) string {
	return filepath.Join(root, ".locus", "tenants")
}

// listTenantJSONFiles snapshots the *.json files currently present in a
// directory. A missing directory is reported as an empty listing.
func listTenantJSONFiles(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", dir, err)
	}

	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tmp") {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

// requireNoTenantState asserts that neither the metadata file nor the tenant
// storage directory for tenantID exists under root.
func requireNoTenantState(t *testing.T, root string, tenantID string) {
	t.Helper()

	metadataFile := filepath.Join(tenantMetadataDir(root), tenantID+".json")
	if _, err := os.Stat(metadataFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only lookup materialized metadata file %q (stat error = %v)", metadataFile, err)
	}

	tenantDir := filepath.Join(root, tenantID)
	if _, err := os.Stat(tenantDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only lookup materialized tenant directory %q (stat error = %v)", tenantDir, err)
	}
}

// TestTryGetTenantDoesNotCreateTenant is the regression test for the read-only
// lookup: with auto-create enabled, probing an unknown tenant must report
// ok=false and must not write any tenant state.
func TestTryGetTenantDoesNotCreateTenant(t *testing.T) {
	manager, root := newAutoCreateManager(t)
	ctx := context.Background()

	metaDir := tenantMetadataDir(root)
	before := listTenantJSONFiles(t, metaDir)

	for attempt := 1; attempt <= 3; attempt++ {
		tenant, ok, err := manager.TryGetTenant(ctx, "readonly-probe")
		if err != nil {
			t.Fatalf("TryGetTenant() #%d error = %v, want nil for a missing tenant", attempt, err)
		}
		if ok {
			t.Fatalf("TryGetTenant() #%d ok = true, want false", attempt)
		}
		if tenant.ID != "" {
			t.Fatalf("TryGetTenant() #%d tenant ID = %q, want zero value", attempt, tenant.ID)
		}
	}

	after := listTenantJSONFiles(t, metaDir)
	if !slices.Equal(before, after) {
		t.Fatalf("metadata files changed by TryGetTenant: before %v, after %v", before, after)
	}
	requireNoTenantState(t, root, "readonly-probe")
}

// TestIsTenantEnabledDoesNotCreateTenant is the direct regression for the old
// IsTenantEnabled implementation, which called GetTenant and therefore
// auto-created the tenant it was only supposed to inspect.
func TestIsTenantEnabledDoesNotCreateTenant(t *testing.T) {
	manager, root := newAutoCreateManager(t)
	ctx := context.Background()

	metaDir := tenantMetadataDir(root)
	before := listTenantJSONFiles(t, metaDir)

	enabled, err := manager.IsTenantEnabled(ctx, "enabled-probe")
	if err != nil {
		t.Fatalf("IsTenantEnabled() error = %v, want nil for a missing tenant", err)
	}
	if enabled {
		t.Fatal("IsTenantEnabled() = true for an unknown tenant, want false")
	}

	after := listTenantJSONFiles(t, metaDir)
	if !slices.Equal(before, after) {
		t.Fatalf("metadata files changed by IsTenantEnabled: before %v, after %v", before, after)
	}
	requireNoTenantState(t, root, "enabled-probe")
}

// TestGetTenantAutoCreateStillWorks proves the read-only change did not disable
// auto-creation: GetTenant still materializes the tenant, and the persisted
// record is then visible to TryGetTenant through a fresh manager.
func TestGetTenantAutoCreateStillWorks(t *testing.T) {
	manager, root := newAutoCreateManager(t)
	ctx := context.Background()
	const tenantID = "auto-created"

	created, err := manager.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	if created.ID != tenantID {
		t.Fatalf("GetTenant() ID = %q, want %q", created.ID, tenantID)
	}

	metadataFile := filepath.Join(tenantMetadataDir(root), tenantID+".json")
	if _, err := os.Stat(metadataFile); err != nil {
		t.Fatalf("GetTenant() did not persist %q: %v", metadataFile, err)
	}

	found, ok, err := manager.TryGetTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("TryGetTenant() error = %v", err)
	}
	if !ok {
		t.Fatal("TryGetTenant() ok = false for an auto-created tenant, want true")
	}
	if found.Status != created.Status {
		t.Fatalf("TryGetTenant() status = %v, want %v", found.Status, created.Status)
	}

	// A second manager reading the same root must see the persisted tenant
	// without creating anything.
	second, err := NewTenantManager(&TenantManagerOptions{
		RootPath:         root,
		EnableAutoCreate: true,
	})
	if err != nil {
		t.Fatalf("second NewTenantManager() error = %v", err)
	}

	reloaded, ok, err := second.TryGetTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("second TryGetTenant() error = %v", err)
	}
	if !ok {
		t.Fatal("second TryGetTenant() ok = false, want true")
	}
	if reloaded.Status != created.Status {
		t.Fatalf("second TryGetTenant() status = %v, want %v", reloaded.Status, created.Status)
	}
}

// TestTryGetTenantReportsStatus covers a known disabled tenant and a known
// enabled tenant through both read paths.
func TestTryGetTenantReportsStatus(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		disable       bool
		wantStatus    core.TenantStatus
		wantIsEnabled bool
	}{
		{name: "enabled tenant", disable: false, wantStatus: core.TenantStatusEnabled, wantIsEnabled: true},
		{name: "disabled tenant", disable: true, wantStatus: core.TenantStatusDisabled, wantIsEnabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, _ := newAutoCreateManager(t)
			const tenantID = "known-tenant"

			if err := manager.CreateTenant(ctx, tenantID); err != nil {
				t.Fatalf("CreateTenant() error = %v", err)
			}
			if tt.disable {
				if err := manager.DisableTenant(ctx, tenantID); err != nil {
					t.Fatalf("DisableTenant() error = %v", err)
				}
			}

			tenant, ok, err := manager.TryGetTenant(ctx, tenantID)
			if err != nil {
				t.Fatalf("TryGetTenant() error = %v", err)
			}
			if !ok {
				t.Fatal("TryGetTenant() ok = false for a known tenant, want true")
			}
			if tenant.ID != tenantID {
				t.Fatalf("TryGetTenant() ID = %q, want %q", tenant.ID, tenantID)
			}
			if tenant.Status != tt.wantStatus {
				t.Fatalf("TryGetTenant() status = %v, want %v", tenant.Status, tt.wantStatus)
			}

			enabled, err := manager.IsTenantEnabled(ctx, tenantID)
			if err != nil {
				t.Fatalf("IsTenantEnabled() error = %v", err)
			}
			if enabled != tt.wantIsEnabled {
				t.Fatalf("IsTenantEnabled() = %v, want %v", enabled, tt.wantIsEnabled)
			}
		})
	}
}

// TestTryGetTenantRejectsInvalidID asserts that an identifier which could not
// have been created cannot be probed either, and that the rejection writes
// nothing.
func TestTryGetTenantRejectsInvalidID(t *testing.T) {
	ctx := context.Background()

	invalidIDs := []string{"", "../escape", `..\..\escape`, "sub/nested"}

	for _, id := range invalidIDs {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			manager, root := newAutoCreateManager(t)

			metaDir := tenantMetadataDir(root)
			before := listTenantJSONFiles(t, metaDir)

			tenant, ok, err := manager.TryGetTenant(ctx, id)
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("TryGetTenant(%q) error = %v, want %v", id, err, core.ErrInvalidArgument)
			}
			if ok {
				t.Fatalf("TryGetTenant(%q) ok = true, want false", id)
			}
			if tenant.ID != "" {
				t.Fatalf("TryGetTenant(%q) tenant ID = %q, want zero value", id, tenant.ID)
			}

			after := listTenantJSONFiles(t, metaDir)
			if !slices.Equal(before, after) {
				t.Fatalf("metadata files changed: before %v, after %v", before, after)
			}

			// Nothing may have escaped the metadata directory either.
			escaped := filepath.Join(root, filepath.FromSlash(id)+".json")
			if _, err := os.Stat(escaped); err == nil {
				t.Fatalf("TryGetTenant(%q) created %q", id, escaped)
			}
		})
	}
}
