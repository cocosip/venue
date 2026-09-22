package tenant

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestTenantManagerRejectsUnsafeTenantIDs is the regression test for the tenant
// metadata path-traversal defect: a caller-supplied tenant ID must never be used
// as a path segment before it is validated.
func TestTenantManagerRejectsUnsafeTenantIDs(t *testing.T) {
	unsafeIDs := []string{
		"..",
		".",
		"../evil",
		"../../evil",
		`..\..\evil`,
		"a/../../evil",
		"sub/nested",
		"sub\\nested",
		"evil\x00",
		"evil\n",
		"evil:stream",
		"C:evil",
		"con",
		"CON.json",
		"trailing.",
		"trailing ",
		" leading",
		".locus",
		strings.Repeat("x", 256),
	}

	for _, id := range unsafeIDs {
		t.Run("create "+id, func(t *testing.T) {
			manager, root := setupTestManager(t)
			ctx := context.Background()

			if err := manager.CreateTenant(ctx, id); !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("CreateTenant(%q) error = %v, want ErrInvalidArgument", id, err)
			}

			// The tenant metadata store must not have been touched, and neither
			// must any path reached by traversal from it.
			storeRoot := filepath.Join(root, ".locus", "tenants")
			escaped := filepath.Join(storeRoot, id+".json")
			if _, err := os.Stat(escaped); err == nil {
				t.Fatalf("CreateTenant(%q) wrote %q", id, escaped)
			}
		})

		t.Run("get "+id, func(t *testing.T) {
			manager, _ := setupTestManager(t)
			ctx := context.Background()

			if _, err := manager.GetTenant(ctx, id); !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("GetTenant(%q) error = %v, want ErrInvalidArgument", id, err)
			}
		})
	}
}

// TestTenantManagerAcceptsValidTenantIDs guards against over-restrictive validation.
func TestTenantManagerAcceptsValidTenantIDs(t *testing.T) {
	validIDs := []string{
		"tenant-001",
		"tenant_1",
		"tenant.1",
		"TENANT001",
		"a",
		"0f7b3c9d2a1e4f8b9c3d2e1a4b5c6d7e",
		"租户-一",
	}

	for _, id := range validIDs {
		t.Run(id, func(t *testing.T) {
			manager, root := setupTestManager(t)
			ctx := context.Background()

			if err := manager.CreateTenant(ctx, id); err != nil {
				t.Fatalf("CreateTenant(%q) error = %v", id, err)
			}
			if _, err := manager.GetTenant(ctx, id); err != nil {
				t.Fatalf("GetTenant(%q) error = %v", id, err)
			}

			want := filepath.Join(root, ".locus", "tenants", id+".json")
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("expected tenant metadata at %q: %v", want, err)
			}
		})
	}
}

// TestMetadataStoreRejectsUnsafeTenantIDs asserts the store cannot be used to
// read or write arbitrary *.json files even when called directly.
func TestMetadataStoreRejectsUnsafeTenantIDs(t *testing.T) {
	root := t.TempDir()
	store, err := NewMetadataStore(root)
	if err != nil {
		t.Fatalf("NewMetadataStore() error = %v", err)
	}

	for _, id := range []string{"../../evil", `..\..\evil`, "a/b", "..", "con"} {
		metadata := createDefaultMetadata(id, root)
		if err := store.Save(metadata); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("Save(%q) error = %v, want ErrInvalidArgument", id, err)
		}
		if _, err := store.Load(id); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("Load(%q) error = %v, want ErrInvalidArgument", id, err)
		}
		if _, err := store.Exists(id); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("Exists(%q) error = %v, want ErrInvalidArgument", id, err)
		}
		if err := store.Delete(id); !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("Delete(%q) error = %v, want ErrInvalidArgument", id, err)
		}
	}
}

// TestTenantManagerDisableRejectsUnsafeTenantID covers the status-update path.
func TestTenantManagerDisableRejectsUnsafeTenantID(t *testing.T) {
	manager, _ := setupTestManager(t)
	ctx := context.Background()

	if err := manager.DisableTenant(ctx, "../../evil"); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("DisableTenant() error = %v, want ErrInvalidArgument", err)
	}
	if err := manager.EnableTenant(ctx, "../../evil"); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("EnableTenant() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := manager.IsTenantEnabled(ctx, "../../evil"); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("IsTenantEnabled() error = %v, want ErrInvalidArgument", err)
	}
}
