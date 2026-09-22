package volume

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestLocalFileSystemVolume_BuildPhysicalPathRejectsUnsafeTenantID verifies
// that a tenant identifier is validated before it is used as a storage
// directory segment (AGENTS.md tenant-isolation rule).
func TestLocalFileSystemVolume_BuildPhysicalPathRejectsUnsafeTenantID(t *testing.T) {
	const fileKey = "550e8400e29b41d4a716446655440000"

	tests := []struct {
		name     string
		tenantID string
	}{
		{name: "empty", tenantID: ""},
		{name: "parent traversal", tenantID: ".."},
		{name: "traversal prefix", tenantID: "../evil"},
		{name: "embedded forward separator", tenantID: "tenant/child"},
		{name: "embedded back separator", tenantID: `tenant\child`},
		{name: "windows reserved device", tenantID: "CON"},
		{name: "windows reserved device with extension", tenantID: "con.json"},
		{name: "reserved dot prefix", tenantID: ".hidden"},
		{name: "trailing dot", tenantID: "tenant."},
		{name: "alternate data stream", tenantID: "tenant:ads"},
		{name: "leading whitespace", tenantID: " tenant"},
		{name: "overlong", tenantID: strings.Repeat("a", core.MaxTenantIDLength+1)},
		{name: "invalid utf-8", tenantID: "bad\xff"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vol, _ := createTestVolume(t, 2)

			built, err := vol.BuildPhysicalPath(tc.tenantID, fileKey, ".txt")
			if err == nil {
				t.Fatalf("expected tenant ID %q to be rejected, built %q", tc.tenantID, built)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Errorf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	t.Run("valid tenant ID", func(t *testing.T) {
		vol, _ := createTestVolume(t, 2)

		built, err := vol.BuildPhysicalPath("tenant-1", fileKey, ".txt")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		want := filepath.Join("tenant-1", "55", "0e", fileKey+".txt")
		if built != want {
			t.Errorf("built path = %q, want %q", built, want)
		}
	})
}
