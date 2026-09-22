package core

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTenantID(t *testing.T) {
	tests := []struct {
		name      string
		tenantID  string
		wantValid bool
		traversal bool
	}{
		{name: "simple", tenantID: "tenant-001", wantValid: true},
		{name: "underscore and dot", tenantID: "tenant_1.v2", wantValid: true},
		{name: "digits", tenantID: "0123456789abcdef", wantValid: true},
		{name: "unicode", tenantID: "租户-一", wantValid: true},
		{name: "max length", tenantID: strings.Repeat("x", MaxTenantIDLength), wantValid: true},
		{name: "empty", tenantID: "", wantValid: false},
		{name: "too long", tenantID: strings.Repeat("x", MaxTenantIDLength+1), wantValid: false},
		{name: "dot", tenantID: ".", wantValid: false, traversal: true},
		{name: "dot dot", tenantID: "..", wantValid: false, traversal: true},
		{name: "parent traversal", tenantID: "../evil", wantValid: false, traversal: true},
		{name: "deep traversal", tenantID: "../../evil", wantValid: false, traversal: true},
		{name: "windows traversal", tenantID: `..\..\evil`, wantValid: false, traversal: true},
		{name: "nested path", tenantID: "sub/nested", wantValid: false, traversal: true},
		{name: "nested backslash", tenantID: `sub\nested`, wantValid: false, traversal: true},
		{name: "mixed traversal", tenantID: "a/../../evil", wantValid: false, traversal: true},
		{name: "nul", tenantID: "evil\x00", wantValid: false},
		{name: "newline", tenantID: "evil\n", wantValid: false},
		{name: "del", tenantID: "evil\x7f", wantValid: false},
		{name: "alternate data stream", tenantID: "evil:stream", wantValid: false},
		{name: "drive relative", tenantID: "C:evil", wantValid: false},
		{name: "wildcard", tenantID: "evil*", wantValid: false},
		{name: "device name", tenantID: "con", wantValid: false},
		{name: "device name upper", tenantID: "CON", wantValid: false},
		{name: "device name with extension", tenantID: "con.json", wantValid: false},
		{name: "lpt device", tenantID: "lpt1", wantValid: false},
		{name: "trailing dot", tenantID: "trailing.", wantValid: false},
		{name: "trailing space", tenantID: "trailing ", wantValid: false},
		{name: "leading space", tenantID: " leading", wantValid: false},
		{name: "leading dot", tenantID: ".locus", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantID(tt.tenantID)
			if tt.wantValid {
				if err != nil {
					t.Fatalf("ValidateTenantID(%q) error = %v, want nil", tt.tenantID, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateTenantID(%q) = nil, want error", tt.tenantID)
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("ValidateTenantID(%q) error = %v, want ErrInvalidArgument", tt.tenantID, err)
			}
			if tt.traversal && !errors.Is(err, ErrPathTraversalAttempt) {
				t.Errorf("ValidateTenantID(%q) error = %v, want ErrPathTraversalAttempt", tt.tenantID, err)
			}
		})
	}
}
