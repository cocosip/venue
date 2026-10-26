package core_test

import (
	"errors"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestParsePostImportAction covers the strict parse: every documented value
// maps to its action, and an unknown value is an error wrapping
// ErrInvalidArgument instead of a silent Delete. A misspelled configuration
// value used to degrade to "delete the source file after import", which is the
// most destructive possible default for a typo.
func TestParsePostImportAction(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  core.PostImportAction
	}{
		{name: "move", value: "Move", want: core.PostImportActionMove},
		{name: "keep", value: "Keep", want: core.PostImportActionKeep},
		{name: "delete", value: "Delete", want: core.PostImportActionDelete},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := core.ParsePostImportAction(test.value)
			if err != nil {
				t.Fatalf("ParsePostImportAction(%q) error = %v, want nil", test.value, err)
			}
			if got != test.want {
				t.Errorf("ParsePostImportAction(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}

// TestParsePostImportActionRejectsUnknownValues pins the defensive standard of
// ParsePermanentlyFailedDisposition: an unrecognized value is reported, never
// silently defaulted.
func TestParsePostImportActionRejectsUnknownValues(t *testing.T) {
	for _, value := range []string{"", "move", "DELETE", "archive", "Move "} {
		got, err := core.ParsePostImportAction(value)
		if err == nil {
			t.Errorf("ParsePostImportAction(%q) = %v, want an error", value, got)
		} else if !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("ParsePostImportAction(%q) error = %v, want it to wrap ErrInvalidArgument", value, err)
		}
	}
}
