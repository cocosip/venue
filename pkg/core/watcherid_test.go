package core_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestValidateWatcherID covers the watcher-identifier boundary. The rules mirror
// ValidateTenantID because a watcher ID also becomes a path segment: it names
// per-watcher state files and the source-cleanup quarantine directories.
func TestValidateWatcherID(t *testing.T) {
	tests := []struct {
		name          string
		watcherID     string
		wantErr       bool
		wantTraversal bool
	}{
		{name: "simple", watcherID: "inbox"},
		{name: "dashes and dots", watcherID: "inbox-eu.west"},
		{name: "unicode", watcherID: "收件箱"},
		{name: "at the length limit", watcherID: strings.Repeat("a", core.MaxWatcherIDLength)},
		{name: "empty", watcherID: "", wantErr: true},
		{name: "over the length limit", watcherID: strings.Repeat("a", core.MaxWatcherIDLength+1), wantErr: true},
		{name: "forward slash", watcherID: "a/b", wantErr: true, wantTraversal: true},
		{name: "backslash", watcherID: `a\b`, wantErr: true, wantTraversal: true},
		{name: "parent segment", watcherID: "..", wantErr: true, wantTraversal: true},
		{name: "current segment", watcherID: ".", wantErr: true, wantTraversal: true},
		{name: "NUL byte", watcherID: "a\x00b", wantErr: true},
		{name: "control character", watcherID: "a\x1fb", wantErr: true},
		{name: "DEL character", watcherID: "a\x7fb", wantErr: true},
		{name: "invalid utf8", watcherID: "a\xffb", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := core.ValidateWatcherID(test.watcherID)

			if !test.wantErr {
				if err != nil {
					t.Fatalf("ValidateWatcherID(%q) error = %v, want nil", test.watcherID, err)
				}
				return
			}

			if err == nil {
				t.Fatalf("ValidateWatcherID(%q) error = nil, want an error", test.watcherID)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Errorf("ValidateWatcherID(%q) error = %v, want it to wrap ErrInvalidArgument", test.watcherID, err)
			}
			if got := errors.Is(err, core.ErrPathTraversalAttempt); got != test.wantTraversal {
				t.Errorf("ValidateWatcherID(%q) wraps ErrPathTraversalAttempt = %v, want %v", test.watcherID, got, test.wantTraversal)
			}
		})
	}
}

// TestValidateWatcherID_RejectsReservedFilenameCharacters pins the Windows
// reserved filename characters out of watcher identifiers. A watcher ID names
// quarantine directories and state files, so a colon or wildcard would make
// those creations fail on Windows after the watcher was already registered.
func TestValidateWatcherID_RejectsReservedFilenameCharacters(t *testing.T) {
	for _, watcherID := range []string{"inbox:eu", "w*d", "w?d", `w"d`, "w<d", "w>d", "w|d"} {
		if err := core.ValidateWatcherID(watcherID); err == nil {
			t.Errorf("ValidateWatcherID(%q) = nil, want error", watcherID)
		} else if !errors.Is(err, core.ErrInvalidArgument) {
			t.Errorf("ValidateWatcherID(%q) error = %v, want ErrInvalidArgument", watcherID, err)
		}
	}
}

// TestValidateWatcherID_RejectsReservedDeviceNames keeps parity with the tenant
// validator: "con" and friends cannot be a quarantine directory name on
// Windows, so they cannot be a watcher ID.
func TestValidateWatcherID_RejectsReservedDeviceNames(t *testing.T) {
	for _, watcherID := range []string{"con", "NUL", "lpt1", "com4", "aux"} {
		if err := core.ValidateWatcherID(watcherID); err == nil {
			t.Errorf("ValidateWatcherID(%q) = nil, want error", watcherID)
		}
	}
}
