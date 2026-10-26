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
