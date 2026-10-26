package core

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxWatcherIDLength bounds a watcher identifier so it always stays a valid
// single path segment on every supported platform.
const MaxWatcherIDLength = 128

// ValidateWatcherID reports whether watcherID is safe to use as a watcher
// identifier and as a single filesystem path segment.
//
// A watcher ID reaches the runtime from configuration and from callers, and is
// used to name per-watcher state files and quarantine directories. It is
// therefore validated at the same trust boundary as a tenant ID: configuration
// validation rejects an unsafe value before any database or volume is opened,
// and the watcher rejects it again when a caller registers one directly.
//
// The returned error always wraps ErrInvalidArgument. An ID containing a path
// separator or consisting of "."/".." additionally wraps
// ErrPathTraversalAttempt.
//
// Rejected inputs: empty, invalid UTF-8, longer than MaxWatcherIDLength bytes,
// path separators, "." and "..", NUL bytes, control characters, the Windows
// reserved filename characters :*?"<>| (a watcher ID names quarantine
// directories and state files, and those characters would make those creations
// fail on Windows), and Windows device names.
func ValidateWatcherID(watcherID string) error {
	if watcherID == "" {
		return fmt.Errorf("watcher ID cannot be empty: %w", ErrInvalidArgument)
	}
	if !utf8.ValidString(watcherID) {
		return fmt.Errorf("watcher ID is not valid UTF-8: %w", ErrInvalidArgument)
	}
	if len(watcherID) > MaxWatcherIDLength {
		return fmt.Errorf("watcher ID exceeds %d bytes: %w", MaxWatcherIDLength, ErrInvalidArgument)
	}

	traversal := watcherID == "." || watcherID == ".."
	if strings.ContainsAny(watcherID, `/\`) {
		traversal = true
		return watcherIDError(watcherID, "contains a path separator", traversal)
	}
	if traversal {
		return watcherIDError(watcherID, "is a relative path segment", traversal)
	}

	for _, r := range watcherID {
		if r == 0 {
			return fmt.Errorf("watcher ID contains a NUL byte: %w", ErrInvalidArgument)
		}
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("watcher ID contains a control character: %w", ErrInvalidArgument)
		}
		switch r {
		case ':', '*', '?', '"', '<', '>', '|':
			return watcherIDError(watcherID, "contains a reserved filename character", false)
		}
	}

	if reservedTenantDeviceNames[reservedDeviceName(watcherID)] {
		return fmt.Errorf("watcher ID is a reserved device name: %w", ErrInvalidArgument)
	}

	return nil
}

// watcherIDError builds a validation error, adding ErrPathTraversalAttempt for
// inputs that could resolve outside the configured root.
func watcherIDError(watcherID, reason string, traversal bool) error {
	base := ErrInvalidArgument
	if traversal {
		base = errors.Join(ErrInvalidArgument, ErrPathTraversalAttempt)
	}
	return fmt.Errorf("watcher ID %q %s: %w", watcherID, reason, base)
}
