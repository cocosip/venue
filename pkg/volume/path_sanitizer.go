package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/cocosip/venue/pkg/core"
)

// PathSanitizer provides path sanitization to prevent path traversal attacks.
type PathSanitizer struct {
	rootPath string
}

// NewPathSanitizer creates a new path sanitizer.
func NewPathSanitizer(rootPath string) *PathSanitizer {
	// Clean and normalize root path
	cleanRoot := filepath.Clean(rootPath)
	return &PathSanitizer{
		rootPath: cleanRoot,
	}
}

// SanitizeAndJoin sanitizes a relative path and joins it with the root path.
// Returns an error if the resulting path would escape the root directory.
//
// The containment check is lexical and needs no Rel round trip: once the
// relative path is clean, not absolute, and does not start with a parent
// segment, filepath.Join cannot place the result above the root. A path that is
// rooted but not absolute ("/etc/passwd" on Windows) is joined below the root,
// which is why the escape rejection comes from the parent/absolute checks and
// not from the join.
//
// It does not resolve symlinks. Venue itself never creates symlinks, so an
// attacker would need pre-existing write access to the volume's local
// filesystem to plant one — at which point the storage is already compromised
// independently of this check. That trusted-local-filesystem model is
// deliberate: resolving every path would put extra syscalls on the hot write
// path for protection that a local attacker with volume write access does not
// need to bypass.
//
// Every segment is also checked for the spellings Windows resolves differently
// than it stores (a trailing dot or space, a reserved device name); see
// validatePathSegments.
func (s *PathSanitizer) SanitizeAndJoin(relativePath string) (string, error) {
	if relativePath == "" {
		return "", fmt.Errorf("relative path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check for suspicious patterns
	if err := s.checkSuspiciousPatterns(relativePath); err != nil {
		return "", err
	}

	// Clean the relative path
	cleanRelative := filepath.Clean(relativePath)

	// Must not be absolute
	if filepath.IsAbs(cleanRelative) {
		return "", fmt.Errorf("path must be relative, not absolute: %s: %w", relativePath, core.ErrInvalidArgument)
	}

	// Must not start with a parent segment. The check is segment-precise: a
	// name that merely starts with two dots ("..data") is a legal segment and
	// stays acceptable, while a leading ".." escapes the parent.
	if isParentReference(cleanRelative) {
		return "", fmt.Errorf("path must not escape parent directory: %s: %w", relativePath, core.ErrPathTraversalAttempt)
	}

	// Reject segments whose spelling Windows does not preserve, so a relative
	// path always names the file it appears to name.
	if err := validatePathSegments(cleanRelative); err != nil {
		return "", err
	}

	// Join with root path. The checks above prove the result cannot leave the
	// root, so no separate Rel-based containment check is needed.
	return filepath.Join(s.rootPath, cleanRelative), nil
}

// reservedDeviceNames are the Windows device names that resolve in every
// directory: "root\NUL" opens the null device, so a payload "written" to one
// never reaches the volume (the write reports success and the bytes vanish,
// while FileExists reports the file as present). pkg/core enforces the same
// rule for tenant identifiers, but its table is unexported and scoped to
// tenant governance, so this package keeps the path-level copy instead of
// exporting a validation detail from core for one caller.
var reservedDeviceNames = map[string]bool{
	"CON": true, "PRN": true, "AUX": true, "NUL": true,
	"COM0": true, "COM1": true, "COM2": true, "COM3": true, "COM4": true,
	"COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true,
	"LPT0": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true,
	"LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true,
}

// validatePathSegments rejects the segment spellings Windows resolves to a
// different name than the one written.
//
// A segment with a trailing dot or space is stripped by the filesystem, so
// "a." and "a " both name "a": two distinct relative paths would collapse onto
// one payload and a write could land on a file another path already owns. A
// reserved device name (case-insensitively, and with any extension: "nul" and
// "con.json" are devices) is resolved outside the volume entirely.
//
// The rule is unconditional rather than GOOS-gated, matching the reserved
// filename characters above: Venue-managed paths are system-generated and never
// carry these spellings, so one invariant is cheaper to reason about than
// platform-dependent acceptance. Trailing dots and spaces are reported like the
// path-identity hazard they are (both ErrInvalidArgument and
// ErrPathTraversalAttempt, mirroring core.ValidateTenantID); a device name is
// ErrInvalidArgument.
func validatePathSegments(cleanPath string) error {
	for _, segment := range strings.Split(cleanPath, string(os.PathSeparator)) {
		if segment == "" {
			continue
		}
		if trimmed := strings.TrimRight(segment, ". "); trimmed != segment {
			return fmt.Errorf(
				"path segment %q ends with a dot or space: %w",
				segment,
				errors.Join(core.ErrInvalidArgument, core.ErrPathTraversalAttempt),
			)
		}
		if reservedDeviceNames[deviceNameKey(segment)] {
			return fmt.Errorf("path segment %q is a reserved device name: %w", segment, core.ErrInvalidArgument)
		}
	}

	return nil
}

// deviceNameKey returns the device-name key of a segment: the part before the
// first dot, upper-cased, because Windows resolves "nul.txt" to the NUL device.
func deviceNameKey(segment string) string {
	if index := strings.IndexByte(segment, '.'); index >= 0 {
		segment = segment[:index]
	}
	return strings.ToUpper(segment)
}

// checkSuspiciousPatterns checks for common path traversal patterns.
func (s *PathSanitizer) checkSuspiciousPatterns(path string) error {
	// Check for null bytes (can be used to bypass checks)
	if strings.Contains(path, "\x00") {
		return fmt.Errorf("path contains null byte: %w", core.ErrPathTraversalAttempt)
	}

	// Check for consecutive dots (more than 2)
	if strings.Contains(path, "...") {
		return fmt.Errorf("path contains suspicious pattern '...': %w", core.ErrPathTraversalAttempt)
	}

	// Reject the Windows reserved filename characters. A colon is the
	// NTFS alternate-data-stream and drive separator: a path that carries
	// one "succeeds" on Windows while the payload lands in a hidden stream,
	// so it is rejected before os.Create can run. The rule is unconditional
	// rather than GOOS-gated: Venue-managed paths are system-generated and
	// never contain these characters, and one invariant is cheaper to reason
	// about than platform-dependent acceptance.
	if index := strings.IndexAny(path, `:*?"<>|`); index >= 0 {
		return fmt.Errorf("path contains reserved filename character %q: %w", path[index], core.ErrInvalidArgument)
	}

	// Check for backslash on Unix systems (potential path confusion)
	// On Windows, backslashes are normal, so only check on non-Windows systems
	if runtime.GOOS != "windows" && strings.Contains(path, "\\") {
		// Path contains backslash but we're not on Windows
		// This might be an attempt to bypass sanitization
		return fmt.Errorf("path contains backslash on non-Windows system: %w", core.ErrPathTraversalAttempt)
	}

	return nil
}

// isParentReference reports whether a cleaned path leaves the directory it is
// joined to. The check is segment-precise: only a leading ".." segment is a
// parent reference, while a segment that merely starts with two dots ("..data")
// is a legal name. filepath.Clean has already collapsed every interior parent
// reference that does not escape, so whatever survives here is leading.
func isParentReference(cleanPath string) bool {
	if cleanPath == ".." {
		return true
	}
	if strings.HasPrefix(cleanPath, "..") {
		return len(cleanPath) > 2 && os.IsPathSeparator(cleanPath[2])
	}
	return false
}

// maxExtensionLength caps the length of an extension that may become part of a
// physical file name. Real-world extensions are a handful of characters; the
// cap is generous enough for compound forms such as .tar.gz while keeping a
// caller-supplied name from turning into an unusable physical segment.
const maxExtensionLength = 64

// SanitizeExtension validates an extension extracted from a caller-supplied
// file name before it becomes part of a physical path or is persisted as
// metadata.
//
// It cannot traverse (filepath.Ext never keeps a separator), but a raw
// extension can still carry control characters, the Windows reserved filename
// characters (a colon would turn the write into an NTFS alternate data
// stream), three consecutive dots (which the path sanitizer rejects after the
// physical layout was already built around the extension), trailing dots or
// spaces (which vanish from the name on Windows and would leave the physical
// file unaddressable), or an absurd length. Every rejected class is reported
// with a wrapped core.ErrInvalidArgument; trailing dots and spaces are
// trimmed, and a remainder that is only a dot carries no extension and yields
// "".
func SanitizeExtension(ext string) (string, error) {
	if ext == "" {
		return "", nil
	}
	if strings.ContainsAny(ext, "/\\") {
		return "", fmt.Errorf("file extension must not contain path separators: %q: %w", ext, core.ErrInvalidArgument)
	}
	if strings.Contains(ext, "\x00") {
		return "", fmt.Errorf("file extension contains null byte: %w", core.ErrInvalidArgument)
	}
	if strings.Contains(ext, "...") {
		return "", fmt.Errorf("file extension contains suspicious pattern '...': %w", core.ErrInvalidArgument)
	}
	if index := strings.IndexAny(ext, `:*?"<>|`); index >= 0 {
		return "", fmt.Errorf("file extension contains reserved filename character %q: %w", ext[index], core.ErrInvalidArgument)
	}
	for _, r := range ext {
		if r < 32 {
			return "", fmt.Errorf("file extension contains control character: %q: %w", ext, core.ErrInvalidArgument)
		}
	}
	if len(ext) > maxExtensionLength {
		return "", fmt.Errorf("file extension exceeds %d characters: %w", maxExtensionLength, core.ErrInvalidArgument)
	}

	trimmed := strings.TrimRight(ext, ". ")
	if trimmed == "" || trimmed == "." {
		return "", nil
	}
	return trimmed, nil
}

// BuildShardedPath builds a sharded path from a file key.
// shardDepth: 0 = no sharding, 1 = one level (ab/), 2 = two levels (ab/cd/), 3 = three levels (ab/cd/ef/)
func BuildShardedPath(fileKey string, shardDepth int) (string, error) {
	if fileKey == "" {
		return "", fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	if shardDepth < 0 || shardDepth > 3 {
		return "", fmt.Errorf("shard depth must be between 0 and 3: %w", core.ErrInvalidArgument)
	}

	// Need at least 2*shardDepth characters
	minLen := shardDepth * 2
	if len(fileKey) < minLen {
		return "", fmt.Errorf("file key too short for shard depth %d (need at least %d chars): %w", shardDepth, minLen, core.ErrInvalidArgument)
	}

	if shardDepth == 0 {
		return fileKey, nil
	}

	// Build shard path
	parts := make([]string, 0, shardDepth+1)

	for i := 0; i < shardDepth; i++ {
		start := i * 2
		end := start + 2
		shard := fileKey[start:end]
		parts = append(parts, shard)
	}

	// Add the file key
	parts = append(parts, fileKey)

	return filepath.Join(parts...), nil
}
