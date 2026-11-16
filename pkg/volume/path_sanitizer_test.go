package volume

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestSanitizeAndJoin_RejectedEscapeClassesRemainRejected pins every escape
// class the sanitizer rejected before the dead Rel-based containment check was
// removed: traversal, platform absolute paths, reserved characters, control
// bytes, and empty input keep their error class.
func TestSanitizeAndJoin_RejectedEscapeClassesRemainRejected(t *testing.T) {
	sanitizer := NewPathSanitizer(filepath.Join("tmp", "test"))

	type rejection struct {
		name string
		path string
		// acceptedErrors lists every error class that proves a rejection; the
		// platform-specific classes differ for paths that are absolute on one
		// platform and merely rooted on the other.
		acceptedErrors []error
	}

	rejections := []rejection{
		{
			name:           "parent",
			path:           "..",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "parent file",
			path:           "../file.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "deep parent",
			path:           "../../file.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "backslash parent",
			path:           `..\file.txt`,
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "interior escape",
			path:           "a/../../file.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "parent followed by separator",
			path:           "..//file.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "windows drive absolute",
			path:           `C:\Windows\win.ini`,
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "windows drive relative",
			path:           "C:evil",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "alternate data stream",
			path:           "file:ads",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "unc share",
			path:           `\\server\share\file.txt`,
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "null byte",
			path:           "file\x00.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "consecutive dots",
			path:           "a/.../file.txt",
			acceptedErrors: []error{core.ErrPathTraversalAttempt},
		},
		{
			name:           "empty",
			path:           "",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
	}

	if runtime.GOOS != "windows" {
		// A POSIX absolute path is not absolute on Windows, where it is a
		// rooted path that the join keeps below the volume root.
		rejections = append(rejections, rejection{
			name:           "posix absolute",
			path:           "/etc/passwd",
			acceptedErrors: []error{core.ErrInvalidArgument},
		})
	}

	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sanitizer.SanitizeAndJoin(tc.path)
			if err == nil {
				t.Fatalf("SanitizeAndJoin(%q) = nil error, want rejection", tc.path)
			}

			for _, expected := range tc.acceptedErrors {
				if errors.Is(err, expected) {
					return
				}
			}

			t.Errorf("SanitizeAndJoin(%q) error = %v, want one of %v", tc.path, err, tc.acceptedErrors)
		})
	}
}

// TestSanitizeAndJoin_NoInputEscapesRoot is the property test for the removed
// Rel-based containment check: any path the sanitizer accepts must still resolve
// lexically below the volume root, and any path it rejects must fail with a
// documented domain error.
func TestSanitizeAndJoin_NoInputEscapesRoot(t *testing.T) {
	rootPath := filepath.Join(t.TempDir(), "volume-root")
	sanitizer := NewPathSanitizer(rootPath)
	rootPath = filepath.Clean(rootPath)

	inputs := []string{
		"file.txt",
		"a/b/c/file.txt",
		"a/../b/file.txt",
		"a/./b/file.txt",
		"./file.txt",
		`a\b\file.txt`,
		"..data",
		"a/..b/c/file.txt",
		"/etc/passwd",
		"file...txt",
		"sub/../../x",
		`..\evil`,
		"../evil",
		"..",
		"C:evil",
		"",
	}

	parentPrefix := ".." + string(filepath.Separator)

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			joined, err := sanitizer.SanitizeAndJoin(input)
			if err != nil {
				if !errors.Is(err, core.ErrInvalidArgument) && !errors.Is(err, core.ErrPathTraversalAttempt) {
					t.Fatalf("SanitizeAndJoin(%q) error = %v, want a domain error", input, err)
				}
				return
			}

			relative, relErr := filepath.Rel(rootPath, joined)
			if relErr != nil {
				t.Fatalf("Rel(%q, %q) error = %v", rootPath, joined, relErr)
			}
			if relative == ".." || strings.HasPrefix(relative, parentPrefix) {
				t.Errorf("SanitizeAndJoin(%q) = %q, which escapes root %q", input, joined, rootPath)
			}
		})
	}
}

// TestNewPathSanitizer tests creating a new path sanitizer.
func TestNewPathSanitizer(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	if sanitizer == nil {
		t.Fatal("Expected non-nil sanitizer")
	}

	// The root is cleaned once at construction; the join is the only public
	// read of it, so it is what the test observes.
	joined, err := sanitizer.SanitizeAndJoin("file.txt")
	if err != nil {
		t.Fatalf("SanitizeAndJoin() error = %v", err)
	}
	if want := filepath.Join(filepath.Clean(rootPath), "file.txt"); joined != want {
		t.Errorf("SanitizeAndJoin() = %q, want the cleaned root join %q", joined, want)
	}
}

// TestSanitizeAndJoin_ValidPaths tests valid paths.
func TestSanitizeAndJoin_ValidPaths(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	testCases := []struct {
		name         string
		relativePath string
		wantPath     string
	}{
		{
			name:         "Simple file",
			relativePath: "file.txt",
			wantPath:     filepath.Join(rootPath, "file.txt"),
		},
		{
			name:         "Subdirectory",
			relativePath: "subdir/file.txt",
			wantPath:     filepath.Join(rootPath, "subdir", "file.txt"),
		},
		{
			name:         "Multiple subdirectories",
			relativePath: "a/b/c/file.txt",
			wantPath:     filepath.Join(rootPath, "a", "b", "c", "file.txt"),
		},
		{
			name:         "Path with dots in filename",
			relativePath: "file.with.dots.txt",
			wantPath:     filepath.Join(rootPath, "file.with.dots.txt"),
		},
		{
			name:         "Path with current directory",
			relativePath: "./file.txt",
			wantPath:     filepath.Join(rootPath, "file.txt"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := sanitizer.SanitizeAndJoin(tc.relativePath)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if result != tc.wantPath {
				t.Errorf("Expected %s, got %s", tc.wantPath, result)
			}
		})
	}
}

// TestSanitizeAndJoin_PathTraversal tests path traversal attempts.
func TestSanitizeAndJoin_PathTraversal(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	testCases := []struct {
		name         string
		relativePath string
		wantError    error
	}{
		{
			name:         "Simple parent directory",
			relativePath: "../file.txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
		{
			name:         "Multiple parent directories",
			relativePath: "../../file.txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
		{
			name:         "Parent directory in middle",
			relativePath: "subdir/../../../file.txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
		// Note: Windows absolute paths (C:\Windows\win.ini and verbatim \\?\
		// forms) are not listed here because the reserved-filename-character
		// rule rejects them with ErrInvalidArgument on every platform, before
		// the absolute-path or backslash checks can run.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sanitizer.SanitizeAndJoin(tc.relativePath)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			// Check if error is the expected type
			if !strings.Contains(err.Error(), tc.wantError.Error()) {
				t.Errorf("Expected error containing %v, got %v", tc.wantError, err)
			}
		})
	}
}

// TestSanitizeAndJoin_SuspiciousPatterns tests detection of suspicious patterns.
func TestSanitizeAndJoin_SuspiciousPatterns(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	testCases := []struct {
		name         string
		relativePath string
		wantError    error
	}{
		{
			name:         "Null byte",
			relativePath: "file\x00.txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
		{
			name:         "Three consecutive dots",
			relativePath: "file...txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
		{
			name:         "Four consecutive dots",
			relativePath: "file....txt",
			wantError:    core.ErrPathTraversalAttempt,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sanitizer.SanitizeAndJoin(tc.relativePath)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			if !strings.Contains(err.Error(), tc.wantError.Error()) {
				t.Errorf("Expected error containing %v, got %v", tc.wantError, err)
			}
		})
	}
}

// TestSanitizeAndJoin_EmptyPath tests empty path handling.
func TestSanitizeAndJoin_EmptyPath(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	_, err := sanitizer.SanitizeAndJoin("")
	if err == nil {
		t.Fatal("Expected error for empty path, got nil")
	}

	if !strings.Contains(err.Error(), core.ErrInvalidArgument.Error()) {
		t.Errorf("Expected ErrInvalidArgument, got %v", err)
	}
}

// TestSanitizeAndJoin_RejectsUnaliasedSegments pins the segment spellings
// Windows resolves to a different name than the one stored. "a." and "a " both
// collapse onto "a", so two distinct relative paths would name one payload; a
// reserved device name (case-insensitively, with or without an extension)
// resolves outside the volume entirely, where a write "succeeds" and vanishes.
func TestSanitizeAndJoin_RejectsUnaliasedSegments(t *testing.T) {
	sanitizer := NewPathSanitizer(filepath.Join("tmp", "test"))

	testCases := []struct {
		name           string
		relativePath   string
		acceptedErrors []error
	}{
		{
			name:           "trailing dot",
			relativePath:   "a.",
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "trailing space",
			relativePath:   "a ",
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "trailing dot in a directory segment",
			relativePath:   "dir./file.txt",
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "trailing space in a directory segment",
			relativePath:   "dir /file.txt",
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "trailing dot and space",
			relativePath:   "a. .txt ",
			acceptedErrors: []error{core.ErrInvalidArgument, core.ErrPathTraversalAttempt},
		},
		{
			name:           "reserved device name",
			relativePath:   "NUL",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "reserved device name in lower case",
			relativePath:   "nul",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "reserved device name with an extension",
			relativePath:   "con.json",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "reserved device name below a directory",
			relativePath:   "tenant/COM1",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
		{
			name:           "reserved device name in a directory segment",
			relativePath:   "lpt9/file.txt",
			acceptedErrors: []error{core.ErrInvalidArgument},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := sanitizer.SanitizeAndJoin(tc.relativePath); err == nil {
				t.Fatalf("SanitizeAndJoin(%q) = nil error, want rejection", tc.relativePath)
			} else {
				for _, expected := range tc.acceptedErrors {
					if errors.Is(err, expected) {
						return
					}
				}
				t.Errorf("SanitizeAndJoin(%q) error = %v, want one of %v", tc.relativePath, err, tc.acceptedErrors)
			}
		})
	}

	// The rule must not over-reach: names that merely resemble a device, and
	// dots inside a name, stay usable.
	for _, relativePath := range []string{"console", "concept/file.txt", "a.b/c.d", ".venue-write-warmup-123"} {
		if _, err := sanitizer.SanitizeAndJoin(relativePath); err != nil {
			t.Errorf("SanitizeAndJoin(%q) error = %v, want acceptance", relativePath, err)
		}
	}
}

// TestBuildShardedPath tests sharded path building.
func TestBuildShardedPath(t *testing.T) {
	testCases := []struct {
		name       string
		fileKey    string
		shardDepth int
		wantResult string
		wantError  bool
	}{
		{
			name:       "No sharding",
			fileKey:    "a1b2c3d4e5f6",
			shardDepth: 0,
			wantResult: "a1b2c3d4e5f6",
			wantError:  false,
		},
		{
			name:       "One level sharding",
			fileKey:    "a1b2c3d4e5f6",
			shardDepth: 1,
			wantResult: filepath.Join("a1", "a1b2c3d4e5f6"),
			wantError:  false,
		},
		{
			name:       "Two level sharding",
			fileKey:    "a1b2c3d4e5f6",
			shardDepth: 2,
			wantResult: filepath.Join("a1", "b2", "a1b2c3d4e5f6"),
			wantError:  false,
		},
		{
			name:       "Three level sharding",
			fileKey:    "a1b2c3d4e5f6",
			shardDepth: 3,
			wantResult: filepath.Join("a1", "b2", "c3", "a1b2c3d4e5f6"),
			wantError:  false,
		},
		{
			name:       "UUID without dashes",
			fileKey:    "550e8400e29b41d4a716446655440000",
			shardDepth: 2,
			wantResult: filepath.Join("55", "0e", "550e8400e29b41d4a716446655440000"),
			wantError:  false,
		},
		{
			name:       "Empty file key",
			fileKey:    "",
			shardDepth: 1,
			wantError:  true,
		},
		{
			name:       "File key too short for depth",
			fileKey:    "a1",
			shardDepth: 2,
			wantError:  true,
		},
		{
			name:       "Invalid shard depth (negative)",
			fileKey:    "a1b2c3d4",
			shardDepth: -1,
			wantError:  true,
		},
		{
			name:       "Invalid shard depth (too large)",
			fileKey:    "a1b2c3d4",
			shardDepth: 4,
			wantError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := BuildShardedPath(tc.fileKey, tc.shardDepth)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if result != tc.wantResult {
					t.Errorf("Expected %s, got %s", tc.wantResult, result)
				}
			}
		})
	}
}

// TestBuildShardedPath_Consistency tests that sharded paths are consistent.
func TestBuildShardedPath_Consistency(t *testing.T) {
	fileKey := "550e8400e29b41d4a716446655440000"

	// Build path multiple times
	path1, err1 := BuildShardedPath(fileKey, 2)
	path2, err2 := BuildShardedPath(fileKey, 2)
	path3, err3 := BuildShardedPath(fileKey, 2)

	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("Expected no errors, got %v, %v, %v", err1, err2, err3)
	}

	if path1 != path2 || path2 != path3 {
		t.Errorf("Expected consistent paths, got %s, %s, %s", path1, path2, path3)
	}
}

// TestSanitizeAndJoin_WithinRoot tests that paths stay within root.
func TestSanitizeAndJoin_WithinRoot(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	// These should all be within root
	validPaths := []string{
		"file.txt",
		"a/file.txt",
		"a/b/file.txt",
		"a/b/c/file.txt",
		"./file.txt",
		"./a/./b/./file.txt",
	}

	for _, path := range validPaths {
		result, err := sanitizer.SanitizeAndJoin(path)
		if err != nil {
			t.Errorf("Path %s should be valid, got error: %v", path, err)
			continue
		}

		// Verify result starts with root path (normalize both to forward slashes for comparison)
		normalizedResult := filepath.ToSlash(result)
		normalizedRoot := filepath.ToSlash(filepath.Clean(rootPath))

		if !strings.HasPrefix(normalizedResult, normalizedRoot) {
			t.Errorf("Path %s resulted in %s which is outside root %s", path, result, rootPath)
		}
	}
}

// TestSanitizeExtension_RejectsReservedFilenameCharacters pins the Windows
// reserved filename characters out of caller-supplied extensions. A colon is
// the NTFS alternate-data-stream separator: letting it through turns a
// "successful" write into a hidden stream on the main file, so the extension
// gate must reject it before it reaches a physical path.
func TestSanitizeExtension_RejectsReservedFilenameCharacters(t *testing.T) {
	testCases := []struct {
		name string
		ext  string
	}{
		{name: "colon", ext: ".txt:ads"},
		{name: "bare colon", ext: ":"},
		{name: "wildcard", ext: ".txt*"},
		{name: "question mark", ext: ".tx?"},
		{name: "quote", ext: `.tx"t`},
		{name: "less than", ext: ".tx<t"},
		{name: "greater than", ext: ".tx>t"},
		{name: "pipe", ext: ".tx|t"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := SanitizeExtension(tc.ext); err == nil {
				t.Fatalf("SanitizeExtension(%q) = nil error, want rejection", tc.ext)
			} else if !strings.Contains(err.Error(), core.ErrInvalidArgument.Error()) {
				t.Errorf("SanitizeExtension(%q) error = %v, want ErrInvalidArgument", tc.ext, err)
			}
		})
	}
}

// TestSanitizeExtension_RejectsConsecutiveDots keeps the extension gate aligned
// with the path sanitizer's '...' rule: an extension carrying three consecutive
// dots would be rejected by SanitizeAndJoin after the physical layout was
// already built around it, so it must fail here with the extension error.
func TestSanitizeExtension_RejectsConsecutiveDots(t *testing.T) {
	for _, ext := range []string{".a...b", ".txt..."} {
		if _, err := SanitizeExtension(ext); err == nil {
			t.Errorf("SanitizeExtension(%q) = nil error, want rejection", ext)
		}
	}
}

// TestSanitizeExtension_AcceptsPlainExtensions keeps the reserved-character
// rejection from over-reaching: ordinary extensions stay usable.
func TestSanitizeExtension_AcceptsPlainExtensions(t *testing.T) {
	for _, ext := range []string{".txt", ".tar.gz", ".PDF", ".日志", ".a..b"} {
		if _, err := SanitizeExtension(ext); err != nil {
			t.Errorf("SanitizeExtension(%q) error = %v, want nil", ext, err)
		}
	}
}

// TestSanitizeAndJoin_RejectsColon pins the colon rejection of the path
// sanitizer: a colon in a physical path is an ADS or drive separator on
// Windows and never appears in a Venue-generated path.
func TestSanitizeAndJoin_RejectsColon(t *testing.T) {
	sanitizer := NewPathSanitizer(filepath.Join("tmp", "test"))

	for _, relativePath := range []string{"file:stream.txt", "a/b:c", "C:evil"} {
		if _, err := sanitizer.SanitizeAndJoin(relativePath); err == nil {
			t.Errorf("SanitizeAndJoin(%q) = nil error, want rejection", relativePath)
		}
	}
}

// TestSanitizeAndJoin_ParentSegmentPrecision pins the difference between a
// real parent reference and a name that merely starts with two dots: "..data"
// is a legal directory name and must be accepted, while a leading ".."
// segment must keep being rejected.
func TestSanitizeAndJoin_ParentSegmentPrecision(t *testing.T) {
	sanitizer := NewPathSanitizer(filepath.Join("tmp", "test"))

	accepted := []string{"..data", "a/..b", "a/..b/c/file.txt"}
	for _, relativePath := range accepted {
		if _, err := sanitizer.SanitizeAndJoin(relativePath); err != nil {
			t.Errorf("SanitizeAndJoin(%q) error = %v, want acceptance", relativePath, err)
		}
	}

	rejected := []string{`..\evil`, "../evil", "a/../../b", ".."}
	for _, relativePath := range rejected {
		if _, err := sanitizer.SanitizeAndJoin(relativePath); err == nil {
			t.Errorf("SanitizeAndJoin(%q) = nil error, want rejection", relativePath)
		}
	}
}
