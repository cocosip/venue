package volume

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestNewPathSanitizer tests creating a new path sanitizer.
func TestNewPathSanitizer(t *testing.T) {
	rootPath := filepath.Join("tmp", "test")
	sanitizer := NewPathSanitizer(rootPath)

	if sanitizer == nil {
		t.Fatal("Expected non-nil sanitizer")
	}

	if sanitizer.GetRootPath() != filepath.Clean(rootPath) {
		t.Errorf("Expected root path %s, got %s", filepath.Clean(rootPath), sanitizer.GetRootPath())
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
		{
			name:         "Windows absolute path",
			relativePath: "C:\\Windows\\System32",
			wantError:    core.ErrInvalidArgument, // Absolute path is invalid argument
		},
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

// TestValidateRelativePath tests relative path validation.
func TestValidateRelativePath(t *testing.T) {
	sanitizer := NewPathSanitizer(filepath.Join("tmp", "test"))

	testCases := []struct {
		name         string
		relativePath string
		wantError    bool
		expectedErr  error
	}{
		{
			name:         "Valid relative path",
			relativePath: "file.txt",
			wantError:    false,
		},
		{
			name:         "Valid subdirectory",
			relativePath: "subdir/file.txt",
			wantError:    false,
		},
		{
			name:         "Empty path",
			relativePath: "",
			wantError:    true,
			expectedErr:  core.ErrInvalidArgument,
		},
		{
			name:         "Windows absolute path",
			relativePath: "C:\\Windows\\System32",
			wantError:    true,
			expectedErr:  core.ErrInvalidArgument, // Absolute path check
		},
		{
			name:         "Parent directory",
			relativePath: "../file.txt",
			wantError:    true,
			expectedErr:  core.ErrPathTraversalAttempt,
		},
		{
			name:         "Null byte",
			relativePath: "file\x00.txt",
			wantError:    true,
			expectedErr:  core.ErrPathTraversalAttempt,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := sanitizer.ValidateRelativePath(tc.relativePath)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if tc.expectedErr != nil && !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Errorf("Expected error containing %v, got %v", tc.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

// TestSanitizeFilename tests filename sanitization.
func TestSanitizeFilename(t *testing.T) {
	testCases := []struct {
		name        string
		filename    string
		wantResult  string
		wantError   bool
		expectedErr error
	}{
		{
			name:       "Valid filename",
			filename:   "file.txt",
			wantResult: "file.txt",
			wantError:  false,
		},
		{
			name:       "Filename with spaces",
			filename:   "my file.txt",
			wantResult: "my file.txt",
			wantError:  false,
		},
		{
			name:       "Filename with dots",
			filename:   "file.with.dots.txt",
			wantResult: "file.with.dots.txt",
			wantError:  false,
		},
		{
			name:        "Empty filename",
			filename:    "",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename with forward slash",
			filename:    "path/file.txt",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename with backslash",
			filename:    "path\\file.txt",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename with null byte",
			filename:    "file\x00.txt",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename is dot",
			filename:    ".",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename is double dot",
			filename:    "..",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
		{
			name:        "Filename with control character",
			filename:    "file\x01.txt",
			wantError:   true,
			expectedErr: core.ErrInvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := SanitizeFilename(tc.filename)

			if tc.wantError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if tc.expectedErr != nil && !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Errorf("Expected error containing %v, got %v", tc.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if result != tc.wantResult {
					t.Errorf("Expected %s, got %s", tc.wantResult, result)
				}
			}
		})
	}
}

// TestExtractExtension tests extension extraction.
func TestExtractExtension(t *testing.T) {
	testCases := []struct {
		name       string
		filename   string
		wantResult string
	}{
		{
			name:       "File with extension",
			filename:   "file.txt",
			wantResult: ".txt",
		},
		{
			name:       "File with multiple dots",
			filename:   "file.tar.gz",
			wantResult: ".gz",
		},
		{
			name:       "File without extension",
			filename:   "README",
			wantResult: "",
		},
		{
			name:       "Hidden file",
			filename:   ".gitignore",
			wantResult: ".gitignore", // On Windows, this is treated as extension
		},
		{
			name:       "Empty filename",
			filename:   "",
			wantResult: "",
		},
		{
			name:       "Path with extension",
			filename:   "path/to/file.txt",
			wantResult: ".txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractExtension(tc.filename)
			if result != tc.wantResult {
				t.Errorf("Expected %s, got %s", tc.wantResult, result)
			}
		})
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

// TestParseShardedPath tests extracting file key from sharded path.
func TestParseShardedPath(t *testing.T) {
	testCases := []struct {
		name        string
		shardedPath string
		wantResult  string
	}{
		{
			name:        "No sharding",
			shardedPath: "a1b2c3d4e5f6",
			wantResult:  "a1b2c3d4e5f6",
		},
		{
			name:        "One level sharding",
			shardedPath: filepath.Join("a1", "a1b2c3d4e5f6"),
			wantResult:  "a1b2c3d4e5f6",
		},
		{
			name:        "Two level sharding",
			shardedPath: filepath.Join("a1", "b2", "a1b2c3d4e5f6"),
			wantResult:  "a1b2c3d4e5f6",
		},
		{
			name:        "Three level sharding",
			shardedPath: filepath.Join("a1", "b2", "c3", "a1b2c3d4e5f6"),
			wantResult:  "a1b2c3d4e5f6",
		},
		{
			name:        "Path with extension",
			shardedPath: filepath.Join("a1", "b2", "a1b2c3d4e5f6.pdf"),
			wantResult:  "a1b2c3d4e5f6.pdf",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseShardedPath(tc.shardedPath)
			if result != tc.wantResult {
				t.Errorf("Expected %s, got %s", tc.wantResult, result)
			}
		})
	}
}

// TestIsAbsolute tests absolute path detection.
func TestIsAbsolute(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		wantResult bool
	}{
		{
			name:       "Relative path",
			path:       "file.txt",
			wantResult: false,
		},
		{
			name:       "Relative subdirectory",
			path:       "subdir/file.txt",
			wantResult: false,
		},
		{
			name:       "Windows absolute path",
			path:       "C:\\Windows\\System32",
			wantResult: true,
		},
		{
			name:       "Current directory",
			path:       ".",
			wantResult: false,
		},
		{
			name:       "Parent directory",
			path:       "..",
			wantResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsAbsolute(tc.path)
			if result != tc.wantResult {
				t.Errorf("Expected %v, got %v", tc.wantResult, result)
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
