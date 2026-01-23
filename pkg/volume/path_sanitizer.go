package volume

import (
	"fmt"
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

	// Must not start with ..
	if strings.HasPrefix(cleanRelative, "..") {
		return "", fmt.Errorf("path must not escape parent directory: %s: %w", relativePath, core.ErrPathTraversalAttempt)
	}

	// Join with root path
	fullPath := filepath.Join(s.rootPath, cleanRelative)

	// Verify the path doesn't escape root
	if !s.isWithinRoot(fullPath) {
		return "", fmt.Errorf("path traversal attempt detected: %s: %w", relativePath, core.ErrPathTraversalAttempt)
	}

	return fullPath, nil
}

// ValidateRelativePath validates a relative path without joining it with root.
func (s *PathSanitizer) ValidateRelativePath(relativePath string) error {
	if relativePath == "" {
		return fmt.Errorf("relative path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check for suspicious patterns
	if err := s.checkSuspiciousPatterns(relativePath); err != nil {
		return err
	}

	// Clean path
	cleanPath := filepath.Clean(relativePath)

	// Must not be absolute
	if filepath.IsAbs(cleanPath) {
		return fmt.Errorf("path must be relative, not absolute: %s: %w", relativePath, core.ErrInvalidArgument)
	}

	// Must not start with ..
	if strings.HasPrefix(cleanPath, "..") {
		return fmt.Errorf("path must not escape parent directory: %s: %w", relativePath, core.ErrPathTraversalAttempt)
	}

	return nil
}

// isWithinRoot checks if a path is within the root directory.
func (s *PathSanitizer) isWithinRoot(fullPath string) bool {
	// Get relative path from root
	rel, err := filepath.Rel(s.rootPath, fullPath)
	if err != nil {
		return false
	}

	// Check if relative path escapes (starts with ..)
	if strings.HasPrefix(rel, "..") {
		return false
	}

	return true
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

	// Check for backslash on Unix systems (potential path confusion)
	// On Windows, backslashes are normal, so only check on non-Windows systems
	if runtime.GOOS != "windows" && strings.Contains(path, "\\") {
		// Path contains backslash but we're not on Windows
		// This might be an attempt to bypass sanitization
		return fmt.Errorf("path contains backslash on non-Windows system: %w", core.ErrPathTraversalAttempt)
	}

	return nil
}

// GetRootPath returns the root path.
func (s *PathSanitizer) GetRootPath() string {
	return s.rootPath
}

// IsAbsolute checks if a path is absolute.
func IsAbsolute(path string) bool {
	return filepath.IsAbs(path)
}

// SanitizeFilename sanitizes a filename (without path separators).
// This is useful for sanitizing file keys or original file names.
func SanitizeFilename(filename string) (string, error) {
	if filename == "" {
		return "", fmt.Errorf("filename cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check for path separators
	if strings.ContainsAny(filename, "/\\") {
		return "", fmt.Errorf("filename must not contain path separators: %s: %w", filename, core.ErrInvalidArgument)
	}

	// Check for null bytes
	if strings.Contains(filename, "\x00") {
		return "", fmt.Errorf("filename contains null byte: %w", core.ErrInvalidArgument)
	}

	// Check for control characters
	for _, r := range filename {
		if r < 32 && r != '\t' {
			return "", fmt.Errorf("filename contains control character: %w", core.ErrInvalidArgument)
		}
	}

	// Clean the filename
	clean := filepath.Base(filename)

	// Ensure it's not . or ..
	if clean == "." || clean == ".." {
		return "", fmt.Errorf("filename cannot be '.' or '..': %w", core.ErrInvalidArgument)
	}

	return clean, nil
}

// ExtractExtension extracts file extension from a filename.
// Returns empty string if no extension.
func ExtractExtension(filename string) string {
	if filename == "" {
		return ""
	}

	ext := filepath.Ext(filename)
	return ext
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

// ParseShardedPath parses a sharded path to extract the file key.
func ParseShardedPath(shardedPath string) string {
	// The file key is always the last component
	return filepath.Base(shardedPath)
}
