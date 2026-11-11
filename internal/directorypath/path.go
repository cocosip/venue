// Package directorypath normalizes logical storage-directory identifiers.
package directorypath

import "strings"

// Normalize returns a rooted, slash-separated logical directory path.
//
// The common cases are allocation free: the root and a path that is already
// normalized are returned as-is, so callers that pass a canonical path do not
// pay for segment splitting and joining on every write.
func Normalize(value string) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if normalized == "/" {
		return "/"
	}
	if isNormalized(normalized) {
		return normalized
	}

	segments := strings.Split(normalized, "/")
	collapsed := make([]string, 0, len(segments))
	for _, segment := range segments {
		switch segment {
		case "", ".":
			continue
		case "..":
			if len(collapsed) > 0 {
				collapsed = collapsed[:len(collapsed)-1]
			}
		default:
			collapsed = append(collapsed, segment)
		}
	}
	if len(collapsed) == 0 {
		return "/"
	}
	return "/" + strings.Join(collapsed, "/")
}

// isNormalized reports whether a trimmed, slash-separated path is already in
// canonical form: rooted, without empty, "." or ".." segments, and without a
// trailing separator.
func isNormalized(path string) bool {
	return strings.HasPrefix(path, "/") &&
		!strings.HasSuffix(path, "/") &&
		!strings.Contains(path, "//") &&
		!strings.Contains(path, "/./") &&
		!strings.Contains(path, "/../") &&
		!strings.HasSuffix(path, "/.") &&
		!strings.HasSuffix(path, "/..")
}
