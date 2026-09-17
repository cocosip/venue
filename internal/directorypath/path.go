// Package directorypath normalizes logical storage-directory identifiers.
package directorypath

import "strings"

// Normalize returns a rooted, slash-separated logical directory path.
func Normalize(value string) string {
	normalized := strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
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
