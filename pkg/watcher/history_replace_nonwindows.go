//go:build !windows

package watcher

import "os"

// replaceFile atomically publishes a file on platforms where rename replaces
// an existing destination.
func replaceFile(sourcePath string, targetPath string) error {
	return os.Rename(sourcePath, targetPath)
}
