//go:build windows

package watcher

import "golang.org/x/sys/windows"

// replaceFile atomically publishes a file, replacing an existing destination
// on Windows where os.Rename does not replace an existing file.
func replaceFile(sourcePath string, targetPath string) error {
	source, err := windows.UTF16PtrFromString(sourcePath)
	if err != nil {
		return err
	}
	target, err := windows.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(source, target, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
