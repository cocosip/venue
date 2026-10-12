//go:build windows

package watcher

import (
	"errors"
	"time"

	"golang.org/x/sys/windows"
)

// replaceRetryAttempts bounds the retry window for transient sharing
// violations, with backoff of 5ms..80ms (about 155ms in total).
const replaceRetryAttempts = 6

// replaceFile atomically publishes a file, replacing an existing destination
// on Windows where os.Rename does not replace an existing file.
//
// MoveFileEx reports ERROR_ACCESS_DENIED when another process holds the
// destination open without FILE_SHARE_DELETE. Real-time virus scanners and
// search indexers do this for a short window after the destination was
// written, so the denial is transient; the retry mirrors the standard
// library's treatment of Windows sharing violations.
func replaceFile(sourcePath string, targetPath string) error {
	source, err := windows.UTF16PtrFromString(sourcePath)
	if err != nil {
		return err
	}
	target, err := windows.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}

	delay := 5 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err = windows.MoveFileEx(source, target, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
		if err == nil {
			return nil
		}

		if attempt >= replaceRetryAttempts || !isTransientReplaceError(err) {
			return err
		}

		time.Sleep(delay)
		delay *= 2
	}
}

// isTransientReplaceError reports whether the MoveFileEx failure may resolve
// on its own once an external holder releases the file.
func isTransientReplaceError(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}
