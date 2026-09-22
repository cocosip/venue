//go:build windows

package watcher

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isNotSameDeviceRenameError recognizes the locale-independent Windows status
// returned when a rename crosses drive or volume boundaries.
func isNotSameDeviceRenameError(err error) bool {
	return errors.Is(err, windows.ERROR_NOT_SAME_DEVICE)
}
