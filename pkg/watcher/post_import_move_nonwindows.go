//go:build !windows

package watcher

// Unix reports cross-device rename as syscall.EXDEV, which the shared
// isCrossDeviceRenameError check handles directly.
func isNotSameDeviceRenameError(error) bool {
	return false
}
