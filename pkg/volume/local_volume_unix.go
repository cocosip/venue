//go:build !windows

package volume

import (
	"context"
	"fmt"
	"syscall"
)

// probeDiskSpace reads total capacity and available space with one statfs call
// (Unix implementation).
func (v *LocalFileSystemVolume) probeDiskSpace(ctx context.Context) (int64, int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(v.mountPath, &stat); err != nil {
		return 0, 0, fmt.Errorf("failed to get volume capacity: %w", err)
	}

	total := int64(stat.Blocks) * int64(stat.Bsize)
	available := int64(stat.Bavail) * int64(stat.Bsize)
	return total, available, nil
}

// isNotSameDeviceErrno reports a platform-specific cross-device status. Unix
// reports syscall.EXDEV, which the shared cross-device check already recognises,
// so no extra errno applies here.
func isNotSameDeviceErrno(error) bool {
	return false
}
