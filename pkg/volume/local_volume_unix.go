//go:build !windows

package volume

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

// syncDirectory flushes a directory's entries so a newly created file name is
// durable, not just its contents.
//
// It is best-effort by contract: the caller treats a failure as a lost
// optimization, because not every filesystem supports fsync on a directory, and
// the file contents were already synced.
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()

	return directory.Sync()
}

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
