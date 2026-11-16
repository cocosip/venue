//go:build windows

package volume

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// syncDirectory is a no-op on Windows.
//
// A directory name is made durable by the filesystem when the handle that
// created the entry is closed, and Windows exposes no directory-fsync operation:
// os.Open on a directory needs FILE_FLAG_BACKUP_SEMANTICS and
// FlushFileBuffers rejects a directory handle with ERROR_ACCESS_DENIED. Calling
// through to the Unix implementation would therefore issue a syscall that can
// only fail, on every durable write; the caller treats a directory sync failure
// as a lost optimization anyway, so the honest implementation is this no-op.
// File contents are still synced by WriteFile through its file handle.
func syncDirectory(string) error {
	return nil
}

// isNotSameDeviceErrno reports whether err is the Windows
// ERROR_NOT_SAME_DEVICE status that a rename across drives returns. Go does not
// map that status onto syscall.EXDEV, so it has to be recognised by value; the
// localized message text cannot be relied on.
func isNotSameDeviceErrno(err error) bool {
	return errors.Is(err, windows.ERROR_NOT_SAME_DEVICE)
}

// probeDiskSpace reads total capacity and available space with one
// GetDiskFreeSpaceEx call (Windows implementation).
func (v *LocalFileSystemVolume) probeDiskSpace(ctx context.Context) (int64, int64, error) {
	// Convert path to UTF-16
	pathPtr, err := windows.UTF16PtrFromString(v.mountPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to convert path: %w", err)
	}

	var freeBytesAvailable uint64
	var totalBytes uint64
	var totalFreeBytes uint64

	// Call Windows API
	err = windows.GetDiskFreeSpaceEx(
		pathPtr,
		&freeBytesAvailable,
		&totalBytes,
		&totalFreeBytes,
	)

	if err != nil {
		return 0, 0, fmt.Errorf("GetDiskFreeSpaceEx failed: %w", err)
	}

	return int64(totalBytes), int64(freeBytesAvailable), nil
}
