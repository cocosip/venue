//go:build windows

package volume

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// isNotSameDeviceErrno reports whether err is the Windows
// ERROR_NOT_SAME_DEVICE status that a rename across drives returns. Go does not
// map that status onto syscall.EXDEV, so it has to be recognised by value; the
// localized message text cannot be relied on.
func isNotSameDeviceErrno(err error) bool {
	return errors.Is(err, windows.ERROR_NOT_SAME_DEVICE)
}

// TotalCapacity returns the total capacity in bytes (Windows implementation).
func (v *LocalFileSystemVolume) TotalCapacity(ctx context.Context) (int64, error) {
	// Convert path to UTF-16
	pathPtr, err := windows.UTF16PtrFromString(v.mountPath)
	if err != nil {
		return 0, fmt.Errorf("failed to convert path: %w", err)
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
		return 0, fmt.Errorf("GetDiskFreeSpaceEx failed: %w", err)
	}

	return int64(totalBytes), nil
}

// AvailableSpace returns the available space in bytes (Windows implementation).
func (v *LocalFileSystemVolume) AvailableSpace(ctx context.Context) (int64, error) {
	// Convert path to UTF-16
	pathPtr, err := windows.UTF16PtrFromString(v.mountPath)
	if err != nil {
		return 0, fmt.Errorf("failed to convert path: %w", err)
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
		return 0, fmt.Errorf("GetDiskFreeSpaceEx failed: %w", err)
	}

	return int64(freeBytesAvailable), nil
}
