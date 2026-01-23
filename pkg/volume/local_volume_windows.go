// go:build windows
//go:build windows
// +build windows

package volume

import (
	"context"
	"fmt"

	"golang.org/x/sys/windows"
)

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
