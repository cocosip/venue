//go:build !windows

package volume

import (
	"context"
	"fmt"
	"syscall"
)

// TotalCapacity returns the total capacity in bytes (Unix implementation).
func (v *LocalFileSystemVolume) TotalCapacity(ctx context.Context) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(v.mountPath, &stat); err != nil {
		return 0, fmt.Errorf("failed to get volume capacity: %w", err)
	}

	// Total = blocks * block size
	total := int64(stat.Blocks) * int64(stat.Bsize)
	return total, nil
}

// AvailableSpace returns the available space in bytes (Unix implementation).
func (v *LocalFileSystemVolume) AvailableSpace(ctx context.Context) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(v.mountPath, &stat); err != nil {
		return 0, fmt.Errorf("failed to get available space: %w", err)
	}

	// Available = available blocks * block size
	available := int64(stat.Bavail) * int64(stat.Bsize)
	return available, nil
}
