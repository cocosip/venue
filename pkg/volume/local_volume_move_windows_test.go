//go:build windows

package volume

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// TestIsNotSameDeviceErrno_Windows pins the locale-independent detection of a
// cross-drive rename failure: Windows reports ERROR_NOT_SAME_DEVICE, which Go
// does not map onto syscall.EXDEV and whose message text is localized.
func TestIsNotSameDeviceErrno_Windows(t *testing.T) {
	renameErr := &os.LinkError{
		Op:  "rename",
		Old: `C:\volume\payload.bin`,
		New: `D:\volume\payload.bin`,
		Err: windows.ERROR_NOT_SAME_DEVICE,
	}

	if !isNotSameDeviceErrno(renameErr) {
		t.Error("isNotSameDeviceErrno() = false for ERROR_NOT_SAME_DEVICE")
	}
	if !isCrossDeviceError(renameErr) {
		t.Error("isCrossDeviceError() = false for ERROR_NOT_SAME_DEVICE")
	}

	deniedErr := &os.LinkError{Op: "rename", Old: "src", New: "dst", Err: windows.ERROR_ACCESS_DENIED}
	if isNotSameDeviceErrno(deniedErr) {
		t.Error("isNotSameDeviceErrno() = true for ERROR_ACCESS_DENIED")
	}
	if isCrossDeviceError(deniedErr) {
		t.Error("isCrossDeviceError() = true for ERROR_ACCESS_DENIED")
	}
}

// TestSyncDirectoryIsNoOpOnWindows pins the Windows implementation of the
// directory durability step: Windows cannot flush a directory handle
// (FlushFileBuffers rejects it with ERROR_ACCESS_DENIED), so the write path must
// not issue a syscall that can only fail on every durable write.
func TestSyncDirectoryIsNoOpOnWindows(t *testing.T) {
	dir := t.TempDir()

	if err := syncDirectory(dir); err != nil {
		t.Fatalf("syncDirectory(%q) = %v, want nil on Windows", dir, err)
	}
}

// TestMoveFileCopyFallbackTreatsSourceRemovalFailureAsBestEffort is the
// regression test for the cross-device move that reported failure after its
// destination was already committed: the caller saw an error for a move that had
// happened, and a retry then failed at "destination already exists", so the
// payload could never be moved.
//
// A handle opened without FILE_SHARE_DELETE denies the source deletion while
// still allowing the copy to read it, which is exactly what a reader that still
// has the file open does on Windows.
func TestMoveFileCopyFallbackTreatsSourceRemovalFailureAsBestEffort(t *testing.T) {
	volume, mountPath := createTestVolume(t, 0)

	sourcePath := filepath.Join(mountPath, "src", "payload.bin")
	destinationPath := filepath.Join(mountPath, "dst", "payload.bin")

	writeVolumeFile(t, volume, "src/payload.bin", "move payload")
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0755); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}

	name, err := windows.UTF16PtrFromString(sourcePath)
	if err != nil {
		t.Fatalf("UTF16PtrFromString() error = %v", err)
	}
	handle, err := windows.CreateFile(
		name,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("CreateFile(%q) error = %v", sourcePath, err)
	}
	t.Cleanup(func() {
		if handle != 0 {
			_ = windows.CloseHandle(handle)
		}
	})

	if err := volume.copyFileAndRemoveSource(context.Background(), sourcePath, destinationPath); err != nil {
		t.Fatalf("copyFileAndRemoveSource() = %v, want success for a committed move whose cleanup failed", err)
	}

	if got := readVolumeFile(t, volume, "dst/payload.bin"); got != "move payload" {
		t.Errorf("destination content = %q, want %q", got, "move payload")
	}
	assertVolumeFileExists(t, volume, "src/payload.bin", true)

	// Releasing the handle lets the duplicate be reclaimed, so the leftover is a
	// retryable condition rather than a permanently stuck payload.
	if err := windows.CloseHandle(handle); err != nil {
		t.Fatalf("CloseHandle() error = %v", err)
	}
	handle = 0
	if err := os.Remove(sourcePath); err != nil {
		t.Fatalf("Remove(%q) after releasing the handle error = %v", sourcePath, err)
	}
}
