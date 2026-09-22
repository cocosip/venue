//go:build windows

package volume

import (
	"os"
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
