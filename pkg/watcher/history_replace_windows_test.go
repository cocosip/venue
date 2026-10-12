//go:build windows

package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// TestReplaceFileRetriesTransientDestinationLock covers the Windows access
// denied window during history publication: real-time scanners open a freshly
// written destination without FILE_SHARE_DELETE, which makes MoveFileEx fail
// until the scanner releases the file. The lock below is released inside the
// retry window, so replaceFile must eventually succeed instead of surfacing
// the transient denial to the caller.
func TestReplaceFileRetriesTransientDestinationLock(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, importedFilesHistoryTempFileName)
	target := filepath.Join(dir, importedFilesHistoryFileName)

	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	if err := os.WriteFile(source, []byte("new"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(target),
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("open destination without FILE_SHARE_DELETE: %v", err)
	}

	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(25 * time.Millisecond)
		_ = windows.CloseHandle(handle)
	}()

	err = replaceFile(source, target)

	<-released

	if err != nil {
		t.Fatalf("replaceFile after transient lock: %v", err)
	}

	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read published destination: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("published content = %q, want %q", data, "new")
	}
}
