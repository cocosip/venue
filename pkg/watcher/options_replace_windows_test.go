//go:build windows

package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/logging"

	"golang.org/x/sys/windows"
)

// TestSetEnabledSurvivesTransientOptionsFileLock covers the Windows access
// denied window during options publication: real-time scanners hold the
// freshly written options document open without FILE_SHARE_DELETE, which
// makes the publishing rename fail until the scanner releases the file. The
// lock below is released inside the retry window, so the enable decision must
// still reach the restarted service.
func TestSetEnabledSurvivesTransientOptionsFileLock(t *testing.T) {
	configRoot := t.TempDir()

	newService := func() *BackgroundFileWatcherService {
		t.Helper()

		service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
			FileWatcher:          newStubFileWatcher(),
			Logging:              logging.Disabled(),
			ConfigurationRootDir: configRoot,
		})
		if err != nil {
			t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
		}

		return service
	}

	service := newService()
	service.SetEnabled(false) // Creates the options document.

	optionsPath := filepath.Join(configRoot, fileWatcherOptionsFileName)

	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(optionsPath),
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("open options document without FILE_SHARE_DELETE: %v", err)
	}

	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(25 * time.Millisecond)
		_ = windows.CloseHandle(handle)
	}()

	service.SetEnabled(true)

	<-released

	if _, err := os.Stat(optionsPath); err != nil {
		t.Fatalf("options document missing after publish: %v", err)
	}

	restarted := newService()
	if !restarted.IsEnabled() {
		t.Fatalf("IsEnabled() = false after restart, want the persisted enable decision")
	}
}
