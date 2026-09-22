package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// boolPointer returns a pointer to value, so a test can express an explicit
// "unset" (nil) as distinct from an explicit false.
func boolPointer(value bool) *bool {
	return &value
}

// --- InitialEnabled at construction ----------------------------------------

func TestBackgroundFileWatcherService_InitialEnabled(t *testing.T) {
	tests := []struct {
		name    string
		initial *bool
		want    bool
	}{
		{name: "nil keeps the default-safe enabled state", initial: nil, want: true},
		{name: "true starts enabled", initial: boolPointer(true), want: true},
		{name: "false starts disabled", initial: boolPointer(false), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := newServiceWithState(t, func(opts *BackgroundFileWatcherServiceOptions) {
				opts.InitialEnabled = test.initial
			})

			if got := service.IsEnabled(); got != test.want {
				t.Fatalf("IsEnabled() = %v, want %v", got, test.want)
			}
			if got := service.Options().Enabled; got != test.want {
				t.Fatalf("Options().Enabled = %v, want %v", got, test.want)
			}
		})
	}
}

// TestBackgroundFileWatcherService_InitialEnabledFalseIsNotPersisted pins the
// interaction between the two inputs: construction-time input is a default, not
// an operator decision of record, so only SetEnabled/UpdateOptions write the
// options document.
func TestBackgroundFileWatcherService_InitialEnabledFalseIsNotPersisted(t *testing.T) {
	configRoot := t.TempDir()

	newService := func(initial *bool) *BackgroundFileWatcherService {
		t.Helper()

		service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
			FileWatcher:          newStubFileWatcher(),
			Logging:              logging.Disabled(),
			ConfigurationRootDir: configRoot,
			InitialEnabled:       initial,
		})
		if err != nil {
			t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
		}

		return service
	}

	if newService(boolPointer(false)).IsEnabled() {
		t.Fatalf("IsEnabled() = true, want the construction-time disable to apply")
	}

	if _, err := os.Stat(filepath.Join(configRoot, fileWatcherOptionsFileName)); !os.IsNotExist(err) {
		t.Fatalf("InitialEnabled was persisted as an operator decision: %v", err)
	}

	if !newService(nil).IsEnabled() {
		t.Fatalf("IsEnabled() = false, want the default-safe state after a restart without an operator decision")
	}
}

// TestBackgroundFileWatcherService_PersistedDocumentOutranksInitialEnabled pins
// the documented precedence: the persisted options document wins in both
// directions.
func TestBackgroundFileWatcherService_PersistedDocumentOutranksInitialEnabled(t *testing.T) {
	configRoot := t.TempDir()

	newService := func(initial *bool) *BackgroundFileWatcherService {
		t.Helper()

		service, err := NewBackgroundFileWatcherService(&BackgroundFileWatcherServiceOptions{
			FileWatcher:          newStubFileWatcher(),
			Logging:              logging.Disabled(),
			ConfigurationRootDir: configRoot,
			InitialEnabled:       initial,
		})
		if err != nil {
			t.Fatalf("NewBackgroundFileWatcherService() error = %v", err)
		}

		return service
	}

	// The operator disables the service at runtime: the decision is persisted.
	operator := newService(nil)
	operator.SetEnabled(false)

	if newService(boolPointer(true)).IsEnabled() {
		t.Fatalf("IsEnabled() = true, want the persisted disable to outrank InitialEnabled=true")
	}

	// And the reverse: a persisted enable outranks a construction-time disable.
	operator.SetEnabled(true)

	if !newService(boolPointer(false)).IsEnabled() {
		t.Fatalf("IsEnabled() = false, want the persisted enable to outrank InitialEnabled=false")
	}
}

// TestBackgroundFileWatcherService_InitialEnabledFalseDoesNotScan proves the
// override reaches the run loop: an operator who writes enabled: false gets no
// scanning at all.
func TestBackgroundFileWatcherService_InitialEnabledFalseDoesNotScan(t *testing.T) {
	watcher := newStubFileWatcher(&core.FileWatcherConfiguration{
		WatcherID: "w1",
		Enabled:   true,
	})

	service := newBackgroundService(t, watcher, func(opts *BackgroundFileWatcherServiceOptions) {
		opts.InitialEnabled = boolPointer(false)
	})

	if service.IsEnabled() {
		t.Fatalf("IsEnabled() = true, want the construction-time disable")
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	time.Sleep(80 * time.Millisecond)

	if scanned := watcher.scannedIDs(); len(scanned) != 0 {
		t.Fatalf("scanned = %v, want nothing while the service was constructed disabled", scanned)
	}

	// SetEnabled keeps its existing runtime semantics: the override is only the
	// construction-time default.
	service.SetEnabled(true)

	if !service.IsEnabled() {
		t.Fatalf("IsEnabled() = false after SetEnabled(true)")
	}
}
