package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	// watcherStateFileName is the persisted runtime enable/disable decision,
	// keyed by watcher ID, under FileWatcherOptions.ConfigurationRootDir.
	watcherStateFileName = "watcher-state.json"

	// watcherStateTempFilePattern names the staging file for the atomic state
	// write. The "*" makes os.CreateTemp generate a unique name in the same
	// directory as the target, which is required for an atomic rename.
	watcherStateTempFilePattern = "watcher-state-*.tmp"

	// watcherStateVersion is the schema version of the persisted state document.
	// A document with any other version is treated as unreadable state.
	watcherStateVersion = 1
)

// watcherStateDocument is the persisted runtime enable/disable decision for
// watcher IDs.
//
// It deliberately stores no watcher configuration: configuration belongs to the
// application entry point, so only the operator's runtime decision is persisted
// here. Because the key space is watcher IDs, the document cannot grow with the
// number of imports or scans.
type watcherStateDocument struct {
	// Version is the document schema version.
	Version int `json:"version"`

	// Watchers maps a watcher ID to its persisted enabled flag.
	Watchers map[string]bool `json:"watchers"`
}

// loadWatcherState reads the persisted runtime enable/disable decisions.
//
// A missing file is normal and yields no state. A corrupt, unreadable, or
// unknown-version document is reported to the caller so it can warn and
// continue: persisted runtime state must never prevent the watcher from
// starting.
func (w *fileWatcher) loadWatcherState() error {
	statePath := filepath.Join(w.configRoot, watcherStateFileName)

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed to read watcher state: %w", err)
	}

	var document watcherStateDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("failed to parse watcher state: %w", err)
	}

	if document.Version != watcherStateVersion {
		return fmt.Errorf("unsupported watcher state version %d", document.Version)
	}

	if len(document.Watchers) == 0 {
		return nil
	}

	// A blank key can never be a registered watcher ID, so it is dropped instead
	// of becoming unreachable state.
	state := make(map[string]bool, len(document.Watchers))
	for watcherID, enabled := range document.Watchers {
		if watcherID == "" {
			continue
		}

		state[watcherID] = enabled
	}

	w.watcherStateMu.Lock()
	w.watcherState = state
	w.watcherStateMu.Unlock()

	return nil
}

// persistedEnabled returns the persisted runtime decision for a watcher ID and
// whether one exists.
func (w *fileWatcher) persistedEnabled(watcherID string) (bool, bool) {
	w.watcherStateMu.Lock()
	defer w.watcherStateMu.Unlock()

	enabled, ok := w.watcherState[watcherID]

	return enabled, ok
}

// recordWatcherState stores one runtime decision and persists the state file.
//
// Persisting is best-effort by design: the state file records a runtime
// decision for one watcher ID, not configuration of record, so a write failure
// keeps the in-memory decision, logs a warning, and never fails the caller's
// Enable/Disable operation.
func (w *fileWatcher) recordWatcherState(ctx context.Context, watcherID string, enabled bool) {
	w.watcherStateMu.Lock()

	if w.watcherState == nil {
		w.watcherState = make(map[string]bool)
	}

	w.watcherState[watcherID] = enabled
	err := w.saveWatcherStateLocked()
	w.watcherStateMu.Unlock()

	if err != nil {
		w.emit(ctx, slog.LevelWarn, "watcher_state_save_failed", "Failed to persist watcher runtime state",
			slog.String("watcher_id", watcherID), errorTypeAttr(err))
	}
}

// removeWatcherState drops one watcher's persisted decision and persists the
// state file.
//
// It is best-effort for the same reason as recordWatcherState: the in-memory
// unregistration has already happened, so a failed write must not turn a
// successful unregister into an error. The stale entry is only a runtime
// decision and is replaced the next time the watcher is unregistered.
func (w *fileWatcher) removeWatcherState(ctx context.Context, watcherID string) {
	w.watcherStateMu.Lock()

	if w.watcherState == nil {
		w.watcherStateMu.Unlock()

		return
	}

	if _, ok := w.watcherState[watcherID]; !ok {
		w.watcherStateMu.Unlock()

		return
	}

	delete(w.watcherState, watcherID)
	err := w.saveWatcherStateLocked()
	w.watcherStateMu.Unlock()

	if err != nil {
		w.emit(ctx, slog.LevelWarn, "watcher_state_save_failed", "Failed to persist watcher runtime state",
			slog.String("watcher_id", watcherID), errorTypeAttr(err))
	}
}

// saveWatcherStateLocked writes the state document atomically.
//
// The document is staged in a unique temp file in the same directory, synced,
// closed, and then renamed over the target: a reader therefore never observes a
// partial document, and every failure path removes the staging file so no
// leftover remains. The caller must hold watcherStateMu.
func (w *fileWatcher) saveWatcherStateLocked() error {
	document := watcherStateDocument{
		Version:  watcherStateVersion,
		Watchers: make(map[string]bool, len(w.watcherState)),
	}

	for watcherID, enabled := range w.watcherState {
		document.Watchers[watcherID] = enabled
	}

	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode watcher state: %w", err)
	}

	statePath := filepath.Join(w.configRoot, watcherStateFileName)

	temp, err := os.CreateTemp(w.configRoot, watcherStateTempFilePattern)
	if err != nil {
		return fmt.Errorf("failed to create watcher state staging file: %w", err)
	}

	tempPath := temp.Name()

	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to write watcher state staging file: %w", err)
	}

	// Sync before publishing so a crash after the rename cannot leave an empty
	// document behind.
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to sync watcher state staging file: %w", err)
	}

	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to close watcher state staging file: %w", err)
	}

	if err := os.Rename(tempPath, statePath); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to publish watcher state: %w", err)
	}

	return nil
}
