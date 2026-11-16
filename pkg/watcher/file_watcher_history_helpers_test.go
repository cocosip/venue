package watcher

import "time"

// The two helpers below have no production caller; they exist so focused history
// tests can seed and inspect the in-memory de-duplication view directly instead
// of going through a full scan.

// storeImportedRecordSync records a simple non-pending entry without scheduling a
// history write.
func (w *fileWatcher) storeImportedRecordSync(filePath, fingerprint string, importedAt time.Time) {
	w.storeImportedStateLoaded(filePath, importedFileRecord{
		Fingerprint:    fingerprint,
		ImportedAtUnix: importedAt.Unix(),
	})
}

// importedEntryCount returns the current number of history entries.
func (w *fileWatcher) importedEntryCount() int {
	w.importedFilesMu.Lock()
	defer w.importedFilesMu.Unlock()

	return w.importedCount
}
