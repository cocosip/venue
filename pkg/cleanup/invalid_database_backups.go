package cleanup

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

const (
	// defaultCorruptedDatabaseRetention is how long a quarantined database
	// directory is kept when no retention is configured. It mirrors the runtime
	// default of the metadata quarantine (pkg/metadata/corrupted_database.go).
	defaultCorruptedDatabaseRetention = 72 * time.Hour

	// corruptedDatabaseMarker separates a quarantined database from its live
	// sibling: "<dbDir>.corrupted.<stamp>".
	corruptedDatabaseMarker = ".corrupted."
)

// isQuarantinedDatabaseName reports whether a base name follows the quarantine
// naming "<dbDir>.corrupted.<stamp>": both the database name and the stamp must
// be present, and the marker is matched case-insensitively like Locus does
// (StorageCleanupService.cs:2544).
func isQuarantinedDatabaseName(name string) bool {
	index := strings.Index(strings.ToLower(name), corruptedDatabaseMarker)
	if index <= 0 {
		return false
	}
	return index+len(corruptedDatabaseMarker) < len(name)
}

// CleanupInvalidDatabaseBackups removes expired quarantined database directories
// ("<dbDir>.corrupted.<stamp>") from the metadata and quota database trees.
//
// An entry is removed when its modification time is older than the retention. A
// zero CorruptedDatabaseRetention selects the runtime default of 72 hours; a
// negative value disables the sweep and returns an empty statistic. Removals are
// counted in InvalidDatabaseBackupsRemoved and their bytes in SpaceFreed.
//
// The sweep is best-effort: an unavailable root, an unreadable directory or a
// locked quarantine never fails the rest of the sweep.
func (s *cleanupService) CleanupInvalidDatabaseBackups(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if ctx == nil {
		ctx = context.Background()
	}

	retention := s.corruptedDatabaseRetention
	if retention < 0 {
		// A negative retention disables the sweep entirely.
		return stats, nil
	}
	if retention == 0 {
		retention = defaultCorruptedDatabaseRetention
	}
	cutoff := time.Now().Add(-retention)

	for _, root := range s.databaseScanRoots() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		removed, freed, err := s.cleanupInvalidDatabaseBackupsInRoot(ctx, root, cutoff)
		stats.InvalidDatabaseBackupsRemoved += removed
		stats.SpaceFreed += freed
		if err != nil {
			return stats, err
		}
	}

	return stats, nil
}

// databaseScanRoots returns the configured, de-duplicated quarantine roots.
func (s *cleanupService) databaseScanRoots() []string {
	roots := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)

	for _, root := range []string{s.metadataDirectory, s.quotaDirectory} {
		if root == "" {
			continue
		}
		clean := filepath.Clean(root)
		if _, duplicate := seen[clean]; duplicate {
			continue
		}
		seen[clean] = struct{}{}
		roots = append(roots, clean)
	}

	return roots
}

// quarantinedEntry is one expired quarantine candidate found by the scan.
type quarantinedEntry struct {
	path  string
	isDir bool
	depth int
}

// cleanupInvalidDatabaseBackupsInRoot removes the expired quarantines below one
// database root.
//
// An unavailable root is logged and skipped rather than reported, so one broken
// tree cannot fail the sweep; only context cancellation is returned as an error.
func (s *cleanupService) cleanupInvalidDatabaseBackupsInRoot(ctx context.Context, root string, cutoff time.Time) (int, int64, error) {
	entries, err := s.expiredQuarantinesInRoot(ctx, root, cutoff)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, 0, ctxErr
		}
		s.emit(ctx, slog.LevelWarn, "invalid_database_backup_scan_failed",
			"Failed to scan a database root for expired quarantines", errorTypeAttr(err))
		return 0, 0, nil
	}

	// Deepest paths first: a candidate nested inside another candidate is removed
	// (and its bytes counted) before its parent, so no byte is counted twice.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].depth > entries[j].depth
	})

	removed := 0
	var freed int64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return removed, freed, err
		}

		size, sizeErr := quarantinedEntrySize(entry.path, entry.isDir)
		if sizeErr != nil {
			// Already removed with an ancestor, or unreadable: best-effort.
			continue
		}
		if removeErr := os.RemoveAll(entry.path); removeErr != nil {
			s.emit(ctx, slog.LevelWarn, "invalid_database_backup_delete_failed",
				"Failed to remove an expired quarantined database", errorTypeAttr(removeErr))
			continue
		}

		removed++
		freed += size
	}

	return removed, freed, nil
}

// expiredQuarantinesInRoot collects the quarantine-shaped entries below root
// whose modification time is older than cutoff.
func (s *cleanupService) expiredQuarantinesInRoot(ctx context.Context, root string, cutoff time.Time) ([]quarantinedEntry, error) {
	entries := make([]quarantinedEntry, 0)

	walkErr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			if path == root {
				return err
			}
			return nil
		}
		if info == nil || path == root {
			return nil
		}
		if !isQuarantinedDatabaseName(info.Name()) {
			return nil
		}
		if !info.ModTime().Before(cutoff) {
			return nil
		}

		entries = append(entries, quarantinedEntry{
			path:  path,
			isDir: info.IsDir(),
			depth: pathDepth(path),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	return entries, nil
}

// quarantinedEntrySize returns the bytes an entry occupies: a file's own size, or
// the total size of the files inside a quarantined database directory.
func quarantinedEntrySize(path string, isDir bool) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if !isDir {
		return info.Size(), nil
	}

	var total int64
	walkErr := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info == nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	if walkErr != nil {
		return total, walkErr
	}

	return total, nil
}
