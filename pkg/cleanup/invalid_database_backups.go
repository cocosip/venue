package cleanup

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

const (
	// defaultCorruptedDatabaseRetention is how long a quarantined database is
	// kept when no retention is configured. It mirrors the retention default of
	// the metadata and quota quarantine (pkg/metadata/sqlite_corruption.go,
	// pkg/quota/sqlite_directory_quota_corruption.go).
	defaultCorruptedDatabaseRetention = 72 * time.Hour

	// corruptedDatabaseMarker separates a quarantined database from its live
	// sibling: "<dbFile>.corrupted.<stamp>".
	corruptedDatabaseMarker = ".corrupted."

	// quarantineStampLayout is the UTC stamp of a quarantine name. It is
	// deliberately colon-free because a colon cannot appear in a Windows path
	// segment, and it is identical in both repositories.
	quarantineStampLayout = "20060102T150405Z"

	// quarantineCollisionLimit bounds the numeric collision suffix two
	// quarantines inside one second can add: "<dbFile>.corrupted.<stamp>.<n>".
	quarantineCollisionLimit = 100
)

// quarantinedDatabaseFileNames are the managed database files a quarantine name
// is derived from: the per-tenant metadata and quota databases plus their
// write-ahead log and shared-memory sidecars. A name is only a quarantine when
// its prefix is exactly one of these, which is what keeps a legal tenant
// identifier that merely contains the marker out of the sweep.
var quarantinedDatabaseFileNames = []string{
	"metadata.db",
	"metadata.db-wal",
	"metadata.db-shm",
	"quotas.db",
	"quotas.db-wal",
	"quotas.db-shm",
}

// isQuarantinedDatabaseName reports whether a base name follows the quarantine
// naming "<dbFile>.corrupted.<stamp>", optionally with the numeric collision
// suffix of a second quarantine in the same second.
//
// The check is deliberately strict, because a match is deleted: the prefix
// before the marker must be one of quarantinedDatabaseFileNames and the tail
// must be a parseable stamp. A tenant identifier may contain interior dots, so a
// tenant directory such as "acme.corrupted.prod" contains the marker without
// being a quarantine. Case is ignored, as Locus does
// (StorageCleanupService.cs:2544).
func isQuarantinedDatabaseName(name string) bool {
	_, ok := quarantineStamp(name)
	return ok
}

// quarantineStamp returns the UTC quarantine instant encoded in a quarantine
// base name.
//
// The stamp, not the file's modification time, is the age of a rescue copy: a
// rename preserves the modification time of the database that was damaged, so a
// long-lived database quarantined today would otherwise be swept immediately.
// ok is false for every name that is not a quarantine.
func quarantineStamp(name string) (time.Time, bool) {
	lower := strings.ToLower(name)
	index := strings.Index(lower, corruptedDatabaseMarker)
	if index <= 0 || !slices.Contains(quarantinedDatabaseFileNames, lower[:index]) {
		return time.Time{}, false
	}

	tail := lower[index+len(corruptedDatabaseMarker):]
	// The collision form appends a bounded counter: "<stamp>.<n>".
	if dot := strings.LastIndexByte(tail, '.'); dot > 0 {
		suffix, err := strconv.Atoi(tail[dot+1:])
		if err != nil || suffix < 1 || suffix > quarantineCollisionLimit {
			return time.Time{}, false
		}
		tail = tail[:dot]
	}

	stamp, err := time.Parse(quarantineStampLayout, strings.ToUpper(tail))
	if err != nil {
		return time.Time{}, false
	}
	return stamp.UTC(), true
}

// CleanupInvalidDatabaseBackups removes expired quarantined database files
// ("<dbFile>.corrupted.<stamp>") from the metadata and quota database trees.
//
// Only the managed database files and their sidecars are recognized, and only
// when the trailing stamp parses: a tenant identifier may legally contain the
// marker, so a tenant directory is never a candidate and is never removed. An
// entry is removed when the quarantine instant encoded in its name is older than
// the retention; a name whose stamp cannot be read is left alone. A zero
// CorruptedDatabaseRetention selects the runtime default of 72 hours; a negative
// value disables the sweep and returns an empty statistic. Removals are counted
// in InvalidDatabaseBackupsRemoved and their bytes in SpaceFreed.
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

		size, sizeErr := quarantinedEntrySize(entry.path)
		if sizeErr != nil {
			// Already removed with an ancestor, or unreadable: best-effort.
			continue
		}
		if removeErr := os.Remove(entry.path); removeErr != nil {
			s.emit(ctx, slog.LevelWarn, "invalid_database_backup_delete_failed",
				"Failed to remove an expired quarantined database", errorTypeAttr(removeErr))
			continue
		}

		removed++
		freed += size
	}

	return removed, freed, nil
}

// expiredQuarantinesInRoot collects the quarantine-shaped files below root whose
// quarantine instant is older than cutoff.
//
// The age is read from the stamp in the name, never from the modification time:
// a quarantine is a rename, which preserves the modification time of the
// database that was damaged, so a long-lived database quarantined today would
// otherwise be swept on the next cycle and its rescue copy lost.
//
// Directories are never candidates. A quarantine object is the database file
// renamed aside, while a directory that happens to carry the quarantine name can
// only be a tenant directory whose (legal) identifier contains the marker, and
// deleting it would destroy live data.
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
		if info == nil || path == root || info.IsDir() {
			return nil
		}

		quarantinedAt, ok := quarantineStamp(info.Name())
		if !ok || !quarantinedAt.Before(cutoff) {
			return nil
		}

		entries = append(entries, quarantinedEntry{
			path:  path,
			depth: pathDepth(path),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	return entries, nil
}

// quarantinedEntrySize returns the bytes a quarantined database file occupies.
func quarantinedEntrySize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
