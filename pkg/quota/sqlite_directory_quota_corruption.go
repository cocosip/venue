package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/sqlite"
)

// Quarantine naming and retention policy of the SQLite directory-quota store.
//
// The values match the metadata store's quarantine naming
// (corruptedDatabaseSuffix, corruptedDatabaseTimestampLayout, ...): the
// quarantined object is a file rather than a Badger directory, but an operator
// must be able to recognize a rescue copy in either tree with the same rules.
const (
	// quotaCorruptedDatabaseSuffix marks a quarantined sibling of a quota
	// database file.
	quotaCorruptedDatabaseSuffix = ".corrupted."

	// quotaCorruptedDatabaseTimestampLayout renders the quarantine timestamp. It
	// is deliberately colon-free: a colon cannot appear in a Windows path
	// segment.
	quotaCorruptedDatabaseTimestampLayout = "20060102T150405Z"

	// quotaCorruptedDatabasePathCollisionLimit bounds the search for a free
	// quarantine name when two quarantines land in the same second.
	quotaCorruptedDatabasePathCollisionLimit = 100

	// defaultQuotaCorruptedDatabaseRetention is how long a quarantined quota
	// database is kept when the caller does not choose a retention.
	defaultQuotaCorruptedDatabaseRetention = 72 * time.Hour

	// quotaWalSuffix and quotaShmSuffix name the write-ahead log and shared-memory
	// sidecars of a WAL-mode database. They describe the state of the file that was
	// just moved aside, so a quarantine must handle them explicitly.
	quotaWalSuffix = "-wal"
	quotaShmSuffix = "-shm"

	// quotaIntegrityCheckSQL is the verdict the open path runs on a freshly opened
	// file. The limit of 1 keeps it bounded.
	quotaIntegrityCheckSQL = "PRAGMA integrity_check(1)"
)

// errQuotaDatabaseCorrupt marks a positive corruption verdict that
// sqlite.IsCorruptionError cannot recognise on its own.
//
// A `PRAGMA integrity_check(1)` verdict of anything other than "ok" is corruption
// by definition, but the reported text ("Page 4 is never used", ...) is not one of
// SQLite's canonical corruption messages. The sentinel carries that verdict
// through the classification chain without weakening
// sqlite.IsCorruptionError's conservative rule that a lock, a permission failure
// or an unknown error is never treated as corruption.
var errQuotaDatabaseCorrupt = errors.New("directory quota database is corrupted")

// isQuotaDatabaseCorruptError reports whether err is a positive corruption
// verdict: either the shared classifier recognised a damaged file, or the
// integrity verdict was "not ok".
//
// Quarantine is destructive, so nothing else may be classified as corruption.
func isQuotaDatabaseCorruptError(err error) bool {
	return errors.Is(err, errQuotaDatabaseCorrupt) || sqlite.IsCorruptionError(err)
}

// verifyQuotaDatabase runs the integrity verdict on a freshly opened handle.
//
// sqlite.IntegrityCheck establishes the verdict, but it fails both when the
// verdict is "not ok" and when the check itself could not run. The statement is
// therefore re-run once for a failure the shared classifier cannot recognise, so
// a lock, permission or IO failure stays infrastructure instead of triggering a
// destructive quarantine.
func verifyQuotaDatabase(ctx context.Context, db *sql.DB) error {
	err := sqlite.IntegrityCheck(ctx, db)
	if err == nil {
		return nil
	}
	if sqlite.IsCorruptionError(err) {
		return err
	}

	var reported string
	if probeErr := db.QueryRowContext(ctx, quotaIntegrityCheckSQL).Scan(&reported); probeErr != nil {
		// The check could not run at all: report the original failure unchanged.
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(reported), "ok") {
		return errors.Join(err, errQuotaDatabaseCorrupt)
	}
	return nil
}

// quarantineQuotaDatabaseFile renames a corrupted database file to a timestamped
// sibling and returns the new path.
//
// The stamp is colon-free (quotaCorruptedDatabaseTimestampLayout), because a colon
// cannot appear in a Windows path segment, and same-second collisions append a
// bounded numeric suffix so a quarantine can never overwrite an earlier rescue
// copy.
func quarantineQuotaDatabaseFile(path string, now time.Time) (string, error) {
	base := path + quotaCorruptedDatabaseSuffix + now.UTC().Format(quotaCorruptedDatabaseTimestampLayout)

	target := base
	for suffix := 1; ; suffix++ {
		_, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return "", err
		}
		if suffix > quotaCorruptedDatabasePathCollisionLimit {
			return "", fmt.Errorf("cannot find a free quarantine path for %s", path)
		}
		target = fmt.Sprintf("%s.%d", base, suffix)
	}

	if err := os.Rename(path, target); err != nil {
		return "", err
	}
	return target, nil
}

// removeQuotaDatabaseSidecars moves the sidecars of a quarantined quota database
// aside, and removes them when they cannot be moved.
//
// They belong to the file that was just quarantined: leaving them behind would let
// SQLite replay a foreign write-ahead log into the empty database that replaces
// it. The sidecars are deliberately quarantined rather than deleted so an operator
// keeps every artifact of the damaged store.
//
// It is best-effort by design: the database file has already been moved aside, and
// the sidecars are usually removed by the clean close that preceded the
// quarantine, so a leftover sidecar must not stop the recovery.
func removeQuotaDatabaseSidecars(path string) {
	for _, suffix := range []string{quotaWalSuffix, quotaShmSuffix} {
		sidecar := path + suffix
		if _, err := os.Lstat(sidecar); err != nil {
			continue
		}
		if _, err := quarantineQuotaDatabaseFile(sidecar, time.Now().UTC()); err == nil {
			continue
		}
		_ = os.Remove(sidecar)
	}
}

// pruneQuotaCorruptedDatabaseFiles removes quarantine files of path that outlived
// the retention.
//
// It is best-effort by design: a quarantined file is a rescue copy, and failing to
// prune it must never stop a database from opening. A negative retention disables
// pruning entirely; zero selects the default.
func pruneQuotaCorruptedDatabaseFiles(path string, retention time.Duration, now func() time.Time) {
	if retention < 0 {
		return
	}
	if retention == 0 {
		retention = defaultQuotaCorruptedDatabaseRetention
	}

	parent := filepath.Dir(path)
	prefix := filepath.Base(path) + quotaCorruptedDatabaseSuffix
	cutoff := now().Add(-retention)

	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	for _, entry := range entries {
		// The quarantine object is a file, not a directory: only the plain
		// database file is renamed, so the IsDir test is inverted compared with
		// the Badger quarantine of a database directory.
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(parent, entry.Name()))
	}
}
