package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

const (
	// tenantDirectoryCacheTTL bounds how stale the snapshot of tenant
	// directories below DataPath may be. Enumerating directories is cheap but not
	// free, and a backup or maintenance sweep runs once per cycle rather than once
	// per row.
	tenantDirectoryCacheTTL = 30 * time.Second
)

// quarantineDatabaseFile renames a corrupted database file to a timestamped
// sibling and returns the new path.
//
// The stamp is colon-free (corruptedDatabaseTimestampLayout), because a colon
// cannot appear in a Windows path segment, and same-second collisions append a
// bounded numeric suffix so a quarantine can never overwrite an earlier rescue
// copy.
func quarantineDatabaseFile(path string, now time.Time) (string, error) {
	target, err := freeQuarantinePath(path, now)
	if err != nil {
		return "", err
	}
	if err := os.Rename(path, target); err != nil {
		return "", err
	}
	return target, nil
}

// freeQuarantinePath picks the quarantine name for path without touching the
// file system beyond existence probes.
func freeQuarantinePath(path string, now time.Time) (string, error) {
	base := path + corruptedDatabaseSuffix + now.UTC().Format(corruptedDatabaseTimestampLayout)

	target := base
	for suffix := 1; ; suffix++ {
		_, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			return target, nil
		}
		if err != nil {
			return "", err
		}
		if suffix > corruptedDatabasePathCollisionLimit {
			return "", fmt.Errorf("cannot find a free quarantine path for %s", path)
		}
		target = fmt.Sprintf("%s.%d", base, suffix)
	}
}

// removeDatabaseSidecars moves the write-ahead log and shared-memory files of a
// quarantined database aside. They describe the state of the file that was just
// quarantined, so leaving them behind would let SQLite replay a foreign WAL into
// the fresh database that replaces it.
//
// The sidecars are quarantined alongside the database, so an operator keeps every
// artifact of the damaged store, and are only removed when they cannot be moved.
// now is the repository clock, so the quarantine names stay deterministic under
// an injected clock.
func removeDatabaseSidecars(path string, now func() time.Time) {
	for _, suffix := range []string{walSuffix, shmSuffix} {
		sidecar := path + suffix
		if _, err := os.Lstat(sidecar); err != nil {
			continue
		}
		if _, err := quarantineDatabaseFile(sidecar, now()); err == nil {
			continue
		}
		_ = os.Remove(sidecar)
	}
}

// pruneCorruptedDatabaseFiles removes quarantine files of path that outlived the
// retention.
//
// It is best-effort by design: a quarantined file is a rescue copy, and failing
// to prune it must never stop a database from opening. A negative retention
// disables pruning entirely; zero selects the default.
func pruneCorruptedDatabaseFiles(path string, retention time.Duration, now func() time.Time) {
	if retention < 0 {
		return
	}
	if retention == 0 {
		retention = defaultCorruptedDatabaseRetention
	}

	parent := filepath.Dir(path)
	prefix := filepath.Base(path) + corruptedDatabaseSuffix
	cutoff := now().Add(-retention)

	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	for _, entry := range entries {
		// The quarantine object is a file, not a directory: only the plain
		// database file and its sidecars are renamed, so the IsDir test is
		// inverted compared with the Badger quarantine of a database directory.
		// A sidecar of the main database is included because it carries the same
		// base name.
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if !isCorruptedDatabaseFileName(entry.Name()) && !isCorruptedDatabaseSidecarName(entry.Name()) {
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

// isCorruptedDatabaseFileName reports whether name is a quarantine sibling of
// the metadata database itself.
func isCorruptedDatabaseFileName(name string) bool {
	return !strings.ContainsRune(name, filepath.Separator) &&
		strings.HasPrefix(name, metadataDatabaseFileName+corruptedDatabaseSuffix)
}

// isCorruptedDatabaseSidecarName reports whether name is a quarantined
// write-ahead log or shared-memory sidecar of the metadata database.
func isCorruptedDatabaseSidecarName(name string) bool {
	if strings.ContainsRune(name, filepath.Separator) {
		return false
	}
	for _, suffix := range []string{walSuffix, shmSuffix} {
		if strings.HasPrefix(name, metadataDatabaseFileName+suffix+corruptedDatabaseSuffix) {
			return true
		}
	}
	return false
}

// restoreNewestBackup replaces the freshly recreated database at path with the
// newest readable backup of that tenant.
//
// It is deliberately conservative: every candidate is verified with
// integrity_check(1) at its final path, a candidate that fails the check is
// deleted and the next one is tried, and a missing or unreadable backup
// directory leaves the caller with its empty database instead of failing the
// open. The result reports whether a backup was actually restored.
//
// The caller owns path and must not hold an open handle for it; the quarantine
// step has already closed the damaged file.
func (r *SQLiteMetadataRepository) restoreNewestBackup(ctx context.Context, path string) bool {
	directory := filepath.Join(r.backupDirectory, filepath.Base(filepath.Dir(path)))
	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		warnRepositoryEvent(r.logging, "metadata_restore_unavailable", "backup_scan_failed")
		return false
	}
	if len(backups) == 0 {
		warnRepositoryEvent(r.logging, "metadata_restore_unavailable", "no_backup_available")
		return false
	}

	for _, backup := range backups {
		if err := ctx.Err(); err != nil {
			return false
		}
		if err := restoreBackupCandidate(ctx, backup.path, path); err != nil {
			// The candidate is unusable. It is left in place so an operator can
			// inspect why the newest backup failed verification.
			warnRepositoryEvent(r.logging, "metadata_restore_candidate_failed", classifyBackupError(err))
			continue
		}
		warnRepositoryEvent(r.logging, "metadata_restore_succeeded", "backup_restored")
		return true
	}

	warnRepositoryEvent(r.logging, "metadata_restore_unavailable", "no_readable_backup")
	return false
}

// restoreBackupCandidate copies one backup file onto the metadata database path
// and verifies the result before publishing it.
//
// The copy lands on a staging sibling first and is only renamed onto the final
// path after integrity_check(1) passes, so a truncated or damaged backup can
// never replace the empty database the recovery just created.
func restoreBackupCandidate(ctx context.Context, backupPath string, targetPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	staging := targetPath + backupRestoreSuffix + time.Now().UTC().Format(backupTimestampLayout)
	_ = os.Remove(staging)

	source, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open the metadata backup: %w", err)
	}
	defer func() { _ = source.Close() }()

	staged, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create the restore staging file: %w", err)
	}
	_, copyErr := io.Copy(staged, source)
	if copyErr == nil {
		copyErr = staged.Sync()
	}
	if closeErr := staged.Close(); copyErr == nil && closeErr != nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(staging)
		return fmt.Errorf("failed to stage the metadata backup: %w", copyErr)
	}

	// The staged copy is verified in place: a verification failure must leave the
	// original backup untouched and the target still empty.
	if err := verifyBackupFile(ctx, staging); err != nil {
		_ = os.Remove(staging)
		return err
	}

	if err := os.Rename(staging, targetPath); err != nil {
		_ = os.Remove(staging)
		return fmt.Errorf("failed to publish the restored database: %w", err)
	}
	return nil
}

// listTenantDirectories returns the tenant identifiers that own a first-level
// directory below DataPath, in sorted order.
//
// It reads directory names only and never opens a database file, which is what
// keeps enumeration cheap enough to run for a backup sweep or a health check.
// Names that are not valid tenant identifiers (".locus", for example) are
// skipped, and the result is cached for tenantDirectoryCacheTTL.
func (r *SQLiteMetadataRepository) listTenantDirectories(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.dirMu.Lock()
	if r.dirTenants != nil && r.now().Sub(r.dirScannedA) < tenantDirectoryCacheTTL {
		cached := r.dirTenants
		r.dirMu.Unlock()
		return cached, nil
	}
	r.dirMu.Unlock()

	entries, err := os.ReadDir(r.dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to enumerate the metadata tenants: %w: %w", err, core.ErrDatabaseError)
	}

	tenants := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if core.ValidateTenantID(name) != nil {
			continue
		}
		tenants = append(tenants, name)
	}
	sort.Strings(tenants)

	r.dirMu.Lock()
	r.dirTenants = tenants
	r.dirScannedA = r.now()
	cached := r.dirTenants
	r.dirMu.Unlock()

	return cached, nil
}

// knownTenantIDs merges the tenants with an open handle and the directory
// snapshot into one sorted set.
func (r *SQLiteMetadataRepository) knownTenantIDs(ctx context.Context) ([]string, error) {
	fromDirectory, err := r.listTenantDirectories(ctx)
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	fromHandles := make([]string, 0, len(r.databases))
	for tenantID := range r.databases {
		fromHandles = append(fromHandles, tenantID)
	}
	r.mu.RUnlock()

	seen := make(map[string]struct{}, len(fromDirectory)+len(fromHandles))
	merged := make([]string, 0, len(fromDirectory)+len(fromHandles))
	for _, source := range [][]string{fromDirectory, fromHandles} {
		for _, tenantID := range source {
			if _, ok := seen[tenantID]; ok {
				continue
			}
			seen[tenantID] = struct{}{}
			merged = append(merged, tenantID)
		}
	}
	sort.Strings(merged)
	return merged, nil
}
