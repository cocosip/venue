package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// This file holds the engine-neutral helpers the SQLite metadata store shares
// with its backup, automatic-restore and quarantine paths.
//
// The naming constants and the backup inventory used to live in the BadgerDB-era
// files (backup.go, corrupted_database.go). They survive the engine switch
// unchanged, because a backup or a quarantine directory produced before the
// switch must keep being recognised by the operands that own that naming, and
// because the frozen layout is part of the operational contract rather than of
// the storage engine.

const (
	// backupFilePrefix and backupFileSuffix frame the frozen backup file name
	// "metadata.<yyyyMMddTHHmmssZ>.bak". A backup set scanned by an operator is
	// a glob on the same shape.
	backupFilePrefix = "metadata."
	backupFileSuffix = ".bak"

	// backupTimestampLayout renders the UTC stamp inside a backup file name. It
	// is deliberately colon-free because a colon cannot appear in a Windows
	// path segment.
	backupTimestampLayout = "20060102T150405Z"

	// backupRestoreSuffix marks the staging sibling an automatic restore loads a
	// backup into before it replaces the database file.
	backupRestoreSuffix = ".restore."

	// defaultCorruptedDatabaseRetention is how long a quarantined database is
	// kept when the caller does not choose a retention.
	defaultCorruptedDatabaseRetention = 72 * time.Hour

	// corruptedDatabaseSuffix marks a quarantined sibling of a database path.
	corruptedDatabaseSuffix = ".corrupted."

	// corruptedDatabaseTimestampLayout renders the quarantine timestamp. It is
	// deliberately colon-free: a colon cannot appear in a Windows path segment.
	corruptedDatabaseTimestampLayout = "20060102T150405Z"

	// corruptedDatabasePathCollisionLimit bounds the search for a free
	// quarantine name when two quarantines land in the same second.
	corruptedDatabasePathCollisionLimit = 100
)

// errRepositoryClosed classifies use of a closed repository as an
// infrastructure failure so callers never mistake it for claim contention.
func errRepositoryClosed() error {
	return fmt.Errorf("metadata repository is closed: %w", core.ErrDatabaseError)
}

// newLeaseMismatchError describes the active state that rejected a lease.
func newLeaseMismatchError(lease core.FileProcessingLease, current *core.FileMetadata) error {
	mismatch := &core.FileProcessingLeaseMismatchError{
		TenantID:                       lease.TenantID,
		FileKey:                        lease.FileKey,
		ExpectedProcessingStartTimeUTC: lease.ProcessingStartTimeUTC,
	}
	if current != nil {
		actualStatus := current.Status
		mismatch.ActualStatus = &actualStatus
		if current.Status == core.FileStatusProcessing && current.ProcessingStartTime != nil {
			actualStart := *current.ProcessingStartTime
			mismatch.ActualProcessingStartTimeUTC = &actualStart
		}
	}
	return mismatch
}

// backupFile is one entry of a backup directory.
type backupFile struct {
	path    string
	size    int64
	modTime time.Time
}

// isBackupFileName reports whether name has the frozen backup file shape
// metadata.<stamp>.bak, including the collision suffix two backups inside one
// second can add.
func isBackupFileName(name string) bool {
	if !strings.HasPrefix(name, backupFilePrefix) || !strings.HasSuffix(name, backupFileSuffix) {
		return false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(name, backupFilePrefix), backupFileSuffix)
	if middle == "" {
		return false
	}
	if _, err := parseBackupTimestamp(name); err == nil {
		return true
	}
	// Accept the collision form metadata.<stamp>.<n>.bak as well.
	if index := strings.LastIndex(middle, "."); index > 0 {
		_, err := parseBackupTimestamp(backupFilePrefix + middle[:index] + backupFileSuffix)
		return err == nil
	}
	return false
}

// parseBackupTimestamp extracts the UTC instant encoded in a backup file name.
// A name that does not carry the frozen stamp yields an empty time and an error,
// so a caller can fall back to the file's modification time.
func parseBackupTimestamp(name string) (time.Time, error) {
	middle := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(name), backupFilePrefix), backupFileSuffix)
	stamp, err := time.Parse(backupTimestampLayout, middle)
	if err != nil {
		return time.Time{}, err
	}
	return stamp.UTC(), nil
}

// listBackupsNewestFirst returns every backup file directly inside directory,
// newest first.
//
// Ordering uses the modification time, with the timestamp encoded in the file
// name breaking ties and settling equal modification times, so the order is
// deterministic even when a file system reports coarse timestamps. A missing or
// unreadable directory yields no backups rather than an error: a backup
// directory is an operational artifact and its absence is not a failure of the
// caller's own state.
//
// The order is load-bearing for automatic restore, which walks the result and
// takes the first candidate that verifies.
func listBackupsNewestFirst(directory string) ([]backupFile, error) {
	if directory == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read backup directory %s: %w", directory, err)
	}

	backups := make([]backupFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isBackupFileName(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		backups = append(backups, backupFile{
			path:    filepath.Join(directory, entry.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	sort.SliceStable(backups, func(i, j int) bool {
		if !backups[i].modTime.Equal(backups[j].modTime) {
			return backups[i].modTime.After(backups[j].modTime)
		}
		leftStamp, leftErr := parseBackupTimestamp(filepath.Base(backups[i].path))
		rightStamp, rightErr := parseBackupTimestamp(filepath.Base(backups[j].path))
		if leftErr == nil && rightErr == nil && !leftStamp.Equal(rightStamp) {
			return leftStamp.After(rightStamp)
		}
		return backups[i].path > backups[j].path
	})

	return backups, nil
}

// classifyBackupError reduces an error to a stable, path-free label.
//
// It is used on the recovery path, where a file-system error embeds the
// configured backup directory and the logging rules keep full physical paths out
// of logs.
func classifyBackupError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, core.ErrInvalidArgument):
		return "invalid_argument"
	case errors.Is(err, core.ErrDatabaseError):
		return "database_error"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	default:
		return "error"
	}
}
