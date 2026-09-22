package metadata

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

const (
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

// badgerLockErrorSignatures identify an open failure caused by another owner
// holding the database rather than by a damaged directory. They are matched
// case-insensitively against the error text because BadgerDB reports lock
// contention as a plain wrapped error, and because the exact wording differs
// per platform ("Cannot create lock file" on Windows, "Cannot acquire directory
// lock" elsewhere).
var badgerLockErrorSignatures = []string{
	"cannot create lock file",
	"cannot acquire directory lock",
	"cannot open pid lock file",
	"another process is using this badger database",
	"transaction conflict",
}

// CorruptedDatabaseRecoveryOptions configures how OpenBadgerWithRecovery
// handles a database directory that cannot be opened.
//
// It is the shared policy behind BadgerRepositoryOptions and
// BadgerDirectoryQuotaRepositoryOptions so the metadata and quota stores recover
// identically.
type CorruptedDatabaseRecoveryOptions struct {
	// RecoverCorruptedDatabase quarantines a database directory that cannot be
	// opened and recreates an empty one instead of failing startup.
	//
	// This is a destructive repair: the quarantined data is not automatically
	// re-imported, and callers lose the queued records it held. A lock or
	// ownership failure is never treated as corruption, because the directory
	// may hold a healthy database that another process is using.
	RecoverCorruptedDatabase bool

	// CorruptedDatabaseRetention is how long a quarantined directory is kept
	// before it is pruned during startup. Zero selects
	// defaultCorruptedDatabaseRetention; a negative value disables pruning.
	CorruptedDatabaseRetention time.Duration

	// OnCorruptedDatabase, when set, is called synchronously with the quarantine
	// directory path after a database was quarantined. It runs on the opening
	// goroutine, so it must not block: recovery has to stay faster than the
	// failure it replaces. A panic from it propagates to the caller.
	OnCorruptedDatabase func(quarantinedPath string)
}

// OpenBadgerWithRecovery opens the BadgerDB database stored in dbPath through
// open, quarantining the directory and retrying once when dbPath cannot hold a
// database and recovery is enabled.
//
// open must target dbPath, must be callable more than once, and owns the
// BadgerDB options (this function never builds them, so each package keeps its
// own tuning). It is called at most twice. The returned handle is owned by the
// caller, which must Close it.
//
// Before opening, siblings of dbPath named dbPath+".corrupted.*" that are older
// than the retention are removed best-effort: pruning must never fail startup,
// so those errors are deliberately ignored.
func OpenBadgerWithRecovery(dbPath string, recovery CorruptedDatabaseRecoveryOptions, open func() (*badger.DB, error)) (*badger.DB, error) {
	if open == nil {
		return nil, fmt.Errorf("open function cannot be nil: %w", core.ErrInvalidArgument)
	}

	pruneCorruptedDatabases(dbPath, recovery.CorruptedDatabaseRetention)

	openErr := ensureDatabaseDirectory(dbPath)
	var db *badger.DB
	if openErr == nil {
		if db, openErr = open(); openErr == nil {
			return db, nil
		}
	}

	// Another process holding the lock means dbPath may well be a healthy
	// database: moving it aside would destroy live data, so the flag does not
	// apply. The same is true for any failure that cannot be attributed to a
	// damaged directory.
	if !recovery.RecoverCorruptedDatabase || isBadgerLockError(openErr) {
		return nil, openErr
	}

	quarantinedPath, quarantineErr := quarantineDatabaseDirectory(dbPath)
	if quarantineErr != nil {
		return nil, errors.Join(openErr, fmt.Errorf("failed to quarantine database directory %s: %w", dbPath, quarantineErr))
	}

	if recovery.OnCorruptedDatabase != nil {
		recovery.OnCorruptedDatabase(quarantinedPath)
	}

	if err := ensureDatabaseDirectory(dbPath); err != nil {
		return nil, errors.Join(openErr, err)
	}

	// A fresh open only: if the recreated directory cannot be opened either,
	// report that failure instead of quarantining in a loop.
	db, retryErr := open()
	if retryErr != nil {
		return nil, retryErr
	}
	return db, nil
}

// ensureDatabaseDirectory makes sure dbPath exists as a directory.
func ensureDatabaseDirectory(dbPath string) error {
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database path: %w", err)
	}
	return nil
}

// isBadgerLockError reports whether an open failure means another owner holds
// the database rather than the directory being unusable.
func isBadgerLockError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, badger.ErrConflict) {
		return true
	}

	message := strings.ToLower(err.Error())
	for _, signature := range badgerLockErrorSignatures {
		if strings.Contains(message, signature) {
			return true
		}
	}
	return false
}

// pruneCorruptedDatabases removes expired quarantine siblings of dbPath. It is
// best-effort: quarantined data is a rescue copy, and failing to prune it must
// never stop a database from starting.
func pruneCorruptedDatabases(dbPath string, retention time.Duration) {
	if retention < 0 {
		return
	}
	if retention == 0 {
		retention = defaultCorruptedDatabaseRetention
	}

	parent := filepath.Dir(dbPath)
	prefix := filepath.Base(dbPath) + corruptedDatabaseSuffix
	cutoff := time.Now().Add(-retention)

	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(parent, entry.Name()))
	}
}

// quarantineDatabaseDirectory renames dbPath to a timestamped sibling and
// returns the new path. The name is unique-suffixed when two quarantines happen
// within the same second, so the rename cannot fail on an existing directory.
func quarantineDatabaseDirectory(dbPath string) (string, error) {
	base := dbPath + corruptedDatabaseSuffix + time.Now().UTC().Format(corruptedDatabaseTimestampLayout)

	target := base
	for suffix := 1; ; suffix++ {
		_, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return "", err
		}
		if suffix > corruptedDatabasePathCollisionLimit {
			return "", fmt.Errorf("cannot find a free quarantine path for %s", dbPath)
		}
		target = fmt.Sprintf("%s.%d", base, suffix)
	}

	if err := os.Rename(dbPath, target); err != nil {
		return "", err
	}
	return target, nil
}
