// Package sqlite provides the shared SQLite foundation of Venue's storage
// engine: the pure-Go driver selection, the connection policy, the DSN that
// configures every pooled connection identically, and the maintenance
// primitives (checkpoint, integrity verification, online backup).
//
// The package is deliberately policy-free above that layer. It does not know
// about tenants, metadata rows, quota rows, or the on-disk layout below a
// database file; callers own those decisions. One database file per tenant is
// the intended shape, and every tenant database is opened through [Open] so
// that it inherits the same pragmas and the same single-connection policy.
//
// Driver: modernc.org/sqlite (pure Go, no cgo), registered under
// [DriverName]. The package must build and test with CGO_ENABLED=0; a cgo
// binding is out of scope and no build tag or fallback selects one.
//
// Durability and concurrency: the DSN applies journal_mode, synchronous,
// cache_size, busy_timeout, foreign_keys and temp_store so that a pooled
// connection is configured before the first statement runs, and sets
// _txlock=immediate so that BEGIN IMMEDIATE takes the write lock when a
// transaction starts. [Open] then pins the handle to a single connection
// (SetMaxOpenConns(1)), which makes every statement of one tenant serialize on
// one connection and lets tenant-level maintenance genuinely release file
// handles.
//
// Backup and corruption isolation: [VacuumInto] writes a consistent copy of a
// live database with VACUUM INTO, [IntegrityCheck] verifies one, and
// [IsCorruptionError] separates "this file is damaged" from "this file is
// busy, locked, read-only, or merely in use". Quarantine is destructive, so
// only a positive corruption verdict may trigger it.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	// A named import rather than a blank one: the package registers the
	// "sqlite" driver with database/sql in its init, and its Error type is what
	// makes corruption classification structural instead of string matching.
	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/cocosip/venue/pkg/core"
)

// DriverName is the database/sql driver name registered by the pure-Go
// modernc.org/sqlite driver. It is the only driver this package opens.
const DriverName = "sqlite"

// Default pragma and timeout values, matching the Locus SqliteOptions defaults.
const (
	defaultJournalMode     = "WAL"
	defaultSynchronousMode = "NORMAL"
	defaultCacheSizeKb     = -4000
	defaultBusyTimeoutMs   = 5000
)

// journalModes is the journal-mode whitelist, matched case-insensitively. It
// is the set accepted by the driver's _journal_mode DSN parameter and by
// PRAGMA journal_mode.
var journalModes = map[string]bool{
	"DELETE":   true,
	"TRUNCATE": true,
	"PERSIST":  true,
	"MEMORY":   true,
	"WAL":      true,
	"OFF":      true,
}

// synchronousModes is the synchronous-mode whitelist, matched
// case-insensitively. The numeric forms 0..3 are equivalent to
// OFF, NORMAL, FULL and EXTRA.
var synchronousModes = map[string]bool{
	"OFF":    true,
	"NORMAL": true,
	"FULL":   true,
	"EXTRA":  true,
	"0":      true,
	"1":      true,
	"2":      true,
	"3":      true,
}

// corruptionTextPatterns are the SQLite error-message fragments that identify a
// malformed database file. They back up the driver's structured result codes so
// that corruption is still recognised when an error crosses a boundary that
// drops its Go type (for example a formatted string rebuilt by a caller), and
// for the verdict that PRAGMA integrity_check reports as text rather than as a
// result code. "malformed", "not a database" and "corrupt" are the canonical
// SQLite messages and are never produced by a lock, a constraint violation, or
// a permission failure.
var corruptionTextPatterns = []string{
	"database disk image is malformed",
	"file is not a database",
	"database corrupt",
	"btreeinitpage() returns error code",
	"error code 11",
	"sqlite_corrupt",
}

// Options mirrors the Locus SqliteOptions defaults and is the complete set of
// tuning knobs a caller can apply to a Venue SQLite database file.
//
// The zero value is not usable: use [DefaultOptions] and override the fields
// that need to differ. [Options.Validate] rejects a cache size of zero because
// it would disable the page cache, and a negative busy timeout because it is
// never meaningful.
type Options struct {
	// JournalMode is the SQLite journal mode. WAL allows readers during a
	// write and supports crash recovery; DELETE is the SQLite default. Allowed
	// values, matched case-insensitively: WAL, DELETE, TRUNCATE, PERSIST,
	// MEMORY, OFF.
	JournalMode string
	// SynchronousMode is the SQLite durability level. NORMAL is safe against a
	// process crash while a power failure can lose the most recent commits;
	// FULL fsyncs on every commit. Allowed values, matched
	// case-insensitively: OFF, NORMAL, FULL, EXTRA and the numeric forms
	// 0, 1, 2, 3.
	SynchronousMode string
	// CacheSizeKb is the per-connection page cache. A negative value is a size
	// in kibibytes; a positive value is a number of 4 KiB pages. Zero is
	// rejected by [Options.Validate].
	CacheSizeKb int
	// BusyTimeoutMs is how long a statement waits for a contended lock before
	// it fails with SQLITE_BUSY. Zero disables waiting.
	BusyTimeoutMs int
	// CheckpointAfterBatch requests PRAGMA wal_checkpoint(PASSIVE) after every
	// committed write batch, which bounds WAL growth at the cost of extra I/O.
	//
	// It is configuration data only: [Open] never reads it. The repository
	// layer that commits a batch is responsible for calling [Checkpoint] on
	// that tenant's handle after a successful Commit, and only then. The
	// checkpoint must run outside the transaction, which the single-connection
	// policy of [Open] guarantees when it follows the commit.
	CheckpointAfterBatch bool
}

// DefaultOptions returns the Locus-compatible default options: WAL journaling,
// NORMAL synchronous mode, a 4000 KiB page cache, a 5000 ms busy timeout and no
// checkpoint after each batch.
func DefaultOptions() Options {
	return Options{
		JournalMode:          defaultJournalMode,
		SynchronousMode:      defaultSynchronousMode,
		CacheSizeKb:          defaultCacheSizeKb,
		BusyTimeoutMs:        defaultBusyTimeoutMs,
		CheckpointAfterBatch: false,
	}
}

// Validate reports whether the options can be turned into a DSN. It wraps
// core.ErrInvalidArgument for every rejection, so callers can classify a
// configuration defect with errors.Is.
//
// JournalMode and SynchronousMode are matched case-insensitively against the
// SQLite whitelists.
func (o Options) Validate() error {
	if !journalModes[strings.ToUpper(strings.TrimSpace(o.JournalMode))] {
		return fmt.Errorf(
			"sqlite journal mode %q is not one of WAL, DELETE, TRUNCATE, PERSIST, MEMORY, OFF: %w",
			o.JournalMode, core.ErrInvalidArgument)
	}
	if !synchronousModes[strings.ToUpper(strings.TrimSpace(o.SynchronousMode))] {
		return fmt.Errorf(
			"sqlite synchronous mode %q is not one of OFF, NORMAL, FULL, EXTRA, 0, 1, 2, 3: %w",
			o.SynchronousMode, core.ErrInvalidArgument)
	}
	if o.CacheSizeKb == 0 {
		return fmt.Errorf("sqlite cache size cannot be 0: it would disable the page cache: %w",
			core.ErrInvalidArgument)
	}
	if o.BusyTimeoutMs < 0 {
		return fmt.Errorf("sqlite busy timeout %d cannot be negative: %w",
			o.BusyTimeoutMs, core.ErrInvalidArgument)
	}
	return nil
}

// DSN builds the modernc.org/sqlite data source name for path.
//
// The name uses the file: URI form with mode=rwc, so opening the database also
// creates it, and it accepts a Windows absolute path: backslashes are
// converted to forward slashes and a leading slash is added so that the URI
// path stays absolute instead of being read as a drive-relative one.
//
// Every pragma travels in the DSN, so database/sql configures each connection
// of the pool identically before any statement runs:
//
//	journal_mode, synchronous, cache_size, busy_timeout, foreign_keys(OFF),
//	temp_store(MEMORY)
//
// plus _txlock=immediate, which makes an explicit transaction begin with
// BEGIN IMMEDIATE and take the write lock up front instead of failing on a
// mid-transaction lock upgrade.
//
// DSN validates the options and rejects an empty or NUL-containing path with a
// wrapped core.ErrInvalidArgument.
func (o Options) DSN(path string) (string, error) {
	if err := o.Validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("sqlite database path is empty: %w", core.ErrInvalidArgument)
	}
	if strings.ContainsRune(path, 0) {
		return "", fmt.Errorf("sqlite database path contains a NUL byte: %w", core.ErrInvalidArgument)
	}

	name := filepath.ToSlash(path)
	if isWindowsDrivePath(name) {
		name = "/" + name
	}

	var b strings.Builder
	b.WriteString("file:")
	b.WriteString(name)
	b.WriteString("?mode=rwc")
	fmt.Fprintf(&b, "&_busy_timeout=%d", o.BusyTimeoutMs)
	fmt.Fprintf(&b, "&_journal_mode=%s", strings.ToUpper(strings.TrimSpace(o.JournalMode)))
	fmt.Fprintf(&b, "&_synchronous=%s", strings.ToUpper(strings.TrimSpace(o.SynchronousMode)))
	b.WriteString("&_foreign_keys=off")
	fmt.Fprintf(&b, "&_pragma=cache_size(%d)", o.CacheSizeKb)
	b.WriteString("&_pragma=temp_store(MEMORY)")
	b.WriteString("&_txlock=immediate")
	return b.String(), nil
}

// isWindowsDrivePath reports whether name starts with a drive designator such
// as "C:/" or "C:". A file: URI without a leading slash would otherwise treat
// that drive as a URI scheme or as a drive-relative path.
func isWindowsDrivePath(name string) bool {
	if len(name) < 2 || name[1] != ':' {
		return false
	}
	c := name[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// Open opens (creating it if needed) the SQLite database at path with the
// shared connection policy and verifies that the file can actually be used.
//
// The returned handle is pinned to exactly one connection
// (SetMaxOpenConns(1), SetMaxIdleConns(1), no lifetime limit), matching Locus's
// Pooling=False single long-lived connection per tenant. That policy is what
// makes PRAGMA re-assertions, temporary tables and transaction state
// deterministic, and it is why callers must not raise the limit without
// benchmark evidence. The caller owns the handle and must Close it; closing it
// is what releases the database file on Windows.
//
// Open never creates the parent directory: the caller owns the on-disk layout.
// A missing or non-directory parent, an unusable path, and a file that is not
// a usable SQLite database all fail here, each wrapping core.ErrDatabaseError.
// A file that is specifically recognisable as corrupt additionally reports
// that fact in the message and, through the wrapped chain, satisfies
// [IsCorruptionError], so the caller can tell "damaged" from "locked" before
// deciding whether to quarantine.
func Open(path string, opts Options) (*sql.DB, error) {
	dsn, err := opts.DSN(path)
	if err != nil {
		return nil, err
	}
	if err := checkParentDirectory(path); err != nil {
		return nil, err
	}

	db, err := sql.Open(DriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database %q: %w", path, errors.Join(err, core.ErrDatabaseError))
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.PingContext(context.Background()); err != nil {
		// Best effort: the handle is unusable, and the caller receives the
		// original open failure rather than a close failure.
		_ = db.Close()
		if IsCorruptionError(err) {
			return nil, fmt.Errorf(
				"sqlite database %q is corrupted and cannot be opened: %w",
				path, errors.Join(err, core.ErrDatabaseError))
		}
		return nil, fmt.Errorf("failed to open sqlite database %q: %w", path, errors.Join(err, core.ErrDatabaseError))
	}
	return db, nil
}

// checkParentDirectory fails with a wrapped core.ErrDatabaseError when the
// parent of path is missing or is not a directory. Open deliberately does not
// create it, so the failure has to name the real cause instead of surfacing
// SQLITE_CANTOPEN.
func checkParentDirectory(path string) error {
	parent := filepath.Dir(filepath.Clean(path))
	info, err := os.Stat(parent)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("sqlite parent directory %q does not exist: %w",
			parent, errors.Join(err, core.ErrDatabaseError))
	case err != nil:
		return fmt.Errorf("sqlite parent directory %q is not usable: %w",
			parent, errors.Join(err, core.ErrDatabaseError))
	case !info.IsDir():
		return fmt.Errorf("sqlite parent path %q is not a directory: %w",
			parent, errors.Join(fs.ErrInvalid, core.ErrDatabaseError))
	default:
		return nil
	}
}

// Checkpoint runs PRAGMA wal_checkpoint(PASSIVE) on db.
//
// PASSIVE neither blocks nor waits for readers: it merges as much of the
// write-ahead log back into the main database file as it can, which bounds WAL
// growth without disturbing concurrent readers. It returns nil when the
// checkpoint completes partially or fully; an incomplete pass is a normal
// outcome, not a failure.
//
// This is the statement behind Options.CheckpointAfterBatch and the caller
// decides when to run it, normally right after a successful batch commit.
// wal_checkpoint cannot run inside a transaction: callers must commit (or roll
// back) first. Failures wrap core.ErrDatabaseError.
func Checkpoint(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		return fmt.Errorf("sqlite wal_checkpoint(PASSIVE) failed: %w", errors.Join(err, core.ErrDatabaseError))
	}
	return nil
}

// TruncateCheckpoint runs PRAGMA wal_checkpoint(TRUNCATE) on db.
//
// TRUNCATE performs everything PASSIVE does and then truncates the write-ahead
// log to zero bytes, at the cost of blocking until every reader of the WAL has
// finished. That is the right trade before a shutdown or before a database
// file is moved, renamed or deleted: it is what makes the -wal sidecar empty
// and lets the file handle be released cleanly. It is not appropriate on a hot
// path; use [Checkpoint] there.
//
// Like [Checkpoint], it must run outside a transaction. Failures wrap
// core.ErrDatabaseError.
func TruncateCheckpoint(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("sqlite wal_checkpoint(TRUNCATE) failed: %w", errors.Join(err, core.ErrDatabaseError))
	}
	return nil
}

// IntegrityCheck runs PRAGMA integrity_check(1) on db and reports whether the
// database is healthy.
//
// A healthy database answers with exactly one row containing "ok". Any other
// answer, including several rows of problem descriptions, is a failure and is
// returned with the reported verdict, wrapped in core.ErrDatabaseError and
// classified by [IsCorruptionError]. An error raised by the check itself is
// wrapped the same way.
//
// The limit of 1 keeps the check bounded: SQLite stops after the first problem
// it finds. Never run it on a live database in steady state: it reads every
// page and contends with writers for the page cache. Its intended uses are
// verifying a freshly produced backup and the periodic maintenance path.
func IntegrityCheck(ctx context.Context, db *sql.DB) error {
	var result string
	err := db.QueryRowContext(ctx, "PRAGMA integrity_check(1)").Scan(&result)
	if err != nil {
		return fmt.Errorf("sqlite integrity_check(1) failed: %w", errors.Join(err, core.ErrDatabaseError))
	}
	verdict := strings.TrimSpace(result)
	if strings.EqualFold(verdict, "ok") {
		return nil
	}
	// SQLite reports corruption inside the integrity_check result text rather
	// than as a result code, so the verdict is preserved verbatim: that is both
	// the operator's evidence and what [IsCorruptionError] classifies.
	return fmt.Errorf("sqlite integrity_check(1) reported a damaged database %q: %w",
		verdict, errors.Join(errors.New(verdict), core.ErrDatabaseError))
}

// VacuumInto writes a consistent copy of the database in db into target with
// VACUUM INTO, which reads the source inside a read transaction and rewrites it
// into a new, complete and directly openable SQLite file. The source keeps
// serving readers and writers while the copy is made, and it is never modified
// by the operation: the copy is built from the source's snapshot, so a failure
// leaves the source intact.
//
// target must not exist; SQLite refuses to overwrite a file and this function
// rejects the case up front with a wrapped core.ErrInvalidArgument. Its parent
// directory must already exist, because nothing here creates directories for a
// caller-owned backup layout.
//
// The filename is bound as a parameter, so a target path is never interpreted
// as SQL syntax. Failures wrap core.ErrDatabaseError for the copy itself and
// the underlying cause for any other error.
func VacuumInto(ctx context.Context, db *sql.DB, target string) error {
	if strings.TrimSpace(target) == "" {
		return fmt.Errorf("sqlite vacuum target path is empty: %w", core.ErrInvalidArgument)
	}
	switch _, err := os.Lstat(target); {
	case err == nil:
		return fmt.Errorf("sqlite vacuum target %q already exists: %w", target, core.ErrInvalidArgument)
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("failed to inspect sqlite vacuum target %q: %w", target, err)
	}

	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", target); err != nil {
		return fmt.Errorf("failed to write sqlite database copy %q: %w", target, errors.Join(err, core.ErrDatabaseError))
	}
	return nil
}

// IsCorruptionError reports whether err says that a database file is damaged
// rather than merely unavailable.
//
// A true verdict is reserved for the errors that mean "this file is not a
// usable database": SQLITE_CORRUPT, including its extended codes, which is what
// a failed page check or an inconsistent index produces; SQLITE_NOTADB; and
// SQLITE_IOERR, which the design classifies with corruption so that an I/O
// level failure of the database image is never mistaken for a healthy but
// locked file. It also recognises the canonical SQLite messages when the
// driver's structured error has been lost, including the verdict that
// PRAGMA integrity_check reports as text.
//
// It deliberately returns false for everything else, because quarantine and
// restore are destructive: SQLITE_BUSY and SQLITE_LOCKED mean another process
// may hold a perfectly healthy database, SQLITE_READONLY and SQLITE_CANTOPEN
// mean a permission or deployment defect, and an unknown error is never
// treated as corruption.
func IsCorruptionError(err error) bool {
	if err == nil {
		return false
	}

	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() & 0xff {
		case sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB, sqlite3.SQLITE_IOERR:
			return true
		}
	}
	return matchesCorruptionText(err.Error())
}

// matchesCorruptionText reports whether message carries one of the textual
// corruption verdicts. It is the fallback for errors that no longer carry the
// driver's structured code or that SQLite reported as text, such as the rows
// PRAGMA integrity_check returns.
func matchesCorruptionText(message string) bool {
	msg := strings.ToLower(message)
	for _, pattern := range corruptionTextPatterns {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}
