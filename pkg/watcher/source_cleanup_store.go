package watcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
	"github.com/google/uuid"
)

// SourceCleanupAction describes the source operation to perform after a
// successful Venue import.
type SourceCleanupAction string

const (
	SourceCleanupActionDelete SourceCleanupAction = "Delete"
	SourceCleanupActionMove   SourceCleanupAction = "Move"
	SourceCleanupActionKeep   SourceCleanupAction = "Keep"
)

// SourceCleanupJobState is the durable lifecycle state of a source cleanup
// reservation.
type SourceCleanupJobState string

const (
	SourceCleanupStateImporting   SourceCleanupJobState = "Importing"
	SourceCleanupStatePending     SourceCleanupJobState = "Pending"
	SourceCleanupStateRetrying    SourceCleanupJobState = "Retrying"
	SourceCleanupStateMovePending SourceCleanupJobState = "MovePending"
	SourceCleanupStateFailed      SourceCleanupJobState = "Failed"
	SourceCleanupStateKept        SourceCleanupJobState = "Kept"
)

// ErrSourceCleanupLeaseLost indicates that another worker owns the job lease.
var ErrSourceCleanupLeaseLost = errors.New("source cleanup lease lost")

// ErrSourceCleanupFingerprintChanged indicates that the source path now holds
// a different file revision than the one recorded by the cleanup job.
var ErrSourceCleanupFingerprintChanged = errors.New("source cleanup fingerprint changed")

// ErrSourceCleanupCapacityExceeded indicates that the store's active-job budget
// is exhausted, so the import was not reserved and must be retried on a later
// scan. It is a backpressure signal rather than a failure of the import itself.
var ErrSourceCleanupCapacityExceeded = errors.New("source cleanup capacity exceeded")

// SourceCleanupJob is one durable post-import source action.
type SourceCleanupJob struct {
	JobID             int64
	WatcherID         string
	TenantID          string
	SourcePath        string
	Fingerprint       string
	FileKey           string
	OperationID       string
	Action            SourceCleanupAction
	MoveTargetPath    string
	FailureDirectory  string
	MaxAttempts       int
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	AttemptCount      int
	NextAttemptAt     time.Time
	State             SourceCleanupJobState
	// LastError is the recorded failure type, never a raw error message: it is
	// returned to callers, and a filesystem error embeds full physical paths.
	LastError  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	LeaseUntil time.Time
	LeaseToken string
}

// SourceCleanupStoreOptions controls the durable source cleanup store.
type SourceCleanupStoreOptions struct {
	MaxActiveJobs int

	// Sqlite is the engine policy for the cleanup database. A zero value
	// selects sqlite.DefaultOptions so the durable cleanup database follows the
	// same journal, synchronous and busy-timeout policy as the metadata and
	// quota databases.
	Sqlite sqlite.Options
}

// SourceCleanupStore owns durable reservations and leases for source cleanup.
type SourceCleanupStore interface {
	Close() error
	TryReserve(context.Context, *SourceCleanupJob) (bool, error)
	GetBySource(context.Context, string, string) (*SourceCleanupJob, error)
	MarkImported(context.Context, int64, string, string, time.Time) error
	ClaimDue(context.Context, time.Time, time.Duration, int) ([]SourceCleanupJob, error)
	MarkSucceeded(context.Context, SourceCleanupJob) error
	MarkRetry(context.Context, SourceCleanupJob, time.Time, error) error
	MarkMovePending(context.Context, SourceCleanupJob, time.Time, error) error
	MarkFailed(context.Context, SourceCleanupJob, error) error
	RecoverStaleImports(ctx context.Context, now time.Time, timeout time.Duration, keep func(SourceCleanupJob) bool) (int, error)
	PruneTerminal(context.Context, time.Time, int) (int, error)
	Optimize(context.Context) error
}

type sqliteSourceCleanupStore struct {
	db            *sql.DB
	maxActiveJobs int

	// statSource inspects a job's source path before a terminal row is pruned.
	// It is injectable so the prune's "source is gone" rule can be exercised
	// with the transient stat failures Windows produces; nil selects os.Stat.
	statSource func(string) (os.FileInfo, error)
}

// sourceStat inspects a source path, using the injected stat when one is set.
func (s *sqliteSourceCleanupStore) sourceStat(path string) (os.FileInfo, error) {
	if s.statSource != nil {
		return s.statSource(path)
	}

	return os.Stat(path)
}

const sourceCleanupSchema = `
CREATE TABLE IF NOT EXISTS source_cleanup_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watcher_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    file_key TEXT NOT NULL DEFAULT '',
    operation_id TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    move_target_path TEXT NOT NULL DEFAULT '',
    failure_directory TEXT NOT NULL DEFAULT '',
    max_attempts INTEGER NOT NULL DEFAULT 0,
    retry_initial_delay_nano INTEGER NOT NULL DEFAULT 0,
    retry_max_delay_nano INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_unix_nano INTEGER NOT NULL,
    state TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    created_unix_nano INTEGER NOT NULL,
    updated_unix_nano INTEGER NOT NULL,
    lease_until_unix_nano INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT NOT NULL DEFAULT '',
    UNIQUE(watcher_id, source_path)
);
CREATE INDEX IF NOT EXISTS source_cleanup_due_idx
    ON source_cleanup_jobs(state, next_attempt_unix_nano, lease_until_unix_nano);
CREATE INDEX IF NOT EXISTS source_cleanup_terminal_idx
    ON source_cleanup_jobs(state, updated_unix_nano);
`

// sourceCleanupPruneBatchChunk caps how many row IDs one prune statement binds,
// staying comfortably below SQLite's bound-parameter limit.
const sourceCleanupPruneBatchChunk = 500

// OpenSourceCleanupStore opens or creates the pure-Go SQLite cleanup database.
func OpenSourceCleanupStore(ctx context.Context, path string, options SourceCleanupStoreOptions) (SourceCleanupStore, error) {
	if options.MaxActiveJobs <= 0 {
		return nil, fmt.Errorf("max active jobs must be positive: %w", core.ErrInvalidArgument)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Clean(path)), 0o755); err != nil {
		return nil, fmt.Errorf("create source cleanup database directory: %w", err)
	}
	sqliteOptions := options.Sqlite
	if sqliteOptions == (sqlite.Options{}) {
		sqliteOptions = sqlite.DefaultOptions()
	}
	db, err := sqlite.Open(ctx, path, sqliteOptions)
	if err != nil {
		return nil, fmt.Errorf("open source cleanup database: %w", err)
	}
	store := &sqliteSourceCleanupStore{db: db, maxActiveJobs: options.MaxActiveJobs}
	if _, err := db.ExecContext(ctx, sourceCleanupSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize source cleanup schema: %w", err)
	}
	for _, column := range []struct {
		name       string
		definition string
	}{
		{name: "retry_initial_delay_nano", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "retry_max_delay_nano", definition: "INTEGER NOT NULL DEFAULT 0"},
	} {
		if err := ensureSourceCleanupColumn(db, column.name, column.definition); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("upgrade source cleanup schema: %w", err)
		}
	}
	return store, nil
}

func ensureSourceCleanupColumn(db *sql.DB, name, definition string) error {
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(source_cleanup_jobs)")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return err
		}
		if columnName == name {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = db.ExecContext(context.Background(), "ALTER TABLE source_cleanup_jobs ADD COLUMN "+name+" "+definition)
	return err
}

// sourceCleanupStateConsumesCapacity reports whether state is a non-terminal
// state counted against the store's active-job budget.
func sourceCleanupStateConsumesCapacity(state SourceCleanupJobState) bool {
	switch state {
	case SourceCleanupStateImporting, SourceCleanupStatePending, SourceCleanupStateRetrying, SourceCleanupStateMovePending:
		return true
	default:
		return false
	}
}

func (s *sqliteSourceCleanupStore) Close() error { return s.db.Close() }

func (s *sqliteSourceCleanupStore) TryReserve(ctx context.Context, job *SourceCleanupJob) (bool, error) {
	if job == nil || job.WatcherID == "" || job.SourcePath == "" || job.Fingerprint == "" {
		return false, fmt.Errorf("source cleanup job identity is incomplete: %w", core.ErrInvalidArgument)
	}
	if err := core.ValidateTenantID(job.TenantID); err != nil {
		return false, fmt.Errorf("invalid source cleanup tenant: %w", err)
	}
	// Fast path: an unchanged source (same watcher, path and fingerprint) needs
	// no reservation, and answering that with a plain read avoids taking the
	// database's single write lock for every file a scan re-observes. The
	// transaction below still re-checks, so a concurrent change cannot slip in.
	var reservedFingerprint string
	switch err := s.db.QueryRowContext(ctx,
		`SELECT fingerprint FROM source_cleanup_jobs WHERE watcher_id = ? AND source_path = ?`,
		job.WatcherID, job.SourcePath).Scan(&reservedFingerprint); {
	case err == nil && reservedFingerprint == job.Fingerprint:
		return false, nil
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("read source cleanup reservation: %w", err)
	}

	now := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin source cleanup reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// One read answers both questions the reservation needs: whether this
	// watcher/source pair already has a row, and how many non-terminal rows
	// consume the active-job budget. The LEFT JOIN yields at most one row and
	// still returns the count when no reservation exists.
	var (
		existingID          int64
		existingFingerprint string
		existingState       string
		active              int
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(jobs.id, 0), COALESCE(jobs.fingerprint, ''), COALESCE(jobs.state, ''), capacity.active
		FROM (SELECT COUNT(*) AS active FROM source_cleanup_jobs WHERE state IN (?, ?, ?, ?)) AS capacity
		LEFT JOIN source_cleanup_jobs AS jobs ON jobs.watcher_id = ? AND jobs.source_path = ?`,
		SourceCleanupStateImporting, SourceCleanupStatePending, SourceCleanupStateRetrying, SourceCleanupStateMovePending,
		job.WatcherID, job.SourcePath,
	).Scan(&existingID, &existingFingerprint, &existingState, &active); err != nil {
		return false, fmt.Errorf("find source cleanup reservation: %w", err)
	}

	if existingID != 0 {
		job.JobID = existingID
		if existingFingerprint == job.Fingerprint {
			return false, nil
		}
		// A producer may reuse a source path for a new revision while the old
		// cleanup job is still pending. Drop the old revision immediately so it
		// cannot block the new import or quarantine the replacement file, and
		// release its capacity before the budget check below.
		if sourceCleanupStateConsumesCapacity(SourceCleanupJobState(existingState)) {
			active--
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM source_cleanup_jobs WHERE id = ?`, existingID); err != nil {
			return false, fmt.Errorf("remove superseded source cleanup reservation: %w", err)
		}
		// Continue below and insert a fresh reservation in the same transaction.
	}
	// Only non-terminal states consume capacity: Kept and Failed rows are
	// retained for the terminal retention window (and a Kept row whose source
	// still exists is kept as its dedup tombstone), so counting them would let
	// finished work permanently shrink the import budget.
	if active >= s.maxActiveJobs {
		return false, ErrSourceCleanupCapacityExceeded
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO source_cleanup_jobs (watcher_id, tenant_id, source_path, fingerprint, action, move_target_path, failure_directory, max_attempts, retry_initial_delay_nano, retry_max_delay_nano, next_attempt_unix_nano, state, created_unix_nano, updated_unix_nano) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.WatcherID, job.TenantID, job.SourcePath, job.Fingerprint, job.Action, job.MoveTargetPath, job.FailureDirectory, job.MaxAttempts, job.RetryInitialDelay.Nanoseconds(), job.RetryMaxDelay.Nanoseconds(), now.UnixNano(), SourceCleanupStateImporting, now.UnixNano(), now.UnixNano())
	if err != nil {
		return false, fmt.Errorf("insert source cleanup reservation: %w", err)
	}
	job.JobID, err = result.LastInsertId()
	if err != nil {
		return false, fmt.Errorf("read source cleanup reservation id: %w", err)
	}
	job.State, job.CreatedAt, job.UpdatedAt = SourceCleanupStateImporting, now, now
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit source cleanup reservation: %w", err)
	}
	return true, nil
}

func (s *sqliteSourceCleanupStore) GetBySource(ctx context.Context, watcherID, sourcePath string) (*SourceCleanupJob, error) {
	row := s.db.QueryRowContext(ctx, sourceCleanupSelect+` WHERE watcher_id = ? AND source_path = ?`, watcherID, sourcePath)
	job, err := scanSourceCleanupJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load source cleanup job: %w", err)
	}
	return &job, nil
}

const sourceCleanupSelect = `SELECT id, watcher_id, tenant_id, source_path, fingerprint, file_key, operation_id, action, move_target_path, failure_directory, max_attempts, retry_initial_delay_nano, retry_max_delay_nano, attempt_count, next_attempt_unix_nano, state, last_error, created_unix_nano, updated_unix_nano, lease_until_unix_nano, lease_token FROM source_cleanup_jobs`

type sourceCleanupScanner interface{ Scan(...any) error }

func scanSourceCleanupJob(scanner sourceCleanupScanner) (SourceCleanupJob, error) {
	var job SourceCleanupJob
	var retryInitial, retryMax, next, created, updated, lease int64
	var action, state string
	err := scanner.Scan(&job.JobID, &job.WatcherID, &job.TenantID, &job.SourcePath, &job.Fingerprint, &job.FileKey, &job.OperationID, &action, &job.MoveTargetPath, &job.FailureDirectory, &job.MaxAttempts, &retryInitial, &retryMax, &job.AttemptCount, &next, &state, &job.LastError, &created, &updated, &lease, &job.LeaseToken)
	if err != nil {
		return SourceCleanupJob{}, err
	}
	job.Action, job.State = SourceCleanupAction(action), SourceCleanupJobState(state)
	job.RetryInitialDelay, job.RetryMaxDelay = time.Duration(retryInitial), time.Duration(retryMax)
	job.NextAttemptAt, job.CreatedAt, job.UpdatedAt = time.Unix(0, next), time.Unix(0, created), time.Unix(0, updated)
	if lease != 0 {
		job.LeaseUntil = time.Unix(0, lease)
	}
	return job, nil
}

func (s *sqliteSourceCleanupStore) MarkImported(ctx context.Context, jobID int64, fileKey, operationID string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE source_cleanup_jobs SET file_key = ?, operation_id = ?, state = ?, next_attempt_unix_nano = ?, updated_unix_nano = ? WHERE id = ? AND state = ?`, fileKey, operationID, SourceCleanupStatePending, now.UnixNano(), now.UnixNano(), jobID, SourceCleanupStateImporting)
	if err != nil {
		return fmt.Errorf("mark source cleanup imported: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("source cleanup reservation %d is not importing", jobID)
	}
	return nil
}

func (s *sqliteSourceCleanupStore) ClaimDue(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]SourceCleanupJob, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("claim limit must be positive: %w", core.ErrInvalidArgument)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin source cleanup claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, sourceCleanupSelect+` WHERE state IN (?, ?, ?) AND next_attempt_unix_nano <= ? AND (lease_until_unix_nano = 0 OR lease_until_unix_nano <= ?) ORDER BY next_attempt_unix_nano, id LIMIT ?`, SourceCleanupStatePending, SourceCleanupStateRetrying, SourceCleanupStateMovePending, now.UnixNano(), now.UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("query due source cleanup jobs: %w", err)
	}
	var jobs []SourceCleanupJob
	for rows.Next() {
		job, scanErr := scanSourceCleanupJob(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due source cleanup job: %w", scanErr)
		}
		job.LeaseToken, job.LeaseUntil = uuid.NewString(), now.Add(leaseDuration)
		if _, err := tx.ExecContext(ctx, `UPDATE source_cleanup_jobs SET lease_until_unix_nano = ?, lease_token = ?, updated_unix_nano = ? WHERE id = ? AND (lease_until_unix_nano = 0 OR lease_until_unix_nano <= ?)`, job.LeaseUntil.UnixNano(), job.LeaseToken, now.UnixNano(), job.JobID, now.UnixNano()); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("lease source cleanup job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close due source cleanup rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due source cleanup jobs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit source cleanup claim: %w", err)
	}
	return jobs, nil
}

func (s *sqliteSourceCleanupStore) MarkSucceeded(ctx context.Context, job SourceCleanupJob) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin source cleanup success: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var result sql.Result
	if job.Action == SourceCleanupActionKeep {
		result, err = tx.ExecContext(ctx, `UPDATE source_cleanup_jobs SET state = ?, lease_until_unix_nano = 0, lease_token = '', updated_unix_nano = ? WHERE id = ? AND lease_token = ?`, SourceCleanupStateKept, time.Now().UnixNano(), job.JobID, job.LeaseToken)
	} else {
		result, err = tx.ExecContext(ctx, `DELETE FROM source_cleanup_jobs WHERE id = ? AND lease_token = ?`, job.JobID, job.LeaseToken)
	}
	if err != nil {
		return fmt.Errorf("complete source cleanup job: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrSourceCleanupLeaseLost
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit source cleanup success: %w", err)
	}
	return nil
}

func (s *sqliteSourceCleanupStore) MarkRetry(ctx context.Context, job SourceCleanupJob, next time.Time, actionErr error) error {
	return s.updateLeaseState(ctx, job, SourceCleanupStateRetrying, next, actionErr)
}

func (s *sqliteSourceCleanupStore) MarkMovePending(ctx context.Context, job SourceCleanupJob, next time.Time, actionErr error) error {
	return s.updateLeaseState(ctx, job, SourceCleanupStateMovePending, next, actionErr)
}

func (s *sqliteSourceCleanupStore) MarkFailed(ctx context.Context, job SourceCleanupJob, actionErr error) error {
	return s.updateLeaseState(ctx, job, SourceCleanupStateFailed, time.Time{}, actionErr)
}

func (s *sqliteSourceCleanupStore) updateLeaseState(ctx context.Context, job SourceCleanupJob, state SourceCleanupJobState, next time.Time, actionErr error) error {
	nextUnix := int64(0)
	if !next.IsZero() {
		nextUnix = next.UnixNano()
	}
	// last_error records the error type, never its message. SourceCleanupJob is
	// returned to callers, and a filesystem error embeds the full physical path
	// and the original file name, which the logging and reporting rules forbid.
	message := ""
	if actionErr != nil {
		message = fmt.Sprintf("%T", actionErr)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE source_cleanup_jobs SET state = ?, attempt_count = attempt_count + 1, next_attempt_unix_nano = ?, last_error = ?, lease_until_unix_nano = 0, lease_token = '', updated_unix_nano = ? WHERE id = ? AND lease_token = ?`, state, nextUnix, message, time.Now().UnixNano(), job.JobID, job.LeaseToken)
	if err != nil {
		return fmt.Errorf("update source cleanup job: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrSourceCleanupLeaseLost
	}
	return nil
}

// RecoverStaleImports returns abandoned reservations to the actionable set.
//
// Importing rows with no file key are reservations whose import never finished:
// they are deleted so the next scan re-imports the file. Importing rows that
// already carry a file key were imported but their action never ran; they go
// back to Pending.
//
// The keep predicate protects live work from that recovery: an import that
// legitimately outlives the reservation timeout (a very large file on slow
// storage) is still running in this process, and deleting its reservation would
// turn a successful import into a spurious failure and a duplicate. keep
// receives every stale candidate and reports whether it must be left untouched;
// nil recovers everything stale.
func (s *sqliteSourceCleanupStore) RecoverStaleImports(ctx context.Context, now time.Time, timeout time.Duration, keep func(SourceCleanupJob) bool) (int, error) {
	cutoff := now.Add(-timeout).UnixNano()

	rows, err := s.db.QueryContext(ctx, sourceCleanupSelect+` WHERE state = ? AND updated_unix_nano < ?`, SourceCleanupStateImporting, cutoff)
	if err != nil {
		return 0, fmt.Errorf("find stale source cleanup reservations: %w", err)
	}
	stale := make([]SourceCleanupJob, 0, 8)
	for rows.Next() {
		job, scanErr := scanSourceCleanupJob(rows)
		if scanErr != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan stale source cleanup reservation: %w", scanErr)
		}
		stale = append(stale, job)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("scan stale source cleanup reservations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale source cleanup scan: %w", err)
	}

	count := 0
	for _, job := range stale {
		if keep != nil && keep(job) {
			continue
		}
		if job.FileKey == "" {
			result, delErr := s.db.ExecContext(ctx, `DELETE FROM source_cleanup_jobs WHERE id = ? AND state = ?`, job.JobID, SourceCleanupStateImporting)
			if delErr != nil {
				return count, fmt.Errorf("remove stale source cleanup reservation: %w", delErr)
			}
			if affected, _ := result.RowsAffected(); affected > 0 {
				count++
			}
			continue
		}
		result, recErr := s.db.ExecContext(ctx, `UPDATE source_cleanup_jobs SET state = ?, next_attempt_unix_nano = ?, updated_unix_nano = ? WHERE id = ? AND state = ?`,
			SourceCleanupStatePending, now.UnixNano(), now.UnixNano(), job.JobID, SourceCleanupStateImporting)
		if recErr != nil {
			return count, fmt.Errorf("recover stale imported source cleanup job: %w", recErr)
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			count++
		}
	}
	return count, nil
}

// PruneTerminal retires terminal jobs older than the retention cutoff.
//
// A Kept job whose source file still exists is never pruned: the row is the
// durable dedup tombstone that stops the still-present file from being imported
// again after the retention window (deleting it would re-import every Keep file
// once per retention period). Such a confirmed row is stamped with the
// verification time, so the next prune cycle does not stat the same tombstone
// again before the retention window elapses. A Kept job whose source is gone has
// nothing left to dedup and is retired like a Failed job, which keeps the table
// bounded by the files that actually exist.
func (s *sqliteSourceCleanupStore) PruneTerminal(ctx context.Context, before time.Time, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, state, source_path FROM source_cleanup_jobs WHERE state IN (?, ?) AND updated_unix_nano < ? ORDER BY updated_unix_nano LIMIT ?`,
		SourceCleanupStateFailed, SourceCleanupStateKept, before.UnixNano(), limit)
	if err != nil {
		return 0, fmt.Errorf("list terminal source cleanup jobs: %w", err)
	}
	type terminalCandidate struct {
		id         int64
		state      SourceCleanupJobState
		sourcePath string
	}
	candidates := make([]terminalCandidate, 0, 8)
	for rows.Next() {
		var candidate terminalCandidate
		var state string
		if err := rows.Scan(&candidate.id, &state, &candidate.sourcePath); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan terminal source cleanup job: %w", err)
		}
		candidate.state = SourceCleanupJobState(state)
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("scan terminal source cleanup jobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close terminal source cleanup scan: %w", err)
	}

	verified := make([]int64, 0, len(candidates))
	absent := make([]int64, 0, len(candidates))

	for _, candidate := range candidates {
		// A Kept job keeps its source; a Failed job does not necessarily (the
		// action may have partially completed). Only a source file that is
		// confirmed absent permits pruning, so the tombstone tracks reality.
		//
		// Every other stat outcome keeps the row: a stat that failed for another
		// reason (a transient Windows sharing violation, an unreadable parent)
		// says nothing about the source, and retiring the tombstone on it would
		// let the next scan re-import the same file.
		if _, statErr := s.sourceStat(candidate.sourcePath); !errors.Is(statErr, fs.ErrNotExist) {
			if statErr == nil && candidate.state == SourceCleanupStateKept {
				verified = append(verified, candidate.id)
			}
			continue
		}
		absent = append(absent, candidate.id)
	}

	// One statement per chunk instead of one per row keeps a large prune batch
	// cheap, both for the stamps and for the deletes.
	if len(verified) > 0 {
		if _, err := s.execPruneIDs(ctx, verified,
			`UPDATE source_cleanup_jobs SET updated_unix_nano = ? WHERE state = ? AND id IN (`,
			[]any{time.Now().UnixNano(), SourceCleanupStateKept}); err != nil {
			return 0, fmt.Errorf("stamp verified source cleanup tombstones: %w", err)
		}
	}

	if len(absent) == 0 {
		return 0, nil
	}
	affected, err := s.execPruneIDs(ctx, absent,
		`DELETE FROM source_cleanup_jobs WHERE state IN (?, ?) AND id IN (`,
		[]any{SourceCleanupStateFailed, SourceCleanupStateKept})
	if err != nil {
		return int(affected), fmt.Errorf("prune terminal source cleanup jobs: %w", err)
	}

	return int(affected), nil
}

// execPruneIDs runs one id-list statement over ids in bounded chunks: prefix is
// the statement text before the placeholder list, prefixArgs are its leading
// arguments. It returns how many rows the statements changed.
func (s *sqliteSourceCleanupStore) execPruneIDs(ctx context.Context, ids []int64, prefix string, prefixArgs []any) (int64, error) {
	var affected int64

	for start := 0; start < len(ids); start += sourceCleanupPruneBatchChunk {
		end := min(start+sourceCleanupPruneBatchChunk, len(ids))
		chunk := ids[start:end]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")

		args := make([]any, 0, len(prefixArgs)+len(chunk))
		args = append(args, prefixArgs...)
		for _, id := range chunk {
			args = append(args, id)
		}

		result, err := s.db.ExecContext(ctx, prefix+placeholders+")", args...)
		if err != nil {
			return affected, err
		}
		rows, _ := result.RowsAffected()
		affected += rows
	}

	return affected, nil
}

func (s *sqliteSourceCleanupStore) Optimize(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpoint source cleanup database: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("vacuum source cleanup database: %w", err)
	}
	return nil
}
