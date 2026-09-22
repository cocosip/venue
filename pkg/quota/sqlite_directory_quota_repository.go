package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/sqlite"
)

// Layout and policy constants of the SQLite directory-quota store.
const (
	// quotaDatabaseFileName is the per-tenant quota database below
	// {DataPath}/{tenantId}, mirroring the design's
	// {quotaDirectory}/{tenantId}/quotas.db layout.
	quotaDatabaseFileName = "quotas.db"

	// quotaDatabaseDirPermissions is the mode of a created tenant directory.
	quotaDatabaseDirPermissions = 0o755

	// quotaSchemaVersionKey and quotaSchemaVersion record the logical schema
	// revision inside the database itself, so a future ALTER has a hook that does
	// not depend on the engine's user_version pragma.
	quotaSchemaVersionKey = "schema_version"
	quotaSchemaVersion    = "1"

	// quotaCheckpointTimeout bounds the best-effort write-ahead-log checkpoint a
	// handle release performs. Shutdown and eviction must not hang on a database
	// that another process is holding open.
	quotaCheckpointTimeout = 5 * time.Second

	// quotaCorruptedDatabaseRecoveryAttempts bounds how many quarantines a single
	// open may perform before it gives up. Without a bound, a database whose
	// recreation also fails would quarantine in a loop.
	quotaCorruptedDatabaseRecoveryAttempts = 2
)

// SQLite quota schema. Every statement is idempotent, so the whole script runs on
// every first access to a tenant database without a version check. The table and
// index are exactly the design's §7.2 inventory, including the deliberately
// non-Locus `enabled DEFAULT 0` of decision D-14: a row inserted outside the
// repository must read back as a disabled quota, which is Venue's domain default.
const (
	sqliteQuotasTableDDL = `
CREATE TABLE IF NOT EXISTS quotas (
    directory_path TEXT    PRIMARY KEY NOT NULL,
    current_count  INTEGER NOT NULL DEFAULT 0,
    max_count      INTEGER NOT NULL DEFAULT 0,
    enabled        INTEGER NOT NULL DEFAULT 0,
    last_updated   INTEGER NOT NULL,
    created_at     INTEGER NOT NULL
)`

	sqliteQuotasEnabledIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_quotas_enabled ON quotas(enabled)`

	sqliteQuotaSchemaMetaTableDDL = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
)`
)

// sqliteQuotaSchemaVersionDDL records the logical schema revision of a freshly
// created (or already initialized) quota database. It is idempotent, so it is
// safe to run on every open.
var sqliteQuotaSchemaVersionDDL = fmt.Sprintf(`
INSERT INTO schema_meta(key, value) VALUES ('%s', '%s')
    ON CONFLICT(key) DO NOTHING`, quotaSchemaVersionKey, quotaSchemaVersion)

// sqliteQuotaSchemaStatements is the DDL script every tenant database is
// initialized with. Pragmas are re-asserted first and the schema second,
// mirroring the order the design fixes; the DSN already carries the same pragma
// values, so the PRAGMA statements are an idempotent re-assertion that also makes
// the effective values observable through a query.
var sqliteQuotaSchemaStatements = []string{
	sqliteQuotasTableDDL,
	sqliteQuotasEnabledIndexDDL,
	sqliteQuotaSchemaMetaTableDDL,
	sqliteQuotaSchemaVersionDDL,
}

// sqliteQuotaColumns is the projection every row read uses. It is spelled out
// instead of "SELECT *" so a future additive column cannot silently change the
// scan order.
const sqliteQuotaColumns = `directory_path, current_count, max_count, enabled, last_updated, created_at`

// Runtime statements of the quota store.
const (
	// sqliteQuotaGetOrCreateSQL reads one quota row or creates it with Venue's
	// domain defaults. `DO UPDATE SET directory_path = quotas.directory_path` is
	// the idiom for "DO NOTHING, but with RETURNING": on conflict it returns the
	// existing row unchanged. `enabled` is bound explicitly as 0 (decision D-14).
	sqliteQuotaGetOrCreateSQL = `
INSERT INTO quotas (directory_path, current_count, max_count, enabled, last_updated, created_at)
VALUES (@directory_path, 0, 0, 0, @last_updated, @created_at)
ON CONFLICT(directory_path) DO UPDATE SET directory_path = quotas.directory_path
RETURNING ` + sqliteQuotaColumns

	// sqliteQuotaUpdateSQL is the administrative absolute write. It deliberately
	// leaves created_at alone: a quota's creation instant is immutable.
	sqliteQuotaUpdateSQL = `
UPDATE quotas
SET current_count = @current_count,
    max_count     = @max_count,
    enabled       = @enabled,
    last_updated  = @last_updated
WHERE directory_path = @directory_path`

	// sqliteQuotaCreateDefaultsSQL recreates a missing row with the same defaults
	// GetOrCreate would use. ON CONFLICT DO NOTHING keeps the create race-free.
	sqliteQuotaCreateDefaultsSQL = `
INSERT INTO quotas (directory_path, current_count, max_count, enabled, last_updated, created_at)
VALUES (@directory_path, 0, 0, 0, @last_updated, @created_at)
ON CONFLICT(directory_path) DO NOTHING`

	// sqliteQuotaIncrementSQL is the atomic increment of an existing row.
	sqliteQuotaIncrementSQL = `
UPDATE quotas
SET current_count = current_count + 1,
    last_updated  = @last_updated
WHERE directory_path = @directory_path`

	// sqliteQuotaIncrementCreateSQL creates the row with count 1, or increments
	// the row a concurrent writer created between the UPDATE and this statement.
	// `excluded.last_updated` is applied while the count is read from the stored
	// row, never from the bindings, so no increment is ever lost.
	sqliteQuotaIncrementCreateSQL = `
INSERT INTO quotas (directory_path, current_count, max_count, enabled, last_updated, created_at)
VALUES (@directory_path, 1, 0, 0, @last_updated, @created_at)
ON CONFLICT(directory_path) DO UPDATE SET
    current_count = quotas.current_count + 1,
    last_updated  = excluded.last_updated`

	// sqliteQuotaDecrementSQL floors the count at 0 in the statement itself, so
	// the count can never go negative. A missing row matches nothing, which is
	// the Badger implementation's "nothing to decrement" no-op.
	sqliteQuotaDecrementSQL = `
UPDATE quotas
SET current_count = MAX(current_count - 1, 0),
    last_updated  = @last_updated
WHERE directory_path = @directory_path`

	// sqliteQuotaSelectAllSQL lists one tenant's quotas in a deterministic order.
	// SQLite's default BINARY collation compares the stored bytes, which is the
	// same order the Badger key prefix produced.
	sqliteQuotaSelectAllSQL = `SELECT ` + sqliteQuotaColumns + `
FROM quotas ORDER BY directory_path ASC`
)

// SQLiteDirectoryQuotaRepositoryOptions configures the SQLite directory-quota
// repository.
type SQLiteDirectoryQuotaRepositoryOptions struct {
	// DataPath is the quota root. One database file is created per tenant at
	// {DataPath}/{tenantId}/quotas.db, lazily, the first time the tenant is
	// touched. It must not be empty.
	DataPath string

	// Sqlite is the connection and pragma policy applied to every tenant
	// database. The zero value selects sqlite.DefaultOptions().
	Sqlite sqlite.Options

	// MaxOpenDatabases bounds how many tenant handles stay open at once. Zero
	// means unlimited, which is the default: every touched tenant keeps its
	// handle until Close. A positive value evicts the least recently used idle
	// handle on the next acquisition.
	MaxOpenDatabases int

	// OpenDatabaseIdleTimeout closes a tenant handle that has been idle for this
	// long. Zero disables idle eviction.
	OpenDatabaseIdleTimeout time.Duration

	// RecoverCorruptedDatabase quarantines a database file that cannot be opened
	// because it is corrupted and recreates an empty one instead of failing the
	// caller. A lock or permission failure is never treated as corruption.
	//
	// Quarantining a quota database is not data loss in this runtime: the counts
	// are reconciled from metadata at startup, so a recovered tenant continues
	// with an empty quota store instead of failing.
	RecoverCorruptedDatabase bool

	// CorruptedDatabaseRetention is how long a quarantined database file is kept
	// before it is pruned during the next open. Zero selects 72 hours; a negative
	// value disables pruning.
	CorruptedDatabaseRetention time.Duration

	// Logging receives structured repository diagnostics. A nil runtime means
	// silent, and logging never fails a storage operation.
	Logging *logging.Runtime

	// OnCorruptedDatabase, when set, is called synchronously with the quarantined
	// file path after a database file was quarantined. It runs on the opening
	// goroutine and must not block.
	OnCorruptedDatabase func(quarantinedPath string)

	// now is the clock seam. A nil value selects time.Now. It is unexported
	// because it exists only so tests can drive idle eviction without sleeping.
	now func() time.Time
}

// sqliteDirectoryQuotaRepository is the SQLite-backed
// core.DirectoryQuotaRepository.
//
// One facade serves every tenant: the tenant travels as a method parameter and
// resolves to that tenant's own database file, opened lazily on first access.
// The handle table is guarded by mu; each tenant database additionally serializes
// its own multi-statement work on its own mutex so a slow tenant never blocks
// another tenant's statements.
//
// Close is idempotent and concurrency safe: the first caller performs the
// shutdown and every caller receives that same result.
type sqliteDirectoryQuotaRepository struct {
	dataPath string
	sqlite   sqlite.Options

	maxOpenDatabases int
	idleTimeout      time.Duration

	corruptRecovery  bool
	corruptRetention time.Duration

	logging             *logging.Runtime
	onCorruptedDatabase func(quarantinedPath string)

	now func() time.Time

	mu        sync.RWMutex
	closed    bool
	databases map[string]*sqliteQuotaTenantDatabase

	closeOnce sync.Once
	closeErr  error
}

// sqliteQuotaTenantDatabase is one tenant's database handle and the bookkeeping
// the handle bound and the idle eviction need.
//
// Every handle in the repository's table has a usable connection: a handle is
// published only after its database was opened, and it leaves the table before it
// is closed.
type sqliteQuotaTenantDatabase struct {
	tenantID string
	path     string
	db       *sql.DB

	// mu serializes this tenant's multi-statement work. The single connection of
	// the pooled handle already serializes statements; this mutex is what keeps a
	// maintenance statement (VACUUM, checkpoint) from separating the two
	// statements of an increment, and it is what proves a handle is idle before
	// it is closed.
	mu sync.Mutex

	// refs counts the operations currently using the handle. Eviction never
	// closes a handle whose refs is non-zero.
	refs int64

	// lastUsedNanos is the mirror of lastUsed the eviction scan sorts on.
	lastUsedNanos int64
	lastUsed      time.Time
}

// errSQLiteQuotaRepositoryClosed classifies use of a closed repository as an
// infrastructure failure.
func errSQLiteQuotaRepositoryClosed() error {
	return fmt.Errorf("directory quota repository is closed: %w", core.ErrDatabaseError)
}

// Compile-time proof that the repository satisfies the frozen interface.
var _ core.DirectoryQuotaRepository = (*sqliteDirectoryQuotaRepository)(nil)

// NewSQLiteDirectoryQuotaRepository builds a SQLite directory-quota repository.
//
// The returned facade is shared by every tenant, exactly like the Badger
// implementation, and it opens no database until a tenant is first touched. The
// caller owns the repository and must Close it, which checkpoints and closes
// every open tenant handle.
//
// Errors wrap core.ErrInvalidArgument for a rejected option and
// core.ErrDatabaseError for a store that cannot be opened.
func NewSQLiteDirectoryQuotaRepository(opts *SQLiteDirectoryQuotaRepositoryOptions) (core.DirectoryQuotaRepository, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}
	if strings.TrimSpace(opts.DataPath) == "" {
		return nil, fmt.Errorf("quota data path cannot be empty: %w", core.ErrInvalidArgument)
	}
	if opts.MaxOpenDatabases < 0 {
		return nil, fmt.Errorf("quota max open databases cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.OpenDatabaseIdleTimeout < 0 {
		return nil, fmt.Errorf("quota open database idle timeout cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.CorruptedDatabaseRetention < 0 {
		return nil, fmt.Errorf("quota corrupted database retention cannot be negative: %w", core.ErrInvalidArgument)
	}

	sqliteOptions := opts.Sqlite
	if sqliteOptions == (sqlite.Options{}) {
		sqliteOptions = sqlite.DefaultOptions()
	} else if err := sqliteOptions.Validate(); err != nil {
		return nil, err
	}

	now := opts.now
	if now == nil {
		now = time.Now
	}

	return &sqliteDirectoryQuotaRepository{
		dataPath:            opts.DataPath,
		sqlite:              sqliteOptions,
		maxOpenDatabases:    opts.MaxOpenDatabases,
		idleTimeout:         opts.OpenDatabaseIdleTimeout,
		corruptRecovery:     opts.RecoverCorruptedDatabase,
		corruptRetention:    opts.CorruptedDatabaseRetention,
		logging:             opts.Logging,
		onCorruptedDatabase: opts.OnCorruptedDatabase,
		now:                 now,
		databases:           make(map[string]*sqliteQuotaTenantDatabase),
	}, nil
}

// GetOrCreate retrieves a directory quota or creates it with Venue's domain
// defaults (CurrentCount 0, MaxCount 0, Enabled false).
//
// The two arguments are validated before any database is touched: a tenant ID
// must be usable as a path segment, and the directory path must not be empty.
func (r *sqliteDirectoryQuotaRepository) GetOrCreate(ctx context.Context, tenantID string, directoryPath string) (*core.DirectoryQuota, error) {
	if directoryPath == "" {
		return nil, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.acquire(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.release(handle)

	now := timeToNanos(r.now())
	quota, err := scanDirectoryQuota(handle.db.QueryRowContext(ctx, sqliteQuotaGetOrCreateSQL,
		sql.Named("directory_path", directoryPath),
		sql.Named("last_updated", now),
		sql.Named("created_at", now)))
	if err != nil {
		return nil, fmt.Errorf("failed to get or create directory quota: %w: %w", err, core.ErrDatabaseError)
	}
	return quota, nil
}

// Update replaces a directory quota.
//
// The stored row keeps its original creation instant; the caller's UpdatedAt is
// overwritten with the repository clock, matching the Badger implementation's
// observable side effect. A quota whose directory has no row yet is created with
// the defaults GetOrCreate would have written, and the absolute write is then
// retried once, so Update never becomes a silent no-op.
func (r *sqliteDirectoryQuotaRepository) Update(ctx context.Context, tenantID string, quota *core.DirectoryQuota) error {
	if quota == nil {
		return fmt.Errorf("quota cannot be nil: %w", core.ErrInvalidArgument)
	}
	if quota.DirectoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.acquire(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.release(handle)

	quota.UpdatedAt = r.now()
	now := timeToNanos(quota.UpdatedAt)
	enabled := 0
	if quota.Enabled {
		enabled = 1
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()

	affected, err := execQuotaStatement(ctx, handle,
		sqliteQuotaUpdateSQL,
		sql.Named("current_count", quota.CurrentCount),
		sql.Named("max_count", quota.MaxCount),
		sql.Named("enabled", enabled),
		sql.Named("last_updated", now),
		sql.Named("directory_path", quota.DirectoryPath))
	if err != nil {
		return fmt.Errorf("failed to update directory quota: %w", err)
	}
	if affected > 0 {
		return nil
	}

	if _, err := execQuotaStatement(ctx, handle,
		sqliteQuotaCreateDefaultsSQL,
		sql.Named("directory_path", quota.DirectoryPath),
		sql.Named("last_updated", now),
		sql.Named("created_at", now)); err != nil {
		return fmt.Errorf("failed to create the missing directory quota: %w", err)
	}
	if _, err := execQuotaStatement(ctx, handle,
		sqliteQuotaUpdateSQL,
		sql.Named("current_count", quota.CurrentCount),
		sql.Named("max_count", quota.MaxCount),
		sql.Named("enabled", enabled),
		sql.Named("last_updated", now),
		sql.Named("directory_path", quota.DirectoryPath)); err != nil {
		return fmt.Errorf("failed to update the recreated directory quota: %w", err)
	}
	return nil
}

// IncrementCount atomically increments a directory's file count.
//
// The count is incremented by the statement itself, never by a read-modify-write
// in Go, and the create path of a missing row uses the same conditional upsert,
// so concurrent increments on the same directory can never lose an update.
func (r *sqliteDirectoryQuotaRepository) IncrementCount(ctx context.Context, tenantID string, directoryPath string) error {
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.acquire(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.release(handle)

	handle.mu.Lock()
	defer handle.mu.Unlock()

	now := timeToNanos(r.now())
	affected, err := execQuotaStatement(ctx, handle,
		sqliteQuotaIncrementSQL,
		sql.Named("last_updated", now),
		sql.Named("directory_path", directoryPath))
	if err != nil {
		return fmt.Errorf("failed to increment directory quota count: %w", err)
	}
	if affected > 0 {
		return nil
	}

	// The row did not exist. Create it as the Badger implementation does (count
	// 1, unlimited, disabled) through a conditional upsert, so a concurrent
	// creator's increment is not lost.
	if _, err := execQuotaStatement(ctx, handle,
		sqliteQuotaIncrementCreateSQL,
		sql.Named("directory_path", directoryPath),
		sql.Named("last_updated", now),
		sql.Named("created_at", now)); err != nil {
		return fmt.Errorf("failed to increment the missing directory quota count: %w", err)
	}
	return nil
}

// DecrementCount atomically decrements a directory's file count.
//
// The floor is the statement's own MAX(current_count - 1, 0), so the count can
// never go negative and concurrent decrements cannot race a Go-side check. A
// directory with no row is a no-op, matching the Badger implementation.
func (r *sqliteDirectoryQuotaRepository) DecrementCount(ctx context.Context, tenantID string, directoryPath string) error {
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.acquire(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.release(handle)

	if _, err := execQuotaStatement(ctx, handle,
		sqliteQuotaDecrementSQL,
		sql.Named("last_updated", timeToNanos(r.now())),
		sql.Named("directory_path", directoryPath)); err != nil {
		return fmt.Errorf("failed to decrement directory quota count: %w", err)
	}
	return nil
}

// GetAll returns every directory quota of one tenant, ordered by directory path.
//
// The result is never nil, so an empty tenant yields an empty slice just like the
// Badger implementation.
func (r *sqliteDirectoryQuotaRepository) GetAll(ctx context.Context, tenantID string) ([]*core.DirectoryQuota, error) {
	handle, err := r.acquire(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.release(handle)

	rows, err := handle.db.QueryContext(ctx, sqliteQuotaSelectAllSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory quotas: %w: %w", err, core.ErrDatabaseError)
	}
	defer func() { _ = rows.Close() }()

	quotas := make([]*core.DirectoryQuota, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		quota, scanErr := scanDirectoryQuota(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("failed to read directory quota: %w: %w", scanErr, core.ErrDatabaseError)
		}
		quotas = append(quotas, quota)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list directory quotas: %w: %w", err, core.ErrDatabaseError)
	}
	return quotas, nil
}

// Optimize checkpoints, compacts and reclaims.
//
// Every tenant with an open handle is checkpointed with
// wal_checkpoint(TRUNCATE) and then compacted with VACUUM, which cannot run
// inside a transaction and therefore runs under the tenant's own mutex. Tenants
// without a handle are deliberately skipped, so maintenance never opens a
// database that the runtime is not already using.
//
// When a handle bound or an idle timeout is configured, the same pass first
// reclaims the handles that outlived their policy, which is the maintenance
// cycle's half of the design's idle reclamation.
func (r *sqliteDirectoryQuotaRepository) Optimize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errSQLiteQuotaRepositoryClosed()
	}
	evicted := r.evictLocked("")
	handles := r.snapshotHandlesLocked()
	r.mu.Unlock()
	r.closeEvicted(evicted)

	for _, handle := range handles {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.optimizeHandle(ctx, handle)
	}
	return nil
}

// optimizeHandle compacts one tenant database.
//
// VACUUM and wal_checkpoint(TRUNCATE) cannot run inside a transaction, so the
// tenant mutex is held for the whole pass: that is what keeps them from
// interleaving with a statement sequence on the same single connection. A
// failure is logged and does not stop the remaining tenants.
func (r *sqliteDirectoryQuotaRepository) optimizeHandle(ctx context.Context, handle *sqliteQuotaTenantDatabase) {
	handle.mu.Lock()
	defer handle.mu.Unlock()

	if err := sqlite.TruncateCheckpoint(ctx, handle.db); err != nil {
		warnQuotaRepositoryEvent(r.logging, "quota_optimize_failed", "checkpoint_failed")
		return
	}
	if _, err := handle.db.ExecContext(ctx, "VACUUM"); err != nil {
		warnQuotaRepositoryEvent(r.logging, "quota_optimize_failed", "vacuum_failed")
	}
}

// Close checkpoints and closes every open tenant database. It is idempotent and
// concurrency safe: the first caller performs the shutdown and every caller
// receives that same result, so a second caller can never report success while a
// handle is still open.
func (r *sqliteDirectoryQuotaRepository) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		handles := r.snapshotHandlesLocked()
		r.databases = make(map[string]*sqliteQuotaTenantDatabase)
		r.mu.Unlock()

		var errs []error
		for _, handle := range handles {
			if err := r.closeHandle(handle); err != nil {
				errs = append(errs, err)
			}
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

// closeHandle checkpoints and closes one tenant database. Checkpointing before
// the close is what empties the WAL sidecar and lets the file handle be released
// cleanly, which a Windows file operation such as a quarantine depends on.
func (r *sqliteDirectoryQuotaRepository) closeHandle(handle *sqliteQuotaTenantDatabase) error {
	handle.mu.Lock()
	defer handle.mu.Unlock()

	checkpointCtx, cancel := context.WithTimeout(context.Background(), quotaCheckpointTimeout)
	defer cancel()
	if err := sqlite.TruncateCheckpoint(checkpointCtx, handle.db); err != nil {
		warnQuotaRepositoryEvent(r.logging, "quota_close_failed", "checkpoint_failed")
	}
	// PRAGMA optimize is advisory: it refreshes the query planner's statistics and
	// a failure to do so must never fail a shutdown.
	_, _ = handle.db.ExecContext(context.Background(), "PRAGMA optimize")

	if err := handle.db.Close(); err != nil {
		return fmt.Errorf("failed to close directory quota database for tenant %q: %w: %w",
			handle.tenantID, err, core.ErrDatabaseError)
	}
	return nil
}

// acquire resolves one tenant's handle and registers one reference on it.
//
// The handle table is held across a first open, so every handle in the table
// always has a usable connection and two concurrent first accesses to the same
// tenant cannot each open the database. Tenant opens are rare (once per tenant
// until a handle is evicted), which is what makes that serialization cheap.
//
// The handle bound and the idle timeout are applied on every acquisition. A
// handle that is in use is never reclaimed, so the bound applies to idle handles
// and converges as soon as the operations holding the excess are done.
func (r *sqliteDirectoryQuotaRepository) acquire(ctx context.Context, tenantID string) (*sqliteQuotaTenantDatabase, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := core.ValidateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("invalid tenant ID: %w", err)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errSQLiteQuotaRepositoryClosed()
	}
	if handle, ok := r.databases[tenantID]; ok {
		r.touchLocked(handle)
		evicted := r.evictLocked(tenantID)
		r.mu.Unlock()
		r.closeEvicted(evicted)
		return handle, nil
	}

	db, path, quarantined, err := r.openTenantDatabase(ctx, tenantID)
	if err != nil {
		r.mu.Unlock()
		r.notifyQuarantined(quarantined)
		return nil, err
	}
	if r.closed {
		r.mu.Unlock()
		r.notifyQuarantined(quarantined)
		_ = db.Close()
		return nil, errSQLiteQuotaRepositoryClosed()
	}

	handle := &sqliteQuotaTenantDatabase{tenantID: tenantID, path: path, db: db}
	r.touchLocked(handle)
	r.databases[tenantID] = handle
	// The bound is applied after the handle joined the table, so the acquisition
	// that created the excess is the one that reclaims it.
	evicted := r.evictLocked(tenantID)
	r.mu.Unlock()
	r.closeEvicted(evicted)
	r.notifyQuarantined(quarantined)
	return handle, nil
}

// notifyQuarantined reports every quarantine of one open to the caller's hook.
//
// It runs outside the repository lock, so a hook that re-enters the repository
// cannot deadlock it and a hook that panics cannot leave the handle table locked.
func (r *sqliteDirectoryQuotaRepository) notifyQuarantined(quarantined []string) {
	if r.onCorruptedDatabase == nil {
		return
	}
	for _, quarantinedPath := range quarantined {
		r.onCorruptedDatabase(quarantinedPath)
	}
}

// release drops one reference on a handle acquired by acquire. The usage stamp is
// refreshed so a long operation does not look idle the moment it finishes, and the
// handle bound is re-applied so it converges as soon as the operations that made
// it exceed the limit are done.
func (r *sqliteDirectoryQuotaRepository) release(handle *sqliteQuotaTenantDatabase) {
	if handle == nil {
		return
	}
	r.mu.Lock()
	if handle.refs > 0 {
		handle.refs--
	}
	handle.lastUsed = r.now()
	handle.lastUsedNanos = handle.lastUsed.UnixNano()

	var evicted []*sqliteQuotaTenantDatabase
	if r.maxOpenDatabases > 0 && len(r.databases) > r.maxOpenDatabases {
		evicted = r.evictLocked("")
	}
	r.mu.Unlock()
	r.closeEvicted(evicted)
}

// touchLocked refreshes a handle's usage and reference count. The caller must
// hold mu.
func (r *sqliteDirectoryQuotaRepository) touchLocked(handle *sqliteQuotaTenantDatabase) {
	handle.refs++
	handle.lastUsed = r.now()
	handle.lastUsedNanos = handle.lastUsed.UnixNano()
}

// evictLocked removes the handles that exceed the bound or sat idle for too long
// from the handle table and returns them for the caller to close.
//
// exclude names the handle the caller is about to use, which is never removed. A
// handle with an active reference is skipped rather than closed: it is in use
// right now, and reclaiming it would break the operation that holds it. Handles
// are removed oldest-first, so eviction is deterministic.
//
// The caller must hold mu and must close the returned handles after releasing it.
func (r *sqliteDirectoryQuotaRepository) evictLocked(exclude string) []*sqliteQuotaTenantDatabase {
	if r.maxOpenDatabases <= 0 && r.idleTimeout <= 0 {
		return nil
	}

	candidates := make([]*sqliteQuotaTenantDatabase, 0, len(r.databases))
	for tenantID, handle := range r.databases {
		if tenantID == exclude || handle.refs > 0 {
			continue
		}
		candidates = append(candidates, handle)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].lastUsedNanos == candidates[j].lastUsedNanos {
			return candidates[i].tenantID < candidates[j].tenantID
		}
		return candidates[i].lastUsedNanos < candidates[j].lastUsedNanos
	})

	var evicted []*sqliteQuotaTenantDatabase

	// Idle eviction can close every candidate.
	if r.idleTimeout > 0 {
		cutoff := r.now().Add(-r.idleTimeout).UnixNano()
		for _, handle := range candidates {
			if handle.lastUsedNanos > cutoff {
				break
			}
			delete(r.databases, handle.tenantID)
			evicted = append(evicted, handle)
		}
	}

	// The bound is enforced after idle eviction, so only the excess is closed.
	if r.maxOpenDatabases > 0 {
		for _, handle := range candidates {
			if len(r.databases) <= r.maxOpenDatabases {
				break
			}
			if _, ok := r.databases[handle.tenantID]; !ok {
				continue
			}
			delete(r.databases, handle.tenantID)
			evicted = append(evicted, handle)
		}
	}
	return evicted
}

// closeEvicted closes the handles an eviction removed from the table.
//
// The handles have already left the table and carry no reference, so no
// concurrent acquisition can reach them and closing them here cannot stall
// another tenant's acquisition through the repository lock.
func (r *sqliteDirectoryQuotaRepository) closeEvicted(handles []*sqliteQuotaTenantDatabase) {
	for _, handle := range handles {
		if err := r.closeHandle(handle); err != nil {
			warnQuotaRepositoryEvent(r.logging, "quota_handle_eviction_failed", "close_failed")
		}
	}
}

// snapshotHandlesLocked returns a stable, deterministically ordered copy of the
// open handles. The caller must hold mu.
func (r *sqliteDirectoryQuotaRepository) snapshotHandlesLocked() []*sqliteQuotaTenantDatabase {
	handles := make([]*sqliteQuotaTenantDatabase, 0, len(r.databases))
	for _, handle := range r.databases {
		handles = append(handles, handle)
	}
	sort.Slice(handles, func(i, j int) bool { return handles[i].tenantID < handles[j].tenantID })
	return handles
}

// openHandleCount reports how many tenant handles are open. It exists so a test
// can assert the configured handle bound.
func (r *sqliteDirectoryQuotaRepository) openHandleCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.databases)
}

// openTenantIDs returns the tenants that currently have an open handle, in stable
// order. It exists so a test can assert which handle an eviction reclaimed.
func (r *sqliteDirectoryQuotaRepository) openTenantIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tenantIDs := make([]string, 0, len(r.databases))
	for tenantID := range r.databases {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

// openTenantDatabase creates the tenant directory, recovers a damaged database
// when the caller asked for it, and opens the SQLite handle.
//
// The returned quarantined paths are reported to the caller's hook by the
// acquisition, outside the repository lock.
func (r *sqliteDirectoryQuotaRepository) openTenantDatabase(ctx context.Context, tenantID string) (*sql.DB, string, []string, error) {
	directory := filepath.Join(r.dataPath, tenantID)
	if err := os.MkdirAll(directory, quotaDatabaseDirPermissions); err != nil {
		return nil, "", nil, fmt.Errorf("failed to create the tenant quota directory: %w: %w", err, core.ErrDatabaseError)
	}

	path := filepath.Join(directory, quotaDatabaseFileName)
	pruneQuotaCorruptedDatabaseFiles(path, r.corruptRetention, r.now)

	db, quarantined, err := r.openDatabaseWithRecovery(ctx, path)
	if err != nil {
		return nil, "", quarantined, err
	}
	return db, path, quarantined, nil
}

// openDatabaseWithRecovery opens path, quarantining it and recreating an empty
// database when the failure is a positive corruption verdict and recovery is
// enabled. A lock, permission, or path failure is never treated as corruption.
//
// It returns the paths of the files it quarantined, so the caller can report them
// after it released every repository lock.
//
// A quarantined quota database is not data loss in this runtime — the counts are
// reconciled from metadata at startup — so a successful recovery is reported as
// success and only a quarantine that cannot be performed at all is an error.
func (r *sqliteDirectoryQuotaRepository) openDatabaseWithRecovery(ctx context.Context, path string) (*sql.DB, []string, error) {
	var (
		lastErr     error
		quarantined []string
	)

	for attempt := 0; attempt < quotaCorruptedDatabaseRecoveryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, quarantined, err
		}

		db, err := r.openTenantDatabaseOnce(ctx, path)
		if err == nil {
			return db, quarantined, nil
		}
		if !isQuotaDatabaseCorruptError(err) {
			return nil, quarantined, err
		}
		lastErr = err

		if !r.corruptRecovery {
			return nil, quarantined, err
		}

		quarantinePath, quarantineErr := quarantineQuotaDatabaseFile(path, r.now())
		if quarantineErr != nil {
			return nil, quarantined, errors.Join(err, fmt.Errorf("failed to quarantine the corrupted directory quota database: %w", quarantineErr))
		}
		removeQuotaDatabaseSidecars(path)
		warnQuotaRepositoryEvent(r.logging, "quota_database_quarantined", "corruption_detected")
		quarantined = append(quarantined, quarantinePath)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("directory quota database could not be recovered: %w", core.ErrDatabaseError)
	}
	return nil, quarantined, lastErr
}

// openTenantDatabaseOnce opens one connection through the shared SQLite
// foundation, re-asserts the pragmas, verifies the file with integrity_check(1),
// and creates the schema.
//
// The integrity check is the second half of the corruption verdict: a file can be
// structurally readable and still hold a malformed b-tree, and that state is
// exactly what the quarantine path is for.
func (r *sqliteDirectoryQuotaRepository) openTenantDatabaseOnce(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sqlite.Open(path, r.sqlite)
	if err != nil {
		return nil, err
	}

	if err := r.applyPragmas(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to configure the directory quota database: %w: %w", err, core.ErrDatabaseError)
	}

	if err := verifyQuotaDatabase(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := applyQuotaSchema(ctx, db); err != nil {
		_ = db.Close()
		if isQuotaDatabaseCorruptError(err) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to initialize the directory quota schema: %w: %w", err, core.ErrDatabaseError)
	}
	return db, nil
}

// applyPragmas re-asserts the pragma set the DSN already carries. The statements
// are idempotent; keeping them makes the effective values observable through a
// query and catches a DSN-construction regression.
func (r *sqliteDirectoryQuotaRepository) applyPragmas(ctx context.Context, db *sql.DB) error {
	statements := []string{
		"PRAGMA journal_mode=" + strings.ToUpper(strings.TrimSpace(r.sqlite.JournalMode)),
		"PRAGMA synchronous=" + strings.ToUpper(strings.TrimSpace(r.sqlite.SynchronousMode)),
		fmt.Sprintf("PRAGMA cache_size=%d", r.sqlite.CacheSizeKb),
		fmt.Sprintf("PRAGMA busy_timeout=%d", r.sqlite.BusyTimeoutMs),
		"PRAGMA foreign_keys=OFF",
		"PRAGMA temp_store=MEMORY",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// applyQuotaSchema runs the idempotent DDL script of the quota database.
func applyQuotaSchema(ctx context.Context, db *sql.DB) error {
	for _, statement := range sqliteQuotaSchemaStatements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// execQuotaStatement runs one write statement of a tenant handle and reports how
// many rows it changed.
func execQuotaStatement(ctx context.Context, handle *sqliteQuotaTenantDatabase, statement string, args ...any) (int64, error) {
	result, err := handle.db.ExecContext(ctx, statement, args...)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", err, core.ErrDatabaseError)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read the affected row count: %w: %w", err, core.ErrDatabaseError)
	}
	return affected, nil
}

// sqliteQuotaScanner is the row surface shared by *sql.Row and *sql.Rows.
type sqliteQuotaScanner interface {
	Scan(dest ...any) error
}

// scanDirectoryQuota maps one database row onto the domain model. Timestamps are
// stored as Unix nanoseconds in UTC, and `enabled` is an INTEGER flag.
func scanDirectoryQuota(scanner sqliteQuotaScanner) (*core.DirectoryQuota, error) {
	var (
		directoryPath string
		currentCount  int64
		maxCount      int64
		enabled       int64
		lastUpdated   int64
		createdAt     int64
	)
	if err := scanner.Scan(&directoryPath, &currentCount, &maxCount, &enabled, &lastUpdated, &createdAt); err != nil {
		return nil, err
	}
	return &core.DirectoryQuota{
		DirectoryPath: directoryPath,
		CurrentCount:  int(currentCount),
		MaxCount:      int(maxCount),
		Enabled:       enabled != 0,
		CreatedAt:     nanosToTime(createdAt),
		UpdatedAt:     nanosToTime(lastUpdated),
	}, nil
}

// timeToNanos writes a domain time into an INTEGER column.
func timeToNanos(t time.Time) int64 {
	return t.UTC().UnixNano()
}

// nanosToTime reads an INTEGER column back into a domain time, always in UTC.
func nanosToTime(nanos int64) time.Time {
	return time.Unix(0, nanos).UTC()
}

// warnQuotaRepositoryEvent emits one path-free structured warning.
//
// Raw errors, tenant file names and physical paths are deliberately excluded: a
// file-system error embeds the tenant directory, and the logging rules keep full
// physical paths out of logs.
func warnQuotaRepositoryEvent(rt *logging.Runtime, event string, reason string) {
	if rt == nil || !rt.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	rt.Emit(context.Background(), logging.Record{
		Level:     slog.LevelWarn,
		Component: "directory_quota_repository",
		Event:     event,
		Message:   "sqlite directory quota repository degraded",
		Attrs: []slog.Attr{
			slog.String("reason", reason),
		},
	})
}
