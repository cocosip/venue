package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/sqlite"
	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Layout and policy constants of the SQLite metadata store. The file-name
// constants are deliberately shared with their Badger counterparts
// (corruptedDatabaseSuffix, corruptedDatabaseTimestampLayout, backupFilePrefix,
// ...) so a backup or quarantine produced before the engine switch keeps being
// recognized by the operands that own that naming.
const (
	// metadataDatabaseFileName is the per-tenant database file below
	// {DataPath}/{tenantId}.
	metadataDatabaseFileName = "metadata.db"

	// walSuffix and shmSuffix name the write-ahead log and shared-memory
	// sidecars of a WAL-mode database. A quarantine or a restore must handle them
	// explicitly, because they belong to the database file that is being replaced.
	walSuffix = "-wal"
	shmSuffix = "-shm"

	// metadataDatabaseDirPermissions is the mode of a created tenant directory
	// and of the backup tree below it.
	metadataDatabaseDirPermissions = 0o755

	// sqliteSchemaVersionKey and sqliteSchemaVersion record the logical schema
	// revision inside the database itself, so a future ALTER has a hook that
	// does not depend on the engine's user_version pragma.
	sqliteSchemaVersionKey = "schema_version"
	sqliteSchemaVersion    = "2"

	// sqliteUnlimitedQueryLimit bounds a status query that asks for no limit.
	// SQLite accepts a negative LIMIT as "unlimited"; -1 is bound explicitly so
	// the intent is visible in the SQL and in a query trace.
	sqliteUnlimitedQueryLimit = -1

	// corruptedDatabaseRecoveryAttempts bounds how many quarantines a single
	// open may perform before it gives up. Without a bound, a database whose
	// recreation also fails would quarantine in a loop.
	corruptedDatabaseRecoveryAttempts = 2
)

// SQLite metadata schema. Every statement is idempotent, so the whole script can
// run on every first access to a tenant database without a version check. The
// index set is exactly the design's §6.2 inventory.
const (
	sqliteFilesTableDDL = `
CREATE TABLE IF NOT EXISTS files (
    file_key                        TEXT    PRIMARY KEY NOT NULL,
    tenant_id                       TEXT    NOT NULL,
    volume_id                       TEXT    NOT NULL,
    physical_path                   TEXT    NOT NULL,
    directory_path                  TEXT    NOT NULL,
    file_size                       INTEGER NOT NULL DEFAULT 0,
    created_at                      INTEGER NOT NULL,
    updated_at                      INTEGER NOT NULL,
    status                          INTEGER NOT NULL DEFAULT 0,
    retry_count                     INTEGER NOT NULL DEFAULT 0,
    last_failed_at                  INTEGER,
    last_error                      TEXT    NOT NULL DEFAULT '',
    processing_start_time           INTEGER,
    released_processing_start_time  INTEGER,
    completed_at                    INTEGER,
    dead_lettered_at                INTEGER,
    available_for_processing_at     INTEGER NOT NULL DEFAULT 0,
    original_file_name              TEXT    NOT NULL DEFAULT '',
    file_extension                  TEXT    NOT NULL DEFAULT '',
    import_operation_id             TEXT
)`

	sqliteFilesImportOperationIndexDDL = `
CREATE UNIQUE INDEX IF NOT EXISTS idx_files_import_operation
    ON files(tenant_id, import_operation_id)
    WHERE import_operation_id IS NOT NULL AND import_operation_id <> ''`

	sqliteFilesStatusAvailableIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_files_status_available
    ON files(status, available_for_processing_at, created_at, file_key)`

	sqliteFilesStatusCreatedAtIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_files_status_created_at
    ON files(status, created_at, file_key)`

	sqliteFilesStatusCompletedAtIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_files_status_completed_at
    ON files(status, completed_at, file_key)`

	sqliteFilesStatusLastFailedAtIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_files_status_last_failed_at
    ON files(status, last_failed_at, file_key)`

	sqliteFilesProcessingStartIndexDDL = `
CREATE INDEX IF NOT EXISTS idx_files_processing_start
    ON files(processing_start_time, file_key) WHERE status = 1`

	sqliteSchemaMetaTableDDL = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
)`

	sqliteSchemaVersionDDL = `
INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2')
    ON CONFLICT(key) DO UPDATE SET value = excluded.value`
)

// sqliteFileMetadataColumns is the projection every row read uses. It is
// spelled out instead of "SELECT *" so a future additive column cannot silently
// change the scan order.
const sqliteFileMetadataColumns = `file_key, tenant_id, volume_id, physical_path, directory_path,
    file_size, created_at, updated_at, status, retry_count,
    last_failed_at, last_error, processing_start_time, released_processing_start_time,
    completed_at, dead_lettered_at, available_for_processing_at,
    original_file_name, file_extension, import_operation_id`

// sqliteFileMetadataUpsertSQL inserts or replaces one file record.
//
// The upsert is an unconditional last-write-wins, exactly like the Badger
// implementation: the engine write does not compare UpdatedAt, and the cache is
// the only layer that keeps a monotonic guard.
const sqliteFileMetadataUpsertSQL = `
INSERT INTO files (
    file_key, tenant_id, volume_id, physical_path, directory_path, file_size,
    created_at, updated_at, status, retry_count, last_failed_at, last_error,
    processing_start_time, released_processing_start_time, completed_at, dead_lettered_at,
    available_for_processing_at, original_file_name, file_extension, import_operation_id)
VALUES (
    @file_key, @tenant_id, @volume_id, @physical_path, @directory_path, @file_size,
    @created_at, @updated_at, @status, @retry_count, @last_failed_at, @last_error,
    @processing_start_time, @released_processing_start_time, @completed_at, @dead_lettered_at,
    @available_for_processing_at, @original_file_name, @file_extension, @import_operation_id)
ON CONFLICT(file_key) DO UPDATE SET
    tenant_id                       = excluded.tenant_id,
    volume_id                       = excluded.volume_id,
    physical_path                   = excluded.physical_path,
    directory_path                  = excluded.directory_path,
    file_size                       = excluded.file_size,
    created_at                      = excluded.created_at,
    updated_at                      = excluded.updated_at,
    status                          = excluded.status,
    retry_count                     = excluded.retry_count,
    last_failed_at                  = excluded.last_failed_at,
    last_error                      = excluded.last_error,
    processing_start_time           = excluded.processing_start_time,
    released_processing_start_time  = excluded.released_processing_start_time,
    completed_at                    = excluded.completed_at,
    dead_lettered_at                = excluded.dead_lettered_at,
    available_for_processing_at     = excluded.available_for_processing_at,
    original_file_name              = excluded.original_file_name,
    file_extension                  = excluded.file_extension,
    import_operation_id             = excluded.import_operation_id`

// SQLiteRepositoryOptions configures the SQLite metadata repository.
type SQLiteRepositoryOptions struct {
	// DataPath is the metadata root. One database file is created per tenant at
	// {DataPath}/{tenantId}/metadata.db, lazily, the first time that tenant is
	// touched. It must not be empty.
	DataPath string

	// CacheTTL is the time-to-live of a cached active record. Only active
	// records (Pending, Processing, Failed) are cached. Zero selects 5 minutes.
	CacheTTL time.Duration

	// MaxCacheEntries bounds the active-metadata cache. Zero selects 10000.
	MaxCacheEntries int

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
	// because it is corrupted and recreates an empty one instead of failing
	// startup. A lock or permission failure is never treated as corruption.
	RecoverCorruptedDatabase bool

	// CorruptedDatabaseRetention is how long a quarantined database file is kept
	// before it is pruned during the next open. Zero selects 72 hours; a
	// negative value disables pruning.
	CorruptedDatabaseRetention time.Duration

	// BackupDirectory is the root of the per-tenant backup tree, which expands
	// one level per tenant: {BackupDirectory}/{tenantId}. An empty value
	// disables automatic restore.
	BackupDirectory string

	// BackupRetention is retained for symmetry with the backup runner's policy.
	// The repository itself does not prune backups: it only reads them when
	// automatic recovery needs one, and pruning there would race the runner that
	// owns the directory.
	BackupRetention time.Duration

	// AutoRestoreFromBackup loads the newest readable backup of a tenant whose
	// database had to be quarantined. It requires RecoverCorruptedDatabase and a
	// non-empty BackupDirectory. A backup that cannot be loaded leaves the
	// tenant empty and degraded rather than failing startup.
	AutoRestoreFromBackup bool

	// SkipBackupVerification skips PRAGMA integrity_check(1) on a produced
	// backup. Verification is on by default because an unverified backup is only
	// discovered to be unusable during a recovery.
	SkipBackupVerification bool

	// OptimizeIdleTenantDatabases lets Optimize open, compact and close tenants
	// that currently have no handle. Disabled by default because it multiplies
	// file-handle churn during maintenance.
	OptimizeIdleTenantDatabases bool

	// Logging receives structured repository diagnostics. A nil runtime means
	// silent, and logging never fails a storage operation.
	Logging *logging.Runtime

	// StatisticsRecorder receives metadata persistence counters from the commit
	// path. A nil recorder disables statistics.
	StatisticsRecorder core.StatisticsRecorder

	// OnCorruptedDatabase, when set, is called synchronously with the quarantined
	// file path after a database file was quarantined. It runs on the opening
	// goroutine and must not block.
	OnCorruptedDatabase func(quarantinedPath string)

	// OnRecoveryIncomplete, when set, is called synchronously after a database
	// file was quarantined and no backup could be restored into its place, so
	// the repository is about to serve an empty database. It receives the same
	// quarantine path as OnCorruptedDatabase.
	OnRecoveryIncomplete func(quarantinedPath string)

	// now is the clock seam. A nil value selects time.Now. It is unexported
	// because it exists only so tests can drive idle eviction and backup stamps
	// without sleeping.
	now func() time.Time
}

// SQLiteMetadataRepository is the SQLite-backed core.MetadataRepository.
//
// One façade serves every tenant: the tenant travels as a method parameter and
// resolves to that tenant's own database file, opened lazily on first access.
// The handle table is guarded by mu; each tenant database additionally
// serializes its own work on its own mutex so a slow tenant never blocks another
// tenant's open path.
//
// Close is idempotent and concurrency safe: the first caller performs the
// shutdown and every caller receives that same result.
type SQLiteMetadataRepository struct {
	dataPath string
	sqlite   sqlite.Options

	cache *metadataCache

	maxOpenDatabases int
	idleTimeout      time.Duration

	corruptRecovery  bool
	corruptRetention time.Duration
	backupDirectory  string
	autoRestore      bool
	skipBackupVerify bool
	optimizeIdle     bool

	logging    *logging.Runtime
	statistics core.StatisticsRecorder

	onCorruptedDatabase  func(quarantinedPath string)
	onRecoveryIncomplete func(quarantinedPath string)

	now func() time.Time

	mu        sync.RWMutex
	closed    bool
	databases map[string]*sqliteTenantDatabase
	// openings tracks first opens that are still in progress. A handle is
	// published to databases only after its pool is usable, so a concurrent
	// access to the same tenant waits here instead of receiving a handle whose
	// db is still nil.
	openings map[string]*sqliteTenantOpening

	dirMu       sync.Mutex
	dirTenants  []string
	dirScannedA time.Time

	closeOnce sync.Once
	closeErr  error
}

// sqliteTenantOpening is one in-flight first open of a tenant database.
//
// The winner of the open publishes the outcome here and closes done, so a caller
// that arrives while the open runs can wait for the real handle (or the real
// error) instead of observing a half-built one.
type sqliteTenantOpening struct {
	done chan struct{}
	err  error
}

// sqliteTenantDatabase is one tenant's database handle and the bookkeeping the
// handle bound and the idle eviction need.
type sqliteTenantDatabase struct {
	tenantID string
	path     string
	db       *sql.DB

	// mu serializes this tenant's transactions. The single connection of the
	// pooled handle already serializes statements; this mutex is what keeps a
	// maintenance statement (VACUUM, checkpoint) from interleaving with a
	// transaction body on that same connection.
	mu sync.Mutex

	// refs counts the operations currently using the handle. Eviction never
	// closes a handle whose refs is non-zero.
	refs int64

	// lastUsedNanos is the atomic mirror of lastUsed that the eviction scan
	// reads without taking mu.
	lastUsedNanos int64
	lastUsed      time.Time
}

// SQLiteMetadataTenantBackupService is the optional capability that exposes one
// tenant's backup as a single, complete, directly openable SQLite file instead of
// a whole-repository stream.
//
// It is declared here rather than in pkg/core because core is frozen for this
// change: the capability is additive, it is discovered by a type assertion like
// every other optional capability, and it exists so the per-tenant backup runner
// and an offline operator restore can act on one tenant at a time.
type SQLiteMetadataTenantBackupService interface {
	// BackupTenant writes a consistent snapshot of one tenant's metadata to
	// destPath as a complete, openable SQLite database file. destPath must not
	// exist. It returns core.ErrDatabaseError when the tenant has no database.
	BackupTenant(ctx context.Context, tenantID string, destPath string) error

	// KnownTenantIDs returns the tenant identifiers this repository can back up,
	// in stable sorted order. The set is the open handles merged with a cached
	// snapshot of the tenant directories below DataPath, and enumerating it never
	// opens a database file.
	KnownTenantIDs(ctx context.Context) ([]string, error)
}

// Compile-time proof that the repository provides every capability callers are
// told to type-assert for.
var (
	_ core.MetadataRepository           = (*SQLiteMetadataRepository)(nil)
	_ core.StatusPageReader             = (*SQLiteMetadataRepository)(nil)
	_ core.MetadataBackupService        = (*SQLiteMetadataRepository)(nil)
	_ SQLiteMetadataTenantBackupService = (*SQLiteMetadataRepository)(nil)
)

// NewSQLiteMetadataRepository builds a SQLite metadata repository.
//
// The returned façade is shared by every tenant, exactly like the Badger
// implementation, and it opens no database until a tenant is first touched. The
// caller owns the repository and must Close it, which checkpoints and closes
// every open tenant handle.
//
// Errors wrap core.ErrInvalidArgument for a rejected option and
// core.ErrDatabaseError for a store that cannot be opened.
func NewSQLiteMetadataRepository(opts *SQLiteRepositoryOptions) (core.MetadataRepository, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}
	if strings.TrimSpace(opts.DataPath) == "" {
		return nil, fmt.Errorf("metadata data path cannot be empty: %w", core.ErrInvalidArgument)
	}
	if opts.CacheTTL < 0 {
		return nil, fmt.Errorf("metadata cache TTL cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.MaxCacheEntries < 0 {
		return nil, fmt.Errorf("metadata max cache entries cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.MaxOpenDatabases < 0 {
		return nil, fmt.Errorf("metadata max open databases cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.OpenDatabaseIdleTimeout < 0 {
		return nil, fmt.Errorf("metadata open database idle timeout cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.CorruptedDatabaseRetention < 0 {
		return nil, fmt.Errorf("metadata corrupted database retention cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.BackupRetention < 0 {
		return nil, fmt.Errorf("metadata backup retention cannot be negative: %w", core.ErrInvalidArgument)
	}
	if opts.AutoRestoreFromBackup && opts.BackupDirectory == "" {
		return nil, fmt.Errorf("metadata auto restore from backup requires a backup directory: %w", core.ErrInvalidArgument)
	}

	sqliteOptions := opts.Sqlite
	if sqliteOptions.JournalMode == "" && sqliteOptions.SynchronousMode == "" &&
		sqliteOptions.CacheSizeKb == 0 && sqliteOptions.BusyTimeoutMs == 0 {
		sqliteOptions = sqlite.DefaultOptions()
	} else if err := sqliteOptions.Validate(); err != nil {
		return nil, err
	}

	cacheTTL := opts.CacheTTL
	if cacheTTL == 0 {
		cacheTTL = 5 * time.Minute
	}

	maxCacheEntries := opts.MaxCacheEntries
	if maxCacheEntries == 0 {
		maxCacheEntries = 10000
	}

	corruptRetention := opts.CorruptedDatabaseRetention
	if corruptRetention == 0 {
		corruptRetention = defaultCorruptedDatabaseRetention
	}

	now := opts.now
	if now == nil {
		now = time.Now
	}

	repository := &SQLiteMetadataRepository{
		dataPath:             opts.DataPath,
		sqlite:               sqliteOptions,
		cache:                newMetadataCacheWithSize(cacheTTL, maxCacheEntries),
		maxOpenDatabases:     opts.MaxOpenDatabases,
		idleTimeout:          opts.OpenDatabaseIdleTimeout,
		corruptRecovery:      opts.RecoverCorruptedDatabase,
		corruptRetention:     corruptRetention,
		backupDirectory:      opts.BackupDirectory,
		autoRestore:          opts.AutoRestoreFromBackup,
		skipBackupVerify:     opts.SkipBackupVerification,
		optimizeIdle:         opts.OptimizeIdleTenantDatabases,
		logging:              opts.Logging,
		statistics:           opts.StatisticsRecorder,
		onCorruptedDatabase:  opts.OnCorruptedDatabase,
		onRecoveryIncomplete: opts.OnRecoveryIncomplete,
		now:                  now,
		databases:            make(map[string]*sqliteTenantDatabase),
		openings:             make(map[string]*sqliteTenantOpening),
	}

	return repository, nil
}

// recordPersistedBatch records the metadata persistence statistics of one
// committed batch. It is called exactly once per committed transaction, after
// the commit succeeded, and never for a batch that was rejected or rolled back.
func (r *SQLiteMetadataRepository) recordPersistedBatch(operations int) {
	if r.statistics == nil || operations <= 0 {
		return
	}
	now := r.now()
	r.statistics.Record(core.StatisticMetadataPersistedBatchCount, 1, now, nil)
	r.statistics.Record(core.StatisticMetadataPersistedOperationCount, int64(operations), now, nil)
}

// AddOrUpdate adds or updates file metadata atomically.
func (r *SQLiteMetadataRepository) AddOrUpdate(ctx context.Context, metadata *core.FileMetadata) error {
	if metadata == nil {
		return fmt.Errorf("metadata cannot be nil: %w", core.ErrInvalidArgument)
	}
	if metadata.FileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}
	if metadata.TenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, metadata.TenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	if err := validateTenantIDForHandle(handle, metadata.TenantID); err != nil {
		return err
	}

	if err := handle.inTx(ctx, func(tx *sql.Tx) error {
		return sqliteUpsertMetadata(ctx, tx, metadata)
	}); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	r.recordPersistedBatch(1)
	r.cacheRecord(metadata)

	return nil
}

// AddOrUpdateBatch adds or updates several records in one transaction.
//
// The whole slice is validated before the transaction opens, so a rejected batch
// writes nothing and the error names the offending entry. One database file
// belongs to one tenant, so every entry must carry the same tenant ID.
func (r *SQLiteMetadataRepository) AddOrUpdateBatch(ctx context.Context, metadata []*core.FileMetadata) error {
	if len(metadata) == 0 {
		return nil
	}

	for i, m := range metadata {
		switch {
		case m == nil:
			return fmt.Errorf("metadata[%d] cannot be nil: %w", i, core.ErrInvalidArgument)
		case m.FileKey == "":
			return fmt.Errorf("metadata[%d] file key cannot be empty: %w", i, core.ErrInvalidArgument)
		case m.TenantID == "":
			return fmt.Errorf("metadata[%d] tenant ID cannot be empty: %w", i, core.ErrInvalidArgument)
		}
	}

	handle, err := r.begin(ctx, metadata[0].TenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	for i, m := range metadata {
		if m.TenantID != handle.tenantID {
			return fmt.Errorf("metadata[%d] belongs to tenant %q but the repository handle serves tenant %q: %w",
				i, m.TenantID, handle.tenantID, core.ErrInvalidArgument)
		}
	}

	if err := handle.inTx(ctx, func(tx *sql.Tx) error {
		for _, m := range metadata {
			if err := sqliteUpsertMetadata(ctx, tx, m); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to batch update metadata: %w", err)
	}
	r.recordPersistedBatch(len(metadata))

	for _, m := range metadata {
		r.cacheRecord(m)
	}
	return nil
}

// Get retrieves file metadata by key.
//
// Errors:
//   - ErrFileNotFound if no metadata exists for (tenantID, fileKey)
func (r *SQLiteMetadataRepository) Get(ctx context.Context, tenantID string, fileKey string) (*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	if cached := r.cache.get(tenantID, fileKey); cached != nil {
		return cached, nil
	}

	metadata, err := sqliteSelectMetadata(ctx, handle.db, tenantID, fileKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrFileNotFound
		}
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	r.cacheRecord(metadata)
	return metadata, nil
}

// GetByImportOperationID returns the metadata record that owns operationID for
// one tenant. The lookup is served directly by SQLite's unique partial index;
// no operation-ID map is retained in process memory.
func (r *SQLiteMetadataRepository) GetByImportOperationID(
	ctx context.Context,
	tenantID string,
	operationID string,
) (*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if strings.TrimSpace(operationID) == "" {
		return nil, fmt.Errorf("import operation ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	row := handle.db.QueryRowContext(ctx,
		`SELECT `+sqliteFileMetadataColumns+`
		 FROM files
		 WHERE tenant_id = @tenant AND import_operation_id = @operation_id
		 LIMIT 1`,
		sql.Named("tenant", tenantID),
		sql.Named("operation_id", operationID))
	metadata, err := sqliteScanMetadata(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, core.ErrFileNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata by import operation ID: %w", err)
	}
	return metadata, nil
}

// Delete removes file metadata. Deleting a record that does not exist is not an
// error, matching the Badger implementation's engine delete.
func (r *SQLiteMetadataRepository) Delete(ctx context.Context, tenantID string, fileKey string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	if err := handle.inTx(ctx, func(tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx,
			`DELETE FROM files WHERE tenant_id = @tenant AND file_key = @key`,
			sql.Named("tenant", tenantID), sql.Named("key", fileKey))
		return execErr
	}); err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	r.recordPersistedBatch(1)
	r.cache.delete(tenantID, fileKey)

	return nil
}

// DeleteBatch removes several records in one transaction, so a rejected batch
// deletes nothing.
func (r *SQLiteMetadataRepository) DeleteBatch(ctx context.Context, tenantID string, fileKeys []string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if len(fileKeys) == 0 {
		return nil
	}

	for i, fileKey := range fileKeys {
		if fileKey == "" {
			return fmt.Errorf("fileKeys[%d] cannot be empty: %w", i, core.ErrInvalidArgument)
		}
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	if err := handle.inTx(ctx, func(tx *sql.Tx) error {
		statement, prepareErr := tx.PrepareContext(ctx,
			`DELETE FROM files WHERE tenant_id = @tenant AND file_key = @key`)
		if prepareErr != nil {
			return prepareErr
		}
		defer func() { _ = statement.Close() }()

		for _, fileKey := range fileKeys {
			if _, execErr := statement.ExecContext(ctx,
				sql.Named("tenant", tenantID), sql.Named("key", fileKey)); execErr != nil {
				return execErr
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to batch delete metadata: %w", err)
	}
	r.recordPersistedBatch(len(fileKeys))

	for _, fileKey := range fileKeys {
		r.cache.delete(tenantID, fileKey)
	}
	return nil
}

// GetByStatus retrieves records with one status in queue order
// (availability, creation time, file key). A non-positive limit means unlimited.
func (r *SQLiteMetadataRepository) GetByStatus(ctx context.Context, tenantID string, status core.FileProcessingStatus, limit int) ([]*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	rows, err := handle.db.QueryContext(ctx, sqliteSelectByStatusSQL,
		sql.Named("tenant", tenantID),
		sql.Named("status", int(status)),
		sql.Named("limit", sqliteLimit(limit)))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results, err := sqliteScanMetadataRows(ctx, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status: %w", err)
	}
	return results, nil
}

const sqliteSelectByStatusSQL = `
SELECT ` + sqliteFileMetadataColumns + `
FROM files
WHERE tenant_id = @tenant AND status = @status
ORDER BY available_for_processing_at ASC, created_at ASC, file_key ASC
LIMIT @limit`

// GetPendingFiles retrieves files that are ready for processing: Pending and
// already inside their availability window. The comparison is inclusive,
// matching the Badger implementation's "not after now" rule.
func (r *SQLiteMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	rows, err := handle.db.QueryContext(ctx, sqliteSelectPendingSQL,
		sql.Named("tenant", tenantID),
		sql.Named("status", int(core.FileStatusPending)),
		sql.Named("now", r.now().UTC().UnixNano()),
		sql.Named("limit", sqliteLimit(limit)))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results, err := sqliteScanMetadataRows(ctx, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}
	return results, nil
}

const sqliteSelectPendingSQL = `
SELECT ` + sqliteFileMetadataColumns + `
FROM files
WHERE tenant_id = @tenant
  AND status = @status
  AND available_for_processing_at <= @now
ORDER BY available_for_processing_at ASC, created_at ASC, file_key ASC
LIMIT @limit`

// UpdateStatus applies a status change unconditionally.
//
// This is the administrative/repair operation the interface describes: it
// performs no lease and no availability check, so callers that manage the queue
// must use the compare-and-swap methods instead.
func (r *SQLiteMetadataRepository) UpdateStatus(ctx context.Context, tenantID string, fileKey string, newStatus core.FileProcessingStatus) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	var updated *core.FileMetadata
	if err := handle.inTx(ctx, func(tx *sql.Tx) error {
		result, execErr := tx.ExecContext(ctx,
			`UPDATE files SET status = @status, updated_at = @now
			 WHERE tenant_id = @tenant AND file_key = @key`,
			sql.Named("status", int(newStatus)),
			sql.Named("now", r.now().UTC().UnixNano()),
			sql.Named("tenant", tenantID),
			sql.Named("key", fileKey))
		if execErr != nil {
			return execErr
		}
		affected, affectedErr := result.RowsAffected()
		if affectedErr != nil {
			return affectedErr
		}
		if affected == 0 {
			return core.ErrFileNotFound
		}
		updated, execErr = sqliteSelectMetadata(ctx, tx, tenantID, fileKey)
		return execErr
	}); err != nil {
		if errors.Is(err, core.ErrFileNotFound) {
			return err
		}
		return fmt.Errorf("failed to update status: %w", err)
	}
	r.recordPersistedBatch(1)

	if updated != nil {
		r.cacheRecord(updated)
	}
	return nil
}

// CompareAndTransitionToProcessing atomically moves a file from Pending to
// Processing.
//
// The transition is a single conditional UPDATE whose RowsAffected decides
// success, followed by a probe SELECT when it reports no row. Inside the
// BEGIN IMMEDIATE transaction the write lock is held from the first statement,
// so the classification cannot race another writer. A lost race, a record that
// is no longer Pending, and a record that is still inside its availability
// window are contention (ErrFileNotClaimable); a missing record is
// ErrFileNotFound; anything else is an infrastructure failure (ErrDatabaseError).
func (r *SQLiteMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, tenantID string, fileKey string) (*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	now := r.now().UTC()

	var updated *core.FileMetadata
	txErr := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return handle.inTx(ctx, func(tx *sql.Tx) error {
			result, execErr := tx.ExecContext(ctx,
				`UPDATE files
				 SET status = @processing,
				     processing_start_time = @leaseStart,
				     updated_at = @now
				 WHERE tenant_id = @tenant
				   AND file_key = @key
				   AND status = @pending
				   AND available_for_processing_at <= @now`,
				sql.Named("processing", int(core.FileStatusProcessing)),
				sql.Named("leaseStart", now.UTC().UnixNano()),
				sql.Named("now", now.UTC().UnixNano()),
				sql.Named("tenant", tenantID),
				sql.Named("key", fileKey),
				sql.Named("pending", int(core.FileStatusPending)))
			if execErr != nil {
				return execErr
			}
			affected, affectedErr := result.RowsAffected()
			if affectedErr != nil {
				return affectedErr
			}
			if affected == 0 {
				return classifyClaimFailure(ctx, tx, tenantID, fileKey, now)
			}

			updated, execErr = sqliteSelectMetadata(ctx, tx, tenantID, fileKey)
			return execErr
		})
	})
	if txErr != nil {
		if errors.Is(txErr, core.ErrFileNotFound) ||
			errors.Is(txErr, core.ErrFileNotClaimable) ||
			errors.Is(txErr, core.ErrInvalidArgument) {
			return nil, txErr
		}
		return nil, fmt.Errorf("failed to transition file to processing: %w: %w", txErr, core.ErrDatabaseError)
	}

	r.cacheRecord(updated)

	result := *updated
	return &result, nil
}

// classifyClaimFailure explains why the conditional claim UPDATE matched no row.
//
// It runs inside the same write transaction as the UPDATE, so the state it reads
// cannot have been changed by a concurrent writer between the two statements.
func classifyClaimFailure(ctx context.Context, tx *sql.Tx, tenantID string, fileKey string, now time.Time) error {
	var status int
	var available int64
	err := tx.QueryRowContext(ctx,
		`SELECT status, available_for_processing_at FROM files
		 WHERE tenant_id = @tenant AND file_key = @key LIMIT 1`,
		sql.Named("tenant", tenantID), sql.Named("key", fileKey)).Scan(&status, &available)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return core.ErrFileNotFound
	case err != nil:
		return err
	}

	if core.FileProcessingStatus(status) != core.FileStatusPending {
		return fmt.Errorf("file is not in pending status (current status is %s): %w",
			core.FileProcessingStatus(status).String(), core.ErrFileNotClaimable)
	}
	if now.UTC().UnixNano() < available {
		return fmt.Errorf("file is not yet available for processing (available at %s): %w",
			nanosToTime(available).Format(time.RFC3339Nano), core.ErrFileNotClaimable)
	}
	// The row is Pending and available, yet the conditional UPDATE matched
	// nothing: another writer changed it inside this transaction's window.
	return fmt.Errorf("claim lost the write race: %w", core.ErrFileNotClaimable)
}

// CompareAndUpdateProcessing updates a Processing record only when the supplied
// lease still owns it.
//
// Releasing the same lease twice is idempotent: the released-lease marker stored
// by the first release identifies the second call, which then returns the stored
// record without writing. A lease superseded by a newer claim, and a lease that
// never existed, are rejected with *core.FileProcessingLeaseMismatchError, so a
// stale worker can never overwrite a newer claim.
func (r *SQLiteMetadataRepository) CompareAndUpdateProcessing(
	ctx context.Context,
	lease core.FileProcessingLease,
	update func(*core.FileMetadata) error,
) (*core.FileMetadata, error) {
	if lease.TenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if lease.FileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}
	if update == nil {
		return nil, fmt.Errorf("update cannot be nil: %w", core.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	handle, err := r.begin(ctx, lease.TenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	var updated *core.FileMetadata
	txErr := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return handle.inTx(ctx, func(tx *sql.Tx) error {
			current, selectErr := sqliteSelectMetadata(ctx, tx, lease.TenantID, lease.FileKey)
			if selectErr != nil {
				if errors.Is(selectErr, sql.ErrNoRows) {
					return newLeaseMismatchError(lease, nil)
				}
				return selectErr
			}

			leaseIsActive := current.Status == core.FileStatusProcessing &&
				current.ProcessingStartTime != nil &&
				current.ProcessingStartTime.Equal(lease.ProcessingStartTimeUTC)
			if !leaseIsActive {
				// A record still under processing belongs to a newer claim, so a
				// stale lease is rejected even when it was released earlier.
				if current.Status != core.FileStatusProcessing &&
					current.ReleasedProcessingStartTimeUTC != nil &&
					current.ReleasedProcessingStartTimeUTC.Equal(lease.ProcessingStartTimeUTC) {
					updated = current
					return nil
				}
				return newLeaseMismatchError(lease, current)
			}

			if updateErr := update(current); updateErr != nil {
				return updateErr
			}
			if current.TenantID != lease.TenantID || current.FileKey != lease.FileKey {
				return fmt.Errorf("update cannot change metadata identity: %w", core.ErrInvalidArgument)
			}

			// Remember which lease this record last released, so a repeated
			// release of the same lease is recognized as a no-op.
			if current.Status != core.FileStatusProcessing ||
				current.ProcessingStartTime == nil ||
				!current.ProcessingStartTime.Equal(lease.ProcessingStartTimeUTC) {
				released := lease.ProcessingStartTimeUTC.UTC()
				current.ReleasedProcessingStartTimeUTC = &released
			}
			current.UpdatedAt = r.now().UTC()

			result, execErr := tx.ExecContext(ctx, sqliteProcessingUpdateSQL, sqliteProcessingUpdateArgs(current, lease)...)
			if execErr != nil {
				return execErr
			}
			affected, affectedErr := result.RowsAffected()
			if affectedErr != nil {
				return affectedErr
			}
			if affected == 0 {
				return newLeaseMismatchError(lease, current)
			}

			updated = current
			return nil
		})
	})
	if txErr != nil {
		if errors.Is(txErr, core.ErrProcessingLeaseMismatch) || errors.Is(txErr, core.ErrInvalidArgument) {
			return nil, txErr
		}
		return nil, fmt.Errorf("failed to conditionally update processing metadata: %w: %w", txErr, core.ErrDatabaseError)
	}

	r.cacheRecord(updated)

	result := *updated
	return &result, nil
}

// sqliteProcessingUpdateSQL is the lease-guarded completion/failure update. The
// status and processing_start_time predicates repeat the in-transaction lease
// check as a second, independent gate on the write itself.
const sqliteProcessingUpdateSQL = `
UPDATE files
SET status                          = @status,
    retry_count                     = @retry_count,
    last_failed_at                  = @last_failed_at,
    last_error                      = @last_error,
    processing_start_time           = @processing_start_time,
    released_processing_start_time  = @released_processing_start_time,
    completed_at                    = @completed_at,
    dead_lettered_at                = @dead_lettered_at,
    available_for_processing_at     = @available_for_processing_at,
    updated_at                      = @updated_at
WHERE tenant_id = @tenant
  AND file_key = @key
  AND status = @processing
  AND processing_start_time = @leaseStart`

// sqliteProcessingUpdateArgs binds the lease-guarded update.
func sqliteProcessingUpdateArgs(metadata *core.FileMetadata, lease core.FileProcessingLease) []any {
	return []any{
		sql.Named("status", int(metadata.Status)),
		sql.Named("retry_count", metadata.RetryCount),
		sql.Named("last_failed_at", timePtrToNanos(metadata.LastFailedAt)),
		sql.Named("last_error", metadata.LastError),
		sql.Named("processing_start_time", timePtrToNanos(metadata.ProcessingStartTime)),
		sql.Named("released_processing_start_time", timePtrToNanos(metadata.ReleasedProcessingStartTimeUTC)),
		sql.Named("completed_at", timePtrToNanos(metadata.CompletedAt)),
		sql.Named("dead_lettered_at", timePtrToNanos(metadata.DeadLetteredAt)),
		sql.Named("available_for_processing_at", optionalTimeToNanos(metadata.AvailableForProcessingAt)),
		sql.Named("updated_at", metadata.UpdatedAt.UTC().UnixNano()),
		sql.Named("tenant", lease.TenantID),
		sql.Named("key", lease.FileKey),
		sql.Named("processing", int(core.FileStatusProcessing)),
		sql.Named("leaseStart", lease.ProcessingStartTimeUTC.UTC().UnixNano()),
	}
}

// GetTimedOutProcessingFiles returns the Processing records whose lease started
// strictly before now-timeout.
//
// The result is only a candidate set: the reclaim action must still pass the
// lease check of CompareAndUpdateProcessing, because the identity of a claim is
// its processing start time rather than its status.
func (r *SQLiteMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	cutoff := r.now().UTC().Add(-timeout).UnixNano()
	rows, err := handle.db.QueryContext(ctx, sqliteSelectTimedOutSQL,
		sql.Named("tenant", tenantID),
		sql.Named("processing", int(core.FileStatusProcessing)),
		sql.Named("cutoff", cutoff))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get timed out files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results, err := sqliteScanMetadataRows(ctx, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get timed out files: %w", err)
	}
	return results, nil
}

const sqliteSelectTimedOutSQL = `
SELECT ` + sqliteFileMetadataColumns + `
FROM files
WHERE tenant_id = @tenant
  AND status = @processing
  AND processing_start_time IS NOT NULL
  AND processing_start_time < @cutoff
ORDER BY processing_start_time ASC, file_key ASC`

// Optimize compacts the tenants that currently have an open handle: it truncates
// the write-ahead log and then VACUUMs the database in place.
//
// Tenants without a handle are skipped by default, because opening every idle
// tenant during maintenance multiplies file-handle churn. Setting
// OptimizeIdleTenantDatabases opens, compacts and closes them instead.
//
// Errors:
//   - the context's error when the context is already canceled.
//   - core.ErrDatabaseError when the repository is closed.
func (r *SQLiteMetadataRepository) Optimize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	open := r.snapshotHandlesLocked()
	r.mu.RUnlock()

	for _, handle := range open {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.optimizeHandle(ctx, handle)
	}

	if !r.optimizeIdle {
		return nil
	}

	known, err := r.knownTenantIDs(ctx)
	if err != nil {
		return err
	}
	for _, tenantID := range known {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.hasOpenHandle(tenantID) {
			continue
		}
		handle, acquireErr := r.begin(ctx, tenantID)
		if acquireErr != nil {
			warnRepositoryEvent(r.logging, "metadata_optimize_skipped", "tenant_open_failed")
			continue
		}
		r.optimizeHandle(ctx, handle)
		r.end(handle)
	}
	return nil
}

// optimizeHandle compacts one tenant database and records the size delta.
//
// VACUUM and wal_checkpoint(TRUNCATE) cannot run inside a transaction, so the
// tenant mutex is held for the whole pass: that is what keeps them from
// interleaving with a transaction body on the same single connection.
func (r *SQLiteMetadataRepository) optimizeHandle(ctx context.Context, handle *sqliteTenantDatabase) {
	handle.mu.Lock()
	defer handle.mu.Unlock()

	before := fileSize(handle.path)
	if err := sqlite.TruncateCheckpoint(ctx, handle.db); err != nil {
		warnRepositoryEvent(r.logging, "metadata_optimize_failed", "checkpoint_failed")
		return
	}
	if _, err := handle.db.ExecContext(ctx, "VACUUM"); err != nil {
		warnRepositoryEvent(r.logging, "metadata_optimize_failed", "vacuum_failed")
		return
	}
	after := fileSize(handle.path)

	if r.statistics != nil {
		now := r.now()
		r.statistics.Record("metadata.database.size_before", before, now, map[string]string{"tenant_id": handle.tenantID})
		r.statistics.Record("metadata.database.size_after", after, now, map[string]string{"tenant_id": handle.tenantID})
	}
}

// Backup writes a zip container holding one SQLite backup file per tenant and
// returns since, which is always 0 for this engine.
//
// since is always 0 because SQLite exposes no engine sequence number, so there is
// no resumable "backup since" position to report. Callers must treat it as opaque
// and must not use it for ordering, comparison, or incremental-backup decisions.
//
// The recovery instant of a restore is not a single instant: each tenant's entry
// inside the container reflects that tenant's backup instant, and tenants may
// differ within one backup cycle.
//
// A nil writer is rejected with core.ErrInvalidArgument, a closed repository with
// core.ErrDatabaseError, and a failure from w is returned unchanged.
func (r *SQLiteMetadataRepository) Backup(ctx context.Context, w io.Writer) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if w == nil {
		return 0, fmt.Errorf("backup writer cannot be nil: %w", core.ErrInvalidArgument)
	}

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return 0, errRepositoryClosed()
	}
	r.mu.RUnlock()

	tenantIDs, err := r.knownTenantIDs(ctx)
	if err != nil {
		return 0, err
	}

	if err := r.writeZipBackup(ctx, w, tenantIDs); err != nil {
		return 0, err
	}
	return 0, nil
}

// Close checkpoints and closes every open tenant database. It is idempotent and
// concurrency safe: the first caller performs the shutdown and every caller
// receives that same result, so a second caller can never report success while a
// handle is still open.
func (r *SQLiteMetadataRepository) Close() error {
	r.closeOnce.Do(func() {
		// Reject new work before tearing the handles down.
		r.mu.Lock()
		r.closed = true
		handles := make([]*sqliteTenantDatabase, 0, len(r.databases))
		for _, handle := range r.databases {
			handles = append(handles, handle)
		}
		r.databases = make(map[string]*sqliteTenantDatabase)
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
// cleanly, which a Windows file operation such as a quarantine or a restore
// depends on.
func (r *SQLiteMetadataRepository) closeHandle(handle *sqliteTenantDatabase) error {
	handle.mu.Lock()
	defer handle.mu.Unlock()

	if err := sqlite.TruncateCheckpoint(context.Background(), handle.db); err != nil {
		warnRepositoryEvent(r.logging, "metadata_close_failed", "checkpoint_failed")
	}
	// PRAGMA optimize is advisory: it refreshes the query planner's statistics
	// and a failure to do so must never fail a shutdown.
	_, _ = handle.db.ExecContext(context.Background(), "PRAGMA optimize")

	if err := handle.db.Close(); err != nil {
		return fmt.Errorf("failed to close metadata database: %w: %w", err, core.ErrDatabaseError)
	}
	return nil
}

// begin resolves one tenant's handle and registers one reference on it.
//
// It applies the handle bound and the idle timeout before returning, so the
// number of open handles never exceeds the configured limit for longer than one
// acquisition.
func (r *SQLiteMetadataRepository) begin(ctx context.Context, tenantID string) (*sqliteTenantDatabase, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if err := core.ValidateTenantID(tenantID); err != nil {
		return nil, fmt.Errorf("invalid tenant ID: %w", err)
	}

	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, errRepositoryClosed()
		}
		if handle, ok := r.databases[tenantID]; ok {
			r.touchLocked(handle)
			r.mu.Unlock()
			return handle, nil
		}
		if pending, ok := r.openings[tenantID]; ok {
			// Another caller is opening this tenant. Wait for it instead of
			// publishing a second handle or observing a half-built one.
			r.mu.Unlock()
			select {
			case <-pending.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if pending.err != nil {
				return nil, pending.err
			}
			// The winner published the handle; look it up on the next pass.
			continue
		}

		// Reserve the open before releasing the lock, so two concurrent first
		// accesses to the same tenant do not each open a database.
		pending := &sqliteTenantOpening{done: make(chan struct{})}
		r.openings[tenantID] = pending
		r.mu.Unlock()

		db, path, err := r.openTenantDatabase(ctx, tenantID)

		r.mu.Lock()
		delete(r.openings, tenantID)
		handle := &sqliteTenantDatabase{tenantID: tenantID, path: path, db: db}
		switch {
		case err != nil:
			pending.err = err
		case r.closed:
			// Close ran while the database was being opened: this operation must
			// not resurrect a handle in a closed repository.
			pending.err = errRepositoryClosed()
		default:
			r.databases[tenantID] = handle
			r.touchLocked(handle)
			r.evictLocked(tenantID)
		}
		r.mu.Unlock()

		if pending.err != nil {
			if db != nil {
				_ = db.Close()
			}
			return nil, pending.err
		}
		close(pending.done)
		return handle, nil
	}
}

// end releases one reference on a handle acquired by begin.
func (r *SQLiteMetadataRepository) end(handle *sqliteTenantDatabase) {
	if handle == nil {
		return
	}
	r.mu.Lock()
	if handle.refs > 0 {
		handle.refs--
	}
	handle.lastUsed = r.now()
	handle.lastUsedNanos = handle.lastUsed.UnixNano()
	r.mu.Unlock()
}

// touchLocked refreshes a handle's usage and reference count.
func (r *SQLiteMetadataRepository) touchLocked(handle *sqliteTenantDatabase) {
	handle.refs++
	handle.lastUsed = r.now()
	handle.lastUsedNanos = handle.lastUsed.UnixNano()
}

// evictLocked closes handles that exceed the bound or sat idle for too long.
//
// exclude names the handle the caller is about to use, which is never closed. A
// handle with an active reference is skipped rather than waited for: the caller
// that holds it is already inside its own operation, and blocking the repository
// lock on it would serialize unrelated tenants.
//
// Eviction is deterministic: handles are closed oldest-first, and when the bound
// must be met the oldest idle handles are closed first. The handle leaves the
// table before it is closed, so a concurrent acquisition either sees it and uses
// it or does not see it and reopens the tenant.
func (r *SQLiteMetadataRepository) evictLocked(exclude string) {
	if r.maxOpenDatabases <= 0 && r.idleTimeout <= 0 {
		return
	}

	candidates := make([]*sqliteTenantDatabase, 0, len(r.databases))
	for tenantID, handle := range r.databases {
		if tenantID == exclude || handle.refs > 0 || handle.db == nil {
			continue
		}
		candidates = append(candidates, handle)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastUsedNanos < candidates[j].lastUsedNanos
	})

	// Idle eviction can close every candidate.
	if r.idleTimeout > 0 {
		cutoff := r.now().Add(-r.idleTimeout).UnixNano()
		for _, handle := range candidates {
			if handle.lastUsedNanos > cutoff {
				break
			}
			r.closeEvictedLocked(handle)
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
			r.closeEvictedLocked(handle)
		}
	}
}

// closeEvictedLocked removes one handle from the table and closes it. The caller
// must hold mu.
//
// The close runs on a separate goroutine so releasing a file handle cannot stall
// another tenant's acquisition; the handle is already unreachable by tenant ID,
// so nothing can observe it half closed.
func (r *SQLiteMetadataRepository) closeEvictedLocked(handle *sqliteTenantDatabase) {
	delete(r.databases, handle.tenantID)
	go func() {
		_ = r.closeHandle(handle)
	}()
}

// snapshotHandlesLocked returns a stable, deterministically ordered copy of the
// open handles. The caller must hold mu.
func (r *SQLiteMetadataRepository) snapshotHandlesLocked() []*sqliteTenantDatabase {
	handles := make([]*sqliteTenantDatabase, 0, len(r.databases))
	for _, handle := range r.databases {
		handles = append(handles, handle)
	}
	sort.Slice(handles, func(i, j int) bool { return handles[i].tenantID < handles[j].tenantID })
	return handles
}

// hasOpenHandle reports whether tenantID currently has a handle.
func (r *SQLiteMetadataRepository) hasOpenHandle(tenantID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.databases[tenantID]
	return ok
}

// openHandleCount reports how many tenant handles are open. It exists so a test
// can assert the configured handle bound.
func (r *SQLiteMetadataRepository) openHandleCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.databases)
}

// openTenantDatabase creates the tenant directory, recovers a damaged database
// when the caller asked for it, and opens the SQLite handle.
func (r *SQLiteMetadataRepository) openTenantDatabase(ctx context.Context, tenantID string) (*sql.DB, string, error) {
	directory := filepath.Join(r.dataPath, tenantID)
	if err := os.MkdirAll(directory, metadataDatabaseDirPermissions); err != nil {
		return nil, "", fmt.Errorf("failed to create the tenant metadata directory: %w: %w", err, core.ErrDatabaseError)
	}

	path := filepath.Join(directory, metadataDatabaseFileName)
	pruneCorruptedDatabaseFiles(path, r.corruptRetention, r.now)

	db, err := r.openDatabaseWithRecovery(ctx, path)
	if err != nil {
		return nil, "", err
	}
	return db, path, nil
}

// openDatabaseWithRecovery opens path, quarantining it and recreating an empty
// database when the failure is a positive corruption verdict and recovery is
// enabled. A lock, permission, or path failure is never treated as corruption.
func (r *SQLiteMetadataRepository) openDatabaseWithRecovery(ctx context.Context, path string) (*sql.DB, error) {
	var lastErr error

	for attempt := 0; attempt < corruptedDatabaseRecoveryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		db, err := r.openTenantDatabaseOnce(ctx, path)
		if err == nil {
			return db, nil
		}
		if !sqlite.IsCorruptionError(err) {
			return nil, err
		}
		lastErr = err

		if !r.corruptRecovery {
			return nil, err
		}

		quarantinePath, quarantineErr := quarantineDatabaseFile(path, r.now())
		if quarantineErr != nil {
			return nil, errors.Join(err, fmt.Errorf("failed to quarantine the corrupted metadata database: %w", quarantineErr))
		}
		removeDatabaseSidecars(path, r.now)
		warnRepositoryEvent(r.logging, "metadata_database_quarantined", "corruption_detected")

		if r.onCorruptedDatabase != nil {
			r.onCorruptedDatabase(quarantinePath)
		}

		restored := false
		if r.autoRestore && r.backupDirectory != "" {
			restored = r.restoreNewestBackup(ctx, path)
		}
		if !restored && r.onRecoveryIncomplete != nil {
			r.onRecoveryIncomplete(quarantinePath)
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("metadata database could not be recovered: %w", core.ErrDatabaseError)
	}
	return nil, lastErr
}

// openTenantDatabaseOnce opens one connection through the shared SQLite
// foundation, re-asserts the pragmas, verifies the file with integrity_check(1),
// and creates the schema.
//
// The integrity check is the second half of the corruption verdict: a file can
// be structurally readable and still hold a malformed b-tree, and that state is
// exactly what the quarantine and automatic-restore paths are for.
func (r *SQLiteMetadataRepository) openTenantDatabaseOnce(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sqlite.Open(path, r.sqlite)
	if err != nil {
		return nil, err
	}

	if err := r.applyPragmas(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to configure the metadata database: %w: %w", err, core.ErrDatabaseError)
	}

	if err := sqlite.IntegrityCheck(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := r.applySchema(ctx, db); err != nil {
		_ = db.Close()
		if sqlite.IsCorruptionError(err) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to initialize the metadata schema: %w: %w", err, core.ErrDatabaseError)
	}
	return db, nil
}

// applyPragmas re-asserts the pragma set the DSN already carries. The statements
// are idempotent; keeping them makes the effective values observable through a
// query and catches a DSN-construction regression.
func (r *SQLiteMetadataRepository) applyPragmas(ctx context.Context, db *sql.DB) error {
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

// applySchema runs the idempotent DDL script, verifies that the database is not
// newer than this binary understands, and then applies the additive-column
// migration so a database created by an older revision of this schema gains a
// column it is missing instead of failing on the first statement that selects it.
func (r *SQLiteMetadataRepository) applySchema(ctx context.Context, db *sql.DB) error {
	// Tables must exist before version inspection and additive migration. The
	// operation-ID index is deliberately created only after the column has been
	// added to databases created by schema version 1.
	for _, statement := range []string{sqliteFilesTableDDL, sqliteSchemaMetaTableDDL} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := verifySchemaVersion(ctx, db); err != nil {
		return err
	}
	if err := migrateAdditiveColumns(ctx, db, "files", sqliteFilesAdditiveColumns); err != nil {
		return err
	}
	for _, statement := range []string{
		sqliteFilesStatusAvailableIndexDDL,
		sqliteFilesStatusCreatedAtIndexDDL,
		sqliteFilesStatusCompletedAtIndexDDL,
		sqliteFilesStatusLastFailedAtIndexDDL,
		sqliteFilesProcessingStartIndexDDL,
		sqliteFilesImportOperationIndexDDL,
		sqliteSchemaVersionDDL,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// verifySchemaVersion rejects a database written by a newer schema revision.
//
// Refusing to serve is the safe answer: an older binary cannot know which
// invariants a newer revision added, and silently working on it is how a
// downgrade corrupts data. An absent or unparsable value is accepted, because the
// DDL that just ran is what creates and stamps it.
func verifySchemaVersion(ctx context.Context, db *sql.DB) error {
	var value string
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT value FROM schema_meta WHERE key = '%s'", sqliteSchemaVersionKey)).Scan(&value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}

	version, parseErr := strconv.Atoi(strings.TrimSpace(value))
	if parseErr != nil {
		return nil
	}
	known, _ := strconv.Atoi(sqliteSchemaVersion)
	if version > known {
		return fmt.Errorf("metadata schema version %s is newer than the supported version %s: %w",
			value, sqliteSchemaVersion, core.ErrDatabaseError)
	}
	return nil
}

// sqliteAdditiveColumn is one column a later schema revision adds to an existing
// table. SQLite stores a NULL default for it, so adding the column is a
// schema-only O(1) operation and never rewrites the table.
type sqliteAdditiveColumn struct {
	name       string
	definition string
}

// sqliteFilesAdditiveColumns is the ordered inventory of columns added to the
// files table after its first revision. It is empty today and exists so a future
// revision appends an entry instead of inventing a second migration path.
var sqliteFilesAdditiveColumns = []sqliteAdditiveColumn{
	{name: "import_operation_id", definition: "TEXT"},
}

// migrateAdditiveColumns adds every column of wanted that the table does not have
// yet.
func migrateAdditiveColumns(ctx context.Context, db *sql.DB, table string, wanted []sqliteAdditiveColumn) error {
	if len(wanted) == 0 {
		return nil
	}

	existing, err := tableColumns(ctx, db, table)
	if err != nil {
		return err
	}
	for _, column := range wanted {
		if existing[column.name] {
			continue
		}
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column.name, column.definition)); err != nil {
			return err
		}
	}
	return nil
}

// tableColumns returns the column names of table.
func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]bool)
	for rows.Next() {
		var (
			index        int
			name         string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&index, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

// inTx runs fn inside one write transaction.
//
// The DSN's _txlock=immediate makes BeginTx issue BEGIN IMMEDIATE, so the write
// lock is taken when the transaction starts instead of failing on a mid-way lock
// upgrade. The rollback is deferred and the commit stays explicit.
func (h *sqliteTenantDatabase) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return classifySQLiteError(ctx, err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return classifySQLiteError(ctx, err)
	}
	return nil
}

// classifySQLiteError maps a database/sql error to the repository's contract:
// a canceled context keeps its own identity, and everything else that is not a
// retryable lock conflict is an infrastructure failure.
func classifySQLiteError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if isSQLiteConflictError(err) {
		return fmt.Errorf("sqlite write conflict: %w: %w", err, core.ErrDatabaseError)
	}
	return fmt.Errorf("%w: %w", err, core.ErrDatabaseError)
}

// isSQLiteConflictError reports whether err is a transient SQLITE_BUSY or
// SQLITE_LOCKED outcome. Those are the only errors a write is allowed to retry:
// they mean another connection holds the lock right now, not that the data or
// the storage is broken.
func isSQLiteConflictError(err error) bool {
	if err == nil {
		return false
	}

	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() & 0xff {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
			return true
		}
	}

	message := strings.ToLower(err.Error())
	for _, signature := range sqliteConflictErrorSignatures {
		if strings.Contains(message, signature) {
			return true
		}
	}
	return false
}

// sqliteConflictErrorSignatures back up the structured result codes for an error
// that crossed a boundary that dropped its Go type.
var sqliteConflictErrorSignatures = []string{
	"database is locked",
	"database table is locked",
	"sqlite_busy",
	"sqlite_locked",
}

// validateTenantIDForHandle rejects a write whose metadata claims a tenant other
// than the one the handle serves.
func validateTenantIDForHandle(handle *sqliteTenantDatabase, tenantID string) error {
	if tenantID != handle.tenantID {
		return fmt.Errorf("metadata belongs to tenant %q but the repository handle serves tenant %q: %w",
			tenantID, handle.tenantID, core.ErrInvalidArgument)
	}
	return nil
}

// sqliteUpsertMetadata writes one record, binding every column explicitly.
func sqliteUpsertMetadata(ctx context.Context, tx *sql.Tx, metadata *core.FileMetadata) error {
	_, err := tx.ExecContext(ctx, sqliteFileMetadataUpsertSQL,
		sql.Named("file_key", metadata.FileKey),
		sql.Named("tenant_id", metadata.TenantID),
		sql.Named("volume_id", metadata.VolumeID),
		sql.Named("physical_path", metadata.PhysicalPath),
		sql.Named("directory_path", metadata.DirectoryPath),
		sql.Named("file_size", metadata.FileSize),
		sql.Named("created_at", timeToNanos(metadata.CreatedAt)),
		sql.Named("updated_at", timeToNanos(metadata.UpdatedAt)),
		sql.Named("status", int(metadata.Status)),
		sql.Named("retry_count", metadata.RetryCount),
		sql.Named("last_failed_at", timePtrToNanos(metadata.LastFailedAt)),
		sql.Named("last_error", metadata.LastError),
		sql.Named("processing_start_time", timePtrToNanos(metadata.ProcessingStartTime)),
		sql.Named("released_processing_start_time", timePtrToNanos(metadata.ReleasedProcessingStartTimeUTC)),
		sql.Named("completed_at", timePtrToNanos(metadata.CompletedAt)),
		sql.Named("dead_lettered_at", timePtrToNanos(metadata.DeadLetteredAt)),
		sql.Named("available_for_processing_at", optionalTimeToNanos(metadata.AvailableForProcessingAt)),
		sql.Named("original_file_name", metadata.OriginalFileName),
		sql.Named("file_extension", metadata.FileExtension),
		sql.Named("import_operation_id", sql.NullString{
			String: metadata.ImportOperationID,
			Valid:  metadata.ImportOperationID != "",
		}))
	return err
}

// sqliteSelectMetadata reads one record by key. It accepts either a *sql.DB or a
// *sql.Tx, because a lease release must read and write inside one transaction.
func sqliteSelectMetadata(ctx context.Context, queryer sqliteQueryer, tenantID string, fileKey string) (*core.FileMetadata, error) {
	row := queryer.QueryRowContext(ctx,
		`SELECT `+sqliteFileMetadataColumns+`
		 FROM files WHERE tenant_id = @tenant AND file_key = @key LIMIT 1`,
		sql.Named("tenant", tenantID), sql.Named("key", fileKey))

	metadata, err := sqliteScanMetadata(row)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

// sqliteQueryer is the read surface shared by *sql.DB and *sql.Tx.
type sqliteQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sqliteScanner is the row surface shared by *sql.Row and *sql.Rows.
type sqliteScanner interface {
	Scan(dest ...any) error
}

// sqliteScanMetadata maps one database row onto the domain model. Timestamps are
// stored as Unix nanoseconds in UTC; a NULL nullable column becomes a nil
// pointer and the availability sentinel 0 becomes nil as well.
func sqliteScanMetadata(scanner sqliteScanner) (*core.FileMetadata, error) {
	var (
		metadata          core.FileMetadata
		status            int
		createdAt         int64
		updatedAt         int64
		lastFailedAt      sql.NullInt64
		processingStart   sql.NullInt64
		releasedStart     sql.NullInt64
		completedAt       sql.NullInt64
		deadLetteredAt    sql.NullInt64
		availableFor      int64
		lastError         sql.NullString
		volumeID          sql.NullString
		physicalPath      sql.NullString
		directoryPath     sql.NullString
		originalFileName  sql.NullString
		fileExtensionText sql.NullString
		importOperationID sql.NullString
	)
	if err := scanner.Scan(
		&metadata.FileKey,
		&metadata.TenantID,
		&volumeID,
		&physicalPath,
		&directoryPath,
		&metadata.FileSize,
		&createdAt,
		&updatedAt,
		&status,
		&metadata.RetryCount,
		&lastFailedAt,
		&lastError,
		&processingStart,
		&releasedStart,
		&completedAt,
		&deadLetteredAt,
		&availableFor,
		&originalFileName,
		&fileExtensionText,
		&importOperationID,
	); err != nil {
		return nil, err
	}

	metadata.VolumeID = volumeID.String
	metadata.PhysicalPath = physicalPath.String
	metadata.DirectoryPath = directoryPath.String
	metadata.LastError = lastError.String
	metadata.OriginalFileName = originalFileName.String
	metadata.FileExtension = fileExtensionText.String
	metadata.ImportOperationID = importOperationID.String
	metadata.Status = core.FileProcessingStatus(status)
	metadata.CreatedAt = nanosToTime(createdAt)
	metadata.UpdatedAt = nanosToTime(updatedAt)
	metadata.LastFailedAt = nanosToTimePtr(lastFailedAt)
	metadata.ProcessingStartTime = nanosToTimePtr(processingStart)
	metadata.ReleasedProcessingStartTimeUTC = nanosToTimePtr(releasedStart)
	metadata.CompletedAt = nanosToTimePtr(completedAt)
	metadata.DeadLetteredAt = nanosToTimePtr(deadLetteredAt)
	metadata.AvailableForProcessingAt = nanosToOptionalTime(availableFor)

	return &metadata, nil
}

// sqliteScanMetadataRows drains a result set in queue order.
func sqliteScanMetadataRows(ctx context.Context, rows *sql.Rows) ([]*core.FileMetadata, error) {
	results := make([]*core.FileMetadata, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		metadata, err := sqliteScanMetadata(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, metadata)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// sqliteLimit converts the domain's "non-positive means unlimited" convention
// into the SQL LIMIT value: -1 is SQLite's unlimited marker.
func sqliteLimit(limit int) int {
	if limit <= 0 {
		return sqliteUnlimitedQueryLimit
	}
	return limit
}

// timeToNanos writes a domain time into an INTEGER column.
func timeToNanos(t time.Time) int64 {
	return t.UTC().UnixNano()
}

// nanosToTime reads an INTEGER column back into a domain time, always in UTC.
func nanosToTime(nanos int64) time.Time {
	return time.Unix(0, nanos).UTC()
}

// timePtrToNanos encodes a nullable timestamp: a nil pointer becomes SQL NULL.
func timePtrToNanos(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UTC().UnixNano(), Valid: true}
}

// nanosToTimePtr decodes a nullable timestamp: SQL NULL becomes a nil pointer.
func nanosToTimePtr(nanos sql.NullInt64) *time.Time {
	if !nanos.Valid {
		return nil
	}
	value := nanosToTime(nanos.Int64)
	return &value
}

// nanosToOptionalTime decodes the availability sentinel: 0 means "immediately
// available" and is represented as nil in the domain model, because the Badger
// index used the same polarity.
func nanosToOptionalTime(nanos int64) *time.Time {
	if nanos == 0 {
		return nil
	}
	value := nanosToTime(nanos)
	return &value
}

// optionalTimeToNanos encodes a nullable timestamp into a NOT NULL column that
// uses 0 as its "no value" sentinel. Only available_for_processing_at uses this
// polarity; every other nullable timestamp is a real SQL NULL through
// timePtrToNanos.
func optionalTimeToNanos(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UTC().UnixNano()
}

// cacheRecord publishes one record under the cache's monotonic UpdatedAt guard:
// an active status is cached, any other status is removed from the cache.
func (r *SQLiteMetadataRepository) cacheRecord(metadata *core.FileMetadata) {
	if metadata == nil {
		return
	}
	if isActiveMetadataStatus(metadata.Status) {
		r.cache.set(metadata)
		return
	}
	r.cache.delete(metadata.TenantID, metadata.FileKey)
}

// isActiveMetadataStatus reports whether one status is cached. Active statuses
// are Pending, Processing and Failed; Completed, PermanentlyFailed, the reserved
// Locus lifecycle values and DeadLettered are never cached.
func isActiveMetadataStatus(status core.FileProcessingStatus) bool {
	return status == core.FileStatusPending ||
		status == core.FileStatusProcessing ||
		status == core.FileStatusFailed
}

// fileSize returns the size of path, or 0 when it cannot be read. It is only
// used for advisory statistics.
func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// warnRepositoryEvent emits one path-free structured warning.
//
// Raw errors and physical paths are deliberately excluded: a file-system error
// embeds the database or backup directory, and the logging rules keep full
// physical paths out of logs.
func warnRepositoryEvent(rt *logging.Runtime, event string, reason string) {
	if rt == nil || !rt.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	rt.Emit(context.Background(), logging.Record{
		Level:     slog.LevelWarn,
		Component: "metadata_repository",
		Event:     event,
		Message:   "sqlite metadata repository degraded",
		Attrs: []slog.Attr{
			slog.String("reason", reason),
		},
	})
}
