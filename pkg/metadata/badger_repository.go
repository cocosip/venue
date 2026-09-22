package metadata

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// BadgerRepositoryOptions configures a BadgerDB metadata repository.
type BadgerRepositoryOptions struct {
	// TenantID is the unique identifier for this tenant.
	TenantID string

	// DataPath is the root directory where tenant databases are stored.
	DataPath string

	// CacheTTL is the time-to-live for cached metadata.
	// Only active files (Pending/Processing/Failed) are cached.
	CacheTTL time.Duration

	// MaxCacheEntries is the maximum number of entries in the cache.
	// Default: 10000
	MaxCacheEntries int

	// GCInterval is the interval for running BadgerDB garbage collection.
	// Default: 10 minutes
	GCInterval time.Duration

	// GCDiscardRatio is the discard ratio for GC (0.0 - 1.0).
	// Files with this ratio of outdated data will be rewritten.
	// Values outside (0,1) are clamped, because BadgerDB rejects them.
	// Default: 0.5 (50%)
	GCDiscardRatio float64

	// MemTableSize is the size of each memtable in bytes.
	// Default: 32MB for metadata
	MemTableSize int64

	// ValueLogFileSize is the size of each value log file in bytes.
	// Default: 64MB
	ValueLogFileSize int64

	// BlockCacheSize is the size of the block cache in bytes.
	// Default: 64MB for metadata
	BlockCacheSize int64

	// SyncWrites enables synchronous writes. Disable for better performance.
	// Default: false
	SyncWrites bool

	// RecoverCorruptedDatabase quarantines a database directory that cannot be
	// opened and recreates an empty one instead of failing startup.
	//
	// The directory is renamed to a sibling named
	// "<dbPath>.corrupted.<UTC timestamp>", so the unusable data is preserved
	// rather than deleted, and the tenant's queued records are lost until an
	// operator restores them. A lock or ownership failure (another process using
	// the database) is never treated as corruption: that directory may hold a
	// healthy database, so it is reported as a startup failure even when this
	// flag is true.
	RecoverCorruptedDatabase bool

	// CorruptedDatabaseRetention is how long a quarantined directory is kept.
	// Quarantined siblings older than this are pruned best-effort during the
	// next open. Zero selects 72 hours; a negative value disables pruning.
	CorruptedDatabaseRetention time.Duration

	// OnCorruptedDatabase, when set, is called synchronously with the quarantine
	// directory path after a database was quarantined. It runs on the opening
	// goroutine and must not block; a panic from it propagates to the caller.
	OnCorruptedDatabase func(quarantinedPath string)

	// BackupDirectory is where periodic consistent backups of this repository
	// are written, and where automatic recovery looks for one. An empty value
	// disables both.
	//
	// See BackupService for the periodic runner and AutoRestoreFromBackup for
	// the recovery path.
	BackupDirectory string

	// BackupRetention is how long a backup file is kept. It is applied by the
	// periodic runner (see BackupService, whose BackupServiceOptions.Retention
	// carries the same policy), not by the repository itself: a repository only
	// reads backups when automatic recovery needs one, and pruning then would
	// race the runner that owns the directory. A non-positive value disables
	// pruning.
	BackupRetention time.Duration

	// AutoRestoreFromBackup loads the newest readable backup into the database
	// directory when that directory had to be quarantined during open.
	//
	// This is a repair aid, not a replacement for an operator: Venue's
	// recoverability granularity is the backup interval, so every record
	// committed after the newest backup is gone. It only applies when
	// RecoverCorruptedDatabase is enabled and BackupDirectory is set, it never
	// overwrites a database that opens successfully, and a backup that cannot be
	// loaded leaves the repository empty and degraded rather than failing
	// startup.
	AutoRestoreFromBackup bool

	// Logging receives structured repository diagnostics. A nil runtime means
	// silent, and logging never fails a storage operation.
	Logging *logging.Runtime

	// StatisticsRecorder receives metadata persistence counters from the commit
	// path. A nil recorder disables statistics.
	StatisticsRecorder core.StatisticsRecorder
}

const (
	// defaultGCDiscardRatio is used when no discard ratio is configured.
	defaultGCDiscardRatio = 0.5

	// maxGCDiscardRatio keeps the discard ratio strictly below 1: BadgerDB
	// rejects a ratio of 1.0 or more with ErrInvalidRequest.
	maxGCDiscardRatio = 0.99

	// closeTimeout bounds how long Close waits for BadgerDB to release its
	// handles before reporting the failure to the caller.
	closeTimeout = 30 * time.Second
)

// BadgerMetadataRepository implements MetadataRepository using BadgerDB.
//
// A repository owns one BadgerDB handle for one tenant. Close releases the
// handle and is idempotent: every caller receives the result of the single
// shutdown attempt, so a caller can never observe success while the handle is
// still open.
type BadgerMetadataRepository struct {
	db             *badger.DB
	cache          *metadataCache
	gcInterval     time.Duration
	gcDiscardRatio float64
	gcStopCh       chan struct{}
	gcWg           sync.WaitGroup
	stopGCOnce     sync.Once
	closeOnce      sync.Once
	closeErr       error
	mu             sync.RWMutex
	closed         bool

	// statistics receives the metadata persistence counters of committed
	// batches. A nil recorder is disabled.
	statistics core.StatisticsRecorder
}

// errRepositoryClosed classifies use of a closed repository as an
// infrastructure failure so callers never mistake it for claim contention.
func errRepositoryClosed() error {
	return fmt.Errorf("metadata repository is closed: %w", core.ErrDatabaseError)
}

// NewBadgerMetadataRepository creates a new BadgerDB metadata repository.
func NewBadgerMetadataRepository(opts *BadgerRepositoryOptions) (core.MetadataRepository, error) {
	dbPath, badgerOpts, err := newBadgerOptions(opts)
	if err != nil {
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

	gcInterval := opts.GCInterval
	if gcInterval == 0 {
		gcInterval = 10 * time.Minute
	}

	// A discard ratio outside (0,1) makes every BadgerDB GC run fail with
	// ErrInvalidRequest, so clamp it here instead of trusting callers.
	gcDiscardRatio := opts.GCDiscardRatio
	switch {
	case !(gcDiscardRatio > 0): // Also covers NaN.
		gcDiscardRatio = defaultGCDiscardRatio
	case gcDiscardRatio >= 1:
		gcDiscardRatio = maxGCDiscardRatio
	}

	// A database that cannot be opened would otherwise block startup forever, so
	// recovery is handled by the shared helper: quarantine the unusable
	// directory and open a fresh one when the caller asked for it. The callback
	// is wrapped because automatic recovery must know that a quarantine
	// happened, not only that it was offered the data.
	quarantined := false
	db, err := OpenBadgerWithRecovery(dbPath, CorruptedDatabaseRecoveryOptions{
		RecoverCorruptedDatabase:   opts.RecoverCorruptedDatabase,
		CorruptedDatabaseRetention: opts.CorruptedDatabaseRetention,
		OnCorruptedDatabase: func(quarantinedPath string) {
			quarantined = true
			if opts.OnCorruptedDatabase != nil {
				opts.OnCorruptedDatabase(quarantinedPath)
			}
		},
	}, func() (*badger.DB, error) {
		return badger.Open(badgerOpts)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// A quarantined database is empty. Replace it with the newest readable
	// backup before the repository starts serving, so a restart after corruption
	// resumes from the newest backup instead of from nothing.
	if quarantined && opts.AutoRestoreFromBackup && opts.BackupDirectory != "" {
		db = restoreNewestBackupIntoDatabase(dbPath, badgerOpts, opts, db)
	}

	if err := migrateLegacyMetadata(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to migrate metadata: %w", err)
	}

	// Create cache with size limit
	cache := newMetadataCacheWithSize(cacheTTL, maxCacheEntries)

	repo := &BadgerMetadataRepository{
		db:             db,
		cache:          cache,
		gcInterval:     gcInterval,
		gcDiscardRatio: gcDiscardRatio,
		gcStopCh:       make(chan struct{}),
		statistics:     opts.StatisticsRecorder,
	}

	// Start background GC
	repo.startGC()

	return repo, nil
}

// newBadgerOptions validates the repository options and derives the database
// path and the engine options both the repository open and an offline restore
// use, so a restored database is opened exactly like a live one.
func newBadgerOptions(opts *BadgerRepositoryOptions) (string, badger.Options, error) {
	if opts == nil {
		return "", badger.Options{}, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	// The tenant ID becomes the database directory segment, so it must be a safe
	// single path segment and cannot be empty.
	if err := core.ValidateTenantID(opts.TenantID); err != nil {
		return "", badger.Options{}, fmt.Errorf("invalid repository tenant ID: %w", err)
	}

	if opts.DataPath == "" {
		return "", badger.Options{}, fmt.Errorf("data path cannot be empty: %w", core.ErrInvalidArgument)
	}

	memTableSize := opts.MemTableSize
	if memTableSize == 0 {
		memTableSize = 32 << 20 // 32MB default
	}

	valueLogFileSize := opts.ValueLogFileSize
	if valueLogFileSize == 0 {
		valueLogFileSize = 64 << 20 // 64MB default
	}

	blockCacheSize := opts.BlockCacheSize
	if blockCacheSize == 0 {
		blockCacheSize = 64 << 20 // 64MB default
	}

	// Create tenant-specific database path
	dbPath := filepath.Join(opts.DataPath, opts.TenantID, "metadata")

	// Open BadgerDB with optimized settings for production workloads
	// These settings balance memory usage, write performance, and durability
	badgerOpts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable BadgerDB logging
		WithMemTableSize(memTableSize).
		WithValueLogFileSize(valueLogFileSize).
		WithNumMemtables(3).             // 3 memtables for smoother L0 flush
		WithNumLevelZeroTables(3).       // 3 L0 tables before compaction
		WithNumLevelZeroTablesStall(6).  // Stall threshold to prevent too many L0 tables
		WithValueThreshold(1 << 10).     // 1KB threshold for value log
		WithCompression(options.Snappy). // Enable Snappy compression for better I/O
		WithBlockCacheSize(blockCacheSize).
		WithIndexCacheSize(32 << 20). // 32MB index cache
		WithNumCompactors(3).         // 3 concurrent compactors
		WithCompactL0OnClose(true).   // Compact L0 on close for faster restart
		WithSyncWrites(opts.SyncWrites)
	// Note: Must keep conflict detection enabled for concurrent file allocation

	return dbPath, badgerOpts, nil
}

// restoreNewestBackupIntoDatabase replaces a freshly created empty database with
// the newest readable metadata backup.
//
// It is the automatic half of the repair aid behind
// BadgerRepositoryOptions.AutoRestoreFromBackup, and it is deliberately
// conservative:
//
//   - The backup is loaded into a temporary sibling directory first and only
//     swapped into place after it opens successfully, so a damaged or truncated
//     backup can never damage the empty database the repository just created.
//   - Every failure is contained. A missing, unreadable, or wrongly shaped
//     backup leaves the caller with its original empty database and a single
//     structured warning, because failing startup would turn a degraded runtime
//     into no runtime at all.
//   - A backup that cannot be read is skipped in favor of the next newest one.
//
// The returned handle is owned by the caller and must be closed. The guarantee
// is bounded by the backup interval: records committed after the newest backup
// are not recoverable this way.
func restoreNewestBackupIntoDatabase(dbPath string, badgerOpts badger.Options, opts *BadgerRepositoryOptions, emptyDB *badger.DB) *badger.DB {
	backups, err := listBackupsNewestFirst(opts.BackupDirectory)
	if err != nil {
		warnRepositoryOpen(opts.Logging, "backup_scan_failed", err)
		return emptyDB
	}
	if len(backups) == 0 {
		warnRepositoryOpen(opts.Logging, "no_backup_available", nil)
		return emptyDB
	}

	for _, backup := range backups {
		restorePath, restoreErr := restoreBackupSibling(dbPath, badgerOpts, backup.path)
		if restoreErr != nil {
			warnRepositoryOpen(opts.Logging, "backup_restore_failed", restoreErr)
			continue
		}

		// The staged database is complete and openable. Release the empty handle
		// before its directory is replaced: Windows will not rename over an open
		// database.
		if closeErr := emptyDB.Close(); closeErr != nil {
			warnRepositoryOpen(opts.Logging, "restore_swap_failed", closeErr)
			_ = os.RemoveAll(restorePath)
			return reopenEmptyDatabase(emptyDB, badgerOpts)
		}

		if _, statErr := os.Stat(dbPath); statErr == nil {
			if removeErr := os.RemoveAll(dbPath); removeErr != nil {
				warnRepositoryOpen(opts.Logging, "restore_swap_failed", removeErr)
				_ = os.RemoveAll(restorePath)
				return reopenEmptyDatabase(emptyDB, badgerOpts)
			}
		}
		if renameErr := os.Rename(restorePath, dbPath); renameErr != nil {
			warnRepositoryOpen(opts.Logging, "restore_swap_failed", renameErr)
			_ = os.RemoveAll(restorePath)
			return reopenEmptyDatabase(emptyDB, badgerOpts)
		}

		restored, openErr := badger.Open(badgerOpts)
		if openErr != nil {
			// The staged copy opened once, so this is an environment failure.
			// Keep the runtime alive on an empty database instead of failing
			// startup; the unusable restored directory is removed because the
			// quarantine path is unavailable here.
			warnRepositoryOpen(opts.Logging, "restore_reopen_failed", openErr)
			_ = os.RemoveAll(dbPath)
			return reopenEmptyDatabase(emptyDB, badgerOpts)
		}
		return restored
	}

	warnRepositoryOpen(opts.Logging, "backup_restore_unavailable", nil)
	return emptyDB
}

// reopenEmptyDatabase restores the empty-database fallback after a failed swap.
//
// The handle passed in was already closed, so it can no longer serve reads or
// writes; a fresh handle over the recreated directory is opened instead. When
// even that fails the caller keeps the old handle, which is the only value that
// preserves the "startup never fails because of a restore" guarantee.
func reopenEmptyDatabase(closedDB *badger.DB, badgerOpts badger.Options) *badger.DB {
	reopened, err := badger.Open(badgerOpts)
	if err != nil {
		return closedDB
	}
	return reopened
}

// restoreBackupSibling loads backupPath into a temporary sibling of dbPath and
// returns that directory after verifying it opens. The caller owns the returned
// directory and must remove or rename it.
func restoreBackupSibling(dbPath string, badgerOpts badger.Options, backupPath string) (string, error) {
	restorePath := dbPath + backupRestoreSuffix + time.Now().UTC().Format(backupTimestampLayout)
	if err := os.RemoveAll(restorePath); err != nil {
		return "", fmt.Errorf("failed to clear the restore staging directory: %w", err)
	}

	file, err := os.Open(backupPath)
	if err != nil {
		return "", fmt.Errorf("failed to open the metadata backup: %w", err)
	}
	defer func() { _ = file.Close() }()

	stagedOpts := badgerOpts.WithDir(restorePath).WithValueDir(restorePath)
	// A restore target must never quarantine: it is a staging directory this
	// function created, and a quarantine there would consume disk while leaving
	// the empty database in place.
	if err := restoreDatabaseFromStream(context.Background(), restorePath, stagedOpts, file); err != nil {
		_ = os.RemoveAll(restorePath)
		return "", err
	}

	// Verify the staged database is openable before it is allowed to replace the
	// live one. A backup whose keys load but whose tables cannot be read must be
	// rejected here, not after the swap.
	verify, err := badger.Open(stagedOpts)
	if err != nil {
		_ = os.RemoveAll(restorePath)
		return "", fmt.Errorf("failed to open the staged restore: %w", err)
	}
	if err := verify.Close(); err != nil {
		_ = os.RemoveAll(restorePath)
		return "", fmt.Errorf("failed to close the staged restore: %w", err)
	}
	return restorePath, nil
}

// warnRepositoryOpen emits one path-free warning about automatic recovery.
//
// Raw errors and paths are deliberately excluded: a file-system error embeds the
// database or backup directory, and the logging rules keep full physical paths
// out of logs.
func warnRepositoryOpen(rt *logging.Runtime, reason string, err error) {
	if rt == nil || !rt.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	rt.Emit(context.Background(), logging.Record{
		Level:     slog.LevelWarn,
		Component: "metadata_repository",
		Event:     "metadata_auto_restore_failed",
		Message:   "automatic metadata recovery did not restore a backup",
		Attrs: []slog.Attr{
			slog.String("reason", reason),
			slog.String("error_kind", classifyBackupError(err)),
		},
	})
}

// recordPersistedBatch records the metadata persistence statistics of one
// committed batch. It is called exactly once per committed transaction, after
// the commit succeeded, and never for a batch that was rejected or rolled back.
func (r *BadgerMetadataRepository) recordPersistedBatch(operations int) {
	if r.statistics == nil || operations <= 0 {
		return
	}
	now := time.Now()
	r.statistics.Record(core.StatisticMetadataPersistedBatchCount, 1, now, nil)
	r.statistics.Record(core.StatisticMetadataPersistedOperationCount, int64(operations), now, nil)
}

// AddOrUpdate adds or updates file metadata atomically.
// Also maintains secondary indexes for efficient status-based queries.
func (r *BadgerMetadataRepository) AddOrUpdate(ctx context.Context, metadata *core.FileMetadata) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	if metadata == nil {
		return fmt.Errorf("metadata cannot be nil: %w", core.ErrInvalidArgument)
	}

	if metadata.FileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}
	if metadata.TenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Serialize metadata
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Write to BadgerDB with index maintenance. A concurrent writer on the same
	// record surfaces as badger.ErrConflict, which is retried a bounded number of
	// times before being reported.
	err = retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			// Get old metadata to update indexes
			oldMetadata, _ := r.getMetadataInTxn(txn, metadata.TenantID, metadata.FileKey)

			// Write primary data
			key := r.buildKey(metadata.TenantID, metadata.FileKey)
			if err := txn.Set(key, data); err != nil {
				return err
			}

			// Update secondary indexes if the index key changed or this is a new file
			if r.statusIndexChanged(oldMetadata, metadata) {
				// Delete old status index if exists
				if oldMetadata != nil {
					oldIndexKey := r.buildStatusIndexKey(oldMetadata)
					if err := txn.Delete(oldIndexKey); err != nil {
						return err
					}
				}
				// Add new status index
				newIndexKey := r.buildStatusIndexKey(metadata)
				if err := txn.Set(newIndexKey, []byte(metadata.FileKey)); err != nil {
					return err
				}
			}

			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	r.recordPersistedBatch(1)

	// Cache if active
	if r.isActiveStatus(metadata.Status) {
		r.cache.set(metadata)
	} else {
		// Remove from cache if no longer active
		r.cache.delete(metadata.TenantID, metadata.FileKey)
	}

	return nil
}

// AddOrUpdateBatch adds or updates multiple file metadata atomically in a single transaction.
// More efficient than calling AddOrUpdate multiple times for bulk operations.
func (r *BadgerMetadataRepository) AddOrUpdateBatch(ctx context.Context, metadata []*core.FileMetadata) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	if len(metadata) == 0 {
		return nil // Nothing to do
	}

	// Validate the whole slice before opening the transaction: a rejected batch
	// must write nothing, and the error must name the offending entry.
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

	// Perform batch update in a single transaction
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			for _, m := range metadata {
				// Serialize metadata
				data, err := json.Marshal(m)
				if err != nil {
					return fmt.Errorf("failed to serialize metadata for %s: %w", m.FileKey, err)
				}

				// Get old metadata to update indexes
				oldMetadata, _ := r.getMetadataInTxn(txn, m.TenantID, m.FileKey)

				// Write primary data
				key := r.buildKey(m.TenantID, m.FileKey)
				if err := txn.Set(key, data); err != nil {
					return err
				}

				// Update secondary indexes if the index key changed or this is a new file
				if r.statusIndexChanged(oldMetadata, m) {
					// Delete old status index if exists
					if oldMetadata != nil {
						oldIndexKey := r.buildStatusIndexKey(oldMetadata)
						if err := txn.Delete(oldIndexKey); err != nil {
							return err
						}
					}
					// Add new status index
					newIndexKey := r.buildStatusIndexKey(m)
					if err := txn.Set(newIndexKey, []byte(m.FileKey)); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("failed to batch update metadata: %w", err)
	}
	r.recordPersistedBatch(len(metadata))

	// Update cache after successful transaction
	for _, m := range metadata {
		if r.isActiveStatus(m.Status) {
			r.cache.set(m)
		} else {
			r.cache.delete(m.TenantID, m.FileKey)
		}
	}

	return nil
}

// Get retrieves file metadata by key.
func (r *BadgerMetadataRepository) Get(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check cache first
	if cached := r.cache.get(tenantID, fileKey); cached != nil {
		return cached, nil
	}

	// Read from BadgerDB
	var metadata *core.FileMetadata
	err := r.db.View(func(txn *badger.Txn) error {
		key := r.buildKey(tenantID, fileKey)
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return core.ErrFileNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			metadata = &core.FileMetadata{}
			return json.Unmarshal(val, metadata)
		})
	})

	if err != nil {
		// Don't wrap ErrFileNotFound
		if err == core.ErrFileNotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	// Cache if active
	if r.isActiveStatus(metadata.Status) {
		r.cache.set(metadata)
	}

	return metadata, nil
}

// Delete removes file metadata and its secondary indexes.
func (r *BadgerMetadataRepository) Delete(ctx context.Context, tenantID, fileKey string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Delete from BadgerDB including indexes
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			// Get metadata first to delete index
			metadata, _ := r.getMetadataInTxn(txn, tenantID, fileKey)
			if metadata != nil {
				indexKey := r.buildStatusIndexKey(metadata)
				if err := txn.Delete(indexKey); err != nil {
					return err
				}
			}

			// Delete primary data
			key := r.buildKey(tenantID, fileKey)
			return txn.Delete(key)
		})
	})

	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}
	r.recordPersistedBatch(1)

	// Remove from cache
	r.cache.delete(tenantID, fileKey)

	return nil
}

// DeleteBatch removes multiple file metadata atomically in a single transaction.
func (r *BadgerMetadataRepository) DeleteBatch(ctx context.Context, tenantID string, fileKeys []string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if len(fileKeys) == 0 {
		return nil // Nothing to do
	}

	// Validate the whole slice before opening the transaction: a rejected batch
	// must delete nothing, and the error must name the offending entry.
	for i, fileKey := range fileKeys {
		if fileKey == "" {
			return fmt.Errorf("fileKeys[%d] cannot be empty: %w", i, core.ErrInvalidArgument)
		}
	}

	// Perform batch delete in a single transaction
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			for _, fileKey := range fileKeys {
				// Get metadata first to delete index
				metadata, _ := r.getMetadataInTxn(txn, tenantID, fileKey)
				if metadata != nil {
					indexKey := r.buildStatusIndexKey(metadata)
					if err := txn.Delete(indexKey); err != nil {
						return err
					}
				}

				// Delete primary data
				key := r.buildKey(tenantID, fileKey)
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
	})

	if err != nil {
		return fmt.Errorf("failed to batch delete metadata: %w", err)
	}
	r.recordPersistedBatch(len(fileKeys))

	// Remove from cache
	for _, fileKey := range fileKeys {
		r.cache.delete(tenantID, fileKey)
	}

	return nil
}

// GetByStatus retrieves files by status with optional limit.
// Uses secondary index for O(log n) lookup instead of O(n) full scan.
//
// Active statuses are read from the index as well: the metadata cache is
// bounded and eviction-ordered, so serving queries from it would silently
// truncate the result. The cache only accelerates Get.
func (r *BadgerMetadataRepository) GetByStatus(ctx context.Context, tenantID string, status core.FileProcessingStatus, limit int) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Use secondary index for efficient lookup
	var results []*core.FileMetadata
	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan status index: v3:idx:status:{tenant}:{status}:{availableUTC}:{createdUTC}:{fileKey}
		prefix := r.buildStatusIndexPrefix(tenantID, status)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			fileKeyBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				continue
			}
			fileKey := string(fileKeyBytes)

			// Get actual metadata
			metadata, err := r.getMetadataInTxn(txn, tenantID, fileKey)
			if err != nil {
				continue // Skip if file not found (shouldn't happen)
			}

			results = append(results, metadata)

			// Check limit
			if limit > 0 && len(results) >= limit {
				break
			}
		}

		return nil
	})

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status: %w", err)
	}

	return results, nil
}

// GetPendingFiles retrieves files ready for processing.
// Uses secondary index for efficient O(log n) lookup.
func (r *BadgerMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	now := time.Now()
	var results []*core.FileMetadata

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan the Pending status index; it sorts by availability then arrival,
		// so the first results are the oldest claimable files (FIFO).
		prefix := r.buildStatusIndexPrefix(tenantID, core.FileStatusPending)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			fileKeyBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				continue
			}
			fileKey := string(fileKeyBytes)
			if fileKey == "" {
				continue
			}

			// Get actual metadata to verify availability
			metadata, err := r.getMetadataInTxn(txn, tenantID, fileKey)
			if err != nil {
				continue // Skip if file not found
			}

			// Check if the file is ready for processing. The comparison is
			// inclusive: a file whose availability equals the current instant is
			// claimable, because coarse platform clock granularity (about 15 ms
			// on Windows) can hand a just-recovered file the same timestamp as
			// the claim.
			if metadata.AvailableForProcessingAt == nil || !metadata.AvailableForProcessingAt.After(now) {
				results = append(results, metadata)

				// Check limit
				if limit > 0 && len(results) >= limit {
					break
				}
			}
		}

		return nil
	})

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	return results, nil
}

// UpdateStatus atomically updates file status and maintains secondary indexes.
func (r *BadgerMetadataRepository) UpdateStatus(ctx context.Context, tenantID, fileKey string, newStatus core.FileProcessingStatus) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Update in BadgerDB with index maintenance
	var updatedMetadata *core.FileMetadata
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			key := r.buildKey(tenantID, fileKey)

			// Get existing metadata
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return core.ErrFileNotFound
				}
				return err
			}

			var metadata *core.FileMetadata
			err = item.Value(func(val []byte) error {
				metadata = &core.FileMetadata{}
				return json.Unmarshal(val, metadata)
			})
			if err != nil {
				return err
			}

			// Delete old status index if it changes
			oldIndexKey := r.buildStatusIndexKey(metadata)

			// Update status
			metadata.Status = newStatus

			// Serialize and save
			data, err := json.Marshal(metadata)
			if err != nil {
				return err
			}

			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}
			// Add new status index
			newIndexKey := r.buildStatusIndexKey(metadata)
			if err := txn.Set(newIndexKey, []byte(metadata.FileKey)); err != nil {
				return err
			}

			// Store for cache update
			updatedMetadata = metadata

			return txn.Set(key, data)
		})
	})

	if err != nil {
		// Don't wrap ErrFileNotFound
		if err == core.ErrFileNotFound {
			return err
		}
		return fmt.Errorf("failed to update status: %w", err)
	}
	r.recordPersistedBatch(1)

	// Update cache
	if r.isActiveStatus(newStatus) {
		r.cache.set(updatedMetadata)
	} else {
		r.cache.delete(tenantID, fileKey)
	}

	return nil
}

// CompareAndTransitionToProcessing atomically transitions a file to Processing status
// if and only if it is currently in Pending status.
// Also updates secondary indexes.
//
// A file that another worker already claimed, or that is still inside its
// availability window, is contention and yields ErrFileNotClaimable (wrapped).
// A missing record yields ErrFileNotFound; infrastructure failures are wrapped
// with ErrDatabaseError so callers can continue on contention only.
func (r *BadgerMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, tenantID, fileKey string) (*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	var updatedMetadata *core.FileMetadata

	// Perform atomic compare-and-swap in a transaction. Contention on the same
	// record is retried a bounded number of times; a conflict that survives the
	// budget is still classified as a lost claim below.
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			key := r.buildKey(tenantID, fileKey)

			// Get existing metadata
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return core.ErrFileNotFound
				}
				return err
			}

			var metadata *core.FileMetadata
			err = item.Value(func(val []byte) error {
				metadata = &core.FileMetadata{}
				return json.Unmarshal(val, metadata)
			})
			if err != nil {
				return err
			}

			// Check if file is in Pending status
			if metadata.Status != core.FileStatusPending {
				return fmt.Errorf("file is not in pending status (current status is %s): %w",
					metadata.Status.String(), core.ErrFileNotClaimable)
			}

			// Check if available for processing
			if metadata.AvailableForProcessingAt != nil && time.Now().Before(*metadata.AvailableForProcessingAt) {
				return fmt.Errorf("file is not yet available for processing (available at %s): %w",
					metadata.AvailableForProcessingAt.UTC().Format(time.RFC3339Nano), core.ErrFileNotClaimable)
			}

			// Delete old Pending status index
			oldIndexKey := r.buildStatusIndexKey(metadata)
			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}

			// Update to Processing status
			now := time.Now()
			metadata.Status = core.FileStatusProcessing
			metadata.ProcessingStartTime = &now
			metadata.UpdatedAt = now

			// Serialize and save
			data, err := json.Marshal(metadata)
			if err != nil {
				return err
			}

			// Add new Processing status index
			newIndexKey := r.buildStatusIndexKey(metadata)
			if err := txn.Set(newIndexKey, []byte(metadata.FileKey)); err != nil {
				return err
			}

			updatedMetadata = metadata

			return txn.Set(key, data)
		})
	})

	if err != nil {
		// Don't wrap known domain errors.
		if errors.Is(err, core.ErrFileNotFound) ||
			errors.Is(err, core.ErrFileNotClaimable) ||
			errors.Is(err, core.ErrInvalidArgument) {
			return nil, err
		}
		if errors.Is(err, badger.ErrConflict) {
			// A concurrent writer touched the same record, so this worker lost
			// the race. That is contention, not an infrastructure failure.
			return nil, fmt.Errorf("claim lost the write race: %w: %w", err, core.ErrFileNotClaimable)
		}
		return nil, fmt.Errorf("failed to transition file to processing: %w: %w", err, core.ErrDatabaseError)
	}

	// Update cache with a copy
	r.cache.set(updatedMetadata)

	// Return a copy to prevent concurrent modification
	result := *updatedMetadata
	return &result, nil
}

// CompareAndUpdateProcessing atomically updates metadata when the active
// processing state still matches the supplied lease.
//
// Releasing the same lease twice is idempotent: once a lease has moved the file
// out of Processing, the suppressed release marker identifies it and the same
// lease succeeds again without changing anything. A lease superseded by a newer
// claim is still rejected with ErrProcessingLeaseMismatch, as is an unknown
// lease.
func (r *BadgerMetadataRepository) CompareAndUpdateProcessing(
	ctx context.Context,
	lease core.FileProcessingLease,
	update func(*core.FileMetadata) error,
) (*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()

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

	var updatedMetadata *core.FileMetadata
	// A lease release races with concurrent claims and timeout reclaims for the
	// same record, so a conflict is retried like every other write path. The
	// transaction is re-run from scratch on each attempt, which re-reads the
	// record and re-applies the caller's pure update callback.
	err := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		return r.db.Update(func(txn *badger.Txn) error {
			current, err := r.getMetadataInTxn(txn, lease.TenantID, lease.FileKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return newLeaseMismatchError(lease, nil)
				}
				return err
			}
			if current == nil {
				return newLeaseMismatchError(lease, nil)
			}

			leaseIsActive := current.Status == core.FileStatusProcessing &&
				current.ProcessingStartTime != nil &&
				current.ProcessingStartTime.Equal(lease.ProcessingStartTimeUTC)
			if !leaseIsActive {
				// A file still under processing belongs to a newer claim, so a stale
				// lease is rejected even when it was released earlier.
				if current.Status != core.FileStatusProcessing &&
					current.ReleasedProcessingStartTimeUTC != nil &&
					current.ReleasedProcessingStartTimeUTC.Equal(lease.ProcessingStartTimeUTC) {
					// Already released: report the stored record unchanged.
					updatedMetadata = current
					return nil
				}
				return newLeaseMismatchError(lease, current)
			}

			oldIndexKey := r.buildStatusIndexKey(current)
			if err := update(current); err != nil {
				return err
			}
			if current.TenantID != lease.TenantID || current.FileKey != lease.FileKey {
				return fmt.Errorf("update cannot change metadata identity: %w", core.ErrInvalidArgument)
			}

			// Remember which lease this record last released so a repeated release
			// of the same lease can be recognized as a no-op.
			if current.Status != core.FileStatusProcessing ||
				current.ProcessingStartTime == nil ||
				!current.ProcessingStartTime.Equal(lease.ProcessingStartTimeUTC) {
				released := lease.ProcessingStartTimeUTC
				current.ReleasedProcessingStartTimeUTC = &released
			}

			data, err := json.Marshal(current)
			if err != nil {
				return err
			}
			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}
			if err := txn.Set(r.buildStatusIndexKey(current), []byte(current.FileKey)); err != nil {
				return err
			}
			if err := txn.Set(r.buildKey(current.TenantID, current.FileKey), data); err != nil {
				return err
			}

			updatedMetadata = current
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, core.ErrProcessingLeaseMismatch) || errors.Is(err, core.ErrInvalidArgument) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to conditionally update processing metadata: %w: %w", err, core.ErrDatabaseError)
	}

	if r.isActiveStatus(updatedMetadata.Status) {
		r.cache.set(updatedMetadata)
	} else {
		r.cache.delete(updatedMetadata.TenantID, updatedMetadata.FileKey)
	}

	result := *updatedMetadata
	return &result, nil
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

// GetTimedOutProcessingFiles retrieves files in Processing status that exceed timeout.
// Uses secondary index for efficient lookup.
func (r *BadgerMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, tenantID string, timeout time.Duration) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	now := time.Now()
	var results []*core.FileMetadata

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan Processing status index
		prefix := r.buildStatusIndexPrefix(tenantID, core.FileStatusProcessing)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			fileKeyBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				continue
			}
			fileKey := string(fileKeyBytes)

			// Get actual metadata
			metadata, err := r.getMetadataInTxn(txn, tenantID, fileKey)
			if err != nil {
				continue // Skip if file not found
			}

			// Check if processing has timed out
			if metadata.ProcessingStartTime != nil {
				elapsed := now.Sub(*metadata.ProcessingStartTime)
				if elapsed > timeout {
					results = append(results, metadata)
				}
			}
		}

		return nil
	})

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get timed out files: %w", err)
	}

	return results, nil
}

// Optimize triggers BadgerDB garbage collection to reclaim disk space.
func (r *BadgerMetadataRepository) Optimize(ctx context.Context) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return errRepositoryClosed()
	}
	r.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := r.db.RunValueLogGC(r.gcDiscardRatio)
		if err == nil {
			continue
		}
		if errors.Is(err, badger.ErrNoRewrite) {
			return nil
		}
		return fmt.Errorf("failed to optimize BadgerDB: %w", err)
	}
}

// Backup writes a consistent online snapshot of the whole repository to w and
// returns the BadgerDB sequence number the snapshot was taken at.
//
// The snapshot is taken by the engine's stream, so it runs while the repository
// keeps serving reads and writes: no whole-operation lock is held, and callers
// observe a consistent point in time rather than a stopped runtime. Records
// committed after the returned sequence number are not part of this stream and
// belong to the next backup, which is why Venue's recoverability granularity is
// the backup interval.
//
// A canceled context or a nil writer is rejected before any work starts, and a
// closed repository reports core.ErrDatabaseError like every other rejected
// operation. A failure from w is reported unchanged (wrapped with %w): the
// engine cancels its stream when the writer fails and surfaces that cancellation
// instead, so the writer's own error is captured and reported in its place.
func (r *BadgerMetadataRepository) Backup(ctx context.Context, w io.Writer) (uint64, error) {
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
	db := r.db
	r.mu.RUnlock()

	captured := &capturingWriter{w: w}
	since, err := db.Backup(captured, 0)
	if captured.err != nil {
		return 0, fmt.Errorf("failed to write metadata backup: %w", captured.err)
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, fmt.Errorf("metadata backup canceled: %w", ctxErr)
		}
		return 0, fmt.Errorf("failed to write metadata backup: %w", err)
	}
	return since, nil
}

// capturingWriter remembers the first write failure so a caller can report the
// writer's own error even when the engine replaces it with its internal stream
// cancellation.
type capturingWriter struct {
	w   io.Writer
	err error
}

// Write forwards to the wrapped writer and records the first failure.
func (c *capturingWriter) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.w.Write(p)
	if err != nil && c.err == nil {
		c.err = err
	}
	return n, err
}

// Close closes the repository and releases resources.
// It is safe to call repeatedly and from multiple goroutines: the first caller
// performs the shutdown and every caller receives that same result, so a second
// caller can never report success while the database handle is still open. If
// BadgerDB does not release its handles within closeTimeout the failure is
// returned instead of being hidden.
func (r *BadgerMetadataRepository) Close() error {
	r.closeOnce.Do(func() {
		// Reject new work before tearing the database down.
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		// Stop the GC goroutine before closing the database.
		r.stopGCOnce.Do(func() { close(r.gcStopCh) })
		r.gcWg.Wait()

		r.closeErr = r.closeDatabase()
	})
	return r.closeErr
}

// closeDatabase closes the BadgerDB handle, bounding the wait so shutdown
// cannot hang forever.
func (r *BadgerMetadataRepository) closeDatabase() error {
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- r.db.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			return fmt.Errorf("failed to close BadgerDB: %w", err)
		}
		return nil
	case <-time.After(closeTimeout):
		// The handle may still be open; report it so the caller does not
		// mistake a leak for a clean shutdown.
		return fmt.Errorf("BadgerDB close timed out after %s", closeTimeout)
	}
}

// buildKey builds a tenant-scoped BadgerDB key for a file key.
func (r *BadgerMetadataRepository) buildKey(tenantID, fileKey string) []byte {
	return buildMetadataKey(tenantID, fileKey)
}

// isActiveStatus checks if a status is considered "active" for caching.
// Active statuses: Pending, Processing, Failed
// Inactive statuses: Completed, PermanentlyFailed
func (r *BadgerMetadataRepository) isActiveStatus(status core.FileProcessingStatus) bool {
	return status == core.FileStatusPending ||
		status == core.FileStatusProcessing ||
		status == core.FileStatusFailed
}

// startGC starts the background garbage collection goroutine.
func (r *BadgerMetadataRepository) startGC() {
	r.gcWg.Add(1)
	go func() {
		defer r.gcWg.Done()

		ticker := time.NewTicker(r.gcInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.runGC()
			case <-r.gcStopCh:
				return
			}
		}
	}()
}

// runGC runs BadgerDB garbage collection.
func (r *BadgerMetadataRepository) runGC() {
	_ = r.Optimize(context.Background())
}

// GetCacheStats returns cache statistics for monitoring.
func (r *BadgerMetadataRepository) GetCacheStats() map[string]interface{} {
	return r.cache.getStats()
}

// getMetadataInTxn retrieves metadata within a transaction (no locking).
func (r *BadgerMetadataRepository) getMetadataInTxn(txn *badger.Txn, tenantID, fileKey string) (*core.FileMetadata, error) {
	key := r.buildKey(tenantID, fileKey)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	var metadata *core.FileMetadata
	err = item.Value(func(val []byte) error {
		metadata = &core.FileMetadata{}
		return json.Unmarshal(val, metadata)
	})
	return metadata, err
}

// buildStatusIndexKey builds the secondary index key for status.
// Format: v3:idx:status:{tenant}:{status}:{availableUTC}:{createdUTC}:{fileKey}
// This allows efficient querying by status with FIFO ordering: the availability
// segment comes first, the arrival timestamp breaks ties.
func (r *BadgerMetadataRepository) buildStatusIndexKey(metadata *core.FileMetadata) []byte {
	return buildStatusIndexKey(metadata)
}

// buildStatusIndexPrefix builds the prefix for scanning by status.
func (r *BadgerMetadataRepository) buildStatusIndexPrefix(tenantID string, status core.FileProcessingStatus) []byte {
	return []byte(fmt.Sprintf("%s%s:%d:", statusIndexPrefix, encodeTenantID(tenantID), status))
}

// statusIndexChanged reports whether the secondary index entry differs between
// two revisions of the same record. The index key contains status, availability,
// creation time, and tenant, so all of them must be compared before skipping the
// index update.
func (r *BadgerMetadataRepository) statusIndexChanged(old, current *core.FileMetadata) bool {
	if old == nil {
		return true
	}
	return old.Status != current.Status ||
		!old.CreatedAt.Equal(current.CreatedAt) ||
		!r.equalAvailableTime(old.AvailableForProcessingAt, current.AvailableForProcessingAt)
}

func buildMetadataKey(tenantID, fileKey string) []byte {
	return []byte(fmt.Sprintf("v2:file:%s:%s", encodeTenantID(tenantID), fileKey))
}

func encodeTenantID(tenantID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(tenantID))
}

// equalAvailableTime compares two time pointers for equality.
func (r *BadgerMetadataRepository) equalAvailableTime(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
