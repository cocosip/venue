package quota

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// BadgerDirectoryQuotaRepositoryOptions configures the BadgerDB quota repository.
type BadgerDirectoryQuotaRepositoryOptions struct {
	// DataPath is the root directory where the quota database is stored.
	DataPath string

	// GCInterval is the interval for running BadgerDB garbage collection.
	// Default: 10 minutes
	GCInterval time.Duration

	// GCDiscardRatio is the discard ratio for GC (0.0 - 1.0).
	// Default: 0.5 (50%)
	GCDiscardRatio float64

	// MemTableSize is the size of each memtable in bytes.
	// Default: 16MB for quota
	MemTableSize int64

	// ValueLogFileSize is the size of each value log file in bytes.
	// Default: 32MB
	ValueLogFileSize int64

	// BlockCacheSize is the size of the block cache in bytes.
	// Default: 32MB for quota
	BlockCacheSize int64

	// SyncWrites enables synchronous writes. Disable for better performance.
	// Default: false
	SyncWrites bool

	// RecoverCorruptedDatabase quarantines a database directory that cannot be
	// opened and recreates an empty one instead of failing startup.
	//
	// The directory is renamed to a sibling named
	// "<dbPath>.corrupted.<UTC timestamp>", so the unusable data is preserved
	// rather than deleted, and the stored quota counters are lost until an
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
}

// badgerDirectoryQuotaRepository implements DirectoryQuotaRepository using BadgerDB.
type badgerDirectoryQuotaRepository struct {
	db             *badger.DB
	gcInterval     time.Duration
	gcDiscardRatio float64
	gcStopCh       chan struct{}
	gcWg           sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
	closeOnce      sync.Once
	closedCh       chan struct{}
	closeErr       error
}

// NewBadgerDirectoryQuotaRepository creates a new BadgerDB quota repository.
func NewBadgerDirectoryQuotaRepository(opts *BadgerDirectoryQuotaRepositoryOptions) (core.DirectoryQuotaRepository, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.DataPath == "" {
		return nil, fmt.Errorf("data path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Set defaults
	gcInterval := opts.GCInterval
	if gcInterval == 0 {
		gcInterval = 10 * time.Minute
	}

	gcDiscardRatio := opts.GCDiscardRatio
	if gcDiscardRatio == 0 {
		gcDiscardRatio = 0.5
	}

	memTableSize := opts.MemTableSize
	if memTableSize == 0 {
		memTableSize = 16 << 20 // 16MB default for quota
	}

	valueLogFileSize := opts.ValueLogFileSize
	if valueLogFileSize == 0 {
		valueLogFileSize = 32 << 20 // 32MB default
	}

	blockCacheSize := opts.BlockCacheSize
	if blockCacheSize == 0 {
		blockCacheSize = 32 << 20 // 32MB default for quota
	}

	// Create database path
	dbPath := filepath.Join(opts.DataPath, "quota")

	// Open BadgerDB with optimized settings for production workloads
	badgerOpts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable BadgerDB logging
		WithMemTableSize(memTableSize).
		WithValueLogFileSize(valueLogFileSize).
		WithNumMemtables(2).             // 2 memtables for quota operations
		WithNumLevelZeroTables(2).       // 2 L0 tables
		WithNumLevelZeroTablesStall(4).  // Stall threshold
		WithValueThreshold(1 << 10).     // 1KB threshold for value log
		WithCompression(options.Snappy). // Enable Snappy compression
		WithBlockCacheSize(blockCacheSize).
		WithCompactL0OnClose(true). // Compact on close
		WithSyncWrites(opts.SyncWrites)

	// The quota database gets the same recovery path as the metadata database,
	// through the shared helper: an unopenable directory is quarantined instead
	// of blocking startup, and a held lock is never quarantined.
	db, err := metadata.OpenBadgerWithRecovery(dbPath, metadata.CorruptedDatabaseRecoveryOptions{
		RecoverCorruptedDatabase:   opts.RecoverCorruptedDatabase,
		CorruptedDatabaseRetention: opts.CorruptedDatabaseRetention,
		OnCorruptedDatabase:        opts.OnCorruptedDatabase,
	}, func() (*badger.DB, error) {
		return badger.Open(badgerOpts)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	repo := &badgerDirectoryQuotaRepository{
		db:             db,
		gcInterval:     gcInterval,
		gcDiscardRatio: gcDiscardRatio,
		gcStopCh:       make(chan struct{}),
		closedCh:       make(chan struct{}),
	}

	// Start background GC
	repo.startGC()

	return repo, nil
}

// GetOrCreate retrieves directory quota or creates with defaults.
func (r *badgerDirectoryQuotaRepository) GetOrCreate(ctx context.Context, tenantID string, directoryPath string) (*core.DirectoryQuota, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if directoryPath == "" {
		return nil, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	var quota *core.DirectoryQuota

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(tenantID, directoryPath)

		// Try to get existing quota
		item, err := txn.Get(key)
		if err == nil {
			// Quota exists, deserialize it
			return item.Value(func(val []byte) error {
				quota = &core.DirectoryQuota{}
				return json.Unmarshal(val, quota)
			})
		}

		if err != badger.ErrKeyNotFound {
			return err
		}

		// Quota doesn't exist, create default
		now := time.Now()
		quota = &core.DirectoryQuota{
			DirectoryPath: directoryPath,
			CurrentCount:  0,
			MaxCount:      0, // Unlimited by default
			Enabled:       false,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		// Save default quota
		data, err := json.Marshal(quota)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get or create quota: %w", err)
	}

	return quota, nil
}

// Update updates directory quota atomically.
func (r *badgerDirectoryQuotaRepository) Update(ctx context.Context, tenantID string, quota *core.DirectoryQuota) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if quota == nil {
		return fmt.Errorf("quota cannot be nil: %w", core.ErrInvalidArgument)
	}

	if quota.DirectoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Update timestamp
	quota.UpdatedAt = time.Now()

	// Serialize quota
	data, err := json.Marshal(quota)
	if err != nil {
		return fmt.Errorf("failed to serialize quota: %w", err)
	}

	// Write to BadgerDB
	err = r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(tenantID, quota.DirectoryPath)
		return txn.Set(key, data)
	})

	if err != nil {
		return fmt.Errorf("failed to update quota: %w", err)
	}

	return nil
}

// IncrementCount atomically increments the file count.
func (r *badgerDirectoryQuotaRepository) IncrementCount(ctx context.Context, tenantID string, directoryPath string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(tenantID, directoryPath)

		// Get current quota
		item, err := txn.Get(key)
		if err != nil {
			// If quota doesn't exist, this should not happen
			// but we'll create a default one
			if err == badger.ErrKeyNotFound {
				now := time.Now()
				quota := &core.DirectoryQuota{
					DirectoryPath: directoryPath,
					CurrentCount:  1,
					MaxCount:      0,
					Enabled:       false,
					CreatedAt:     now,
					UpdatedAt:     now,
				}

				data, _ := json.Marshal(quota)
				return txn.Set(key, data)
			}
			return err
		}

		var quota *core.DirectoryQuota
		err = item.Value(func(val []byte) error {
			quota = &core.DirectoryQuota{}
			return json.Unmarshal(val, quota)
		})
		if err != nil {
			return err
		}

		// Increment count
		quota.CurrentCount++
		quota.UpdatedAt = time.Now()

		// Serialize and save
		data, err := json.Marshal(quota)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})

	if err != nil {
		return fmt.Errorf("failed to increment count: %w", err)
	}

	return nil
}

// DecrementCount atomically decrements the file count.
func (r *badgerDirectoryQuotaRepository) DecrementCount(ctx context.Context, tenantID string, directoryPath string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(tenantID, directoryPath)

		// Get current quota
		item, err := txn.Get(key)
		if err != nil {
			// If quota doesn't exist, nothing to decrement
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		var quota *core.DirectoryQuota
		err = item.Value(func(val []byte) error {
			quota = &core.DirectoryQuota{}
			return json.Unmarshal(val, quota)
		})
		if err != nil {
			return err
		}

		// Decrement count (don't go below 0)
		if quota.CurrentCount > 0 {
			quota.CurrentCount--
		}
		quota.UpdatedAt = time.Now()

		// Serialize and save
		data, err := json.Marshal(quota)
		if err != nil {
			return err
		}

		return txn.Set(key, data)
	})

	if err != nil {
		return fmt.Errorf("failed to decrement count: %w", err)
	}

	return nil
}

// GetAll returns all directory quotas for one tenant.
func (r *badgerDirectoryQuotaRepository) GetAll(ctx context.Context, tenantID string) ([]*core.DirectoryQuota, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	prefix := r.buildTenantPrefix(tenantID)
	quotas := make([]*core.DirectoryQuota, 0)
	err := r.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var quota core.DirectoryQuota
			if err := iterator.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &quota)
			}); err != nil {
				return err
			}
			quotaCopy := quota
			quotas = append(quotas, &quotaCopy)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list directory quotas: %w", err)
	}
	return quotas, nil
}

// Optimize triggers BadgerDB garbage collection to reclaim disk space.
func (r *badgerDirectoryQuotaRepository) Optimize(ctx context.Context) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Stop as soon as Close signals shutdown so the database handle is never
		// closed underneath an in-flight value-log GC.
		select {
		case <-r.gcStopCh:
			return nil
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

// Close closes the repository and releases resources. It is idempotent and safe
// to call concurrently with in-flight Optimize or GC runs.
//
// Close never holds mu while it waits for the GC goroutine: the GC goroutine's
// Optimize takes mu.RLock(), so holding the write lock across gcWg.Wait() would
// deadlock whenever a GC tick lands inside that window. The close work itself is
// serialized by closeOnce, and mu is only taken to publish the closed flag and to
// guard the close error.
func (r *badgerDirectoryQuotaRepository) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		// Stop the GC goroutine, then wait for the final run without holding mu.
		close(r.gcStopCh)
		r.gcWg.Wait()

		// Close BadgerDB
		err := r.db.Close()
		if err != nil {
			err = fmt.Errorf("failed to close BadgerDB: %w", err)
		}

		r.mu.Lock()
		r.closeErr = err
		r.mu.Unlock()

		close(r.closedCh)
	})

	// Every caller observes the first close outcome; concurrent callers wait for
	// the handle to be released instead of returning early.
	<-r.closedCh

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closeErr
}

// buildKey builds a BadgerDB key for a directory path.
func (r *badgerDirectoryQuotaRepository) buildKey(tenantID string, directoryPath string) []byte {
	return append(r.buildTenantPrefix(tenantID), directoryPath...)
}

func (r *badgerDirectoryQuotaRepository) buildTenantPrefix(tenantID string) []byte {
	encodedTenantID := base64.RawURLEncoding.EncodeToString([]byte(tenantID))
	return []byte(fmt.Sprintf("dirquota:%s:", encodedTenantID))
}

// startGC starts the background garbage collection goroutine.
func (r *badgerDirectoryQuotaRepository) startGC() {
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
func (r *badgerDirectoryQuotaRepository) runGC() {
	_ = r.Optimize(context.Background())
}
