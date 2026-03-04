package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
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
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database path: %w", err)
	}

	// Open BadgerDB with optimized settings for production workloads
	badgerOpts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable BadgerDB logging
		WithMemTableSize(memTableSize).
		WithValueLogFileSize(valueLogFileSize).
		WithNumMemtables(2).                          // 2 memtables for quota operations
		WithNumLevelZeroTables(2).                    // 2 L0 tables
		WithNumLevelZeroTablesStall(4).               // Stall threshold
		WithValueThreshold(1 << 10).                  // 1KB threshold for value log
		WithCompression(options.Snappy).              // Enable Snappy compression
		WithBlockCacheSize(blockCacheSize).
		WithCompactL0OnClose(true).                   // Compact on close
		WithSyncWrites(opts.SyncWrites)

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	repo := &badgerDirectoryQuotaRepository{
		db:             db,
		gcInterval:     gcInterval,
		gcDiscardRatio: gcDiscardRatio,
		gcStopCh:       make(chan struct{}),
	}

	// Start background GC
	repo.startGC()

	return repo, nil
}

// GetOrCreate retrieves directory quota or creates with defaults.
func (r *badgerDirectoryQuotaRepository) GetOrCreate(ctx context.Context, directoryPath string) (*core.DirectoryQuota, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if directoryPath == "" {
		return nil, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	var quota *core.DirectoryQuota

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(directoryPath)

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
func (r *badgerDirectoryQuotaRepository) Update(ctx context.Context, quota *core.DirectoryQuota) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

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
		key := r.buildKey(quota.DirectoryPath)
		return txn.Set(key, data)
	})

	if err != nil {
		return fmt.Errorf("failed to update quota: %w", err)
	}

	return nil
}

// IncrementCount atomically increments the file count.
func (r *badgerDirectoryQuotaRepository) IncrementCount(ctx context.Context, directoryPath string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(directoryPath)

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
func (r *badgerDirectoryQuotaRepository) DecrementCount(ctx context.Context, directoryPath string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(directoryPath)

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

// Close closes the repository and releases resources.
func (r *badgerDirectoryQuotaRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	// Stop GC
	close(r.gcStopCh)
	r.gcWg.Wait()

	// Close BadgerDB
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}

	return nil
}

// buildKey builds a BadgerDB key for a directory path.
func (r *badgerDirectoryQuotaRepository) buildKey(directoryPath string) []byte {
	return []byte(fmt.Sprintf("dirquota:%s", directoryPath))
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
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return
	}
	r.mu.RUnlock()

	// Run GC until no more rewriting is needed
	for {
		err := r.db.RunValueLogGC(r.gcDiscardRatio)
		if err != nil {
			// No more GC needed or error occurred
			break
		}
	}
}
