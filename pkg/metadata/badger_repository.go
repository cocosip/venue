package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
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
}

// BadgerMetadataRepository implements MetadataRepository using BadgerDB.
type BadgerMetadataRepository struct {
	tenantID       string
	db             *badger.DB
	cache          *metadataCache
	gcInterval     time.Duration
	gcDiscardRatio float64
	gcStopCh       chan struct{}
	gcWg           sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
}

// NewBadgerMetadataRepository creates a new BadgerDB metadata repository.
func NewBadgerMetadataRepository(opts *BadgerRepositoryOptions) (core.MetadataRepository, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.TenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if opts.DataPath == "" {
		return nil, fmt.Errorf("data path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Set defaults
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

	gcDiscardRatio := opts.GCDiscardRatio
	if gcDiscardRatio == 0 {
		gcDiscardRatio = 0.5
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
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database path: %w", err)
	}

	// Open BadgerDB with optimized settings for production workloads
	// These settings balance memory usage, write performance, and durability
	badgerOpts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable BadgerDB logging
		WithMemTableSize(memTableSize).
		WithValueLogFileSize(valueLogFileSize).
		WithNumMemtables(3).                          // 3 memtables for smoother L0 flush
		WithNumLevelZeroTables(3).                    // 3 L0 tables before compaction
		WithNumLevelZeroTablesStall(6).               // Stall threshold to prevent too many L0 tables
		WithValueThreshold(1 << 10).                  // 1KB threshold for value log
		WithCompression(options.Snappy).              // Enable Snappy compression for better I/O
		WithBlockCacheSize(blockCacheSize).
		WithIndexCacheSize(32 << 20).                 // 32MB index cache
		WithNumCompactors(3).                         // 3 concurrent compactors
		WithCompactL0OnClose(true).                   // Compact L0 on close for faster restart
		WithSyncWrites(opts.SyncWrites)
		// Note: Must keep conflict detection enabled for concurrent file allocation

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// Create cache with size limit
	cache := newMetadataCacheWithSize(cacheTTL, maxCacheEntries)

	repo := &BadgerMetadataRepository{
		tenantID:       opts.TenantID,
		db:             db,
		cache:          cache,
		gcInterval:     gcInterval,
		gcDiscardRatio: gcDiscardRatio,
		gcStopCh:       make(chan struct{}),
	}

	// Start background GC
	repo.startGC()

	return repo, nil
}

// AddOrUpdate adds or updates file metadata atomically.
// Also maintains secondary indexes for efficient status-based queries.
func (r *BadgerMetadataRepository) AddOrUpdate(ctx context.Context, metadata *core.FileMetadata) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if metadata == nil {
		return fmt.Errorf("metadata cannot be nil: %w", core.ErrInvalidArgument)
	}

	if metadata.FileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Serialize metadata
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Write to BadgerDB with index maintenance
	err = r.db.Update(func(txn *badger.Txn) error {
		// Get old metadata to update indexes
		oldMetadata, _ := r.getMetadataInTxn(txn, metadata.FileKey)

		// Write primary data
		key := r.buildKey(metadata.FileKey)
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Update secondary indexes if status changed or this is a new file
		if oldMetadata == nil || oldMetadata.Status != metadata.Status ||
			!r.equalAvailableTime(oldMetadata.AvailableForProcessingAt, metadata.AvailableForProcessingAt) {
			// Delete old status index if exists
			if oldMetadata != nil {
				oldIndexKey := r.buildStatusIndexKey(oldMetadata)
				_ = txn.Delete(oldIndexKey)
			}
			// Add new status index
			newIndexKey := r.buildStatusIndexKey(metadata)
			if err := txn.Set(newIndexKey, []byte(metadata.FileKey)); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Cache if active
	if r.isActiveStatus(metadata.Status) {
		r.cache.set(metadata.FileKey, metadata)
	} else {
		// Remove from cache if no longer active
		r.cache.delete(metadata.FileKey)
	}

	return nil
}

// AddOrUpdateBatch adds or updates multiple file metadata atomically in a single transaction.
// More efficient than calling AddOrUpdate multiple times for bulk operations.
func (r *BadgerMetadataRepository) AddOrUpdateBatch(ctx context.Context, metadata []*core.FileMetadata) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if len(metadata) == 0 {
		return nil // Nothing to do
	}

	// Perform batch update in a single transaction
	err := r.db.Update(func(txn *badger.Txn) error {
		for _, m := range metadata {
			if m == nil || m.FileKey == "" {
				continue // Skip invalid entries
			}

			// Serialize metadata
			data, err := json.Marshal(m)
			if err != nil {
				return fmt.Errorf("failed to serialize metadata for %s: %w", m.FileKey, err)
			}

			// Get old metadata to update indexes
			oldMetadata, _ := r.getMetadataInTxn(txn, m.FileKey)

			// Write primary data
			key := r.buildKey(m.FileKey)
			if err := txn.Set(key, data); err != nil {
				return err
			}

			// Update secondary indexes if status changed or this is a new file
			if oldMetadata == nil || oldMetadata.Status != m.Status ||
				!r.equalAvailableTime(oldMetadata.AvailableForProcessingAt, m.AvailableForProcessingAt) {
				// Delete old status index if exists
				if oldMetadata != nil {
					oldIndexKey := r.buildStatusIndexKey(oldMetadata)
					_ = txn.Delete(oldIndexKey)
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

	if err != nil {
		return fmt.Errorf("failed to batch update metadata: %w", err)
	}

	// Update cache after successful transaction
	for _, m := range metadata {
		if m == nil || m.FileKey == "" {
			continue
		}
		if r.isActiveStatus(m.Status) {
			r.cache.set(m.FileKey, m)
		} else {
			r.cache.delete(m.FileKey)
		}
	}

	return nil
}

// Get retrieves file metadata by key.
func (r *BadgerMetadataRepository) Get(ctx context.Context, fileKey string) (*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check cache first
	if cached := r.cache.get(fileKey); cached != nil {
		return cached, nil
	}

	// Read from BadgerDB
	var metadata *core.FileMetadata
	err := r.db.View(func(txn *badger.Txn) error {
		key := r.buildKey(fileKey)
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
		r.cache.set(fileKey, metadata)
	}

	return metadata, nil
}

// Delete removes file metadata and its secondary indexes.
func (r *BadgerMetadataRepository) Delete(ctx context.Context, fileKey string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Delete from BadgerDB including indexes
	err := r.db.Update(func(txn *badger.Txn) error {
		// Get metadata first to delete index
		metadata, _ := r.getMetadataInTxn(txn, fileKey)
		if metadata != nil {
			indexKey := r.buildStatusIndexKey(metadata)
			_ = txn.Delete(indexKey)
		}

		// Delete primary data
		key := r.buildKey(fileKey)
		return txn.Delete(key)
	})

	if err != nil {
		return fmt.Errorf("failed to delete metadata: %w", err)
	}

	// Remove from cache
	r.cache.delete(fileKey)

	return nil
}

// DeleteBatch removes multiple file metadata atomically in a single transaction.
func (r *BadgerMetadataRepository) DeleteBatch(ctx context.Context, fileKeys []string) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if len(fileKeys) == 0 {
		return nil // Nothing to do
	}

	// Perform batch delete in a single transaction
	err := r.db.Update(func(txn *badger.Txn) error {
		for _, fileKey := range fileKeys {
			if fileKey == "" {
				continue
			}

			// Get metadata first to delete index
			metadata, _ := r.getMetadataInTxn(txn, fileKey)
			if metadata != nil {
				indexKey := r.buildStatusIndexKey(metadata)
				_ = txn.Delete(indexKey)
			}

			// Delete primary data
			key := r.buildKey(fileKey)
			_ = txn.Delete(key)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to batch delete metadata: %w", err)
	}

	// Remove from cache
	for _, fileKey := range fileKeys {
		r.cache.delete(fileKey)
	}

	return nil
}

// GetByStatus retrieves files by status with optional limit.
// Uses secondary index for O(log n) lookup instead of O(n) full scan.
// tenantID parameter is ignored since this repository is tenant-specific.
func (r *BadgerMetadataRepository) GetByStatus(ctx context.Context, tenantID string, status core.FileProcessingStatus, limit int) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	// For active statuses, try cache first
	if r.isActiveStatus(status) {
		cachedResults := r.cache.listByStatus(status)
		if len(cachedResults) > 0 {
			// Apply limit
			if limit > 0 && len(cachedResults) > limit {
				return cachedResults[:limit], nil
			}
			return cachedResults, nil
		}
	}

	// Use secondary index for efficient lookup
	var results []*core.FileMetadata
	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan status index: idx:status:{status}:{availableTime}:{fileKey}
		prefix := r.buildStatusIndexPrefix(status)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Extract fileKey from index key
			indexKey := string(it.Item().Key())
			fileKey := r.extractFileKeyFromIndex(indexKey)

			// Get actual metadata
			metadata, err := r.getMetadataInTxn(txn, fileKey)
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
		return nil, fmt.Errorf("failed to get files by status: %w", err)
	}

	return results, nil
}

// GetPendingFiles retrieves files ready for processing.
// Uses secondary index for efficient O(log n) lookup.
// tenantID parameter is ignored since this repository is tenant-specific.
func (r *BadgerMetadataRepository) GetPendingFiles(ctx context.Context, tenantID string, limit int) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	now := time.Now()
	var results []*core.FileMetadata

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan Pending status index: idx:status:0:{availableTime}:{fileKey}
		prefix := r.buildStatusIndexPrefix(core.FileStatusPending)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Extract fileKey from index
			indexKey := string(it.Item().Key())
			fileKey := r.extractFileKeyFromIndex(indexKey)
			if fileKey == "" {
				continue
			}

			// Get actual metadata to verify availability
			metadata, err := r.getMetadataInTxn(txn, fileKey)
			if err != nil {
				continue // Skip if file not found
			}

			// Check if file is ready for processing
			if metadata.AvailableForProcessingAt == nil || metadata.AvailableForProcessingAt.Before(now) {
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
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	return results, nil
}

// UpdateStatus atomically updates file status and maintains secondary indexes.
func (r *BadgerMetadataRepository) UpdateStatus(ctx context.Context, fileKey string, newStatus core.FileProcessingStatus) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if fileKey == "" {
		return fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Update in BadgerDB with index maintenance
	var updatedMetadata *core.FileMetadata
	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(fileKey)

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

		// Delete old status index if status changed
		if metadata.Status != newStatus {
			oldIndexKey := r.buildStatusIndexKey(metadata)
			_ = txn.Delete(oldIndexKey)
		}

		// Update status
		metadata.Status = newStatus

		// Serialize and save
		data, err := json.Marshal(metadata)
		if err != nil {
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

	if err != nil {
		// Don't wrap ErrFileNotFound
		if err == core.ErrFileNotFound {
			return err
		}
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Update cache
	if r.isActiveStatus(newStatus) {
		r.cache.set(fileKey, updatedMetadata)
	} else {
		r.cache.delete(fileKey)
	}

	return nil
}

// CompareAndTransitionToProcessing atomically transitions a file to Processing status
// if and only if it is currently in Pending status.
// Also updates secondary indexes.
func (r *BadgerMetadataRepository) CompareAndTransitionToProcessing(ctx context.Context, fileKey string) (*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	if fileKey == "" {
		return nil, fmt.Errorf("file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	var updatedMetadata *core.FileMetadata

	// Perform atomic compare-and-swap in a transaction
	err := r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(fileKey)

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
			return fmt.Errorf("file is not in pending status: current status is %s", metadata.Status.String())
		}

		// Check if available for processing
		if metadata.AvailableForProcessingAt != nil && time.Now().Before(*metadata.AvailableForProcessingAt) {
			return fmt.Errorf("file is not yet available for processing")
		}

		// Delete old Pending status index
		oldIndexKey := r.buildStatusIndexKey(metadata)
		_ = txn.Delete(oldIndexKey)

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

	if err != nil {
		// Don't wrap known errors
		if err == core.ErrFileNotFound {
			return nil, err
		}
		return nil, fmt.Errorf("failed to transition file to processing: %w", err)
	}

	// Update cache with a copy
	r.cache.set(fileKey, updatedMetadata)

	// Return a copy to prevent concurrent modification
	result := *updatedMetadata
	return &result, nil
}

// GetTimedOutProcessingFiles retrieves files in Processing status that exceed timeout.
// Uses secondary index for efficient lookup.
func (r *BadgerMetadataRepository) GetTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) ([]*core.FileMetadata, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("repository is closed")
	}
	r.mu.RUnlock()

	now := time.Now()
	var results []*core.FileMetadata

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys from index
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan Processing status index
		prefix := r.buildStatusIndexPrefix(core.FileStatusProcessing)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			// Extract fileKey from index
			indexKey := string(it.Item().Key())
			fileKey := r.extractFileKeyFromIndex(indexKey)

			// Get actual metadata
			metadata, err := r.getMetadataInTxn(txn, fileKey)
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
		return nil, fmt.Errorf("failed to get timed out files: %w", err)
	}

	return results, nil
}

// Close closes the repository and releases resources.
func (r *BadgerMetadataRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	// Stop GC
	close(r.gcStopCh)
	r.gcWg.Wait()

	// Close BadgerDB with timeout to prevent indefinite hangs
	// Use a channel to implement timeout
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- r.db.Close()
	}()

	// Wait for close with 30 second timeout
	select {
	case err := <-closeDone:
		if err != nil {
			return fmt.Errorf("failed to close BadgerDB: %w", err)
		}
		return nil
	case <-time.After(30 * time.Second):
		// Force return after timeout
		// Note: This may leak goroutines, but prevents test hangs
		return fmt.Errorf("BadgerDB close timed out after 30 seconds")
	}
}

// buildKey builds a BadgerDB key for a file key.
func (r *BadgerMetadataRepository) buildKey(fileKey string) []byte {
	return []byte(fmt.Sprintf("file:%s", fileKey))
}

// buildPrefix builds the prefix for all file metadata keys.
func (r *BadgerMetadataRepository) buildPrefix() []byte {
	return []byte("file:")
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

// GetCacheStats returns cache statistics for monitoring.
func (r *BadgerMetadataRepository) GetCacheStats() map[string]interface{} {
	return r.cache.getStats()
}


// getMetadataInTxn retrieves metadata within a transaction (no locking).
func (r *BadgerMetadataRepository) getMetadataInTxn(txn *badger.Txn, fileKey string) (*core.FileMetadata, error) {
	key := r.buildKey(fileKey)
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
// Format: idx:status:{status}:{availableTime}:{fileKey}
// This allows efficient querying by status and sorting by available time.
func (r *BadgerMetadataRepository) buildStatusIndexKey(metadata *core.FileMetadata) []byte {
	var availableTimeStr string
	if metadata.AvailableForProcessingAt != nil {
		availableTimeStr = metadata.AvailableForProcessingAt.Format(time.RFC3339Nano)
	} else {
		availableTimeStr = "0000-00-00T00:00:00Z" // Sort nil times first
	}
	return []byte(fmt.Sprintf("idx:status:%d:%s:%s", metadata.Status, availableTimeStr, metadata.FileKey))
}

// buildStatusIndexPrefix builds the prefix for scanning by status.
func (r *BadgerMetadataRepository) buildStatusIndexPrefix(status core.FileProcessingStatus) []byte {
	return []byte(fmt.Sprintf("idx:status:%d:", status))
}

// extractFileKeyFromIndex extracts the fileKey from an index key.
// Index format: idx:status:{status}:{availableTime}:{fileKey}
// The availableTime is in RFC3339 format which may contain colons.
// We find the last colon, everything after is the fileKey.
func (r *BadgerMetadataRepository) extractFileKeyFromIndex(indexKey string) string {
	// Find the last colon - everything after it is the fileKey
	lastColon := -1
	for i := len(indexKey) - 1; i >= 0; i-- {
		if indexKey[i] == ':' {
			lastColon = i
			break
		}
	}
	if lastColon >= 0 && lastColon+1 < len(indexKey) {
		return indexKey[lastColon+1:]
	}
	return ""
}

// parsePendingIndexKey parses a Pending status index key.
// Returns availableTime and fileKey.
func (r *BadgerMetadataRepository) parsePendingIndexKey(indexKey string) (*time.Time, string, error) {
	// Format: idx:status:0:{availableTime}:{fileKey}
	const prefix = "idx:status:0:"
	if !strings.HasPrefix(indexKey, prefix) {
		return nil, "", fmt.Errorf("invalid pending index key")
	}

	// Extract available time and fileKey
	remaining := indexKey[len(prefix):]
	// Find the separator between availableTime and fileKey (last colon might be in fileKey)
	// Format: 0000-00-00T00:00:00Z:fileKey
	idx := strings.Index(remaining, ":")
	if idx == -1 {
		return nil, "", fmt.Errorf("invalid index key format")
	}

	timeStr := remaining[:idx]
	fileKey := remaining[idx+1:]

	// Parse time
	var availableTime *time.Time
	if timeStr != "0000-00-00T00:00:00Z" {
		t, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			// Try without nano
			t, err = time.Parse(time.RFC3339, timeStr)
			if err != nil {
				return nil, "", err
			}
		}
		availableTime = &t
	}

	return availableTime, fileKey, nil
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
