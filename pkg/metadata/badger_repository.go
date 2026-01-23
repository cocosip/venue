package metadata

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

	// GCInterval is the interval for running BadgerDB garbage collection.
	// Default: 10 minutes
	GCInterval time.Duration

	// GCDiscardRatio is the discard ratio for GC (0.0 - 1.0).
	// Files with this ratio of outdated data will be rewritten.
	// Default: 0.5 (50%)
	GCDiscardRatio float64
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

	gcInterval := opts.GCInterval
	if gcInterval == 0 {
		gcInterval = 10 * time.Minute
	}

	gcDiscardRatio := opts.GCDiscardRatio
	if gcDiscardRatio == 0 {
		gcDiscardRatio = 0.5
	}

	// Create tenant-specific database path
	dbPath := filepath.Join(opts.DataPath, opts.TenantID, "metadata")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database path: %w", err)
	}

	// Open BadgerDB
	badgerOpts := badger.DefaultOptions(dbPath).
		WithLogger(nil). // Disable BadgerDB logging
		WithMemTableSize(1 << 20).                    // 1MB memtable (default is 64MB)
		WithValueLogFileSize(1 << 20).                // 1MB value log files (default is 1GB)
		WithNumMemtables(2).                          // Keep at least 2 memtables for smoother operation
		WithNumLevelZeroTables(2).                    // Keep at least 2 L0 tables
		WithNumLevelZeroTablesStall(4).               // Reasonable stall threshold
		WithValueThreshold(1 << 10).                  // 1KB threshold for value log
		WithCompression(0)                            // Disable compression for better performance
		// Note: Must keep conflict detection enabled for concurrent file allocation

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	// Create cache
	cache := newMetadataCache(cacheTTL)

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

	// Write to BadgerDB
	err = r.db.Update(func(txn *badger.Txn) error {
		key := r.buildKey(metadata.FileKey)
		return txn.Set(key, data)
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

// Delete removes file metadata.
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

	// Delete from BadgerDB
	err := r.db.Update(func(txn *badger.Txn) error {
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

// GetByStatus retrieves files by status with optional limit.
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

	// Scan BadgerDB
	var results []*core.FileMetadata
	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := r.buildPrefix()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				metadata := &core.FileMetadata{}
				if err := json.Unmarshal(val, metadata); err != nil {
					return err
				}

				if metadata.Status == status {
					results = append(results, metadata)
					// Check limit
					if limit > 0 && len(results) >= limit {
						return nil
					}
				}

				return nil
			})

			if err != nil {
				return err
			}

			// Break if limit reached
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
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := r.buildPrefix()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				metadata := &core.FileMetadata{}
				if err := json.Unmarshal(val, metadata); err != nil {
					return err
				}

				// Check if file is ready for processing
				if metadata.Status == core.FileStatusPending {
					// Check if available time has passed
					if metadata.AvailableForProcessingAt == nil || metadata.AvailableForProcessingAt.Before(now) {
						results = append(results, metadata)
						// Check limit
						if limit > 0 && len(results) >= limit {
							return nil
						}
					}
				}

				return nil
			})

			if err != nil {
				return err
			}

			// Break if limit reached
			if limit > 0 && len(results) >= limit {
				break
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get pending files: %w", err)
	}

	return results, nil
}

// UpdateStatus atomically updates file status.
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

	// Update in BadgerDB
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

		// Update status
		metadata.Status = newStatus

		// Serialize and save
		data, err := json.Marshal(metadata)
		if err != nil {
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
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := r.buildPrefix()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				metadata := &core.FileMetadata{}
				if err := json.Unmarshal(val, metadata); err != nil {
					return err
				}

				// Check if file is in Processing status and has timed out
				if metadata.Status == core.FileStatusProcessing {
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
				return err
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
