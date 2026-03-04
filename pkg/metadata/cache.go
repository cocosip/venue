package metadata

import (
	"container/list"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// lruCacheEntry represents a cached metadata entry with expiration and LRU position.
type lruCacheEntry struct {
	metadata  *core.FileMetadata
	expiresAt time.Time
	listElem  *list.Element // Position in LRU list
}

// isExpired checks if the cache entry has expired.
func (e *lruCacheEntry) isExpired() bool {
	return time.Now().After(e.expiresAt)
}

// metadataCache is a TTL + LRU cache for file metadata.
// Only active files (Pending/Processing/Failed) are cached.
// When cache reaches maxSize, least recently used entries are evicted.
type metadataCache struct {
	entries map[string]*lruCacheEntry
	ttl     time.Duration
	maxSize int
	mu      sync.RWMutex
	lruList *list.List // Front = most recent, Back = least recent
}

// newMetadataCacheWithSize creates a new metadata cache with specified max size.
func newMetadataCacheWithSize(ttl time.Duration, maxSize int) *metadataCache {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &metadataCache{
		entries: make(map[string]*lruCacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
		lruList: list.New(),
	}
}

// get retrieves a file metadata from the cache.
// Returns nil if not found or expired.
// Returns a copy to prevent concurrent modification.
// Updates LRU position on hit.
func (c *metadataCache) get(fileKey string) *core.FileMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[fileKey]
	if !exists {
		return nil
	}

	if entry.isExpired() {
		// Remove expired entry
		c.removeEntry(fileKey)
		return nil
	}

	// Update LRU position (move to front)
	c.lruList.MoveToFront(entry.listElem)

	// Return a copy to avoid data races when multiple goroutines access the same cached object
	metadataCopy := *entry.metadata
	return &metadataCopy
}

// set adds or updates a file metadata in the cache.
// Stores a copy to prevent concurrent modification.
// Evicts oldest entries if cache is at capacity.
func (c *metadataCache) set(fileKey string, metadata *core.FileMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if entry already exists
	if existing, exists := c.entries[fileKey]; exists {
		// Update existing entry
		metadataCopy := *metadata
		existing.metadata = &metadataCopy
		existing.expiresAt = time.Now().Add(c.ttl)
		c.lruList.MoveToFront(existing.listElem)
		return
	}

	// Evict oldest entries if at capacity
	for len(c.entries) >= c.maxSize {
		c.evictLRU()
	}

	// Store a copy to avoid data races when the caller modifies the original
	metadataCopy := *metadata
	entry := &lruCacheEntry{
		metadata:  &metadataCopy,
		expiresAt: time.Now().Add(c.ttl),
	}

	// Add to front of LRU list (most recent)
	entry.listElem = c.lruList.PushFront(fileKey)
	c.entries[fileKey] = entry
}

// delete removes a file metadata from the cache.
func (c *metadataCache) delete(fileKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeEntry(fileKey)
}

// removeEntry removes an entry from both map and LRU list (must hold lock).
func (c *metadataCache) removeEntry(fileKey string) {
	if entry, exists := c.entries[fileKey]; exists {
		c.lruList.Remove(entry.listElem)
		delete(c.entries, fileKey)
	}
}

// evictLRU removes the least recently used entry (must hold lock).
func (c *metadataCache) evictLRU() {
	// Get oldest element from back of list
	elem := c.lruList.Back()
	if elem == nil {
		return
	}

	fileKey := elem.Value.(string)
	c.lruList.Remove(elem)
	delete(c.entries, fileKey)
}

// listByStatus returns all cached metadata with the specified status.
// Expired entries are automatically removed.
func (c *metadataCache) listByStatus(status core.FileProcessingStatus) []*core.FileMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()

	var results []*core.FileMetadata
	now := time.Now()

	// Collect results and remove expired entries
	for key, entry := range c.entries {
		if entry.expiresAt.Before(now) {
			// Expired, remove it
			c.lruList.Remove(entry.listElem)
			delete(c.entries, key)
			continue
		}

		if entry.metadata.Status == status {
			results = append(results, entry.metadata)
		}
	}

	return results
}

// getStats returns cache statistics for monitoring.
func (c *metadataCache) getStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_entries"] = len(c.entries)
	stats["max_size"] = c.maxSize
	stats["usage_percent"] = float64(len(c.entries)) * 100.0 / float64(c.maxSize)

	// Count by status
	statusCounts := make(map[core.FileProcessingStatus]int)
	expiredCount := 0
	now := time.Now()

	for _, entry := range c.entries {
		if entry.expiresAt.Before(now) {
			expiredCount++
		} else {
			statusCounts[entry.metadata.Status]++
		}
	}

	stats["expired_entries"] = expiredCount
	stats["pending_count"] = statusCounts[core.FileStatusPending]
	stats["processing_count"] = statusCounts[core.FileStatusProcessing]
	stats["failed_count"] = statusCounts[core.FileStatusFailed]

	return stats
}
