package metadata

import (
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// cacheEntry represents a cached metadata entry with expiration.
type cacheEntry struct {
	metadata  *core.FileMetadata
	expiresAt time.Time
}

// isExpired checks if the cache entry has expired.
func (e *cacheEntry) isExpired() bool {
	return time.Now().After(e.expiresAt)
}

// metadataCache is a TTL cache for file metadata.
// Only active files (Pending/Processing/Failed) are cached.
type metadataCache struct {
	entries map[string]*cacheEntry
	ttl     time.Duration
	mu      sync.RWMutex
}

// newMetadataCache creates a new metadata cache.
func newMetadataCache(ttl time.Duration) *metadataCache {
	return &metadataCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
}

// get retrieves a file metadata from the cache.
// Returns nil if not found or expired.
func (c *metadataCache) get(fileKey string) *core.FileMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[fileKey]
	if !exists {
		return nil
	}

	if entry.isExpired() {
		return nil
	}

	return entry.metadata
}

// set adds or updates a file metadata in the cache.
func (c *metadataCache) set(fileKey string, metadata *core.FileMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[fileKey] = &cacheEntry{
		metadata:  metadata,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// delete removes a file metadata from the cache.
func (c *metadataCache) delete(fileKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
			delete(c.entries, key)
			continue
		}

		if entry.metadata.Status == status {
			results = append(results, entry.metadata)
		}
	}

	return results
}

// cleanup removes all expired entries from the cache.
func (c *metadataCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.entries {
		if entry.expiresAt.Before(now) {
			delete(c.entries, key)
		}
	}
}

// clear removes all entries from the cache.
func (c *metadataCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*cacheEntry)
}

// getStats returns cache statistics for monitoring.
func (c *metadataCache) getStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_entries"] = len(c.entries)

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
