package metadata

import (
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestMetadataCacheRejectsStalePublish verifies that a late publish of an older
// revision cannot overwrite newer state. Cache writes happen after the database
// transaction commits, so concurrent writers can otherwise republish stale
// metadata within the TTL window.
func TestMetadataCacheRejectsStalePublish(t *testing.T) {
	base := time.Date(2026, time.June, 7, 8, 9, 10, 0, time.UTC)

	tests := []struct {
		name       string
		first      *core.FileMetadata
		second     *core.FileMetadata
		wantStatus core.FileProcessingStatus
		wantRetry  int
	}{
		{
			name: "older record cannot overwrite newer state",
			first: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusCompleted,
				RetryCount: 3, UpdatedAt: base.Add(time.Minute),
			},
			second: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusProcessing,
				RetryCount: 1, UpdatedAt: base,
			},
			wantStatus: core.FileStatusCompleted,
			wantRetry:  3,
		},
		{
			name: "newer record replaces older state",
			first: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusProcessing,
				RetryCount: 1, UpdatedAt: base,
			},
			second: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusCompleted,
				RetryCount: 3, UpdatedAt: base.Add(time.Minute),
			},
			wantStatus: core.FileStatusCompleted,
			wantRetry:  3,
		},
		{
			name: "same revision still publishes the incoming record",
			first: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusProcessing,
				UpdatedAt: base,
			},
			second: &core.FileMetadata{
				FileKey: "file", TenantID: "tenant", Status: core.FileStatusCompleted,
				UpdatedAt: base,
			},
			wantStatus: core.FileStatusCompleted,
			wantRetry:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newMetadataCacheWithSize(time.Minute, 10)

			cache.set(tt.first)
			cache.set(tt.second)

			got := cache.get("tenant", "file")
			if got == nil {
				t.Fatal("cache entry missing")
			}
			if got.Status != tt.wantStatus || got.RetryCount != tt.wantRetry {
				t.Errorf("cached record = status %s retry %d, want status %s retry %d",
					got.Status, got.RetryCount, tt.wantStatus, tt.wantRetry)
			}
		})
	}
}

// TestMetadataCacheEvictsLeastRecentlyUsedEntries keeps the cache bounded: a
// store with more active records than the configured capacity must evict, and a
// read must refresh recency so the hottest entry survives.
func TestMetadataCacheEvictsLeastRecentlyUsedEntries(t *testing.T) {
	base := time.Date(2026, time.June, 7, 8, 9, 10, 0, time.UTC)
	entry := func(fileKey string, offset time.Duration) *core.FileMetadata {
		return &core.FileMetadata{
			FileKey: fileKey, TenantID: "tenant", Status: core.FileStatusPending,
			UpdatedAt: base.Add(offset),
		}
	}

	cache := newMetadataCacheWithSize(time.Minute, 2)
	cache.set(entry("first", 0))
	cache.set(entry("second", time.Second))

	// A read makes "first" the most recently used entry, so the third insert
	// must evict "second" instead.
	if cached := cache.get("tenant", "first"); cached == nil {
		t.Fatal("cache entry first missing before eviction")
	}
	cache.set(entry("third", 2*time.Second))

	if cached := cache.get("tenant", "first"); cached == nil {
		t.Error("recently read entry was evicted")
	}
	if cached := cache.get("tenant", "third"); cached == nil {
		t.Error("newest entry is missing")
	}
	if cached := cache.get("tenant", "second"); cached != nil {
		t.Errorf("least recently used entry = %+v, want it evicted", cached)
	}
	if size := len(cache.entries); size != 2 {
		t.Fatalf("cache holds %d entries, want the configured maximum 2", size)
	}
}

// TestMetadataCacheExpiresEntries keeps the TTL contract: an entry older than
// the TTL is not served and is removed on the read that observes it.
func TestMetadataCacheExpiresEntries(t *testing.T) {
	cache := newMetadataCacheWithSize(time.Millisecond, 10)
	cache.set(&core.FileMetadata{FileKey: "file", TenantID: "tenant", Status: core.FileStatusPending})

	time.Sleep(5 * time.Millisecond)

	if cached := cache.get("tenant", "file"); cached != nil {
		t.Fatalf("cache entry = %+v, want an expired entry to be invisible", cached)
	}
	if size := len(cache.entries); size != 0 {
		t.Fatalf("cache holds %d entries, want the expired entry removed", size)
	}
}

// TestMetadataCacheScopesEntriesByTenantAndKey keeps the cache keyed by
// (tenantID, fileKey), so one tenant can never observe another tenant's record
// under the same file key.
func TestMetadataCacheScopesEntriesByTenantAndKey(t *testing.T) {
	cache := newMetadataCacheWithSize(time.Minute, 10)
	cache.set(&core.FileMetadata{FileKey: "shared", TenantID: "tenant-a", Status: core.FileStatusPending, RetryCount: 1})
	cache.set(&core.FileMetadata{FileKey: "shared", TenantID: "tenant-b", Status: core.FileStatusPending, RetryCount: 2})

	for _, testCase := range []struct {
		tenantID  string
		wantRetry int
	}{
		{tenantID: "tenant-a", wantRetry: 1},
		{tenantID: "tenant-b", wantRetry: 2},
	} {
		cached := cache.get(testCase.tenantID, "shared")
		if cached == nil {
			t.Fatalf("cache entry for %q missing", testCase.tenantID)
		}
		if cached.RetryCount != testCase.wantRetry || cached.TenantID != testCase.tenantID {
			t.Fatalf("cache entry for %q = %+v, want retry %d", testCase.tenantID, cached, testCase.wantRetry)
		}
	}

	cache.delete("tenant-a", "shared")
	if cached := cache.get("tenant-a", "shared"); cached != nil {
		t.Fatalf("deleted entry = %+v, want nil", cached)
	}
	if cached := cache.get("tenant-b", "shared"); cached == nil {
		t.Fatal("deleting one tenant's entry removed another tenant's entry")
	}
}
