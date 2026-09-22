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
