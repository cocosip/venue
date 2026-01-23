package core

import (
	"testing"
	"time"
)

func TestTenantContext_IsEnabled(t *testing.T) {
	tests := []struct {
		name   string
		status TenantStatus
		want   bool
	}{
		{
			name:   "enabled tenant",
			status: TenantStatusEnabled,
			want:   true,
		},
		{
			name:   "disabled tenant",
			status: TenantStatusDisabled,
			want:   false,
		},
		{
			name:   "suspended tenant",
			status: TenantStatusSuspended,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := TenantContext{
				ID:     "test-tenant",
				Status: tt.status,
			}
			if got := tc.IsEnabled(); got != tt.want {
				t.Errorf("TenantContext.IsEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTenantStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status TenantStatus
		want   string
	}{
		{"enabled", TenantStatusEnabled, "Enabled"},
		{"disabled", TenantStatusDisabled, "Disabled"},
		{"suspended", TenantStatusSuspended, "Suspended"},
		{"unknown", TenantStatus(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("TenantStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileProcessingStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status FileProcessingStatus
		want   string
	}{
		{"pending", FileStatusPending, "Pending"},
		{"processing", FileStatusProcessing, "Processing"},
		{"completed", FileStatusCompleted, "Completed"},
		{"failed", FileStatusFailed, "Failed"},
		{"permanently failed", FileStatusPermanentlyFailed, "PermanentlyFailed"},
		{"unknown", FileProcessingStatus(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("FileProcessingStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileMetadata_ToFileLocation(t *testing.T) {
	now := time.Now()
	metadata := &FileMetadata{
		FileKey:          "test-key",
		TenantID:         "test-tenant",
		VolumeID:         "vol-001",
		PhysicalPath:     "/path/to/file",
		FileSize:         1024,
		FileExtension:    ".txt",
		OriginalFileName: "test.txt",
		Status:           FileStatusPending,
		RetryCount:       0,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	location := metadata.ToFileLocation()

	if location.FileKey != metadata.FileKey {
		t.Errorf("FileKey mismatch: got %v, want %v", location.FileKey, metadata.FileKey)
	}
	if location.TenantID != metadata.TenantID {
		t.Errorf("TenantID mismatch: got %v, want %v", location.TenantID, metadata.TenantID)
	}
	if location.Status != metadata.Status {
		t.Errorf("Status mismatch: got %v, want %v", location.Status, metadata.Status)
	}
}

func TestFileMetadata_ToFileInfo(t *testing.T) {
	now := time.Now()
	metadata := &FileMetadata{
		FileKey:   "test-key",
		FileSize:  1024,
		Status:    FileStatusPending,
		CreatedAt: now,
	}

	info := metadata.ToFileInfo()

	if info.FileKey != metadata.FileKey {
		t.Errorf("FileKey mismatch: got %v, want %v", info.FileKey, metadata.FileKey)
	}
	if info.FileSize != metadata.FileSize {
		t.Errorf("FileSize mismatch: got %v, want %v", info.FileSize, metadata.FileSize)
	}
	if info.Status != metadata.Status {
		t.Errorf("Status mismatch: got %v, want %v", info.Status, metadata.Status)
	}
}

func TestDirectoryQuota_IsUnlimited(t *testing.T) {
	tests := []struct {
		name     string
		maxCount int
		want     bool
	}{
		{"unlimited", 0, true},
		{"limited", 100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &DirectoryQuota{MaxCount: tt.maxCount}
			if got := q.IsUnlimited(); got != tt.want {
				t.Errorf("DirectoryQuota.IsUnlimited() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDirectoryQuota_CanAddFile(t *testing.T) {
	tests := []struct {
		name         string
		currentCount int
		maxCount     int
		enabled      bool
		want         bool
	}{
		{"unlimited", 100, 0, true, true},
		{"disabled", 100, 50, false, true},
		{"within limit", 50, 100, true, true},
		{"at limit", 100, 100, true, false},
		{"over limit", 101, 100, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &DirectoryQuota{
				CurrentCount: tt.currentCount,
				MaxCount:     tt.maxCount,
				Enabled:      tt.enabled,
			}
			if got := q.CanAddFile(); got != tt.want {
				t.Errorf("DirectoryQuota.CanAddFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileRetryPolicy_CalculateRetryDelay(t *testing.T) {
	policy := DefaultFileRetryPolicy()

	tests := []struct {
		name       string
		retryCount int
		want       time.Duration
	}{
		{"first retry", 1, 5 * time.Second},
		{"second retry", 2, 10 * time.Second},
		{"third retry", 3, 20 * time.Second},
		{"fourth retry", 4, 40 * time.Second},
		{"capped at max", 10, 5 * time.Minute}, // Should be capped
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := policy.CalculateRetryDelay(tt.retryCount)
			if got != tt.want {
				t.Errorf("CalculateRetryDelay(%d) = %v, want %v", tt.retryCount, got, tt.want)
			}
		})
	}
}

func TestTenantMetadata_ToTenantContext(t *testing.T) {
	now := time.Now()
	metadata := &TenantMetadata{
		TenantID:  "test-tenant",
		Status:    TenantStatusEnabled,
		CreatedAt: now,
	}

	ctx := metadata.ToTenantContext()

	if ctx.ID != metadata.TenantID {
		t.Errorf("ID mismatch: got %v, want %v", ctx.ID, metadata.TenantID)
	}
	if ctx.Status != metadata.Status {
		t.Errorf("Status mismatch: got %v, want %v", ctx.Status, metadata.Status)
	}
	if ctx.CreatedAt != metadata.CreatedAt {
		t.Errorf("CreatedAt mismatch: got %v, want %v", ctx.CreatedAt, metadata.CreatedAt)
	}
}
