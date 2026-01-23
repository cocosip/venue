package quota

import (
	"context"
	"fmt"

	"github.com/cocosip/venue/pkg/core"
)

// directoryQuotaManager implements DirectoryQuotaManager interface.
type directoryQuotaManager struct {
	repository core.DirectoryQuotaRepository
}

// NewDirectoryQuotaManager creates a new directory quota manager.
func NewDirectoryQuotaManager(repository core.DirectoryQuotaRepository) (core.DirectoryQuotaManager, error) {
	if repository == nil {
		return nil, fmt.Errorf("repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	return &directoryQuotaManager{
		repository: repository,
	}, nil
}

// CanAddFile checks if a file can be added to a directory without exceeding quota.
func (m *directoryQuotaManager) CanAddFile(ctx context.Context, tenantID string, directoryPath string) (bool, error) {
	if directoryPath == "" {
		return false, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get quota for directory
	quota, err := m.repository.GetOrCreate(ctx, directoryPath)
	if err != nil {
		return false, fmt.Errorf("failed to get quota: %w", err)
	}

	// Check if quota is enabled and can add file
	return quota.CanAddFile(), nil
}

// IncrementFileCount atomically increments the file count for a directory.
func (m *directoryQuotaManager) IncrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get current quota
	quota, err := m.repository.GetOrCreate(ctx, directoryPath)
	if err != nil {
		return fmt.Errorf("failed to get quota: %w", err)
	}

	// Check if we can add a file
	if !quota.CanAddFile() {
		return core.ErrDirectoryQuotaExceeded
	}

	// Increment count in repository
	if err := m.repository.IncrementCount(ctx, directoryPath); err != nil {
		return fmt.Errorf("failed to increment count: %w", err)
	}

	return nil
}

// DecrementFileCount atomically decrements the file count for a directory.
func (m *directoryQuotaManager) DecrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Decrement count in repository
	if err := m.repository.DecrementCount(ctx, directoryPath); err != nil {
		return fmt.Errorf("failed to decrement count: %w", err)
	}

	return nil
}

// GetFileCount returns the current file count for a directory.
func (m *directoryQuotaManager) GetFileCount(ctx context.Context, tenantID string, directoryPath string) (int, error) {
	if directoryPath == "" {
		return 0, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get quota
	quota, err := m.repository.GetOrCreate(ctx, directoryPath)
	if err != nil {
		return 0, fmt.Errorf("failed to get quota: %w", err)
	}

	return quota.CurrentCount, nil
}

// SetQuota sets the maximum file count for a directory (0 = unlimited).
func (m *directoryQuotaManager) SetQuota(ctx context.Context, tenantID string, directoryPath string, maxCount int) error {
	if directoryPath == "" {
		return fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	if maxCount < 0 {
		return fmt.Errorf("max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	// Get or create quota
	quota, err := m.repository.GetOrCreate(ctx, directoryPath)
	if err != nil {
		return fmt.Errorf("failed to get quota: %w", err)
	}

	// Update quota settings
	quota.MaxCount = maxCount
	quota.Enabled = maxCount > 0

	// Save updated quota
	if err := m.repository.Update(ctx, quota); err != nil {
		return fmt.Errorf("failed to update quota: %w", err)
	}

	return nil
}

// GetQuota returns the quota configuration for a directory.
func (m *directoryQuotaManager) GetQuota(ctx context.Context, tenantID string, directoryPath string) (*core.DirectoryQuota, error) {
	if directoryPath == "" {
		return nil, fmt.Errorf("directory path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Get quota
	quota, err := m.repository.GetOrCreate(ctx, directoryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get quota: %w", err)
	}

	return quota, nil
}
