package quota

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cocosip/venue/internal/directorypath"
	"github.com/cocosip/venue/pkg/core"
)

// directoryQuotaManager implements DirectoryQuotaManager interface.
type directoryQuotaManager struct {
	repository core.DirectoryQuotaRepository
	locks      [256]sync.Mutex
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
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return false, err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	// Get quota for directory
	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return false, fmt.Errorf("failed to get quota: %w", err)
	}

	// Check if quota is enabled and can add file
	return quota.CanAddFile(), nil
}

// IncrementFileCount atomically increments the file count for a directory.
func (m *directoryQuotaManager) IncrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	// Get current quota
	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return fmt.Errorf("failed to get quota: %w", err)
	}

	// Check if we can add a file
	if !quota.CanAddFile() {
		return core.ErrDirectoryQuotaExceeded
	}

	// Increment count in repository
	if err := m.repository.IncrementCount(ctx, tenantID, normalizedPath); err != nil {
		return fmt.Errorf("failed to increment count: %w", err)
	}

	return nil
}

// DecrementFileCount atomically decrements the file count for a directory.
func (m *directoryQuotaManager) DecrementFileCount(ctx context.Context, tenantID string, directoryPath string) error {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	// Decrement count in repository
	if err := m.repository.DecrementCount(ctx, tenantID, normalizedPath); err != nil {
		return fmt.Errorf("failed to decrement count: %w", err)
	}

	return nil
}

// GetFileCount returns the current file count for a directory.
func (m *directoryQuotaManager) GetFileCount(ctx context.Context, tenantID string, directoryPath string) (int, error) {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return 0, err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	// Get quota
	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return 0, fmt.Errorf("failed to get quota: %w", err)
	}

	return quota.CurrentCount, nil
}

// SetQuota sets the maximum file count for a directory (0 = unlimited).
func (m *directoryQuotaManager) SetQuota(ctx context.Context, tenantID string, directoryPath string, maxCount int) error {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	if maxCount < 0 {
		return fmt.Errorf("max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	// Get or create quota
	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return fmt.Errorf("failed to get quota: %w", err)
	}

	// Update quota settings
	quota.MaxCount = maxCount
	quota.Enabled = maxCount > 0

	// Save updated quota
	if err := m.repository.Update(ctx, tenantID, quota); err != nil {
		return fmt.Errorf("failed to update quota: %w", err)
	}

	return nil
}

// GetQuota returns the quota configuration for a directory.
func (m *directoryQuotaManager) GetQuota(ctx context.Context, tenantID string, directoryPath string) (*core.DirectoryQuota, error) {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return nil, err
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	// Get quota
	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get quota: %w", err)
	}

	return quota, nil
}

// SetFileCount replaces the current count during startup reconciliation.
func (m *directoryQuotaManager) SetFileCount(ctx context.Context, tenantID string, directoryPath string, count int) error {
	normalizedPath, err := validateAndNormalize(tenantID, directoryPath)
	if err != nil {
		return err
	}
	if count < 0 {
		return fmt.Errorf("count cannot be negative: %w", core.ErrInvalidArgument)
	}
	unlock := m.lock(tenantID, normalizedPath)
	defer unlock()

	quota, err := m.repository.GetOrCreate(ctx, tenantID, normalizedPath)
	if err != nil {
		return fmt.Errorf("failed to get quota: %w", err)
	}
	quota.CurrentCount = count
	if err := m.repository.Update(ctx, tenantID, quota); err != nil {
		return fmt.Errorf("failed to update quota: %w", err)
	}
	return nil
}

func validateAndNormalize(tenantID string, directoryPath string) (string, error) {
	if strings.TrimSpace(tenantID) == "" {
		return "", fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	return directorypath.Normalize(directoryPath), nil
}

func (m *directoryQuotaManager) lock(tenantID string, directoryPath string) func() {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	hash := offset32
	for i := 0; i < len(tenantID); i++ {
		hash = (hash ^ uint32(tenantID[i])) * prime32
	}
	hash = (hash ^ 0xff) * prime32
	for i := 0; i < len(directoryPath); i++ {
		hash = (hash ^ uint32(directoryPath[i])) * prime32
	}
	mutex := &m.locks[hash%uint32(len(m.locks))]
	mutex.Lock()
	return mutex.Unlock
}
