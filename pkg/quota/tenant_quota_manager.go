package quota

import (
	"context"
	"fmt"
	"sync"

	"github.com/cocosip/venue/pkg/core"
)

// tenantQuota represents quota information for a tenant.
type tenantQuota struct {
	tenantID     string
	currentCount int
	maxCount     int
	enabled      bool
}

// canAddFile checks if a file can be added without exceeding quota.
func (q *tenantQuota) canAddFile() bool {
	if !q.enabled || q.maxCount == 0 {
		return true // Unlimited
	}
	return q.currentCount < q.maxCount
}

// tenantQuotaManager implements TenantQuotaManager interface.
type tenantQuotaManager struct {
	quotas map[string]*tenantQuota
	mu     sync.RWMutex
}

// NewTenantQuotaManager creates a new tenant quota manager.
func NewTenantQuotaManager() core.TenantQuotaManager {
	return &tenantQuotaManager{
		quotas: make(map[string]*tenantQuota),
	}
}

// CanAddFile checks if a file can be added to a tenant without exceeding quota.
func (m *tenantQuotaManager) CanAddFile(ctx context.Context, tenantID string) (bool, error) {
	if tenantID == "" {
		return false, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	quota, exists := m.quotas[tenantID]
	if !exists {
		// No quota set, unlimited
		return true, nil
	}

	return quota.canAddFile(), nil
}

// IncrementFileCount atomically increments the file count for a tenant.
func (m *tenantQuotaManager) IncrementFileCount(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	quota, exists := m.quotas[tenantID]
	if !exists {
		// No quota set, create default (unlimited)
		m.quotas[tenantID] = &tenantQuota{
			tenantID:     tenantID,
			currentCount: 1,
			maxCount:     0,
			enabled:      false,
		}
		return nil
	}

	// Check if we can add a file
	if !quota.canAddFile() {
		return core.ErrTenantQuotaExceeded
	}

	// Increment count
	quota.currentCount++

	return nil
}

// DecrementFileCount atomically decrements the file count for a tenant.
func (m *tenantQuotaManager) DecrementFileCount(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	quota, exists := m.quotas[tenantID]
	if !exists {
		// No quota, nothing to decrement
		return nil
	}

	// Decrement count (don't go below 0)
	if quota.currentCount > 0 {
		quota.currentCount--
	}

	return nil
}

// GetFileCount returns the current file count for a tenant.
func (m *tenantQuotaManager) GetFileCount(ctx context.Context, tenantID string) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	quota, exists := m.quotas[tenantID]
	if !exists {
		return 0, nil
	}

	return quota.currentCount, nil
}

// SetQuota sets the maximum file count for a tenant (0 = unlimited).
func (m *tenantQuotaManager) SetQuota(ctx context.Context, tenantID string, maxCount int) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if maxCount < 0 {
		return fmt.Errorf("max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	quota, exists := m.quotas[tenantID]
	if !exists {
		// Create new quota
		m.quotas[tenantID] = &tenantQuota{
			tenantID:     tenantID,
			currentCount: 0,
			maxCount:     maxCount,
			enabled:      maxCount > 0,
		}
		return nil
	}

	// Update existing quota
	quota.maxCount = maxCount
	quota.enabled = maxCount > 0

	return nil
}
