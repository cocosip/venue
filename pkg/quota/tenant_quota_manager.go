package quota

import (
	"context"
	"fmt"
	"sync"

	"github.com/cocosip/venue/pkg/core"
)

// Note: sync.Map is used instead of map+mutex for better concurrent performance
// with many tenants. It's optimized for frequent reads and infrequent writes.

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
// Uses sync.Map for better concurrent performance with high tenant counts.
type tenantQuotaManager struct {
	quotas sync.Map // map[string]*tenantQuota
	// Note: sync.Map is optimized for frequent reads and infrequent writes
	// which matches the quota check pattern (many CanAddFile calls,
	// fewer Increment/Decrement calls)
}

// NewTenantQuotaManager creates a new tenant quota manager.
func NewTenantQuotaManager() core.TenantQuotaManager {
	return &tenantQuotaManager{}
}

// CanAddFile checks if a file can be added to a tenant without exceeding quota.
func (m *tenantQuotaManager) CanAddFile(ctx context.Context, tenantID string) (bool, error) {
	if tenantID == "" {
		return false, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if val, ok := m.quotas.Load(tenantID); ok {
		quota := val.(*tenantQuota)
		return quota.canAddFile(), nil
	}

	// No quota set, unlimited
	return true, nil
}

// IncrementFileCount atomically increments the file count for a tenant.
func (m *tenantQuotaManager) IncrementFileCount(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	for {
		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			// No quota set, create default (unlimited)
			newQuota := &tenantQuota{
				tenantID:     tenantID,
				currentCount: 1,
				maxCount:     0,
				enabled:      false,
			}
			if _, loaded := m.quotas.LoadOrStore(tenantID, newQuota); !loaded {
				return nil // Successfully stored
			}
			// Another goroutine stored, retry
			continue
		}

		quota := val.(*tenantQuota)

		// Check if we can add a file
		if !quota.canAddFile() {
			return core.ErrTenantQuotaExceeded
		}

		// Try to increment atomically using CompareAndSwap
		newQuota := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: quota.currentCount + 1,
			maxCount:     quota.maxCount,
			enabled:      quota.enabled,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}

// DecrementFileCount atomically decrements the file count for a tenant.
func (m *tenantQuotaManager) DecrementFileCount(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	for {
		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			return nil // No quota, nothing to decrement
		}

		quota := val.(*tenantQuota)
		if quota.currentCount <= 0 {
			return nil // Already at 0
		}

		newQuota := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: quota.currentCount - 1,
			maxCount:     quota.maxCount,
			enabled:      quota.enabled,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}

// GetFileCount returns the current file count for a tenant.
func (m *tenantQuotaManager) GetFileCount(ctx context.Context, tenantID string) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if val, ok := m.quotas.Load(tenantID); ok {
		return val.(*tenantQuota).currentCount, nil
	}
	return 0, nil
}

// SetQuota sets the maximum file count for a tenant (0 = unlimited).
func (m *tenantQuotaManager) SetQuota(ctx context.Context, tenantID string, maxCount int) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if maxCount < 0 {
		return fmt.Errorf("max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	for {
		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			// Create new quota
			newQuota := &tenantQuota{
				tenantID:     tenantID,
				currentCount: 0,
				maxCount:     maxCount,
				enabled:      maxCount > 0,
			}
			if _, loaded := m.quotas.LoadOrStore(tenantID, newQuota); !loaded {
				return nil
			}
			continue
		}

		quota := val.(*tenantQuota)
		newQuota := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: quota.currentCount,
			maxCount:     maxCount,
			enabled:      maxCount > 0,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}
