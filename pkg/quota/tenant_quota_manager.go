package quota

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cocosip/venue/pkg/core"
)

// Note: sync.Map is used instead of map+mutex for better concurrent performance
// with many tenants. It's optimized for frequent reads and infrequent writes.

// tenantQuota represents the quota state of one tenant.
//
// An entry is either implicit or an override. Entries created implicitly by
// IncrementFileCount or SetFileCount have overridden == false and read their
// effective limit dynamically from the manager's global limit, so a global
// change is visible even when the entry already has a file count. Entries
// created by SetQuota or SetTenantLimit have overridden == true and use
// maxCount as the tenant's own limit; maxCount is only meaningful while
// overridden is true, so the invariant is that a non-overridden entry carries
// maxCount == 0 while keeping currentCount intact.
type tenantQuota struct {
	tenantID     string
	currentCount int
	maxCount     int
	overridden   bool
}

// canAddFile checks if a file can be added without exceeding effectiveLimit.
// A limit of zero or less means unlimited.
func (q *tenantQuota) canAddFile(effectiveLimit int) bool {
	if effectiveLimit <= 0 {
		return true // Unlimited
	}
	return q.currentCount < effectiveLimit
}

// TenantQuotaManager implements the core.TenantQuotaManager interface and the
// optional core.TenantQuotaAdministrator capability.
//
// Limits have two levels: an optional per-tenant override set by SetQuota or
// SetTenantLimit, and a global fallback that applies to every tenant without an
// override. The global fallback is read on every check, so SetGlobalLimit takes
// effect immediately for tenants that already have a file count.
//
// Uses sync.Map for better concurrent performance with high tenant counts: the
// quota check pattern is many reads with fewer writes. A manager must not be
// copied after first use.
type TenantQuotaManager struct {
	quotas sync.Map // map[string]*tenantQuota

	// globalLimit is the fallback limit for tenants without an override.
	// A value of 0 means unlimited. It is mutated at runtime while other
	// goroutines check quotas, so it is stored atomically.
	globalLimit atomic.Int64
}

// The concrete manager must satisfy the base quota interface and the optional
// administration capability.
var (
	_ core.TenantQuotaManager       = (*TenantQuotaManager)(nil)
	_ core.TenantQuotaAdministrator = (*TenantQuotaManager)(nil)
)

// NewTenantQuotaManager creates a new tenant quota manager.
//
// When supplied and positive, the first argument becomes the global limit that
// applies to tenants without their own limit; zero or a negative value means
// unlimited. The concrete manager is returned so callers can use the
// core.TenantQuotaAdministrator capability directly.
func NewTenantQuotaManager(defaultMaxCount ...int) *TenantQuotaManager {
	manager := &TenantQuotaManager{}
	if len(defaultMaxCount) > 0 && defaultMaxCount[0] > 0 {
		manager.globalLimit.Store(int64(defaultMaxCount[0]))
	}
	return manager
}

// effectiveLimit returns the limit that currently applies to tenantID: the
// per-tenant override when one is set, otherwise the global limit. It never
// creates quota state.
func (m *TenantQuotaManager) effectiveLimit(tenantID string) int {
	if val, ok := m.quotas.Load(tenantID); ok {
		if quota := val.(*tenantQuota); quota.overridden {
			return quota.maxCount
		}
	}
	return int(m.globalLimit.Load())
}

// CanAddFile checks if a file can be added to a tenant without exceeding the
// effective limit. A tenant without an entry has a count of zero and is
// therefore always allowed to add a file.
func (m *TenantQuotaManager) CanAddFile(ctx context.Context, tenantID string) (bool, error) {
	if tenantID == "" {
		return false, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	quota := &tenantQuota{}
	if val, ok := m.quotas.Load(tenantID); ok {
		quota = val.(*tenantQuota)
	}

	return quota.canAddFile(m.effectiveLimit(tenantID)), nil
}

// IncrementFileCount atomically increments the file count for a tenant.
//
// The effective limit is re-read on every attempt, so a concurrent
// SetGlobalLimit is honored instead of being frozen into the entry.
func (m *TenantQuotaManager) IncrementFileCount(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	for {
		limit := m.effectiveLimit(tenantID)

		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			// No tenant entry exists yet, so the count is zero and the first
			// file always fits. The entry is implicit: it follows the current
			// global limit rather than owning a limit.
			newQuota := &tenantQuota{
				tenantID:     tenantID,
				currentCount: 1,
			}
			if _, loaded := m.quotas.LoadOrStore(tenantID, newQuota); !loaded {
				return nil // Successfully stored
			}
			// Another goroutine stored, retry
			continue
		}

		quota := val.(*tenantQuota)

		// Check if we can add a file
		if !quota.canAddFile(limit) {
			return core.ErrTenantQuotaExceeded
		}

		// Try to increment atomically using CompareAndSwap
		newQuota := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: quota.currentCount + 1,
			maxCount:     quota.maxCount,
			overridden:   quota.overridden,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}

// SetFileCount replaces the current count during startup reconciliation.
//
// Reconciliation never installs a per-tenant override: the entry keeps whatever
// override it already had and otherwise follows the global limit.
func (m *TenantQuotaManager) SetFileCount(ctx context.Context, tenantID string, count int) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if count < 0 {
		return fmt.Errorf("count cannot be negative: %w", core.ErrInvalidArgument)
	}

	for {
		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			newQuota := &tenantQuota{
				tenantID:     tenantID,
				currentCount: count,
			}
			if _, loaded := m.quotas.LoadOrStore(tenantID, newQuota); !loaded {
				return nil
			}
			continue
		}

		quota := val.(*tenantQuota)
		updated := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: count,
			maxCount:     quota.maxCount,
			overridden:   quota.overridden,
		}
		if m.quotas.CompareAndSwap(tenantID, quota, updated) {
			return nil
		}
	}
}

// DecrementFileCount atomically decrements the file count for a tenant.
func (m *TenantQuotaManager) DecrementFileCount(ctx context.Context, tenantID string) error {
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
			overridden:   quota.overridden,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}

// GetFileCount returns the current file count for a tenant.
func (m *TenantQuotaManager) GetFileCount(ctx context.Context, tenantID string) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if val, ok := m.quotas.Load(tenantID); ok {
		return val.(*tenantQuota).currentCount, nil
	}
	return 0, nil
}

// SetQuota sets the maximum file count for a tenant (0 = unlimited).
//
// It installs a per-tenant override, so the tenant no longer follows the global
// limit until RemoveTenantLimit is called.
func (m *TenantQuotaManager) SetQuota(ctx context.Context, tenantID string, maxCount int) error {
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
				overridden:   true,
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
			overridden:   true,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, newQuota) {
			return nil
		}
		// CAS failed, retry
	}
}

// GetEffectiveLimit returns the limit that currently applies to a tenant: the
// per-tenant limit when one is set, otherwise the global limit. A zero limit
// means unlimited.
func (m *TenantQuotaManager) GetEffectiveLimit(ctx context.Context, tenantID string) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	return m.effectiveLimit(tenantID), nil
}

// SetTenantLimit sets a per-tenant limit that overrides the global limit.
// A negative limit is rejected with core.ErrInvalidArgument; zero means
// unlimited.
func (m *TenantQuotaManager) SetTenantLimit(ctx context.Context, tenantID string, maxCount int) error {
	if maxCount < 0 {
		return fmt.Errorf("max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	return m.SetQuota(ctx, tenantID, maxCount)
}

// RemoveTenantLimit removes a per-tenant limit so the tenant falls back to the
// current global limit. Removing a limit that was never set is not an error and
// never creates quota state. The tenant's file count is preserved.
func (m *TenantQuotaManager) RemoveTenantLimit(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	for {
		val, loaded := m.quotas.Load(tenantID)
		if !loaded {
			return nil // No entry, nothing to remove
		}

		quota := val.(*tenantQuota)
		if !quota.overridden {
			return nil // Implicit entry, no override to remove
		}

		// maxCount is cleared because it is only meaningful for an override;
		// the file count must survive the removal.
		updated := &tenantQuota{
			tenantID:     quota.tenantID,
			currentCount: quota.currentCount,
		}

		if m.quotas.CompareAndSwap(tenantID, quota, updated) {
			return nil
		}
		// CAS failed, retry
	}
}

// SetGlobalLimit sets the fallback limit for tenants without their own limit.
// A negative limit is rejected with core.ErrInvalidArgument; zero means
// unlimited. The change applies immediately to tenants that have no override,
// including tenants whose quota entry already exists.
func (m *TenantQuotaManager) SetGlobalLimit(ctx context.Context, maxCount int) error {
	if maxCount < 0 {
		return fmt.Errorf("global max count cannot be negative: %w", core.ErrInvalidArgument)
	}

	m.globalLimit.Store(int64(maxCount))
	return nil
}

// GetGlobalLimit returns the fallback limit for tenants without their own limit
// (0 means unlimited).
func (m *TenantQuotaManager) GetGlobalLimit(ctx context.Context) (int, error) {
	return int(m.globalLimit.Load()), nil
}
