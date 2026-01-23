package tenant

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// cacheEntry holds cached tenant context with expiration.
type cacheEntry struct {
	context   core.TenantContext
	expiresAt time.Time
}

// isExpired checks if the cache entry has expired.
func (e *cacheEntry) isExpired() bool {
	return time.Now().After(e.expiresAt)
}

// TenantManagerOptions configures the TenantManager.
type TenantManagerOptions struct {
	// RootPath is the root directory for tenant storage.
	RootPath string

	// MetadataPath is the directory for tenant metadata files.
	// If empty, defaults to RootPath/.locus/tenants
	MetadataPath string

	// CacheTTL is the cache time-to-live.
	// Default: 5 minutes
	CacheTTL time.Duration

	// EnableAutoCreate enables automatic tenant creation.
	// Default: false
	EnableAutoCreate bool
}

// DefaultTenantManagerOptions returns default options.
func DefaultTenantManagerOptions(rootPath string) *TenantManagerOptions {
	return &TenantManagerOptions{
		RootPath:         rootPath,
		MetadataPath:     "",
		CacheTTL:         5 * time.Minute,
		EnableAutoCreate: false,
	}
}

// TenantManager manages tenant lifecycle and multi-tenant isolation.
type TenantManager struct {
	opts  *TenantManagerOptions
	store *MetadataStore

	// Cache for tenant contexts
	cache   map[string]*cacheEntry
	cacheMu sync.RWMutex

	// Per-tenant locks for operations
	locks   map[string]*sync.Mutex
	locksMu sync.Mutex
}

// NewTenantManager creates a new TenantManager.
// Returns the core.TenantManager interface to encourage interface-based programming.
func NewTenantManager(opts *TenantManagerOptions) (core.TenantManager, error) {
	if opts == nil {
		return nil, errors.New("options cannot be nil")
	}

	if opts.RootPath == "" {
		return nil, errors.New("root path cannot be empty")
	}

	// Set default metadata path
	metadataPath := opts.MetadataPath
	if metadataPath == "" {
		metadataPath = filepath.Join(opts.RootPath, ".locus", "tenants")
	}

	// Create metadata store
	store, err := NewMetadataStore(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	return &TenantManager{
		opts:  opts,
		store: store,
		cache: make(map[string]*cacheEntry),
		locks: make(map[string]*sync.Mutex),
	}, nil
}

// GetTenant retrieves a tenant context by ID.
// If auto-create is enabled and tenant doesn't exist, creates it automatically.
func (m *TenantManager) GetTenant(ctx context.Context, tenantID string) (core.TenantContext, error) {
	if tenantID == "" {
		return core.TenantContext{}, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Check cache first
	if cached, ok := m.getCached(tenantID); ok {
		return cached, nil
	}

	// Load from disk
	metadata, err := m.store.Load(tenantID)
	if err != nil {
		if errors.Is(err, core.ErrTenantNotFound) {
			// Auto-create if enabled
			if m.opts.EnableAutoCreate {
				return m.createTenantInternal(tenantID)
			}
			return core.TenantContext{}, core.ErrTenantNotFound
		}
		return core.TenantContext{}, fmt.Errorf("failed to load tenant: %w", err)
	}

	// Convert to context and cache
	tenantCtx := metadata.ToTenantContext()
	m.putCache(tenantID, tenantCtx)

	return tenantCtx, nil
}

// IsTenantEnabled checks if a tenant is enabled.
func (m *TenantManager) IsTenantEnabled(ctx context.Context, tenantID string) (bool, error) {
	tenant, err := m.GetTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, core.ErrTenantNotFound) {
			return false, nil
		}
		return false, err
	}

	return tenant.Status == core.TenantStatusEnabled, nil
}

// CreateTenant creates a new tenant.
func (m *TenantManager) CreateTenant(ctx context.Context, tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Acquire tenant lock
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	// Check if already exists (within lock)
	exists, err := m.store.Exists(tenantID)
	if err != nil {
		return fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if exists {
		return core.ErrTenantAlreadyExists
	}

	// Create metadata
	storagePath := filepath.Join(m.opts.RootPath, tenantID)
	metadata := createDefaultMetadata(tenantID, storagePath)

	// Save to disk
	if err := m.store.Save(metadata); err != nil {
		return fmt.Errorf("failed to save tenant metadata: %w", err)
	}

	// Cache
	tenantCtx := metadata.ToTenantContext()
	m.putCache(tenantID, tenantCtx)

	return nil
}

// EnableTenant enables a disabled tenant.
func (m *TenantManager) EnableTenant(ctx context.Context, tenantID string) error {
	return m.updateTenantStatus(tenantID, core.TenantStatusEnabled)
}

// DisableTenant disables a tenant.
func (m *TenantManager) DisableTenant(ctx context.Context, tenantID string) error {
	return m.updateTenantStatus(tenantID, core.TenantStatusDisabled)
}

// GetAllTenants returns all tenants.
func (m *TenantManager) GetAllTenants(ctx context.Context) ([]core.TenantContext, error) {
	tenantIDs, err := m.store.ListAll()
	if err != nil {
		return nil, fmt.Errorf("failed to list tenants: %w", err)
	}

	var tenants []core.TenantContext
	for _, tenantID := range tenantIDs {
		tenant, err := m.GetTenant(ctx, tenantID)
		if err != nil {
			// Skip tenants that failed to load
			continue
		}
		tenants = append(tenants, tenant)
	}

	return tenants, nil
}

// createTenantInternal creates a tenant with proper locking.
func (m *TenantManager) createTenantInternal(tenantID string) (core.TenantContext, error) {
	// Acquire tenant lock
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	// Double-check existence
	exists, err := m.store.Exists(tenantID)
	if err != nil {
		return core.TenantContext{}, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if exists {
		// Already created by another goroutine, load it
		metadata, err := m.store.Load(tenantID)
		if err != nil {
			return core.TenantContext{}, err
		}
		tenantCtx := metadata.ToTenantContext()
		m.putCache(tenantID, tenantCtx)
		return tenantCtx, nil
	}

	// Create metadata
	storagePath := filepath.Join(m.opts.RootPath, tenantID)
	metadata := createDefaultMetadata(tenantID, storagePath)

	// Save to disk
	if err := m.store.Save(metadata); err != nil {
		return core.TenantContext{}, fmt.Errorf("failed to save tenant metadata: %w", err)
	}

	// Cache and return
	tenantCtx := metadata.ToTenantContext()
	m.putCache(tenantID, tenantCtx)

	return tenantCtx, nil
}

// updateTenantStatus updates a tenant's status.
func (m *TenantManager) updateTenantStatus(tenantID string, status core.TenantStatus) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Acquire tenant lock
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	// Load metadata
	metadata, err := m.store.Load(tenantID)
	if err != nil {
		return err
	}

	// Update status
	metadata.Status = status
	metadata.UpdatedAt = time.Now()

	// Save to disk
	if err := m.store.Save(metadata); err != nil {
		return fmt.Errorf("failed to save tenant metadata: %w", err)
	}

	// Invalidate cache
	m.invalidateCache(tenantID)

	return nil
}

// getCached retrieves a tenant from cache if not expired.
func (m *TenantManager) getCached(tenantID string) (core.TenantContext, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()

	entry, ok := m.cache[tenantID]
	if !ok || entry.isExpired() {
		return core.TenantContext{}, false
	}

	return entry.context, true
}

// putCache adds a tenant to cache.
func (m *TenantManager) putCache(tenantID string, tenant core.TenantContext) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	m.cache[tenantID] = &cacheEntry{
		context:   tenant,
		expiresAt: time.Now().Add(m.opts.CacheTTL),
	}
}

// invalidateCache removes a tenant from cache.
func (m *TenantManager) invalidateCache(tenantID string) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	delete(m.cache, tenantID)
}

// getTenantLock gets or creates a lock for a tenant.
func (m *TenantManager) getTenantLock(tenantID string) *sync.Mutex {
	m.locksMu.Lock()
	defer m.locksMu.Unlock()

	lock, ok := m.locks[tenantID]
	if !ok {
		lock = &sync.Mutex{}
		m.locks[tenantID] = lock
	}

	return lock
}
