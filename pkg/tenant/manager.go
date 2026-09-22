package tenant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// defaultCacheTTL is used when TenantManagerOptions.CacheTTL is zero.
const defaultCacheTTL = 5 * time.Minute

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

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	// The manager owns no handler or writer.
	Logging *logging.Runtime
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
	opts   *TenantManagerOptions
	store  *MetadataStore
	logger *logging.Runtime

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

	if opts.CacheTTL == 0 {
		opts.CacheTTL = defaultCacheTTL
	}

	// Create metadata store
	store, err := NewMetadataStore(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	return &TenantManager{
		opts:   opts,
		store:  store,
		logger: logger,
		cache:  make(map[string]*cacheEntry),
		locks:  make(map[string]*sync.Mutex),
	}, nil
}

// GetTenant retrieves a tenant context by ID.
// If auto-create is enabled and tenant doesn't exist, creates it automatically.
//
// The tenant ID is validated before it is used to derive any path, so an
// identifier that could resolve outside the tenant metadata directory is
// rejected with core.ErrInvalidArgument (and core.ErrPathTraversalAttempt).
func (m *TenantManager) GetTenant(ctx context.Context, tenantID string) (core.TenantContext, error) {
	if err := core.ValidateTenantID(tenantID); err != nil {
		return core.TenantContext{}, err
	}
	if err := ctx.Err(); err != nil {
		return core.TenantContext{}, err
	}

	// Check cache first
	if cached, ok := m.getCached(tenantID); ok {
		return cached, nil
	}

	// Hold the per-tenant lock while loading and publishing to the cache so a
	// concurrent status change cannot be overwritten by a pre-change snapshot.
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if cached, ok := m.getCached(tenantID); ok {
		return cached, nil
	}

	// Load from disk
	metadata, err := m.store.Load(tenantID)
	if err != nil {
		if errors.Is(err, core.ErrTenantNotFound) {
			// Auto-create if enabled
			if m.opts.EnableAutoCreate {
				return m.createTenantInternalLocked(tenantID)
			}
			return core.TenantContext{}, fmt.Errorf("tenant %q: %w", tenantID, core.ErrTenantNotFound)
		}
		return core.TenantContext{}, fmt.Errorf("failed to load tenant %q: %w", tenantID, err)
	}

	// Convert to context and cache
	tenantCtx := metadata.ToTenantContext()
	m.putCache(tenantID, tenantCtx)

	return tenantCtx, nil
}

// TryGetTenant retrieves a tenant context by ID without creating it.
//
// It reports ok=false when no tenant with that ID exists. Unlike GetTenant it
// never writes tenant state: a missing tenant is not created even when
// auto-create is enabled, and no tenant directory is materialized as a side
// effect. A malformed identifier is still rejected, because it could not have
// been created in the first place.
func (m *TenantManager) TryGetTenant(ctx context.Context, tenantID string) (core.TenantContext, bool, error) {
	if err := core.ValidateTenantID(tenantID); err != nil {
		return core.TenantContext{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return core.TenantContext{}, false, err
	}

	if cached, ok := m.getCached(tenantID); ok {
		return cached, true, nil
	}

	// The per-tenant lock keeps a read consistent with a concurrent status
	// change; nothing is published to disk while it is held.
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if cached, ok := m.getCached(tenantID); ok {
		return cached, true, nil
	}

	metadata, err := m.store.Load(tenantID)
	if err != nil {
		if errors.Is(err, core.ErrTenantNotFound) {
			return core.TenantContext{}, false, nil
		}
		return core.TenantContext{}, false, fmt.Errorf("failed to load tenant %q: %w", tenantID, err)
	}

	tenantCtx := metadata.ToTenantContext()
	m.putCache(tenantID, tenantCtx)

	return tenantCtx, true, nil
}

// IsTenantEnabled checks if a tenant is enabled.
// Returns false if tenant doesn't exist.
//
// It is a read-only probe: a tenant that does not exist is reported as disabled
// rather than being materialized, so a health check or watcher scan cannot
// create tenant state.
func (m *TenantManager) IsTenantEnabled(ctx context.Context, tenantID string) (bool, error) {
	tenant, ok, err := m.TryGetTenant(ctx, tenantID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return tenant.Status == core.TenantStatusEnabled, nil
}

// CreateTenant creates a new tenant.
//
// Errors:
// - ErrInvalidArgument if the tenant ID is empty or unsafe as a path segment
// - ErrTenantAlreadyExists if tenant already exists
func (m *TenantManager) CreateTenant(ctx context.Context, tenantID string) error {
	if err := core.ValidateTenantID(tenantID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		tenant, err := m.GetTenant(ctx, tenantID)
		if err != nil {
			// A single unreadable tenant file must not hide the remaining
			// tenants, but it must be visible to operators.
			m.emit(ctx, tenantID, err)
			continue
		}
		tenants = append(tenants, tenant)
	}

	return tenants, nil
}

// createTenantInternalLocked creates a tenant. The caller must already hold the
// per-tenant lock and must have validated the tenant ID.
func (m *TenantManager) createTenantInternalLocked(tenantID string) (core.TenantContext, error) {
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
	if err := core.ValidateTenantID(tenantID); err != nil {
		return err
	}

	// Acquire tenant lock
	lock := m.getTenantLock(tenantID)
	lock.Lock()
	defer lock.Unlock()

	// Load metadata
	metadata, err := m.store.Load(tenantID)
	if err != nil {
		if errors.Is(err, core.ErrTenantNotFound) {
			return fmt.Errorf("tenant %q: %w", tenantID, core.ErrTenantNotFound)
		}
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

// emit records a tenant-level diagnostic without exposing file contents or
// physical paths.
func (m *TenantManager) emit(ctx context.Context, tenantID string, cause error) {
	m.logger.Emit(ctx, logging.Record{
		Level:     slog.LevelWarn,
		Component: "tenant.manager",
		Event:     "tenant_load_failed",
		Message:   "Tenant metadata could not be loaded and was skipped",
		Attrs: []slog.Attr{
			slog.String("tenant_id", tenantID),
			slog.String("error_type", fmt.Sprintf("%T", cause)),
		},
	})
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
