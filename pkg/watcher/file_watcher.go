package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// FileWatcherOptions configures the file watcher.
type FileWatcherOptions struct {
	// TenantManager is required for tenant validation and auto-creation.
	TenantManager core.TenantManager

	// StoragePool is required for file imports.
	StoragePool core.StoragePool

	// ConfigurationRootDir is the directory for storing watcher state/configuration.
	// Default: "./.locus/watchers"
	ConfigurationRootDir string
}

// fileWatcher implements the FileWatcher interface.
type fileWatcher struct {
	tenantMgr       core.TenantManager
	storagePool     core.StoragePool
	watchers        sync.Map          // map[string]*core.FileWatcherConfiguration
	configRoot      string            // Configuration root directory
	importedFiles   sync.Map          // map[string]string: filePath -> fileKey (imported files history)
	importedFilesMu sync.RWMutex      // Lock for persisting imported files
}

// NewFileWatcher creates a new file watcher.
func NewFileWatcher(opts *FileWatcherOptions) (core.FileWatcher, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.TenantManager == nil {
		return nil, fmt.Errorf("tenant manager cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.StoragePool == nil {
		return nil, fmt.Errorf("storage pool cannot be nil: %w", core.ErrInvalidArgument)
	}

	// Set default configuration root if not specified
	configRoot := opts.ConfigurationRootDir
	if configRoot == "" {
		configRoot = filepath.Join(".locus", "watchers")
	}

	// Ensure configuration directory exists
	if err := os.MkdirAll(configRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create configuration directory: %w", err)
	}

	fw := &fileWatcher{
		tenantMgr:   opts.TenantManager,
		storagePool: opts.StoragePool,
		configRoot:  configRoot,
	}

	// Load imported files history
	if err := fw.loadImportedFilesHistory(); err != nil {
		slog.Warn("Failed to load imported files history", "error", err)
	}

	return fw, nil
}

// RegisterWatcher adds a new file watcher configuration.
func (w *fileWatcher) RegisterWatcher(ctx context.Context, config *core.FileWatcherConfiguration) error {
	if config == nil {
		return fmt.Errorf("configuration cannot be nil: %w", core.ErrInvalidArgument)
	}

	if config.WatcherID == "" {
		return fmt.Errorf("watcher ID cannot be empty: %w", core.ErrInvalidArgument)
	}

	if config.WatchPath == "" {
		return fmt.Errorf("watch path cannot be empty: %w", core.ErrInvalidArgument)
	}

	// Validate tenant in single-tenant mode
	if !config.MultiTenantMode && config.TenantID == "" {
		return fmt.Errorf("tenant ID required in single-tenant mode: %w", core.ErrInvalidArgument)
	}

	// Set defaults
	if config.PollingInterval == 0 {
		config.PollingInterval = 30 * time.Second
	}

	if config.MinFileAge == 0 {
		config.MinFileAge = 3 * time.Second
	}

	if config.MaxConcurrentImports == 0 {
		config.MaxConcurrentImports = 4
	}

	// Store configuration
	w.watchers.Store(config.WatcherID, config)

	// Auto-create tenant directories if enabled
	if config.MultiTenantMode && config.AutoCreateTenantDirectories {
		if err := w.createTenantDirectories(ctx, config); err != nil {
			slog.Warn("Failed to auto-create tenant directories", "watcherID", config.WatcherID, "error", err)
		}
	}

	return nil
}

// UnregisterWatcher removes a file watcher.
func (w *fileWatcher) UnregisterWatcher(ctx context.Context, watcherID string) error {
	w.watchers.Delete(watcherID)
	return nil
}

// GetWatcher retrieves a watcher configuration by ID.
func (w *fileWatcher) GetWatcher(ctx context.Context, watcherID string) (*core.FileWatcherConfiguration, error) {
	value, ok := w.watchers.Load(watcherID)
	if !ok {
		return nil, fmt.Errorf("watcher not found: %s", watcherID)
	}

	config := value.(*core.FileWatcherConfiguration)
	return config, nil
}

// GetAllWatchers retrieves all watcher configurations.
func (w *fileWatcher) GetAllWatchers(ctx context.Context) ([]*core.FileWatcherConfiguration, error) {
	var configs []*core.FileWatcherConfiguration

	w.watchers.Range(func(key, value interface{}) bool {
		config := value.(*core.FileWatcherConfiguration)
		configs = append(configs, config)
		return true
	})

	return configs, nil
}

// EnableWatcher enables a watcher.
func (w *fileWatcher) EnableWatcher(ctx context.Context, watcherID string) error {
	value, ok := w.watchers.Load(watcherID)
	if !ok {
		return fmt.Errorf("watcher not found: %s", watcherID)
	}

	config := value.(*core.FileWatcherConfiguration)
	config.Enabled = true
	w.watchers.Store(watcherID, config)

	return nil
}

// DisableWatcher disables a watcher.
func (w *fileWatcher) DisableWatcher(ctx context.Context, watcherID string) error {
	value, ok := w.watchers.Load(watcherID)
	if !ok {
		return fmt.Errorf("watcher not found: %s", watcherID)
	}

	config := value.(*core.FileWatcherConfiguration)
	config.Enabled = false
	w.watchers.Store(watcherID, config)

	return nil
}

// ScanNow manually triggers a scan for the specified watcher.
func (w *fileWatcher) ScanNow(ctx context.Context, watcherID string) (*core.FileWatcherScanResult, error) {
	config, err := w.GetWatcher(ctx, watcherID)
	if err != nil {
		return nil, err
	}

	return w.scanWatcher(ctx, config)
}

// ScanAllWatchers scans all enabled watchers.
func (w *fileWatcher) ScanAllWatchers(ctx context.Context) (map[string]*core.FileWatcherScanResult, error) {
	results := make(map[string]*core.FileWatcherScanResult)

	w.watchers.Range(func(key, value interface{}) bool {
		watcherID := key.(string)
		config := value.(*core.FileWatcherConfiguration)

		if !config.Enabled {
			return true // Skip disabled watchers
		}

		result, err := w.scanWatcher(ctx, config)
		if err != nil {
			slog.Error("Failed to scan watcher", "watcherID", watcherID, "error", err)
			result = &core.FileWatcherScanResult{
				Errors: []string{err.Error()},
			}
		}

		results[watcherID] = result
		return true
	})

	return results, nil
}

// scanWatcher performs the actual file scan and import for a watcher.
func (w *fileWatcher) scanWatcher(ctx context.Context, config *core.FileWatcherConfiguration) (*core.FileWatcherScanResult, error) {
	startTime := time.Now()

	result := &core.FileWatcherScanResult{
		Errors: make([]string, 0),
	}

	// Check if watch path exists
	if _, err := os.Stat(config.WatchPath); os.IsNotExist(err) {
		return result, fmt.Errorf("watch path does not exist: %s", config.WatchPath)
	}

	if config.MultiTenantMode {
		// Multi-tenant mode: scan subdirectories
		return w.scanMultiTenant(ctx, config, result, startTime)
	}

	// Single-tenant mode: scan files directly
	return w.scanSingleTenant(ctx, config, result, startTime)
}

// scanSingleTenant scans files in single-tenant mode.
func (w *fileWatcher) scanSingleTenant(ctx context.Context, config *core.FileWatcherConfiguration, result *core.FileWatcherScanResult, startTime time.Time) (*core.FileWatcherScanResult, error) {
	// Get tenant context
	tenant, err := w.tenantMgr.GetTenant(ctx, config.TenantID)
	if err != nil {
		return result, fmt.Errorf("failed to get tenant: %w", err)
	}

	// Discover files
	files, err := w.discoverFiles(config.WatchPath, config)
	if err != nil {
		return result, fmt.Errorf("failed to discover files: %w", err)
	}

	result.FilesDiscovered = len(files)

	// Import files with concurrency control
	semaphore := make(chan struct{}, config.MaxConcurrentImports)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, filePath := range files {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(path string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			imported, bytes, err := w.importFile(ctx, tenant, path, config)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				result.FilesFailed++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", path, err))
			} else if imported {
				result.FilesImported++
				result.BytesImported += bytes
			} else {
				result.FilesSkipped++
			}
		}(filePath)
	}

	wg.Wait()

	result.ScanDuration = time.Since(startTime)
	return result, nil
}

// scanMultiTenant scans files in multi-tenant mode.
func (w *fileWatcher) scanMultiTenant(ctx context.Context, config *core.FileWatcherConfiguration, result *core.FileWatcherScanResult, startTime time.Time) (*core.FileWatcherScanResult, error) {
	// Auto-create tenant directories if enabled
	if config.AutoCreateTenantDirectories {
		if err := w.createTenantDirectories(ctx, config); err != nil {
			slog.Warn("Failed to create tenant directories", "watcherID", config.WatcherID, "error", err)
		}
	}

	// List subdirectories (tenant directories)
	entries, err := os.ReadDir(config.WatchPath)
	if err != nil {
		return result, fmt.Errorf("failed to read watch path: %w", err)
	}

	// Process each tenant directory
	semaphore := make(chan struct{}, config.MaxConcurrentImports)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, entry := range entries {
		if !entry.IsDir() {
			continue // Skip non-directories
		}

		tenantID := entry.Name()
		tenantPath := filepath.Join(config.WatchPath, tenantID)

		wg.Add(1)
		go func(tid string, tpath string) {
			defer wg.Done()

			// Get tenant context
			tenant, err := w.tenantMgr.GetTenant(ctx, tid)
			if err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("tenant %s: %v", tid, err))
				mu.Unlock()
				return
			}

			// Discover files in tenant directory
			files, err := w.discoverFiles(tpath, config)
			if err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("tenant %s: %v", tid, err))
				mu.Unlock()
				return
			}

			mu.Lock()
			result.FilesDiscovered += len(files)
			mu.Unlock()

			// Import files for this tenant
			for _, filePath := range files {
				semaphore <- struct{}{} // Acquire

				imported, bytes, err := w.importFile(ctx, tenant, filePath, config)

				<-semaphore // Release

				mu.Lock()
				if err != nil {
					result.FilesFailed++
					result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", filePath, err))
				} else if imported {
					result.FilesImported++
					result.BytesImported += bytes
				} else {
					result.FilesSkipped++
				}
				mu.Unlock()
			}
		}(tenantID, tenantPath)
	}

	wg.Wait()

	result.ScanDuration = time.Since(startTime)
	return result, nil
}

// discoverFiles discovers files in a directory based on configuration.
func (w *fileWatcher) discoverFiles(dirPath string, config *core.FileWatcherConfiguration) ([]string, error) {
	var files []string

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if info.IsDir() {
			return nil // Skip directories
		}

		// Check file age
		if time.Since(info.ModTime()) < config.MinFileAge {
			return nil // Skip files that are too young
		}

		// Check file size
		if config.MaxFileSizeBytes > 0 && info.Size() > config.MaxFileSizeBytes {
			return nil // Skip files that are too large
		}

		// Check file patterns
		if len(config.FilePatterns) > 0 {
			matched := false
			for _, pattern := range config.FilePatterns {
				if matchPattern(info.Name(), pattern) {
					matched = true
					break
				}
			}
			if !matched {
				return nil // Skip files that don't match patterns
			}
		}

		files = append(files, path)
		return nil
	})

	return files, err
}

// importFile imports a single file into the storage pool.
func (w *fileWatcher) importFile(ctx context.Context, tenant core.TenantContext, filePath string, config *core.FileWatcherConfiguration) (bool, int64, error) {
	// Check if file was already imported (especially important for PostImportAction.Keep mode)
	if w.isFileAlreadyImported(filePath) {
		return false, 0, nil // Skip already imported file
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return false, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return false, 0, fmt.Errorf("failed to stat file: %w", err)
	}

	// Extract original filename
	originalFileName := filepath.Base(filePath)

	// Import to storage pool
	fileKey, err := w.storagePool.WriteFile(ctx, tenant, file, &originalFileName)
	if err != nil {
		return false, 0, fmt.Errorf("failed to import file: %w", err)
	}

	// Mark file as imported to prevent re-importing
	w.markFileAsImported(filePath, fileKey)

	// Post-import action
	if err := w.performPostImportAction(filePath, config); err != nil {
		slog.Warn("Failed to perform post-import action", "file", filePath, "action", config.PostImportAction, "error", err)
	}

	return true, fileInfo.Size(), nil
}

// performPostImportAction performs the configured action after successful import.
func (w *fileWatcher) performPostImportAction(filePath string, config *core.FileWatcherConfiguration) error {
	switch config.PostImportAction {
	case core.PostImportActionDelete:
		return os.Remove(filePath)

	case core.PostImportActionMove:
		if config.MoveToDirectory == "" {
			return fmt.Errorf("move directory not configured")
		}

		// Ensure target directory exists
		if err := os.MkdirAll(config.MoveToDirectory, 0755); err != nil {
			return fmt.Errorf("failed to create move directory: %w", err)
		}

		fileName := filepath.Base(filePath)
		targetPath := filepath.Join(config.MoveToDirectory, fileName)

		return os.Rename(filePath, targetPath)

	case core.PostImportActionKeep:
		// Do nothing
		return nil

	default:
		return fmt.Errorf("unknown post-import action: %d", config.PostImportAction)
	}
}

// createTenantDirectories creates subdirectories for all tenants.
func (w *fileWatcher) createTenantDirectories(ctx context.Context, config *core.FileWatcherConfiguration) error {
	if !config.MultiTenantMode {
		return nil
	}

	// Get all tenants
	tenants, err := w.tenantMgr.GetAllTenants(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tenants: %w", err)
	}

	// Create directory for each tenant
	for _, tenant := range tenants {
		tenantPath := filepath.Join(config.WatchPath, tenant.ID)
		if err := os.MkdirAll(tenantPath, 0755); err != nil {
			slog.Warn("Failed to create tenant directory", "tenant", tenant.ID, "path", tenantPath, "error", err)
		} else {
			slog.Info("Created tenant directory", "tenant", tenant.ID, "path", tenantPath)
		}
	}

	return nil
}

// matchPattern matches a filename against a glob pattern.
func matchPattern(name, pattern string) bool {
	// Simple glob matching: * = any characters, ? = single character
	if pattern == "*" || pattern == "*.*" {
		return true
	}

	// Use filepath.Match for glob patterns
	matched, err := filepath.Match(pattern, name)
	if err != nil {
		return false
	}

	return matched
}

// loadImportedFilesHistory loads the imported files history from persistent storage.
// This prevents re-importing files after restart, especially important for PostImportAction.Keep mode.
func (w *fileWatcher) loadImportedFilesHistory() error {
	historyPath := filepath.Join(w.configRoot, "imported-files.json")

	// Check if file exists
	if _, err := os.Stat(historyPath); os.IsNotExist(err) {
		return nil // File doesn't exist yet, no history to load
	}

	// Read file
	data, err := os.ReadFile(historyPath)
	if err != nil {
		return fmt.Errorf("failed to read imported files history: %w", err)
	}

	// Parse JSON
	var history map[string]string
	if err := json.Unmarshal(data, &history); err != nil {
		return fmt.Errorf("failed to parse imported files history: %w", err)
	}

	// Load into sync.Map
	for filePath, fileKey := range history {
		w.importedFiles.Store(filePath, fileKey)
	}

	slog.Info("Loaded imported files history", "count", len(history))
	return nil
}

// saveImportedFilesHistory saves the imported files history to persistent storage.
func (w *fileWatcher) saveImportedFilesHistory() error {
	w.importedFilesMu.Lock()
	defer w.importedFilesMu.Unlock()

	// Convert sync.Map to regular map
	history := make(map[string]string)
	w.importedFiles.Range(func(key, value interface{}) bool {
		history[key.(string)] = value.(string)
		return true
	})

	// Marshal to JSON
	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal imported files history: %w", err)
	}

	// Write to file
	historyPath := filepath.Join(w.configRoot, "imported-files.json")
	if err := os.WriteFile(historyPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write imported files history: %w", err)
	}

	return nil
}

// isFileAlreadyImported checks if a file has already been imported.
func (w *fileWatcher) isFileAlreadyImported(filePath string) bool {
	_, exists := w.importedFiles.Load(filePath)
	return exists
}

// markFileAsImported marks a file as imported and persists the history.
func (w *fileWatcher) markFileAsImported(filePath, fileKey string) {
	w.importedFiles.Store(filePath, fileKey)

	// Persist to disk asynchronously to avoid blocking
	go func() {
		if err := w.saveImportedFilesHistory(); err != nil {
			slog.Error("Failed to save imported files history", "error", err)
		}
	}()
}
