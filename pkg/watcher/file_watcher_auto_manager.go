package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

const (
	// watcherRootsFileName is the persisted list of root templates under
	// FileWatcherAutoManagerOptions.ConfigurationRootDir.
	watcherRootsFileName = "watcher-roots.json"

	// watcherRootsTempFilePattern names the staging file for the atomic roots
	// write. The "*" makes os.CreateTemp generate a unique name in the same
	// directory as the target, which is required for an atomic rename.
	watcherRootsTempFilePattern = "watcher-roots-*.tmp"

	// watcherRootsVersion is the schema version of the persisted roots document.
	// A document with any other version is treated as unreadable state.
	watcherRootsVersion = 1

	// generatedWatcherIDPrefix marks every watcher derived from a root template.
	// RemoveAllWatchers only ever removes watchers carrying this prefix, so
	// manually registered watchers are never touched.
	generatedWatcherIDPrefix = "auto-"

	// defaultRootNameSegment is used when a root path has no usable base name.
	defaultRootNameSegment = "root"
)

// FileWatcherAutoManagerOptions configures the root-configuration auto manager.
type FileWatcherAutoManagerOptions struct {
	// FileWatcher receives the derived watcher registrations. Required.
	FileWatcher core.FileWatcher

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// ConfigurationRootDir is the directory holding the persisted root list.
	// Default: "./.locus/auto-manager".
	ConfigurationRootDir string

	// Roots are the root templates to apply. They are persisted and merged with
	// (and take precedence over) any roots read back from the state file.
	Roots []core.FileWatcherRootConfiguration
}

// fileWatcherAutoManager derives per-tenant watchers from root templates.
//
// Every watcher it generates has an ID starting with "auto-", which is the only
// thing that marks it as managed: the manager can therefore be constructed
// again, after a restart, and still remove exactly the watchers it owns without
// touching manually registered ones.
//
// The root list is persisted under ConfigurationRootDir, so a restart without
// options still reapplies the previous roots.
type fileWatcherAutoManager struct {
	fileWatcher   core.FileWatcher
	logger        *logging.Runtime
	configRootDir string

	// rootsMu guards roots.
	rootsMu sync.Mutex
	roots   []core.FileWatcherRootConfiguration

	// rootsFileMu serializes roots-file writes so two concurrent updates cannot
	// race on the staging file. It is never held together with rootsMu.
	rootsFileMu sync.Mutex

	// applyMu serializes discovery and apply operations so two concurrent calls
	// cannot interleave the managed watcher set.
	applyMu sync.Mutex
}

// NewFileWatcherAutoManager creates a root-configuration auto manager.
//
// The persisted root list under ConfigurationRootDir is loaded here; roots
// supplied in options win per RootPath, and roots known only from the state file
// are merged in so a restart can rediscover its tenants. A missing state file is
// normal; a corrupt one is ignored with a safe warning.
func NewFileWatcherAutoManager(opts *FileWatcherAutoManagerOptions) (core.FileWatcherAutoManager, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.FileWatcher == nil {
		return nil, fmt.Errorf("file watcher cannot be nil: %w", core.ErrInvalidArgument)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	configRootDir := opts.ConfigurationRootDir
	if configRootDir == "" {
		configRootDir = filepath.Join(".locus", "auto-manager")
	}

	manager := &fileWatcherAutoManager{
		fileWatcher:   opts.FileWatcher,
		logger:        logger,
		configRootDir: configRootDir,
		roots:         mergeRoots(opts.Roots, loadRootConfigurations(configRootDir)),
	}

	return manager, nil
}

// ApplyRootConfiguration creates or updates one watcher per tenant directory
// found under root.RootPath and returns how many watchers it created or updated.
//
// Multi-tenant roots derive one watcher per immediate subdirectory: the watcher
// ID is "auto-<root base name>-<directory name>", the tenant ID is the directory
// name, and the watch path is the directory itself. A directory name that fails
// core.ValidateTenantID is never registered: it is counted as an error and
// reported back to the caller.
//
// A non-multi-tenant root is rejected with core.ErrInvalidArgument, because a
// FileWatcherRootConfiguration carries no tenant ID and root.RootPath alone
// cannot name the tenant the watcher would import for.
//
// Applying the same root twice updates the generated watchers instead of
// duplicating them, and never resurrects a watcher an operator disabled.
func (m *fileWatcherAutoManager) ApplyRootConfiguration(ctx context.Context, root *core.FileWatcherRootConfiguration) (int, error) {
	if root == nil {
		return 0, fmt.Errorf("root configuration cannot be nil: %w", core.ErrInvalidArgument)
	}

	if strings.TrimSpace(root.RootPath) == "" {
		return 0, fmt.Errorf("root path cannot be empty: %w", core.ErrInvalidArgument)
	}

	rootCopy := *root
	rootCopy.FilePatterns = append([]string(nil), root.FilePatterns...)

	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	info, err := os.Stat(rootCopy.RootPath)
	if err != nil {
		return 0, fmt.Errorf("root path is not accessible: %w", err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("root path is not a directory: %w", core.ErrInvalidArgument)
	}

	if !rootCopy.MultiTenantMode {
		// Defence in depth: the frozen config validation already rejects this
		// shape, and the runtime cannot derive a tenant from a root path alone.
		return 0, fmt.Errorf("root path %q requires MultiTenantMode: a non-multi-tenant root carries no tenant ID to import for: %w", rootCopy.RootPath, core.ErrInvalidArgument)
	}

	// Remember the root before applying so a later discovery call reuses exactly
	// the template that produced these watchers.
	m.rememberRoot(rootCopy)

	created, applyErr := m.applyMultiTenantRoot(ctx, &rootCopy)

	m.emit(ctx, slog.LevelInfo, "root_configuration_applied", "Applied file watcher root configuration",
		slog.String("root", rootBaseName(rootCopy.RootPath)),
		slog.Bool("multi_tenant_mode", rootCopy.MultiTenantMode),
		slog.Int("watchers", created))

	return created, applyErr
}

// applyMultiTenantRoot creates or updates one watcher per immediate
// subdirectory of the root and returns how many were applied. Directories whose
// name is not a valid tenant ID are skipped and joined into the returned error.
func (m *fileWatcherAutoManager) applyMultiTenantRoot(ctx context.Context, root *core.FileWatcherRootConfiguration) (int, error) {
	entries, err := os.ReadDir(root.RootPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read root path: %w", err)
	}

	applied := 0
	var applyErrors []error

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return applied, errors.Join(append(applyErrors, err)...)
		}

		// The immediate subdirectories are the tenants; a regular file directly
		// under the root is not a tenant directory and is ignored.
		if !entry.IsDir() {
			continue
		}

		tenantID := entry.Name()

		if err := m.applyTenantDirectory(ctx, root, tenantID); err != nil {
			applyErrors = append(applyErrors, err)

			continue
		}

		applied++
	}

	if len(applyErrors) > 0 {
		return applied, errors.Join(applyErrors...)
	}

	return applied, nil
}

// applyTenantDirectory creates or updates the watcher for one tenant directory
// of a multi-tenant root.
//
// A directory name that fails core.ValidateTenantID is never registered and
// never sanitized into something that looks valid: it is reported as an error so
// the caller can count it and surface the misconfiguration.
func (m *fileWatcherAutoManager) applyTenantDirectory(ctx context.Context, root *core.FileWatcherRootConfiguration, tenantID string) error {
	if err := core.ValidateTenantID(tenantID); err != nil {
		m.emit(ctx, slog.LevelWarn, "tenant_directory_skipped", "Skipped a tenant directory with an invalid name",
			slog.String("root", rootBaseName(root.RootPath)),
			slog.Int("name_length", len(tenantID)))

		return fmt.Errorf("skipping tenant directory with invalid name: %w", err)
	}

	config := m.generatedWatcherConfiguration(root, tenantID)

	if _, err := m.fileWatcher.GetWatcher(ctx, config.WatcherID); err != nil {
		if !errors.Is(err, core.ErrWatcherNotFound) {
			return fmt.Errorf("failed to inspect generated watcher: %w", err)
		}

		if err := m.fileWatcher.RegisterWatcher(ctx, config); err != nil {
			return fmt.Errorf("failed to register generated watcher: %w", err)
		}

		return nil
	}

	// The watcher already exists: replace its configuration in place. The
	// persisted enable/disable decision is owned by the watcher ID, so an
	// operator's disable is never reset by re-applying a root.
	if err := m.fileWatcher.UpdateWatcher(ctx, config); err != nil {
		return fmt.Errorf("failed to update generated watcher: %w", err)
	}

	return nil
}

// generatedWatcherConfiguration builds the watcher configuration for one tenant
// directory of a multi-tenant root.
//
// Every import setting of the root template is carried over, including the
// advanced stability, cache, prune and debounce knobs: a root is the only place
// a multi-tenant deployment configures them, so dropping one here would leave it
// with no runtime consumer for generated watchers.
func (m *fileWatcherAutoManager) generatedWatcherConfiguration(root *core.FileWatcherRootConfiguration, tenantID string) *core.FileWatcherConfiguration {
	return &core.FileWatcherConfiguration{
		WatcherID:                               generatedWatcherID(root.RootPath, tenantID),
		TenantID:                                tenantID,
		WatchPath:                               filepath.Join(root.RootPath, tenantID),
		MultiTenantMode:                         false,
		IncludeSubdirectories:                   root.IncludeSubdirectories,
		PollingInterval:                         root.PollingInterval,
		MinFileAge:                              root.MinFileAge,
		FilePatterns:                            append([]string(nil), root.FilePatterns...),
		MaxFileSizeBytes:                        root.MaxFileSizeBytes,
		MaxConcurrentImports:                    root.MaxConcurrentImports,
		PostImportAction:                        root.PostImportAction,
		MoveToDirectory:                         root.MoveToDirectory,
		Enabled:                                 root.Enabled,
		AutoCreateTenantDirectoriesCacheTTL:     root.AutoCreateTenantDirectoriesCacheTTL,
		FileStabilityCheckDelay:                 root.FileStabilityCheckDelay,
		SkipStabilityCheckAfterAge:              root.SkipStabilityCheckAfterAge,
		EnableImportedFilesPruneThrottle:        root.EnableImportedFilesPruneThrottle,
		ImportedFilesPruneInterval:              root.ImportedFilesPruneInterval,
		EnableImportedFilesHistoryFlushDebounce: root.EnableImportedFilesHistoryFlushDebounce,
		ImportedFilesHistoryFlushInterval:       root.ImportedFilesHistoryFlushInterval,
	}
}

// DiscoverAndCreateWatchers re-applies every root the manager holds and returns
// the number of watchers created or updated.
//
// The roots come from the options and from the persisted state file, so a
// restart that passes no roots still rediscovers tenants. It returns 0 without
// an error when no root is configured.
func (m *fileWatcherAutoManager) DiscoverAndCreateWatchers(ctx context.Context) (int, error) {
	roots := m.RootConfigurations()

	if len(roots) == 0 {
		m.emit(ctx, slog.LevelDebug, "no_root_configuration", "No root configuration is set; nothing to discover")

		return 0, nil
	}

	m.emit(ctx, slog.LevelInfo, "discovering_tenants", "Discovering tenant directories",
		slog.Int("roots", len(roots)))

	total := 0
	var discoverErrors []error

	for i := range roots {
		if err := ctx.Err(); err != nil {
			return total, errors.Join(append(discoverErrors, err)...)
		}

		created, err := m.ApplyRootConfiguration(ctx, &roots[i])
		total += created

		if err != nil {
			discoverErrors = append(discoverErrors, err)
		}
	}

	if len(discoverErrors) > 0 {
		return total, errors.Join(discoverErrors...)
	}

	return total, nil
}

// RemoveAllWatchers removes every watcher whose ID marks it as generated from a
// root configuration. Manually registered watchers are never touched.
//
// Side effect, by design: the roots that produced the removed watchers are
// dropped from the persisted root list as well. A root template is only the
// manager's record of the watchers it owns, so keeping it would make the next
// DiscoverAndCreateWatchers recreate exactly what the caller just removed. A
// root that produced no removable watcher is kept.
func (m *fileWatcherAutoManager) RemoveAllWatchers(ctx context.Context) error {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	watchers, err := m.fileWatcher.GetAllWatchers(ctx)
	if err != nil {
		return fmt.Errorf("failed to list watchers: %w", err)
	}

	removed := make(map[string]bool)
	var removeErrors []error

	for _, config := range watchers {
		if !strings.HasPrefix(config.WatcherID, generatedWatcherIDPrefix) {
			continue
		}

		if err := m.fileWatcher.UnregisterWatcher(ctx, config.WatcherID); err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("failed to unregister generated watcher: %w", err))

			continue
		}

		removed[config.WatcherID] = true
	}

	m.dropRemovedRoots(removed)

	m.emit(ctx, slog.LevelInfo, "watchers_removed", "Removed generated file watchers", slog.Int("count", len(removed)))

	if len(removeErrors) > 0 {
		return errors.Join(removeErrors...)
	}

	return nil
}

// GetRootConfiguration returns the first root configuration the manager holds,
// or nil when none is configured.
//
// The interface exposes a single root, so callers needing all of them should use
// RootConfigurations instead.
func (m *fileWatcherAutoManager) GetRootConfiguration(ctx context.Context) (*core.FileWatcherRootConfiguration, error) {
	roots := m.RootConfigurations()

	if len(roots) == 0 {
		return nil, nil
	}

	first := roots[0]

	return &first, nil
}

// RootConfigurations returns every root configuration the manager holds, in
// application order. The returned slice and its elements are copies.
//
// It is not part of core.FileWatcherAutoManager; it exists so a caller that
// configures multiple roots can inspect all of them.
func (m *fileWatcherAutoManager) RootConfigurations() []core.FileWatcherRootConfiguration {
	m.rootsMu.Lock()
	defer m.rootsMu.Unlock()

	return cloneRootConfigurations(m.roots)
}

// rememberRoot stores a root template, replacing the entry with the same root
// path, and persists the resulting list best-effort.
func (m *fileWatcherAutoManager) rememberRoot(root core.FileWatcherRootConfiguration) {
	m.rootsMu.Lock()

	replaced := false
	for i := range m.roots {
		if m.roots[i].RootPath == root.RootPath {
			m.roots[i] = root
			replaced = true

			break
		}
	}

	if !replaced {
		m.roots = append(m.roots, root)
	}

	roots := cloneRootConfigurations(m.roots)
	m.rootsMu.Unlock()

	if err := m.persistRoots(roots); err != nil {
		m.emit(context.Background(), slog.LevelWarn, "roots_save_failed", "Failed to persist file watcher root configuration",
			slog.String("state_file", watcherRootsFileName), errorTypeAttr(err))
	}
}

// dropRemovedRoots persists the root list after generated watchers were removed.
//
// A root that produced removed watchers is dropped: it is the manager's record
// of the watchers it owns, so keeping it after RemoveAllWatchers would make the
// next discovery recreate exactly what the caller just removed. Roots whose
// generated watchers survive (for example a root that always failed to derive a
// tenant) and roots that produced nothing to remove are kept.
func (m *fileWatcherAutoManager) dropRemovedRoots(removed map[string]bool) {
	m.rootsMu.Lock()

	if len(removed) > 0 {
		kept := make([]core.FileWatcherRootConfiguration, 0, len(m.roots))

		for _, root := range m.roots {
			if anyGeneratedWatcherRemoved(root.RootPath, removed) {
				continue
			}

			kept = append(kept, root)
		}

		m.roots = kept
	}

	roots := cloneRootConfigurations(m.roots)
	m.rootsMu.Unlock()

	if err := m.persistRoots(roots); err != nil {
		m.emit(context.Background(), slog.LevelWarn, "roots_save_failed", "Failed to persist file watcher root configuration",
			slog.String("state_file", watcherRootsFileName), errorTypeAttr(err))
	}
}

// anyGeneratedWatcherRemoved reports whether any removed watcher ID belongs to
// the given root. Generated IDs are "auto-<root base name>-<tenant>", so a
// prefix match on the root segment identifies the root without needing the
// tenant list that may since have changed on disk.
func anyGeneratedWatcherRemoved(rootPath string, removed map[string]bool) bool {
	prefix := generatedWatcherIDPrefix + rootIDBaseName(rootPath) + "-"

	for watcherID := range removed {
		if strings.HasPrefix(watcherID, prefix) {
			return true
		}
	}

	return false
}

// persistRoots writes the root list atomically.
//
// The document is staged in a unique temp file in the same directory, synced,
// closed, and then renamed over the target, so a reader never observes a partial
// document and every failure path removes the staging file.
func (m *fileWatcherAutoManager) persistRoots(roots []core.FileWatcherRootConfiguration) error {
	m.rootsFileMu.Lock()
	defer m.rootsFileMu.Unlock()

	if err := os.MkdirAll(m.configRootDir, 0755); err != nil {
		return fmt.Errorf("failed to create root configuration directory: %w", err)
	}

	document := watcherRootsDocument{
		Version: watcherRootsVersion,
		Roots:   roots,
	}

	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode root configuration: %w", err)
	}

	temp, err := os.CreateTemp(m.configRootDir, watcherRootsTempFilePattern)
	if err != nil {
		return fmt.Errorf("failed to create root configuration staging file: %w", err)
	}

	tempPath := temp.Name()

	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to write root configuration staging file: %w", err)
	}

	// Sync before publishing so a crash after the rename cannot leave an empty
	// document behind.
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to sync root configuration staging file: %w", err)
	}

	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to close root configuration staging file: %w", err)
	}

	if err := os.Rename(tempPath, filepath.Join(m.configRootDir, watcherRootsFileName)); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf("failed to publish root configuration: %w", err)
	}

	return nil
}

// watcherRootsDocument is the persisted root-template list.
type watcherRootsDocument struct {
	// Version is the document schema version.
	Version int `json:"version"`

	// Roots are the root templates to reapply after a restart.
	Roots []core.FileWatcherRootConfiguration `json:"roots"`
}

// loadRootConfigurations reads the persisted root templates.
//
// It returns nil when the document is missing, unreadable, corrupt, or written
// by an unknown schema version: the root list is derived runtime state, so an
// unreadable document must never prevent construction.
func loadRootConfigurations(configRootDir string) []core.FileWatcherRootConfiguration {
	data, err := os.ReadFile(filepath.Join(configRootDir, watcherRootsFileName))
	if err != nil {
		return nil
	}

	var document watcherRootsDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return nil
	}

	if document.Version != watcherRootsVersion {
		return nil
	}

	return cloneRootConfigurations(document.Roots)
}

// generatedWatcherID returns the deterministic ID of the watcher generated for
// one tenant directory of a root.
func generatedWatcherID(rootPath string, tenantID string) string {
	return generatedWatcherIDPrefix + rootIDBaseName(rootPath) + "-" + tenantID
}

// rootIDBaseName returns the root path segment used in a generated watcher ID.
//
// The directory name is used verbatim so the ID stays readable and stable across
// restarts; a path without a usable base name falls back to a constant segment
// rather than producing an empty or trailing-separator ID.
func rootIDBaseName(rootPath string) string {
	base := filepath.Base(filepath.Clean(rootPath))

	switch base {
	case "", ".", "..", string(filepath.Separator):
		return defaultRootNameSegment
	default:
		return base
	}
}

// rootBaseName is the safe logging form of a root path: the base name only,
// never the full physical path.
func rootBaseName(rootPath string) string {
	return rootIDBaseName(rootPath)
}

// cloneRootConfigurations copies a root list, including each element's pattern
// slice, so callers can never mutate stored state.
func cloneRootConfigurations(roots []core.FileWatcherRootConfiguration) []core.FileWatcherRootConfiguration {
	if len(roots) == 0 {
		return nil
	}

	cloned := make([]core.FileWatcherRootConfiguration, len(roots))
	for i, root := range roots {
		cloned[i] = root
		cloned[i].FilePatterns = append([]string(nil), root.FilePatterns...)
	}

	return cloned
}

// mergeRoots overlays optionsRoots on top of persistedRoots: options win per
// RootPath, and a root known only from the state file is preserved.
func mergeRoots(optionsRoots []core.FileWatcherRootConfiguration, persistedRoots []core.FileWatcherRootConfiguration) []core.FileWatcherRootConfiguration {
	merged := cloneRootConfigurations(persistedRoots)

	for _, root := range optionsRoots {
		replaced := false

		for i := range merged {
			if merged[i].RootPath == root.RootPath {
				merged[i] = root
				merged[i].FilePatterns = append([]string(nil), root.FilePatterns...)
				replaced = true

				break
			}
		}

		if !replaced {
			cloned := root
			cloned.FilePatterns = append([]string(nil), root.FilePatterns...)
			merged = append(merged, cloned)
		}
	}

	return merged
}

func (m *fileWatcherAutoManager) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	m.logger.Emit(ctx, logging.Record{
		Level: level, Component: "watcher.auto", Event: event, Message: message, Attrs: attrs,
	})
}
