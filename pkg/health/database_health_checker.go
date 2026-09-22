package health

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// metadataSharedDirectoryName is the shared (tenant-independent) metadata
// directory below the configured metadata root. The real BadgerDB lives one
// level deeper, in metadataDatabaseDirectoryName.
const metadataSharedDirectoryName = "shared"

// metadataDatabaseDirectoryName is the BadgerDB directory name used by the
// metadata repository below its data path.
const metadataDatabaseDirectoryName = "metadata"

// directoryQuotaDatabaseDirectoryName is the BadgerDB directory name used by
// the directory quota repository below its data path.
const directoryQuotaDatabaseDirectoryName = "quota"

// DatabaseHealthCheckerOptions configures the database health checker.
type DatabaseHealthCheckerOptions struct {
	// MetadataDatabasePath is the real BadgerDB directory that stores the shared
	// metadata projection, conventionally "<MetadataDirectory>/shared/metadata".
	// When empty it is derived from MetadataDataPath as
	// filepath.Join(MetadataDataPath, "shared", "metadata").
	MetadataDatabasePath string

	// DirectoryQuotaDatabasePath is the real BadgerDB directory that stores the
	// directory quota projection, conventionally "<QuotaDirectory>/quota".
	// When empty it is derived from DirectoryQuotaDataPath as
	// filepath.Join(DirectoryQuotaDataPath, "quota").
	DirectoryQuotaDatabasePath string

	// MetadataDataPath is the root path for metadata databases. It is the
	// legacy fallback for MetadataDatabasePath and is also used to enumerate
	// tenant IDs for orphan detection.
	MetadataDataPath string

	// DirectoryQuotaDataPath is the legacy root path for the directory quota
	// database. It is the fallback for DirectoryQuotaDatabasePath.
	DirectoryQuotaDataPath string

	// VolumePaths are the storage volume paths to check for orphaned files.
	VolumePaths []string

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime
}

// databaseHealthChecker implements the core.DatabaseHealthChecker interface.
//
// The checker is deliberately non-invasive: it never opens a BadgerDB handle,
// so it can run while the process holds the live database locks. Corruption
// detection is structural (see checkBadgerStructure).
type databaseHealthChecker struct {
	metadataDatabasePath       string
	directoryQuotaDatabasePath string
	metadataDataPath           string
	directoryQuotaDataPath     string
	volumePaths                []string
	logger                     *logging.Runtime
}

// NewDatabaseHealthChecker creates a new database health checker.
//
// Either the explicit badger database paths (MetadataDatabasePath /
// DirectoryQuotaDatabasePath) or their legacy roots (MetadataDataPath /
// DirectoryQuotaDataPath) must identify a metadata database; otherwise
// core.ErrInvalidArgument is returned.
func NewDatabaseHealthChecker(opts *DatabaseHealthCheckerOptions) (core.DatabaseHealthChecker, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	metadataDatabasePath := opts.MetadataDatabasePath
	if metadataDatabasePath == "" {
		metadataDatabasePath = deriveMetadataDatabasePath(opts.MetadataDataPath)
	}
	if metadataDatabasePath == "" {
		return nil, fmt.Errorf("metadata database path cannot be empty: %w", core.ErrInvalidArgument)
	}

	directoryQuotaDatabasePath := opts.DirectoryQuotaDatabasePath
	if directoryQuotaDatabasePath == "" {
		directoryQuotaDatabasePath = deriveDirectoryQuotaDatabasePath(opts.DirectoryQuotaDataPath)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	return &databaseHealthChecker{
		metadataDatabasePath:       metadataDatabasePath,
		directoryQuotaDatabasePath: directoryQuotaDatabasePath,
		metadataDataPath:           opts.MetadataDataPath,
		directoryQuotaDataPath:     opts.DirectoryQuotaDataPath,
		volumePaths:                opts.VolumePaths,
		logger:                     logger,
	}, nil
}

// deriveMetadataDatabasePath returns the conventional badger directory below a
// metadata root, or "" when the root is empty.
func deriveMetadataDatabasePath(root string) string {
	if root == "" {
		return ""
	}

	return filepath.Join(root, metadataSharedDirectoryName, metadataDatabaseDirectoryName)
}

// deriveDirectoryQuotaDatabasePath returns the conventional badger directory
// below a directory quota root, or "" when the root is empty.
func deriveDirectoryQuotaDatabasePath(root string) string {
	if root == "" {
		return ""
	}

	return filepath.Join(root, directoryQuotaDatabaseDirectoryName)
}

// CheckAllDatabases checks the health of all databases.
//
// Orphan detection always runs when volume paths are configured, regardless of
// how many databases were found or reported as corrupted.
func (c *databaseHealthChecker) CheckAllDatabases(ctx context.Context) (*core.DatabaseHealthReport, error) {
	report := &core.DatabaseHealthReport{
		CorruptedDatabases: make([]*core.DatabaseHealthStatus, 0),
		OrphanedTenants:    make([]string, 0),
		DatabaseSizes:      make(map[string]int64),
		AllHealthy:         true,
	}

	if err := c.checkMetadataDatabases(ctx, report); err != nil {
		c.emit(ctx, slog.LevelWarn, "metadata_check_failed", "Error checking metadata databases", errorTypeAttr(err))
	}

	if c.directoryQuotaDatabasePath != "" {
		status, err := c.CheckDirectoryQuotaDatabase(ctx)
		if err != nil {
			c.emit(ctx, slog.LevelWarn, "quota_check_failed", "Error checking directory quota database", errorTypeAttr(err))
		} else if status.IsHealthy {
			report.HealthyDatabases++
		} else if status.Error != "" {
			report.CorruptedDatabases = append(report.CorruptedDatabases, status)
			report.AllHealthy = false
		}
	}

	// Orphan detection is independent from the database counts: a deployment can
	// have healthy databases and still have volume content without metadata.
	orphaned, err := c.DetectOrphanedFiles(ctx)
	if err != nil {
		c.emit(ctx, slog.LevelWarn, "orphan_detection_failed", "Error detecting orphaned files", errorTypeAttr(err))
	} else {
		report.OrphanedTenants = orphaned
	}

	c.collectDatabaseSizes(ctx, report)

	return report, nil
}

// checkMetadataDatabases checks the shared metadata database.
func (c *databaseHealthChecker) checkMetadataDatabases(ctx context.Context, report *core.DatabaseHealthReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	status, err := c.CheckMetadataDatabase(ctx, "")
	if err != nil {
		return err
	}

	switch {
	case status.IsHealthy:
		report.HealthyDatabases++
	case status.Error != "":
		report.CorruptedDatabases = append(report.CorruptedDatabases, status)
		report.AllHealthy = false
	}

	return nil
}

// CheckMetadataDatabase checks a database below the metadata root.
//
// tenantID is a legacy per-tenant selector. An empty tenantID checks the real
// shared metadata database (MetadataDatabasePath). A non-empty tenantID must be
// a plain directory name: separators, "." and ".." are rejected as invalid
// arguments instead of being joined into an arbitrary path. The check is
// structural, so the returned status is healthy as soon as the database
// artifacts are present and readable; it does not prove that the database can
// be opened or that its write lock is free.
func (c *databaseHealthChecker) CheckMetadataDatabase(ctx context.Context, tenantID string) (*core.DatabaseHealthStatus, error) {
	dbPath := c.metadataDatabasePath
	if tenantID != "" {
		if err := validateTenantIDSegment(tenantID); err != nil {
			status := &core.DatabaseHealthStatus{
				DatabaseType: core.DatabaseTypeMetadata,
				TenantID:     tenantID,
				DatabasePath: dbPath,
				IsHealthy:    false,
				Error:        fmt.Sprintf("invalid tenant ID: %v", err),
			}

			return status, fmt.Errorf("invalid tenant ID: %w", err)
		}

		tenantRoot := filepath.Join(c.metadataDataPath, tenantID)
		dbPath = filepath.Join(tenantRoot, metadataDatabaseDirectoryName)
	}

	return c.checkDatabase(ctx, core.DatabaseTypeMetadata, tenantID, dbPath), nil
}

// CheckDirectoryQuotaDatabase checks the directory quota database.
func (c *databaseHealthChecker) CheckDirectoryQuotaDatabase(ctx context.Context) (*core.DatabaseHealthStatus, error) {
	if c.directoryQuotaDatabasePath == "" {
		status := &core.DatabaseHealthStatus{
			DatabaseType: core.DatabaseTypeDirectoryQuota,
			DatabasePath: "",
			IsHealthy:    false,
			Error:        "directory quota database path not configured",
		}

		return status, nil
	}

	return c.checkDatabase(ctx, core.DatabaseTypeDirectoryQuota, "", c.directoryQuotaDatabasePath), nil
}

// checkDatabase performs a cross-platform structural check of a BadgerDB
// directory and never opens the database.
//
// Rationale: Badger v4 rejects read-only mode on Windows
// (ErrWindowsNotSupported), so opening the live database is both impossible and
// undesirable. A missing directory means "no database yet" and is reported as
// healthy with an empty Error; only a directory that exists but lacks required
// artifacts is reported as corrupted.
func (c *databaseHealthChecker) checkDatabase(
	ctx context.Context,
	databaseType core.DatabaseType,
	tenantID string,
	dbPath string,
) *core.DatabaseHealthStatus {
	status := &core.DatabaseHealthStatus{
		DatabaseType: databaseType,
		TenantID:     tenantID,
		DatabasePath: dbPath,
		IsHealthy:    false,
	}

	_, healthy, err := checkBadgerStructure(ctx, dbPath)
	switch {
	case err == nil && healthy:
		status.IsHealthy = true
	case err == nil:
		// Directory is absent: no database has been created yet. This is a
		// normal state for a new deployment and is not corruption.
	default:
		status.Error = err.Error()
	}

	return status
}

// collectDatabaseSizes records the on-disk size of each configured database
// directory so the report carries an operational signal without extra I/O for
// healthy-only layouts.
func (c *databaseHealthChecker) collectDatabaseSizes(ctx context.Context, report *core.DatabaseHealthReport) {
	for _, dbPath := range []string{c.metadataDatabasePath, c.directoryQuotaDatabasePath} {
		if dbPath == "" {
			continue
		}

		if _, err := os.Stat(dbPath); err != nil {
			continue
		}

		size, err := contextualDatabaseSize(ctx, dbPath)
		if err != nil {
			continue
		}

		report.DatabaseSizes[dbPath] = size
	}
}

// DetectOrphanedFiles detects tenants with physical files but no metadata.
func (c *databaseHealthChecker) DetectOrphanedFiles(ctx context.Context) ([]string, error) {
	orphanedTenants := make([]string, 0)

	if len(c.volumePaths) == 0 || c.metadataDataPath == "" {
		return orphanedTenants, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Get existing tenant IDs from the metadata root.
	tenantIDs, err := GetTenantIDsFromMetadata(c.metadataDataPath)
	if err != nil {
		return nil, err
	}

	existingTenants := make(map[string]bool, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		existingTenants[tenantID] = true
	}

	// Check each volume for tenant directories with files.
	for _, volumePath := range c.volumePaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if _, err := os.Stat(volumePath); os.IsNotExist(err) {
			continue
		}

		entries, err := os.ReadDir(volumePath)
		if err != nil {
			c.emit(ctx, slog.LevelWarn, "volume_scan_failed", "Failed to read volume directory", errorTypeAttr(err))
			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			tenantID := entry.Name()
			if existingTenants[tenantID] {
				continue
			}

			if hasFiles(filepath.Join(volumePath, tenantID)) {
				orphanedTenants = append(orphanedTenants, tenantID)
			}
		}
	}

	return orphanedTenants, nil
}

func (c *databaseHealthChecker) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	c.logger.Emit(ctx, logging.Record{
		Level: level, Component: "health.database_checker", Event: event, Message: message, Attrs: attrs,
	})
}

func errorTypeAttr(err error) slog.Attr {
	return slog.String("error_type", fmt.Sprintf("%T", err))
}

// hasFiles checks if a directory has any files (recursively).
func hasFiles(dirPath string) bool {
	hasAnyFile := false

	_ = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if !info.IsDir() {
			hasAnyFile = true
			return filepath.SkipAll
		}

		return nil
	})

	return hasAnyFile
}

// GetDatabaseSize returns the size of a database directory in bytes.
// A missing directory yields 0.
func GetDatabaseSize(dbPath string) (int64, error) {
	return contextualDatabaseSize(context.Background(), dbPath)
}

// contextualDatabaseSize is GetDatabaseSize with cancellation support.
func contextualDatabaseSize(ctx context.Context, dbPath string) (int64, error) {
	if dbPath == "" {
		return 0, nil
	}

	var size int64

	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		if !info.IsDir() {
			size += info.Size()
		}

		return nil
	})
	if err != nil {
		return size, fmt.Errorf("failed to measure database directory: %w", err)
	}

	return size, nil
}

// IsDatabaseCorrupted reports whether dbPath looks like a corrupted BadgerDB
// directory.
//
// The check is structural: it never opens the database, so it works on Windows
// (where Badger rejects read-only mode) and while the database is locked by the
// running process. A path that does not exist, or that exists but is not a
// database directory at all, is NOT reported as corrupted.
func IsDatabaseCorrupted(dbPath string) bool {
	_, healthy, err := checkBadgerStructure(context.Background(), dbPath)

	return err != nil && !healthy
}

// GetTenantIDsFromMetadata returns all tenant IDs that have metadata databases.
func GetTenantIDsFromMetadata(metadataDataPath string) ([]string, error) {
	tenantIDs := make([]string, 0)

	if metadataDataPath == "" {
		return tenantIDs, nil
	}

	if _, err := os.Stat(metadataDataPath); os.IsNotExist(err) {
		return tenantIDs, nil
	}

	entries, err := os.ReadDir(metadataDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Exclude system directories
			name := entry.Name()
			if !strings.HasPrefix(name, ".") && !strings.HasPrefix(name, "_") {
				tenantIDs = append(tenantIDs, name)
			}
		}
	}

	return tenantIDs, nil
}

// badgerManifestPrefix is the file name prefix BadgerDB uses for its MANIFEST.
const badgerManifestPrefix = "MANIFEST"

// badgerValueLogExtension is the file extension of a BadgerDB value log file.
const badgerValueLogExtension = ".vlog"

// badgerKeyRegistryName is the BadgerDB key registry file name.
const badgerKeyRegistryName = "KEYREGISTRY"

// checkBadgerStructure classifies dbPath structurally and never opens the
// database.
//
// It returns:
//   - (false, false, nil) when dbPath does not exist: no database yet.
//   - (false, false, nil) when dbPath exists but contains no BadgerDB artifacts:
//     the directory is not a database (for example a tenant JSON store, a shared
//     parent directory, or a storage volume root), which is not corruption.
//   - (true,  true,  nil) when a readable non-empty MANIFEST plus a value log or
//     key registry are present.
//   - (true,  false, err) when the directory looks like a database but a required
//     artifact is missing or unreadable; err names the precise defect.
func checkBadgerStructure(ctx context.Context, dbPath string) (isDatabase bool, healthy bool, err error) {
	if dbPath == "" {
		return false, false, nil
	}

	if err := ctx.Err(); err != nil {
		return false, false, err
	}

	info, statErr := os.Stat(dbPath)
	switch {
	case os.IsNotExist(statErr):
		return false, false, nil
	case statErr != nil:
		return false, false, fmt.Errorf("failed to stat database directory: %w", statErr)
	case !info.IsDir():
		return false, false, nil
	}

	entries, readErr := os.ReadDir(dbPath)
	if readErr != nil {
		return false, false, fmt.Errorf("failed to read database directory: %w", readErr)
	}

	hasManifest := false
	hasValueLog := false
	hasKeyRegistry := false

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		switch {
		case strings.HasPrefix(name, badgerManifestPrefix):
			hasManifest = true
		case strings.EqualFold(filepath.Ext(name), badgerValueLogExtension):
			hasValueLog = true
		case strings.EqualFold(name, badgerKeyRegistryName):
			hasKeyRegistry = true
		}
	}

	if !hasManifest && !hasValueLog && !hasKeyRegistry {
		// Not a BadgerDB directory at all.
		return false, false, nil
	}

	if !hasManifest {
		return true, false, fmt.Errorf("required BadgerDB artifact %s is missing from the database directory", badgerManifestPrefix)
	}

	if !hasValueLog && !hasKeyRegistry {
		return true, false, fmt.Errorf("required BadgerDB artifact %s or %s is missing from the database directory", badgerValueLogExtension, badgerKeyRegistryName)
	}

	manifestInfo, manifestErr := readManifestInfo(dbPath, entries)
	if manifestErr != nil {
		return true, false, manifestErr
	}

	if manifestInfo.Size() == 0 {
		return true, false, fmt.Errorf("BadgerDB artifact %s is empty at %s", badgerManifestPrefix, manifestInfo.Name())
	}

	return true, true, nil
}

// readManifestInfo returns the file info of the first MANIFEST entry.
func readManifestInfo(dbPath string, entries []os.DirEntry) (os.FileInfo, error) {
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), badgerManifestPrefix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("failed to read BadgerDB artifact %s: %w", badgerManifestPrefix, err)
		}

		return info, nil
	}

	return nil, fmt.Errorf("required BadgerDB artifact %s is missing from the database directory %s", badgerManifestPrefix, dbPath)
}

// validateTenantIDSegment rejects tenant IDs that must never be joined into a
// filesystem path. It delegates to the single canonical validator so the health
// package cannot drift from the tenant and storage layers.
func validateTenantIDSegment(tenantID string) error {
	return core.ValidateTenantID(tenantID)
}
