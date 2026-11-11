package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// metadataDatabaseFileName is the per-tenant SQLite metadata database file
// below the metadata root: {metadataDirectory}/{tenantId}/metadata.db.
const metadataDatabaseFileName = "metadata.db"

// directoryQuotaDatabaseFileName is the per-tenant SQLite directory quota
// database file below the quota root: {quotaDirectory}/{tenantId}/quotas.db.
const directoryQuotaDatabaseFileName = "quotas.db"

// sqliteHeader is the 16-byte magic string every SQLite database file starts
// with ("SQLite format 3" followed by a NUL terminator).
const sqliteHeader = "SQLite format 3\x00"

// minimumSQLiteFileSize is the SQLite file size floor used by the structural
// check: the 100-byte database header page, i.e. the header plus at least one
// complete page of database content. A database can never be smaller.
const minimumSQLiteFileSize = 100

// DatabaseHealthCheckerOptions configures the database health checker.
type DatabaseHealthCheckerOptions struct {
	// MetadataDatabasePath is accepted for backward compatibility and ignored.
	//
	// It used to name the single shared BadgerDB metadata directory
	// ("<MetadataDirectory>/shared/metadata"). The SQLite layout has no shared
	// metadata database: every tenant owns
	// "{MetadataDataPath}/{tenantId}/metadata.db", and the caller has no way to
	// know the tenant set up front, so a single database path cannot describe
	// the layout. Setting the field is not an error; it has no effect.
	MetadataDatabasePath string

	// DirectoryQuotaDatabasePath is accepted for backward compatibility and
	// ignored.
	//
	// It used to name the directory quota BadgerDB directory
	// ("<QuotaDirectory>/quota"). The SQLite layout stores one
	// "{DirectoryQuotaDataPath}/{tenantId}/quotas.db" per tenant, so a single
	// database path cannot describe the layout. Setting the field is not an
	// error; it has no effect.
	DirectoryQuotaDatabasePath string

	// MetadataDataPath is the metadata root that holds the per-tenant metadata
	// databases, conventionally config.Config.MetadataDirectory. The checker
	// derives "{MetadataDataPath}/{tenantId}/metadata.db" from it for every
	// tenant that has physical metadata storage.
	MetadataDataPath string

	// DirectoryQuotaDataPath is the directory quota root that holds the
	// per-tenant quota databases, conventionally config.Config.QuotaDirectory.
	// The checker derives "{DirectoryQuotaDataPath}/{tenantId}/quotas.db" from
	// it. When empty, directory quota databases are not checked.
	DirectoryQuotaDataPath string

	// VolumePaths are the storage volume paths to check for orphaned files.
	VolumePaths []string

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime
}

// databaseHealthChecker implements the core.DatabaseHealthChecker interface.
//
// The checker is deliberately non-invasive: it never opens a SQLite database,
// so it can run while the process holds the live database handles and it never
// touches WAL locks. Corruption detection is structural (see checkSQLiteFile).
type databaseHealthChecker struct {
	metadataDataPath       string
	directoryQuotaDataPath string
	volumePaths            []string
	logger                 *logging.Runtime

	// enumerateTenants reads the tenant set below the metadata root. It is a
	// field so a test can observe the single enumeration the check performs; a
	// nil value falls back to GetTenantIDsFromMetadata.
	enumerateTenants func(metadataDataPath string) ([]string, error)
}

// NewDatabaseHealthChecker creates a new database health checker.
//
// MetadataDataPath must name the metadata root (or, for compatibility, the
// ignored MetadataDatabasePath must be non-empty); otherwise
// core.ErrInvalidArgument is returned. DirectoryQuotaDataPath is optional and
// disables directory quota checks when empty.
//
// MetadataDatabasePath and DirectoryQuotaDatabasePath are accepted and ignored;
// see DatabaseHealthCheckerOptions.
func NewDatabaseHealthChecker(opts *DatabaseHealthCheckerOptions) (core.DatabaseHealthChecker, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.MetadataDataPath == "" && opts.MetadataDatabasePath == "" {
		return nil, fmt.Errorf("metadata data path cannot be empty: %w", core.ErrInvalidArgument)
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	return &databaseHealthChecker{
		metadataDataPath:       opts.MetadataDataPath,
		directoryQuotaDataPath: opts.DirectoryQuotaDataPath,
		volumePaths:            opts.VolumePaths,
		logger:                 logger,
		enumerateTenants:       GetTenantIDsFromMetadata,
	}, nil
}

// tenantIDs reads the tenant set below the metadata root.
func (c *databaseHealthChecker) tenantIDs(metadataDataPath string) ([]string, error) {
	if c.enumerateTenants != nil {
		return c.enumerateTenants(metadataDataPath)
	}
	return GetTenantIDsFromMetadata(metadataDataPath)
}

// CheckAllDatabases checks the health of all databases.
//
// The tenant set is enumerated once from the metadata root, then every
// "{tenantId}/metadata.db" is inspected structurally; the same tenant set
// selects the "{tenantId}/quotas.db" files. Orphan detection always runs when
// volume paths are configured, regardless of how many databases were found or
// reported as corrupted.
func (c *databaseHealthChecker) CheckAllDatabases(ctx context.Context) (*core.DatabaseHealthReport, error) {
	report := &core.DatabaseHealthReport{
		CorruptedDatabases: make([]*core.DatabaseHealthStatus, 0),
		OrphanedTenants:    make([]string, 0),
		DatabaseSizes:      make(map[string]int64),
		AllHealthy:         true,
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tenantIDs, enumerateErr := c.tenantIDs(c.metadataDataPath)
	if enumerateErr != nil {
		// Enumeration failure must not read as health: an unreadable metadata
		// root means zero databases were inspected, and reporting AllHealthy
		// would hide every potentially corrupted database behind a green flag.
		c.emit(ctx, slog.LevelWarn, "metadata_tenant_enumeration_failed", "Error enumerating tenants from the metadata root", errorTypeAttr(enumerateErr))
		report.AllHealthy = false
	}

	metadataDatabases := tenantDatabasePaths(c.metadataDataPath, metadataDatabaseFileName, tenantIDs)

	metadataErr := c.checkMetadataDatabases(ctx, metadataDatabases, report)
	// A cancelled or timed-out check inspected only part of the tenant set. That
	// is not health: without this the report would read as a clean pass over
	// databases that were never opened.
	c.markIncomplete(ctx, report, "metadata", metadataErr)

	if c.directoryQuotaDataPath != "" {
		quotaDatabases := tenantDatabasePaths(c.directoryQuotaDataPath, directoryQuotaDatabaseFileName, tenantIDs)

		healthy, corrupted, quotaErr := c.checkDirectoryQuotaDatabases(ctx, quotaDatabases, report.DatabaseSizes)
		if quotaErr != nil {
			c.emit(ctx, slog.LevelWarn, "quota_check_failed", "Error checking directory quota databases", errorTypeAttr(quotaErr))
			// A quota scan that could not run says nothing about the databases
			// it never inspected, so it must not be reported as healthy either.
			report.AllHealthy = false
			healthy, corrupted = 0, nil
		}

		report.HealthyDatabases += healthy
		if len(corrupted) > 0 {
			report.CorruptedDatabases = append(report.CorruptedDatabases, corrupted...)
			report.AllHealthy = false
		}

		c.markIncomplete(ctx, report, "directory quota", quotaErr)
	}

	// Orphan detection is independent from the database counts: a deployment can
	// have healthy databases and still have volume content without metadata. It
	// reuses the tenant set enumerated above: a second directory read is pure
	// overhead, and it could disagree with the paths that were just checked.
	switch {
	case enumerateErr != nil:
		// The phase could not run without the tenant set, so it must not read as
		// an all-clear either.
		c.emit(ctx, slog.LevelWarn, "orphan_detection_failed", "Error detecting orphaned files", errorTypeAttr(enumerateErr))
		report.AllHealthy = false
	default:
		orphaned, err := c.detectOrphanedFiles(ctx, tenantIDs)
		if err != nil {
			c.emit(ctx, slog.LevelWarn, "orphan_detection_failed", "Error detecting orphaned files", errorTypeAttr(err))
			// A phase that did not run leaves the report incomplete, so it must
			// not read as an all-clear. Cancellation in particular surfaces here
			// rather than in the database loops.
			report.AllHealthy = false
		} else {
			report.OrphanedTenants = orphaned
		}
	}

	return report, nil
}

// markIncomplete records that a scan phase stopped before it inspected every
// database, so the report never presents a partial pass as health.
//
// The verdict is taken from the phase's own result rather than from a later
// ctx.Err(): the context may still be live when the phase stops, and a check
// that ran after the phase could also observe a cancellation that had nothing to
// do with it. Callers pass the error the phase actually returned.
//
// The check is deliberately log-and-flag rather than an error return: the report
// is still the most useful answer available (it carries what was inspected), and
// the health service decides what to do with an unhealthy report.
func (c *databaseHealthChecker) markIncomplete(ctx context.Context, report *core.DatabaseHealthReport, phase string, phaseErr error) {
	if phaseErr == nil {
		return
	}

	report.AllHealthy = false

	c.emit(ctx, slog.LevelWarn, "database_check_incomplete",
		"Database health check did not inspect every database",
		slog.String("phase", phase),
		errorTypeAttr(phaseErr))
}

// checkMetadataDatabases checks every tenant metadata database.
//
// A non-nil error means the phase stopped before every database was inspected,
// so the caller can mark the report incomplete instead of presenting a partial
// pass as health.
func (c *databaseHealthChecker) checkMetadataDatabases(
	ctx context.Context,
	databases []string,
	report *core.DatabaseHealthReport,
) error {
	for _, dbPath := range databases {
		if err := ctx.Err(); err != nil {
			return err
		}

		status := c.checkDatabase(ctx, core.DatabaseTypeMetadata, tenantIDFromDatabasePath(c.metadataDataPath, dbPath), dbPath, report.DatabaseSizes)

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		switch {
		case status.IsHealthy:
			report.HealthyDatabases++
		case status.Error != "":
			report.CorruptedDatabases = append(report.CorruptedDatabases, status)
			report.AllHealthy = false
		}
	}

	return nil
}

// CheckMetadataDatabase checks one tenant's metadata database.
//
// tenantID selects "{MetadataDataPath}/{tenantId}/metadata.db": the SQLite
// layout keys metadata by tenant. An empty tenantID reports the unconfigured
// state, because the migration removed the single shared metadata database this
// method used to check. A non-empty tenantID must be a plain directory name:
// separators, "." and ".." are rejected as invalid arguments instead of being
// joined into an arbitrary path. The check is structural, so the returned
// status is healthy as soon as the database file is present with a valid SQLite
// header and a plausible size; it does not prove that the database can be
// opened or that its write lock is free.
func (c *databaseHealthChecker) CheckMetadataDatabase(ctx context.Context, tenantID string) (*core.DatabaseHealthStatus, error) {
	if tenantID == "" {
		return &core.DatabaseHealthStatus{
			DatabaseType: core.DatabaseTypeMetadata,
			DatabasePath: "",
			IsHealthy:    false,
			Error:        "metadata database is per tenant and requires a tenant ID",
		}, nil
	}

	if err := validateTenantIDSegment(tenantID); err != nil {
		status := &core.DatabaseHealthStatus{
			DatabaseType: core.DatabaseTypeMetadata,
			TenantID:     tenantID,
			DatabasePath: "",
			IsHealthy:    false,
			Error:        fmt.Sprintf("invalid tenant ID: %v", err),
		}

		return status, fmt.Errorf("invalid tenant ID: %w", err)
	}

	dbPath := filepath.Join(c.metadataDataPath, tenantID, metadataDatabaseFileName)

	return c.checkDatabase(ctx, core.DatabaseTypeMetadata, tenantID, dbPath, nil), nil
}

// CheckDirectoryQuotaDatabase checks every tenant directory quota database.
//
// Only the first defect is returned because the method returns a single status;
// CheckAllDatabases reports all of them. A deployment without quota databases is
// healthy with an empty Error: no database has been created yet is not
// corruption.
func (c *databaseHealthChecker) CheckDirectoryQuotaDatabase(ctx context.Context) (*core.DatabaseHealthStatus, error) {
	if c.directoryQuotaDataPath == "" {
		status := &core.DatabaseHealthStatus{
			DatabaseType: core.DatabaseTypeDirectoryQuota,
			DatabasePath: "",
			IsHealthy:    false,
			Error:        "directory quota database path not configured",
		}

		return status, nil
	}

	tenantIDs, err := c.tenantIDs(c.metadataDataPath)
	if err != nil {
		return nil, err
	}

	healthy, corrupted, err := c.checkDirectoryQuotaDatabases(ctx, tenantDatabasePaths(c.directoryQuotaDataPath, directoryQuotaDatabaseFileName, tenantIDs), nil)
	if err != nil {
		return nil, err
	}

	if len(corrupted) > 0 {
		return corrupted[0], nil
	}

	if healthy > 0 {
		return &core.DatabaseHealthStatus{
			DatabaseType: core.DatabaseTypeDirectoryQuota,
			IsHealthy:    true,
		}, nil
	}

	return &core.DatabaseHealthStatus{
		DatabaseType: core.DatabaseTypeDirectoryQuota,
		IsHealthy:    false,
	}, nil
}

// checkDirectoryQuotaDatabases inspects every quota database and returns the
// healthy count plus every corrupted status. When sizes is non-nil it receives
// the size each inspection already measured, so the report does not re-stat the
// same files.
func (c *databaseHealthChecker) checkDirectoryQuotaDatabases(
	ctx context.Context,
	databases []string,
	sizes map[string]int64,
) (healthy int, corrupted []*core.DatabaseHealthStatus, err error) {
	corrupted = make([]*core.DatabaseHealthStatus, 0)

	for _, dbPath := range databases {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return healthy, corrupted, ctxErr
		}

		status := c.checkDatabase(ctx, core.DatabaseTypeDirectoryQuota, tenantIDFromDatabasePath(c.directoryQuotaDataPath, dbPath), dbPath, sizes)

		// A cancellation that landed during the inspection is reported as the
		// phase error rather than as corruption, so the caller marks the report
		// incomplete instead of listing a healthy database as damaged.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return healthy, corrupted, ctxErr
		}

		switch {
		case status.IsHealthy:
			healthy++
		case status.Error != "":
			corrupted = append(corrupted, status)
		}
	}

	// The pre-iteration check cannot catch a cancellation that lands while the
	// last database is being inspected: the loop then exits normally and the
	// phase would look complete. Re-checking after the loop turns that case into
	// the same explicit truncation the caller already handles.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return healthy, corrupted, ctxErr
	}

	return healthy, corrupted, nil
}

// checkDatabase performs a cross-platform structural check of one SQLite
// database file and never opens the database.
//
// Rationale: a read-only SQLite open interacts with WAL "-shm" locking and its
// cost scales with the number of tenant databases (docs/sqlite-storage-design.md
// §18.3 Q6), so the health verdict stays file-level. A missing file means "no
// database yet" and is reported as healthy with an empty Error; only a file that
// exists but fails the SQLite structure rules is reported as corrupted, with a
// path-free reason (DatabasePath already carries the location).
//
// When sizes is non-nil, the size the structural inspection already stat'ed is
// recorded there, so a report does not stat every database a second time.
func (c *databaseHealthChecker) checkDatabase(
	ctx context.Context,
	databaseType core.DatabaseType,
	tenantID string,
	dbPath string,
	sizes map[string]int64,
) *core.DatabaseHealthStatus {
	status := &core.DatabaseHealthStatus{
		DatabaseType: databaseType,
		TenantID:     tenantID,
		DatabasePath: dbPath,
		IsHealthy:    false,
	}

	healthy, size, sizeKnown, err := inspectSQLiteFile(ctx, dbPath)
	if sizeKnown && sizes != nil {
		sizes[dbPath] = size
	}

	switch {
	case err == nil && healthy:
		status.IsHealthy = true
	case err == nil:
		// The file is absent: no database has been created yet. This is a
		// normal state for a new deployment or a tenant without data, and is
		// not corruption.
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		// A cancelled inspection says nothing about the file, so it must not be
		// recorded as corruption. Leaving both flags false marks the database
		// as neither healthy nor corrupted; the phase reports the cancellation
		// through its own error so the report is marked incomplete.
		status.Error = ""
	default:
		status.Error = err.Error()
	}

	return status
}

// DetectOrphanedFiles detects tenants with physical files but no metadata.
func (c *databaseHealthChecker) DetectOrphanedFiles(ctx context.Context) ([]string, error) {
	if len(c.volumePaths) == 0 || c.metadataDataPath == "" {
		return make([]string, 0), nil
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tenantIDs, err := c.tenantIDs(c.metadataDataPath)
	if err != nil {
		return nil, err
	}

	return c.detectOrphanedFiles(ctx, tenantIDs)
}

// detectOrphanedFiles reports the tenants that have physical volume content but
// no entry in tenantIDs.
//
// tenantIDs is the enumeration the caller already performed: re-reading the
// metadata root per health cycle is a second directory scan, and a fresh read
// could disagree with the set the database checks just ran against.
func (c *databaseHealthChecker) detectOrphanedFiles(ctx context.Context, tenantIDs []string) ([]string, error) {
	orphanedTenants := make([]string, 0)

	if len(c.volumePaths) == 0 || c.metadataDataPath == "" {
		return orphanedTenants, nil
	}

	if err := ctx.Err(); err != nil {
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
			// Only a name that is a valid tenant identifier can belong to a
			// tenant directory. This keeps system directories (the dot-prefixed
			// dead-letter root among them) and stray names out of the orphan
			// report instead of reporting them as orphaned tenants forever.
			if err := core.ValidateTenantID(tenantID); err != nil {
				continue
			}
			if existingTenants[tenantID] {
				continue
			}

			if hasFiles(ctx, filepath.Join(volumePath, tenantID)) {
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

// hasFiles checks if a directory has any files (recursively). The walk honours
// ctx so a shutdown does not wait out the traversal of a large volume tree.
func hasFiles(ctx context.Context, dirPath string) bool {
	hasAnyFile := false

	_ = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
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

// GetDatabaseSize returns the size in bytes of one SQLite database file.
// A missing or non-regular path yields 0.
func GetDatabaseSize(dbPath string) (int64, error) {
	return contextualDatabaseSize(context.Background(), dbPath)
}

// contextualDatabaseSize is GetDatabaseSize with cancellation support.
func contextualDatabaseSize(ctx context.Context, dbPath string) (int64, error) {
	if dbPath == "" {
		return 0, nil
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	info, err := os.Stat(dbPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to measure database file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return 0, nil
	}

	return info.Size(), nil
}

// IsDatabaseCorrupted reports whether dbPath is an existing path that fails the
// structural SQLite database rules (wrong header, too short, or not a regular
// file).
//
// The check is structural: it never opens the database, so it works while the
// database is locked by the running process and never touches WAL locks. A path
// that does not exist is not reported as corrupted: no database yet is a normal
// state.
func IsDatabaseCorrupted(dbPath string) bool {
	healthy, _, _, err := inspectSQLiteFile(context.Background(), dbPath)

	return err != nil && !healthy
}

// GetTenantIDsFromMetadata returns the tenant IDs that have physical metadata
// storage below metadataDataPath, in directory order.
//
// Tenant enumeration source: the first-level directories of the metadata root,
// which is the tenant metadata layout this checker already derives paths from
// ({metadataDirectory}/{tenantId}/metadata.db, docs/sqlite-storage-design.md
// §3.1). Directory entries are read once and never opened, so a tenant with
// many databases does not multiply file handles. Entries are skipped when their
// name is not a valid tenant identifier (core.ValidateTenantID), which also
// excludes internal dot-directories such as ".locus"; those directories hold the
// tenant registry JSON files, not databases, and are handled by pkg/tenant.
// Underscore-prefixed directory names stay reserved for system state, as they
// were before the storage migration.
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
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasPrefix(name, "_") {
			continue
		}
		if core.ValidateTenantID(name) != nil {
			continue
		}

		tenantIDs = append(tenantIDs, name)
	}

	return tenantIDs, nil
}

// tenantDatabasePaths returns the per-tenant database path of every tenant in
// "{root}/{tenantId}/{fileName}" form. A tenant without a database is included:
// a missing file means "no database yet", and the caller needs the path to
// report that state.
func tenantDatabasePaths(root, fileName string, tenantIDs []string) []string {
	if root == "" || len(tenantIDs) == 0 {
		return nil
	}

	paths := make([]string, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		paths = append(paths, filepath.Join(root, tenantID, fileName))
	}

	return paths
}

// tenantIDFromDatabasePath recovers the tenant ID of a
// "{root}/{tenantId}/{fileName}" path so a status never needs a second lookup.
func tenantIDFromDatabasePath(root, dbPath string) string {
	if root == "" || dbPath == "" {
		return ""
	}

	relative, err := filepath.Rel(root, dbPath)
	if err != nil {
		return ""
	}

	tenantID := filepath.Dir(relative)
	if tenantID == "." || strings.HasPrefix(tenantID, "..") {
		return ""
	}

	return tenantID
}

// inspectSQLiteFile classifies one database file structurally and never opens
// the database through a driver.
//
// It returns:
//   - (false, 0, true, nil) when dbPath is absent: there is no database file yet.
//   - (true, size, true, nil) when the file is regular, at least
//     minimumSQLiteFileSize bytes long and starts with the 16-byte SQLite header.
//   - (false, size, true, err) when the path exists but fails one of those rules,
//     including a non-regular entry such as a directory. err is a safe, path-free
//     reason that names the precise defect. size is 0 for a non-regular entry,
//     matching what a plain size read could report for it.
//   - (false, 0, false, err) when the file could not be stat'ed: the size is
//     unknown and must not be reported.
//
// size is the size the classification already stat'ed, so a caller that reports
// on-disk database sizes does not have to stat every file again.
func inspectSQLiteFile(ctx context.Context, dbPath string) (healthy bool, size int64, sizeKnown bool, err error) {
	if dbPath == "" {
		return false, 0, false, nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, 0, false, ctxErr
	}

	info, statErr := os.Stat(dbPath)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		return false, 0, true, nil
	case statErr != nil:
		return false, 0, false, fmt.Errorf("failed to stat database file: %w", statErr)
	case !info.Mode().IsRegular():
		return false, 0, true, fmt.Errorf("database path is not a regular file, so it cannot hold a SQLite database")
	}

	if info.Size() < minimumSQLiteFileSize {
		return false, info.Size(), true, fmt.Errorf(
			"database file is %d bytes; a SQLite database needs at least %d bytes for its header page",
			info.Size(), minimumSQLiteFileSize)
	}

	header, readErr := readSQLiteHeader(dbPath)
	if readErr != nil {
		return false, info.Size(), true, readErr
	}

	if string(header) != sqliteHeader {
		return false, info.Size(), true, fmt.Errorf("file does not start with the %d-byte SQLite format 3 header", len(sqliteHeader))
	}

	return true, info.Size(), true, nil
}

// readSQLiteHeader reads the leading SQLite magic bytes without using a
// database driver. The file is closed before returning on every path.
func readSQLiteHeader(dbPath string) ([]byte, error) {
	file, err := os.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file for a header read: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	header := make([]byte, len(sqliteHeader))

	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read the SQLite header: %w", err)
	}

	return header, nil
}

// validateTenantIDSegment rejects tenant IDs that must never be joined into a
// filesystem path. It delegates to the single canonical validator so the health
// package cannot drift from the tenant and storage layers.
func validateTenantIDSegment(tenantID string) error {
	return core.ValidateTenantID(tenantID)
}
