package health

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// sqliteHeaderFixture is a byte-for-byte valid SQLite database header page:
// "SQLite format 3\0", a 4096-byte page size and a zeroed remainder. The
// structural check only needs the magic plus the 100-byte header page, so no
// SQLite engine is involved and no genuinely corrupt database is created.
func sqliteHeaderFixture() []byte {
	header := make([]byte, minimumSQLiteFileSize)
	copy(header, sqliteHeader)
	binary.BigEndian.PutUint16(header[16:18], 4096)

	return header
}

// writeValidSQLiteDatabase writes a structurally valid SQLite database file.
func writeValidSQLiteDatabase(t *testing.T, dbPath string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("create database directory: %v", err)
	}

	if err := os.WriteFile(dbPath, sqliteHeaderFixture(), 0o644); err != nil {
		t.Fatalf("write valid SQLite database: %v", err)
	}
}

// writeCorruptSQLiteDatabase writes a database file whose content does not start
// with the SQLite magic, which is the only "corrupt" shape the structural check
// judges. A genuinely damaged SQLite file is deliberately not created.
func writeCorruptSQLiteDatabase(t *testing.T, dbPath string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("create database directory: %v", err)
	}

	corrupt := make([]byte, minimumSQLiteFileSize)
	copy(corrupt, "This is not a SQLite database header........")

	if err := os.WriteFile(dbPath, corrupt, 0o644); err != nil {
		t.Fatalf("write corrupt SQLite database: %v", err)
	}
}

// writeShortSQLiteDatabase writes a file that carries the SQLite magic but is
// shorter than one header page.
func writeShortSQLiteDatabase(t *testing.T, dbPath string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("create database directory: %v", err)
	}

	if err := os.WriteFile(dbPath, []byte(sqliteHeader), 0o644); err != nil {
		t.Fatalf("write short SQLite database: %v", err)
	}
}

// sqliteVolume carries the per-tenant layout every health check derives:
// {metadataRoot}/{tenantId}/metadata.db and {quotaRoot}/{tenantId}/quotas.db.
type sqliteVolume struct {
	metadataRoot string
	quotaRoot    string
}

// newSQLiteVolume creates the two roots below one temporary directory.
func newSQLiteVolume(t *testing.T) sqliteVolume {
	t.Helper()

	root := t.TempDir()

	return sqliteVolume{
		metadataRoot: filepath.Join(root, "metadata"),
		quotaRoot:    filepath.Join(root, "quota"),
	}
}

// metadataDatabasePath returns the per-tenant metadata database path.
func (v sqliteVolume) metadataDatabasePath(tenantID string) string {
	return filepath.Join(v.metadataRoot, tenantID, metadataDatabaseFileName)
}

// quotaDatabasePath returns the per-tenant quota database path.
func (v sqliteVolume) quotaDatabasePath(tenantID string) string {
	return filepath.Join(v.quotaRoot, tenantID, directoryQuotaDatabaseFileName)
}

// newHealthySQLiteLayout builds a tenant with a valid metadata.db and a valid
// quotas.db and returns the shared volume.
func newHealthySQLiteLayout(t *testing.T) (sqliteVolume, string) {
	t.Helper()

	const tenantID = "tenant-a"

	volume := newSQLiteVolume(t)
	writeValidSQLiteDatabase(t, volume.metadataDatabasePath(tenantID))
	writeValidSQLiteDatabase(t, volume.quotaDatabasePath(tenantID))

	return volume, tenantID
}

// newChecker builds a checker for a volume, failing the test on error.
func newChecker(t *testing.T, opts *DatabaseHealthCheckerOptions) core.DatabaseHealthChecker {
	t.Helper()

	checker, err := NewDatabaseHealthChecker(opts)
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	return checker
}

func TestDatabaseHealthChecker_HealthyLayout(t *testing.T) {
	volume, tenantID := newHealthySQLiteLayout(t)

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true; corrupted = %v", describeStatuses(report.CorruptedDatabases))
	}

	// One tenant metadata database plus one tenant quota database.
	if report.HealthyDatabases != 2 {
		t.Fatalf("HealthyDatabases = %d, want 2", report.HealthyDatabases)
	}

	if len(report.CorruptedDatabases) != 0 {
		t.Fatalf("CorruptedDatabases = %v, want empty", describeStatuses(report.CorruptedDatabases))
	}

	size, ok := report.DatabaseSizes[volume.metadataDatabasePath(tenantID)]
	if !ok {
		t.Fatalf("DatabaseSizes = %v, want an entry for the metadata database", report.DatabaseSizes)
	}
	if size != int64(minimumSQLiteFileSize) {
		t.Fatalf("DatabaseSizes[metadata.db] = %d, want %d", size, minimumSQLiteFileSize)
	}

	size, ok = report.DatabaseSizes[volume.quotaDatabasePath(tenantID)]
	if !ok {
		t.Fatalf("DatabaseSizes = %v, want an entry for the quota database", report.DatabaseSizes)
	}
	if size != int64(minimumSQLiteFileSize) {
		t.Fatalf("DatabaseSizes[quotas.db] = %d, want %d", size, minimumSQLiteFileSize)
	}
}

func TestDatabaseHealthChecker_IgnoresLegacyBadgerDatabasePaths(t *testing.T) {
	volume, tenantID := newHealthySQLiteLayout(t)

	// The pre-migration wiring passed the two Badger database directories. They
	// are still accepted, but the roots are what the checker derives from.
	legacyMetadataPath := filepath.Join(volume.metadataRoot, "shared", "metadata")
	legacyQuotaPath := filepath.Join(volume.quotaRoot, "quota")

	if err := os.MkdirAll(legacyMetadataPath, 0o755); err != nil {
		t.Fatalf("mkdir legacy metadata path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyMetadataPath, "MANIFEST"), []byte("manifest-bytes"), 0o644); err != nil {
		t.Fatalf("write legacy MANIFEST: %v", err)
	}
	if err := os.MkdirAll(legacyQuotaPath, 0o755); err != nil {
		t.Fatalf("mkdir legacy quota path: %v", err)
	}

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:           volume.metadataRoot,
		DirectoryQuotaDataPath:     volume.quotaRoot,
		MetadataDatabasePath:       legacyMetadataPath,
		DirectoryQuotaDatabasePath: legacyQuotaPath,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy || report.HealthyDatabases != 2 {
		t.Fatalf("report = {healthy:%d corrupted:%v allHealthy:%v}, want 2 healthy",
			report.HealthyDatabases, describeStatuses(report.CorruptedDatabases), report.AllHealthy)
	}

	// Nothing is reported for the ignored legacy paths.
	for _, legacyPath := range []string{legacyMetadataPath, legacyQuotaPath} {
		if _, ok := report.DatabaseSizes[legacyPath]; ok {
			t.Fatalf("DatabaseSizes = %v, want no entry for the ignored legacy path", report.DatabaseSizes)
		}
	}

	if _, ok := report.DatabaseSizes[volume.metadataDatabasePath(tenantID)]; !ok {
		t.Fatalf("DatabaseSizes = %v, want the per-tenant metadata database", report.DatabaseSizes)
	}
}

func TestNewDatabaseHealthChecker_RequiresAMetadataRoot(t *testing.T) {
	if _, err := NewDatabaseHealthChecker(nil); err == nil {
		t.Fatalf("NewDatabaseHealthChecker(nil) error = nil, want error")
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath: filepath.Join(t.TempDir(), "shared", "metadata"),
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() with only the legacy path error = %v, want nil for compatibility", err)
	}
	if checker == nil {
		t.Fatalf("checker = nil, want a checker")
	}

	if _, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{}); err == nil {
		t.Fatalf("NewDatabaseHealthChecker(empty) error = nil, want error")
	} else if !strings.Contains(err.Error(), core.ErrInvalidArgument.Error()) {
		t.Fatalf("error = %v, want it to wrap ErrInvalidArgument", err)
	}
}

func TestDatabaseHealthChecker_EmptyDeploymentReportsNoDatabases(t *testing.T) {
	volume := newSQLiteVolume(t)

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if len(report.CorruptedDatabases) != 0 {
		t.Fatalf("CorruptedDatabases = %v, want empty for a new deployment", describeStatuses(report.CorruptedDatabases))
	}

	if report.HealthyDatabases != 0 {
		t.Fatalf("HealthyDatabases = %d, want 0", report.HealthyDatabases)
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true when nothing is corrupted")
	}

	if len(report.DatabaseSizes) != 0 {
		t.Fatalf("DatabaseSizes = %v, want empty when no database file exists", report.DatabaseSizes)
	}
}

func TestDatabaseHealthChecker_NonDatabaseDirectoriesAreNotCorruption(t *testing.T) {
	volume := newSQLiteVolume(t)

	// Directories that a path-based check could mistake for databases: the
	// tenant registry root, a shared parent and a quota root.
	for _, dir := range []string{
		filepath.Join(volume.metadataRoot, ".locus", "tenants"),
		filepath.Join(volume.metadataRoot, "shared"),
		filepath.Join(volume.quotaRoot, "quota"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "tenant.json"), []byte("{}"), 0o644); err != nil {
			t.Fatalf("write tenant.json: %v", err)
		}
	}

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if len(report.CorruptedDatabases) != 0 {
		t.Fatalf("CorruptedDatabases = %v, want empty for non-database directories", describeStatuses(report.CorruptedDatabases))
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true for non-database directories")
	}

	// The internal ".locus" directory is not a tenant and has no database file.
	if report.HealthyDatabases != 0 {
		t.Fatalf("HealthyDatabases = %d, want 0 for directories without a database file", report.HealthyDatabases)
	}
}

func TestDatabaseHealthChecker_DirectoryWhereTheDatabaseFileBelongsIsCorruption(t *testing.T) {
	volume := newSQLiteVolume(t)
	const tenantID = "tenant-a"

	// A directory where metadata.db belongs cannot hold a SQLite database:
	// SQLite would fail to open it, so the structural check reports corruption.
	if err := os.MkdirAll(volume.metadataDatabasePath(tenantID), 0o755); err != nil {
		t.Fatalf("mkdir directory named metadata.db: %v", err)
	}

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
	})

	status, err := checker.CheckMetadataDatabase(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("CheckMetadataDatabase() error = %v", err)
	}
	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false for a directory in place of the database file")
	}
	if !strings.Contains(status.Error, "not a regular file") {
		t.Fatalf("status.Error = %q, want it to name the non-regular path", status.Error)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}
	if report.AllHealthy || len(report.CorruptedDatabases) != 1 {
		t.Fatalf("report = {healthy:%d corrupted:%v allHealthy:%v}, want one corrupted database",
			report.HealthyDatabases, describeStatuses(report.CorruptedDatabases), report.AllHealthy)
	}
}

func TestDatabaseHealthChecker_WrongHeaderIsCorruption(t *testing.T) {
	volume := newSQLiteVolume(t)
	const tenantID = "tenant-a"

	writeCorruptSQLiteDatabase(t, volume.metadataDatabasePath(tenantID))

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if report.AllHealthy {
		t.Fatalf("AllHealthy = true, want false when the SQLite header is wrong")
	}

	if len(report.CorruptedDatabases) != 1 {
		t.Fatalf("CorruptedDatabases = %v, want exactly one entry", describeStatuses(report.CorruptedDatabases))
	}

	status := report.CorruptedDatabases[0]
	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false")
	}

	if status.DatabaseType != core.DatabaseTypeMetadata {
		t.Fatalf("status.DatabaseType = %q, want %q", status.DatabaseType, core.DatabaseTypeMetadata)
	}

	if status.TenantID != tenantID {
		t.Fatalf("status.TenantID = %q, want %q", status.TenantID, tenantID)
	}

	// The report carries the location; the reason must not.
	if status.DatabasePath != volume.metadataDatabasePath(tenantID) {
		t.Fatalf("status.DatabasePath = %q, want the per-tenant metadata database path", status.DatabasePath)
	}

	if !strings.Contains(status.Error, "SQLite") {
		t.Fatalf("status.Error = %q, want it to name the SQLite header", status.Error)
	}

	if strings.Contains(status.Error, volume.metadataRoot) || strings.Contains(status.Error, tenantID) {
		t.Fatalf("status.Error = %q, want a path-free reason", status.Error)
	}
}

func TestDatabaseHealthChecker_TooShortFileIsCorruption(t *testing.T) {
	volume := newSQLiteVolume(t)
	const tenantID = "tenant-a"

	writeShortSQLiteDatabase(t, volume.metadataDatabasePath(tenantID))

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
	})

	status, err := checker.CheckMetadataDatabase(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("CheckMetadataDatabase() error = %v", err)
	}

	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false")
	}

	if !strings.Contains(status.Error, "100 bytes") {
		t.Fatalf("status.Error = %q, want it to name the 100-byte minimum", status.Error)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if report.AllHealthy || report.HealthyDatabases != 0 || len(report.CorruptedDatabases) != 1 {
		t.Fatalf("report = {healthy:%d corrupted:%v allHealthy:%v}, want one corrupted database",
			report.HealthyDatabases, describeStatuses(report.CorruptedDatabases), report.AllHealthy)
	}
}

func TestDatabaseHealthChecker_MissingFileForTenantWithStorageIsAnOrphanNotCorruption(t *testing.T) {
	volume := newSQLiteVolume(t)
	const (
		orphanTenant  = "orphan-tenant"
		healthyTenant = "known-tenant"
		noDatabaseYet = "pending-tenant"
	)

	volumeRoot := filepath.Join(t.TempDir(), "volume")

	// The orphan tenant has physical storage but no metadata layout at all,
	// which is the existing orphaned-tenant signal, not corruption.
	writeTenantPayload(t, volumeRoot, orphanTenant)

	// A tenant with a valid metadata.db and physical files is fully known.
	writeValidSQLiteDatabase(t, volume.metadataDatabasePath(healthyTenant))
	writeTenantPayload(t, volumeRoot, healthyTenant)

	// A tenant directory without a metadata.db yet is known metadata storage: a
	// missing file means "no database yet", so it is neither corruption nor an
	// orphan.
	if err := os.MkdirAll(filepath.Join(volume.metadataRoot, noDatabaseYet), 0o755); err != nil {
		t.Fatalf("mkdir %s metadata directory: %v", noDatabaseYet, err)
	}

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
		VolumePaths:            []string{volumeRoot},
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true; corrupted = %v", describeStatuses(report.CorruptedDatabases))
	}

	if report.HealthyDatabases != 1 {
		t.Fatalf("HealthyDatabases = %d, want 1 (only the tenant with a metadata.db)", report.HealthyDatabases)
	}

	if len(report.OrphanedTenants) != 1 || report.OrphanedTenants[0] != orphanTenant {
		t.Fatalf("OrphanedTenants = %v, want [%s]", report.OrphanedTenants, orphanTenant)
	}
}

// writeTenantPayload creates a tenant directory with one physical file below a
// volume root.
func writeTenantPayload(t *testing.T, volumeRoot, tenantID string) {
	t.Helper()

	tenantDir := filepath.Join(volumeRoot, tenantID)
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		t.Fatalf("mkdir tenant %s: %v", tenantID, err)
	}
	if err := os.WriteFile(filepath.Join(tenantDir, "payload.bin"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write tenant %s payload: %v", tenantID, err)
	}
}

func TestDatabaseHealthChecker_CorruptQuotaDatabaseIsReported(t *testing.T) {
	volume, tenantID := newHealthySQLiteLayout(t)

	writeCorruptSQLiteDatabase(t, volume.quotaDatabasePath(tenantID))

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
	})

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if report.AllHealthy {
		t.Fatalf("AllHealthy = true, want false when a quota database is corrupted")
	}

	if report.HealthyDatabases != 1 {
		t.Fatalf("HealthyDatabases = %d, want 1 (the healthy metadata database)", report.HealthyDatabases)
	}

	if len(report.CorruptedDatabases) != 1 {
		t.Fatalf("CorruptedDatabases = %v, want exactly one entry", describeStatuses(report.CorruptedDatabases))
	}

	status := report.CorruptedDatabases[0]
	if status.DatabaseType != core.DatabaseTypeDirectoryQuota {
		t.Fatalf("status.DatabaseType = %q, want %q", status.DatabaseType, core.DatabaseTypeDirectoryQuota)
	}
	if status.TenantID != tenantID {
		t.Fatalf("status.TenantID = %q, want %q", status.TenantID, tenantID)
	}
	if status.DatabasePath != volume.quotaDatabasePath(tenantID) {
		t.Fatalf("status.DatabasePath = %q, want the per-tenant quota database path", status.DatabasePath)
	}
}

func TestDatabaseHealthChecker_CheckDirectoryQuotaDatabase(t *testing.T) {
	t.Run("valid database is healthy", func(t *testing.T) {
		volume, tenantID := newHealthySQLiteLayout(t)

		checker := newChecker(t, &DatabaseHealthCheckerOptions{
			MetadataDataPath:       volume.metadataRoot,
			DirectoryQuotaDataPath: volume.quotaRoot,
		})

		status, err := checker.CheckDirectoryQuotaDatabase(context.Background())
		if err != nil {
			t.Fatalf("CheckDirectoryQuotaDatabase() error = %v", err)
		}
		if !status.IsHealthy {
			t.Fatalf("status = {healthy:%v error:%q}, want healthy for %s", status.IsHealthy, status.Error, tenantID)
		}
	})

	t.Run("corrupt database is reported", func(t *testing.T) {
		volume, tenantID := newHealthySQLiteLayout(t)

		writeCorruptSQLiteDatabase(t, volume.quotaDatabasePath(tenantID))

		checker := newChecker(t, &DatabaseHealthCheckerOptions{
			MetadataDataPath:       volume.metadataRoot,
			DirectoryQuotaDataPath: volume.quotaRoot,
		})

		status, err := checker.CheckDirectoryQuotaDatabase(context.Background())
		if err != nil {
			t.Fatalf("CheckDirectoryQuotaDatabase() error = %v", err)
		}
		if status.IsHealthy {
			t.Fatalf("status.IsHealthy = true, want false")
		}
		if status.Error == "" {
			t.Fatalf("status.Error = empty, want a corruption reason")
		}
	})

	t.Run("no database yet is not corruption", func(t *testing.T) {
		volume := newSQLiteVolume(t)

		checker := newChecker(t, &DatabaseHealthCheckerOptions{
			MetadataDataPath:       volume.metadataRoot,
			DirectoryQuotaDataPath: volume.quotaRoot,
		})

		status, err := checker.CheckDirectoryQuotaDatabase(context.Background())
		if err != nil {
			t.Fatalf("CheckDirectoryQuotaDatabase() error = %v", err)
		}
		if status.IsHealthy {
			t.Fatalf("status.IsHealthy = true, want false when no quota database exists yet")
		}
		if status.Error != "" {
			t.Fatalf("status.Error = %q, want empty: a missing database is not corruption", status.Error)
		}
	})

	t.Run("unconfigured quota root reports the configuration gap", func(t *testing.T) {
		volume := newSQLiteVolume(t)

		checker := newChecker(t, &DatabaseHealthCheckerOptions{
			MetadataDataPath: volume.metadataRoot,
		})

		status, err := checker.CheckDirectoryQuotaDatabase(context.Background())
		if err != nil {
			t.Fatalf("CheckDirectoryQuotaDatabase() error = %v", err)
		}
		if status.IsHealthy {
			t.Fatalf("status.IsHealthy = true, want false when the quota root is not configured")
		}
		if !strings.Contains(status.Error, "not configured") {
			t.Fatalf("status.Error = %q, want it to name the missing configuration", status.Error)
		}
	})
}

func TestDatabaseHealthChecker_CheckMetadataDatabaseRejectsPathTraversal(t *testing.T) {
	volume, _ := newHealthySQLiteLayout(t)

	outside := filepath.Join(t.TempDir(), "outside")
	writeValidSQLiteDatabase(t, filepath.Join(outside, metadataDatabaseFileName))

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
	})

	for _, tenantID := range []string{"..", "../outside", `..\outside`, "a/b", `a\b`, "."} {
		t.Run(tenantID, func(t *testing.T) {
			status, err := checker.CheckMetadataDatabase(context.Background(), tenantID)
			if err == nil {
				t.Fatalf("CheckMetadataDatabase(%q) error = nil, want error", tenantID)
			}
			if !strings.Contains(err.Error(), core.ErrInvalidArgument.Error()) {
				t.Fatalf("error = %v, want it to wrap ErrInvalidArgument", err)
			}
			if status == nil {
				t.Fatalf("status = nil, want a non-nil status")
			}
			if status.IsHealthy {
				t.Fatalf("status.IsHealthy = true, want false for an invalid tenant ID")
			}
			if !strings.Contains(status.Error, "invalid tenant ID") {
				t.Fatalf("status.Error = %q, want a clear invalid tenant ID message", status.Error)
			}
		})
	}
}

func TestDatabaseHealthChecker_CheckMetadataDatabaseWithoutTenantIsNotShared(t *testing.T) {
	volume, _ := newHealthySQLiteLayout(t)

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
	})

	status, err := checker.CheckMetadataDatabase(context.Background(), "")
	if err != nil {
		t.Fatalf("CheckMetadataDatabase(\"\") error = %v", err)
	}
	if status == nil {
		t.Fatalf("status = nil, want a non-nil status")
	}
	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false: the SQLite layout has no shared metadata database")
	}
	if status.DatabasePath != "" {
		t.Fatalf("status.DatabasePath = %q, want empty: no single metadata database exists", status.DatabasePath)
	}
	if !strings.Contains(status.Error, "tenant") {
		t.Fatalf("status.Error = %q, want it to explain the per-tenant requirement", status.Error)
	}
}

func TestIsDatabaseCorrupted_StructuralVerdict(t *testing.T) {
	root := t.TempDir()

	valid := filepath.Join(root, "valid", metadataDatabaseFileName)
	writeValidSQLiteDatabase(t, valid)

	corrupt := filepath.Join(root, "corrupt", metadataDatabaseFileName)
	writeCorruptSQLiteDatabase(t, corrupt)

	short := filepath.Join(root, "short", metadataDatabaseFileName)
	writeShortSQLiteDatabase(t, short)

	if IsDatabaseCorrupted(valid) {
		t.Fatalf("IsDatabaseCorrupted(%s) = true, want false for a valid SQLite header", valid)
	}

	if !IsDatabaseCorrupted(corrupt) {
		t.Fatalf("IsDatabaseCorrupted(corrupt) = false, want true for a wrong header")
	}

	if !IsDatabaseCorrupted(short) {
		t.Fatalf("IsDatabaseCorrupted(short) = false, want true for a file shorter than one page")
	}

	if IsDatabaseCorrupted(filepath.Join(root, "missing", metadataDatabaseFileName)) {
		t.Fatalf("IsDatabaseCorrupted(missing) = true, want false")
	}

	// A path that is not a regular file cannot hold a database, so it is
	// reported as corrupted even though nothing was opened.
	if !IsDatabaseCorrupted(t.TempDir()) {
		t.Fatalf("IsDatabaseCorrupted(directory) = false, want true for a non-regular database path")
	}
}

func TestGetDatabaseSizeAndTenantIDs(t *testing.T) {
	root := t.TempDir()

	dbPath := filepath.Join(root, "tenant-a", metadataDatabaseFileName)
	writeValidSQLiteDatabase(t, dbPath)

	size, err := GetDatabaseSize(dbPath)
	if err != nil {
		t.Fatalf("GetDatabaseSize() error = %v", err)
	}
	if size != int64(minimumSQLiteFileSize) {
		t.Fatalf("GetDatabaseSize() = %d, want %d", size, minimumSQLiteFileSize)
	}

	size, err = GetDatabaseSize(filepath.Join(root, "does-not-exist", metadataDatabaseFileName))
	if err != nil {
		t.Fatalf("GetDatabaseSize(missing) error = %v", err)
	}
	if size != 0 {
		t.Fatalf("GetDatabaseSize(missing) = %d, want 0", size)
	}

	size, err = GetDatabaseSize(filepath.Join(root, "tenant-a"))
	if err != nil {
		t.Fatalf("GetDatabaseSize(directory) error = %v", err)
	}
	if size != 0 {
		t.Fatalf("GetDatabaseSize(directory) = %d, want 0 for a non-file path", size)
	}

	// Entries that are not valid tenant identifiers are skipped: internal
	// dot-directories and underscore-prefixed system directories.
	for _, name := range []string{"tenant-b", ".locus", "_system"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}

	tenantIDs, err := GetTenantIDsFromMetadata(root)
	if err != nil {
		t.Fatalf("GetTenantIDsFromMetadata() error = %v", err)
	}

	if len(tenantIDs) != 2 || tenantIDs[0] != "tenant-a" || tenantIDs[1] != "tenant-b" {
		t.Fatalf("GetTenantIDsFromMetadata() = %v, want [tenant-a tenant-b]", tenantIDs)
	}

	if tenantIDs, err = GetTenantIDsFromMetadata(filepath.Join(root, "missing-root")); err != nil {
		t.Fatalf("GetTenantIDsFromMetadata(missing) error = %v", err)
	} else if len(tenantIDs) != 0 {
		t.Fatalf("GetTenantIDsFromMetadata(missing) = %v, want empty", tenantIDs)
	}
}

// unhealthyChecker always reports one corrupted database.
type unhealthyChecker struct{ report *core.DatabaseHealthReport }

func (u *unhealthyChecker) CheckAllDatabases(context.Context) (*core.DatabaseHealthReport, error) {
	return u.report, nil
}

func (u *unhealthyChecker) CheckMetadataDatabase(context.Context, string) (*core.DatabaseHealthStatus, error) {
	return u.report.CorruptedDatabases[0], nil
}

func (u *unhealthyChecker) CheckDirectoryQuotaDatabase(context.Context) (*core.DatabaseHealthStatus, error) {
	return &core.DatabaseHealthStatus{DatabaseType: core.DatabaseTypeDirectoryQuota}, nil
}

func (u *unhealthyChecker) DetectOrphanedFiles(context.Context) ([]string, error) {
	return nil, nil
}

func TestDatabaseHealthCheckService_StartOnlyLogsOnUnhealthyReport(t *testing.T) {
	checker := &unhealthyChecker{report: &core.DatabaseHealthReport{
		HealthyDatabases: 0,
		CorruptedDatabases: []*core.DatabaseHealthStatus{{
			DatabaseType: core.DatabaseTypeMetadata,
			TenantID:     "tenant-a",
			DatabasePath: "redacted",
			IsHealthy:    false,
			Error:        "file does not start with the 16-byte SQLite format 3 header",
		}},
		OrphanedTenants: []string{"orphan-tenant"},
		AllHealthy:      false,
	}}

	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: checker,
		InitialDelay:          10 * time.Millisecond,
		MaxRetries:            1,
		RetryDelay:            time.Millisecond,
		CheckOnStartupOnly:    true,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil for an unhealthy report", err)
	}

	if !service.IsRunning() {
		t.Fatalf("IsRunning() = false, want true after Start()")
	}

	report, err := service.CheckNow()
	if err != nil {
		t.Fatalf("CheckNow() error = %v", err)
	}
	if report == nil || report.AllHealthy {
		t.Fatalf("CheckNow() report = %+v, want the unhealthy report", report)
	}

	if err := service.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if service.IsRunning() {
		t.Fatalf("IsRunning() = true, want false after Stop()")
	}
}

func TestDatabaseHealthCheckService_ReportsHealthyLayout(t *testing.T) {
	volume, _ := newHealthySQLiteLayout(t)

	checker := newChecker(t, &DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
	})

	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: checker,
		InitialDelay:          10 * time.Millisecond,
		MaxRetries:            1,
		RetryDelay:            time.Millisecond,
		CheckOnStartupOnly:    true,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	// The startup check runs in a goroutine; poll until it has executed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		report, err := service.CheckNow()
		if err != nil {
			t.Fatalf("CheckNow() error = %v", err)
		}
		if report.HealthyDatabases == 2 && report.AllHealthy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	report, err := service.CheckNow()
	if err != nil {
		t.Fatalf("CheckNow() error = %v", err)
	}
	t.Fatalf("report = {healthy:%d corrupted:%v allHealthy:%v}, want a fully healthy report",
		report.HealthyDatabases, describeStatuses(report.CorruptedDatabases), report.AllHealthy)
}

// describeStatuses renders corrupted statuses without leaking tenant data.
func describeStatuses(statuses []*core.DatabaseHealthStatus) []string {
	out := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if status == nil {
			out = append(out, "<nil>")
			continue
		}
		out = append(out, string(status.DatabaseType)+": "+status.Error)
	}

	return out
}
