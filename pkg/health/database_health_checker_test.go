package health

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// newRealBadgerDirectory creates a genuine BadgerDB directory containing the
// MANIFEST, value log and key registry artifacts a structural check looks for.
func newRealBadgerDirectory(t *testing.T, dir string) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create badger directory: %v", err)
	}

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("health-probe"), []byte("1"))
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("write probe key: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close badger: %v", err)
	}
}

// newHealthyLayout builds the real on-disk layout used by venue:
// <root>/shared/metadata and <root>/quota.
func newHealthyLayout(t *testing.T) (root, metadataPath, quotaPath string) {
	t.Helper()

	root = t.TempDir()
	metadataPath = filepath.Join(root, "shared", "metadata")
	quotaPath = filepath.Join(root, "quota")

	newRealBadgerDirectory(t, metadataPath)
	newRealBadgerDirectory(t, quotaPath)

	return root, metadataPath, quotaPath
}

func TestDatabaseHealthChecker_HealthyLayout(t *testing.T) {
	root, metadataPath, quotaPath := newHealthyLayout(t)

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath:       metadataPath,
		DirectoryQuotaDatabasePath: quotaPath,
		MetadataDataPath:           root,
		DirectoryQuotaDataPath:     root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true; corrupted = %v", describeStatuses(report.CorruptedDatabases))
	}

	if report.HealthyDatabases != 2 {
		t.Fatalf("HealthyDatabases = %d, want 2", report.HealthyDatabases)
	}

	if len(report.CorruptedDatabases) != 0 {
		t.Fatalf("CorruptedDatabases = %v, want empty", describeStatuses(report.CorruptedDatabases))
	}
}

func TestDatabaseHealthChecker_LegacyRootsDeriveRealPaths(t *testing.T) {
	root, _, _ := newHealthyLayout(t)

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDataPath:       root,
		DirectoryQuotaDataPath: root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy || report.HealthyDatabases != 2 {
		t.Fatalf("report = {healthy:%d corrupted:%v allHealthy:%v}, want 2 healthy",
			report.HealthyDatabases, describeStatuses(report.CorruptedDatabases), report.AllHealthy)
	}
}

func TestDatabaseHealthChecker_EmptyDeploymentReportsNoDatabases(t *testing.T) {
	root := t.TempDir()

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDataPath:       root,
		DirectoryQuotaDataPath: root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

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
}

func TestDatabaseHealthChecker_NonDatabaseDirectoriesAreNotCorruption(t *testing.T) {
	root := t.TempDir()

	// Directories the old implementation reported as corrupted: the tenant JSON
	// store, the shared parent and the quota root.
	for _, name := range []string{".locus", "shared", "quota-root"} {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "tenant.json"), []byte("{}"), 0o644); err != nil {
			t.Fatalf("write tenant.json: %v", err)
		}
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDataPath:       root,
		DirectoryQuotaDataPath: root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

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
}

func TestDatabaseHealthChecker_MissingManifestIsCorruption(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "shared", "metadata")

	if err := os.MkdirAll(metadataPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// A directory that clearly claims to be a BadgerDB (value log + registry)
	// but lost its MANIFEST.
	for _, name := range []string{"000001.vlog", "KEYREGISTRY"} {
		if err := os.WriteFile(filepath.Join(metadataPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath: metadataPath,
		MetadataDataPath:     root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if report.AllHealthy {
		t.Fatalf("AllHealthy = true, want false when MANIFEST is missing")
	}

	if len(report.CorruptedDatabases) != 1 {
		t.Fatalf("CorruptedDatabases = %v, want exactly one entry", describeStatuses(report.CorruptedDatabases))
	}

	status := report.CorruptedDatabases[0]
	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false")
	}

	if !strings.Contains(status.Error, "MANIFEST") {
		t.Fatalf("status.Error = %q, want it to name the missing MANIFEST", status.Error)
	}
}

func TestDatabaseHealthChecker_MissingValueLogAndRegistryIsCorruption(t *testing.T) {
	root := t.TempDir()
	metadataPath := filepath.Join(root, "shared", "metadata")

	if err := os.MkdirAll(metadataPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(metadataPath, "MANIFEST"), []byte("manifest-bytes"), 0o644); err != nil {
		t.Fatalf("write MANIFEST: %v", err)
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath: metadataPath,
		MetadataDataPath:     root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	status, err := checker.CheckMetadataDatabase(context.Background(), "")
	if err != nil {
		t.Fatalf("CheckMetadataDatabase() error = %v", err)
	}

	if status.IsHealthy {
		t.Fatalf("status.IsHealthy = true, want false")
	}

	if !strings.Contains(status.Error, ".vlog") || !strings.Contains(status.Error, "KEYREGISTRY") {
		t.Fatalf("status.Error = %q, want it to name .vlog/KEYREGISTRY", status.Error)
	}
}

func TestDatabaseHealthChecker_OrphanDetectionRunsWithHealthyDatabases(t *testing.T) {
	root, metadataPath, quotaPath := newHealthyLayout(t)

	volumeRoot := filepath.Join(t.TempDir(), "volume")

	orphanDir := filepath.Join(volumeRoot, "orphan-tenant")
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatalf("mkdir orphan tenant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(orphanDir, "payload.bin"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write orphan payload: %v", err)
	}

	// A known tenant with a metadata directory and files must not be reported.
	knownDir := filepath.Join(volumeRoot, "known-tenant")
	if err := os.MkdirAll(knownDir, 0o755); err != nil {
		t.Fatalf("mkdir known tenant: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "known-tenant"), 0o755); err != nil {
		t.Fatalf("mkdir known tenant metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(knownDir, "payload.bin"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write known payload: %v", err)
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath:       metadataPath,
		DirectoryQuotaDatabasePath: quotaPath,
		MetadataDataPath:           root,
		DirectoryQuotaDataPath:     root,
		VolumePaths:                []string{volumeRoot},
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if !report.AllHealthy {
		t.Fatalf("AllHealthy = false, want true; corrupted = %v", describeStatuses(report.CorruptedDatabases))
	}

	if len(report.OrphanedTenants) != 1 || report.OrphanedTenants[0] != "orphan-tenant" {
		t.Fatalf("OrphanedTenants = %v, want [orphan-tenant]", report.OrphanedTenants)
	}
}

func TestDatabaseHealthChecker_CheckMetadataDatabaseRejectsPathTraversal(t *testing.T) {
	root, metadataPath, _ := newHealthyLayout(t)

	outside := filepath.Join(t.TempDir(), "outside", "metadata")
	newRealBadgerDirectory(t, outside)

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath: metadataPath,
		MetadataDataPath:     root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

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

func TestIsDatabaseCorrupted_RealBadgerIsHealthy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "metadata")
	newRealBadgerDirectory(t, dir)

	if IsDatabaseCorrupted(dir) {
		t.Fatalf("IsDatabaseCorrupted(%s) = true, want false for a real BadgerDB directory", dir)
	}

	if IsDatabaseCorrupted(filepath.Join(t.TempDir(), "missing")) {
		t.Fatalf("IsDatabaseCorrupted(missing) = true, want false")
	}

	if IsDatabaseCorrupted(t.TempDir()) {
		t.Fatalf("IsDatabaseCorrupted(empty dir) = true, want false for a non-database directory")
	}
}

func TestGetDatabaseSizeAndTenantIDs(t *testing.T) {
	root := t.TempDir()

	payload := []byte("0123456789")
	if err := os.WriteFile(filepath.Join(root, "file.bin"), payload, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	size, err := GetDatabaseSize(root)
	if err != nil {
		t.Fatalf("GetDatabaseSize() error = %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("GetDatabaseSize() = %d, want %d", size, len(payload))
	}

	size, err = GetDatabaseSize(filepath.Join(root, "does-not-exist"))
	if err != nil {
		t.Fatalf("GetDatabaseSize(missing) error = %v", err)
	}
	if size != 0 {
		t.Fatalf("GetDatabaseSize(missing) = %d, want 0", size)
	}

	for _, name := range []string{"tenant-a", "tenant-b", ".locus", "_system"} {
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
			DatabasePath: "redacted",
			IsHealthy:    false,
			Error:        "required BadgerDB artifact MANIFEST is missing",
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
	root, metadataPath, quotaPath := newHealthyLayout(t)

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDatabasePath:       metadataPath,
		DirectoryQuotaDatabasePath: quotaPath,
		MetadataDataPath:           root,
		DirectoryQuotaDataPath:     root,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

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
