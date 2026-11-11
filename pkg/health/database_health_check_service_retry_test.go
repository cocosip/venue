package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// --- retry policy ------------------------------------------------------------

// scriptedCheck is one canned CheckAllDatabases result.
type scriptedCheck struct {
	report *core.DatabaseHealthReport
	err    error
}

// scriptedHealthChecker returns the scripted results in order and repeats the
// last one once the script is exhausted, while counting the calls.
type scriptedHealthChecker struct {
	mu      sync.Mutex
	results []scriptedCheck
	calls   int
}

func (c *scriptedHealthChecker) CheckAllDatabases(context.Context) (*core.DatabaseHealthReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	index := c.calls
	if index >= len(c.results) {
		index = len(c.results) - 1
	}
	c.calls++

	result := c.results[index]
	return result.report, result.err
}

func (c *scriptedHealthChecker) CheckMetadataDatabase(context.Context, string) (*core.DatabaseHealthStatus, error) {
	return &core.DatabaseHealthStatus{IsHealthy: true}, nil
}

func (c *scriptedHealthChecker) CheckDirectoryQuotaDatabase(context.Context) (*core.DatabaseHealthStatus, error) {
	return &core.DatabaseHealthStatus{IsHealthy: true}, nil
}

func (c *scriptedHealthChecker) DetectOrphanedFiles(context.Context) ([]string, error) {
	return nil, nil
}

func (c *scriptedHealthChecker) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// eventRecorder captures the health events emitted through the logging runtime.
type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (h *eventRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *eventRecorder) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "event" {
			h.events = append(h.events, attr.Value.String())
		}
		return true
	})
	return nil
}

func (h *eventRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *eventRecorder) WithGroup(string) slog.Handler { return h }

func (h *eventRecorder) countOf(event string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, got := range h.events {
		if got == event {
			count++
		}
	}
	return count
}

// TestHealthCheckDoesNotRetryANonHealthyReport is the regression test for the
// retry policy: an unhealthy report is a durable inspection result, so rescanning
// every tenant database up to MaxRetries times per cycle cannot change it. Only an
// inspection error is transient enough to retry.
func TestHealthCheckDoesNotRetryANonHealthyReport(t *testing.T) {
	handler := &eventRecorder{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	checker := &scriptedHealthChecker{results: []scriptedCheck{{
		report: &core.DatabaseHealthReport{
			AllHealthy:         false,
			HealthyDatabases:   1,
			CorruptedDatabases: []*core.DatabaseHealthStatus{{TenantID: "tenant-a", Error: "corrupt"}},
		},
	}}}

	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: checker,
		Logging:               rt,
		MaxRetries:            3,
		RetryDelay:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}
	service.ctx = context.Background()

	service.performHealthCheckWithRetry()

	if got := checker.callCount(); got != 1 {
		t.Fatalf("CheckAllDatabases calls = %d, want 1 (an unhealthy report must not be rescanned)", got)
	}
	if got := handler.countOf("check_unhealthy"); got != 1 {
		t.Errorf("check_unhealthy events = %d, want 1 (the report must still be reported)", got)
	}
	if got := handler.countOf("check_retrying"); got != 0 {
		t.Errorf("check_retrying events = %d, want 0", got)
	}
}

// TestHealthCheckRetriesInspectionErrors keeps the transient-error retry covered:
// an error is retried and a later success is reported once.
func TestHealthCheckRetriesInspectionErrors(t *testing.T) {
	handler := &eventRecorder{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	inspectionErr := errors.New("inspection failed")
	checker := &scriptedHealthChecker{results: []scriptedCheck{
		{err: inspectionErr},
		{report: &core.DatabaseHealthReport{AllHealthy: true, HealthyDatabases: 1}},
	}}

	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: checker,
		Logging:               rt,
		MaxRetries:            3,
		RetryDelay:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}
	service.ctx = context.Background()

	service.performHealthCheckWithRetry()

	if got := checker.callCount(); got != 2 {
		t.Fatalf("CheckAllDatabases calls = %d, want 2 (one retry after the error)", got)
	}
	if got := handler.countOf("check_recovered"); got != 1 {
		t.Errorf("check_recovered events = %d, want 1", got)
	}
	if got := handler.countOf("check_healthy"); got != 1 {
		t.Errorf("check_healthy events = %d, want 1 (the recovered report must be reported)", got)
	}
}

// --- single enumeration and reused sizes -------------------------------------

// TestCheckAllDatabasesEnumeratesTenantsOnceAndReusesInspectionData verifies the
// two single-pass properties: the tenant set enumerated for the database checks is
// the tenant set orphan detection uses (no second directory read), and the size
// collected for the report comes from the same stat the structural check already
// performed.
func TestCheckAllDatabasesEnumeratesTenantsOnceAndReusesInspectionData(t *testing.T) {
	volume, tenantID := newHealthySQLiteLayout(t)

	// A tenant without a database file: the report records a zero size, exactly
	// like the re-stat it replaced.
	const databaseLessTenant = "tenant-no-database"
	if err := os.MkdirAll(filepath.Join(volume.metadataRoot, databaseLessTenant), 0o755); err != nil {
		t.Fatalf("mkdir tenant without database: %v", err)
	}

	// A volume tenant with physical content but no metadata database, which is
	// the orphan case orphan detection must report.
	volumeRoot := t.TempDir()
	orphanTenant := "tenant-orphan"
	if err := os.MkdirAll(filepath.Join(volumeRoot, orphanTenant), 0o755); err != nil {
		t.Fatalf("mkdir orphan tenant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(volumeRoot, orphanTenant, "payload.bin"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write orphan payload: %v", err)
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDataPath:       volume.metadataRoot,
		DirectoryQuotaDataPath: volume.quotaRoot,
		VolumePaths:            []string{volumeRoot},
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	concrete, ok := checker.(*databaseHealthChecker)
	if !ok {
		t.Fatalf("NewDatabaseHealthChecker() returned %T, want *databaseHealthChecker", checker)
	}

	enumerations := 0
	concrete.enumerateTenants = func(metadataDataPath string) ([]string, error) {
		enumerations++
		return GetTenantIDsFromMetadata(metadataDataPath)
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}

	if enumerations != 1 {
		t.Fatalf("tenant enumerations = %d, want 1 (orphan detection must reuse the first enumeration)", enumerations)
	}
	if len(report.OrphanedTenants) != 1 || report.OrphanedTenants[0] != orphanTenant {
		t.Fatalf("OrphanedTenants = %v, want [%s]", report.OrphanedTenants, orphanTenant)
	}

	size, ok := report.DatabaseSizes[volume.metadataDatabasePath(tenantID)]
	if !ok || size != int64(minimumSQLiteFileSize) {
		t.Fatalf("DatabaseSizes[tenant metadata] = (%d, %v), want (%d, true)",
			size, ok, minimumSQLiteFileSize)
	}
	size, ok = report.DatabaseSizes[volume.quotaDatabasePath(tenantID)]
	if !ok || size != int64(minimumSQLiteFileSize) {
		t.Fatalf("DatabaseSizes[tenant quota] = (%d, %v), want (%d, true)",
			size, ok, minimumSQLiteFileSize)
	}
	size, ok = report.DatabaseSizes[filepath.Join(volume.metadataRoot, databaseLessTenant, metadataDatabaseFileName)]
	if !ok || size != 0 {
		t.Fatalf("DatabaseSizes[database-less tenant] = (%d, %v), want (0, true)", size, ok)
	}
}

// TestCheckAllDatabasesSkipsOrphansWhenEnumerationFails pins the failure
// behaviour: when the metadata root cannot be enumerated, orphan detection must
// not treat every physical tenant as orphaned.
func TestCheckAllDatabasesSkipsOrphansWhenEnumerationFails(t *testing.T) {
	volume := newSQLiteVolume(t)

	volumeRoot := t.TempDir()
	tenantDir := filepath.Join(volumeRoot, "tenant-a")
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		t.Fatalf("mkdir tenant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tenantDir, "payload.bin"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	checker, err := NewDatabaseHealthChecker(&DatabaseHealthCheckerOptions{
		MetadataDataPath: volume.metadataRoot,
		VolumePaths:      []string{volumeRoot},
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthChecker() error = %v", err)
	}

	concrete := checker.(*databaseHealthChecker)
	concrete.enumerateTenants = func(string) ([]string, error) {
		return nil, fmt.Errorf("enumeration injected failure")
	}

	report, err := checker.CheckAllDatabases(context.Background())
	if err != nil {
		t.Fatalf("CheckAllDatabases() error = %v", err)
	}
	if report.AllHealthy {
		t.Fatal("AllHealthy = true, want false when tenant enumeration failed")
	}
	if len(report.OrphanedTenants) != 0 {
		t.Fatalf("OrphanedTenants = %v, want none after an enumeration failure", report.OrphanedTenants)
	}
}

var _ core.DatabaseHealthChecker = (*scriptedHealthChecker)(nil)
