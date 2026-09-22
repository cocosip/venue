package venue_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// newStatisticsConfig builds a lifecycle config with runtime statistics enabled
// and the supplied mutation applied.
func newStatisticsConfig(t *testing.T, mutate func(*config.StatisticsConfig)) *config.Config {
	t.Helper()

	statisticsConfig := config.NewStatisticsConfig().
		WithEnabled(true).
		WithWindowSize(time.Minute).
		WithRetention(time.Hour).
		WithMaxSeries(1024)
	if mutate != nil {
		mutate(statisticsConfig)
	}

	return newLifecycleConfig(t).WithStatistics(statisticsConfig)
}

// TestStatisticsRecordsStorageOperationsThroughVenue is the public-entry
// regression test for W3: the counters a caller reads back must be the counters
// its own storage operations produced.
func TestStatisticsRecordsStorageOperationsThroughVenue(t *testing.T) {
	runtime, err := venue.NewVenue(newStatisticsConfig(t, nil))
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	tenant := mustTenant(t, runtime, "stats-tenant")

	payload := "payload"
	fileKey, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	reader, err := runtime.StoragePool().ReadFile(ctx, tenant, fileKey)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("reading the file returned error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("closing the reader returned error = %v", err)
	}

	location, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
	if err != nil {
		t.Fatalf("GetNextFileForProcessing() error = %v", err)
	}
	if location == nil || location.Lease == nil {
		t.Fatalf("GetNextFileForProcessing() = %#v, want a leased location", location)
	}
	if err := runtime.StoragePool().MarkAsCompleted(ctx, *location.Lease); err != nil {
		t.Fatalf("MarkAsCompleted() error = %v", err)
	}

	now := time.Now().UTC()
	snapshot := runtime.Statistics().Snapshot(core.StatisticsQuery{
		From: now.Add(-time.Minute),
		To:   now.Add(time.Minute),
	})

	if snapshot.WriteFileCount != 1 {
		t.Errorf("WriteFileCount = %d, want 1", snapshot.WriteFileCount)
	}
	if snapshot.WriteBytes != int64(len(payload)) {
		t.Errorf("WriteBytes = %d, want %d", snapshot.WriteBytes, len(payload))
	}
	if snapshot.DequeuedFileCount != 1 {
		t.Errorf("DequeuedFileCount = %d, want 1", snapshot.DequeuedFileCount)
	}
	if snapshot.ReadFileCount != 1 {
		t.Errorf("ReadFileCount = %d, want 1", snapshot.ReadFileCount)
	}
	if snapshot.CompletedFileCount != 1 {
		t.Errorf("CompletedFileCount = %d, want 1", snapshot.CompletedFileCount)
	}
	if snapshot.WriteMegabytesPerSecond <= 0 {
		t.Errorf("WriteMegabytesPerSecond = %v, want a positive throughput", snapshot.WriteMegabytesPerSecond)
	}

	writeBytes, ok := measurementByName(snapshot.Measurements, core.StatisticStorageWriteBytes)
	if !ok {
		t.Fatalf("measurements = %v, want one named %q", measurementNames(snapshot.Measurements), core.StatisticStorageWriteBytes)
	}
	if writeBytes.Value != float64(len(payload)) {
		t.Errorf("%s value = %v, want %d", core.StatisticStorageWriteBytes, writeBytes.Value, len(payload))
	}
	if got := writeBytes.Dimensions[core.StatisticsDimensionVolumeID]; got != "primary" {
		t.Errorf("%s volume dimension = %q, want %q", core.StatisticStorageWriteBytes, got, "primary")
	}
	if _, retained := writeBytes.Dimensions[core.StatisticsDimensionTenantID]; retained {
		// Retaining tenant_id is off by default because it is high-cardinality.
		t.Errorf("%s dimensions = %v, want no tenant_id dimension", core.StatisticStorageWriteBytes, writeBytes.Dimensions)
	}
}

// TestStatisticsDisabledIsObservableAndSilent covers the disabled path: the
// accessors exist, they never return nil, and no measurement survives a record.
func TestStatisticsDisabledIsObservableAndSilent(t *testing.T) {
	runtime, err := venue.NewVenue(newLifecycleConfig(t))
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	recorder := runtime.StatisticsRecorder()
	if recorder == nil {
		t.Fatal("StatisticsRecorder() = nil, want a usable recorder with statistics disabled")
	}
	recorder.Record(core.StatisticStorageWriteBytes, 4096, time.Now().UTC(), nil)

	if runtime.Statistics() == nil {
		t.Fatal("Statistics() = nil, want a reader with statistics disabled")
	}

	snapshot := runtime.Statistics().Snapshot(core.StatisticsQuery{
		From: time.Now().UTC().Add(-time.Hour),
		To:   time.Now().UTC().Add(time.Hour),
	})
	if snapshot.WriteBytes != 0 || len(snapshot.Measurements) != 0 {
		t.Errorf("snapshot = %#v, want an empty snapshot while statistics are disabled", snapshot)
	}
}

// TestStatisticsTenantDimensionFilterRequiresRetention proves the dimension
// switch has an observable effect: filtering by a dimension that is not retained
// cannot match, because the recorder never stored it.
func TestStatisticsTenantDimensionFilterRequiresRetention(t *testing.T) {
	cfg := newStatisticsConfig(t, func(statisticsConfig *config.StatisticsConfig) {
		statisticsConfig.Dimensions.TenantID = true
	})

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	tenant := mustTenant(t, runtime, "dimension-tenant")
	if _, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	now := time.Now().UTC()
	window := core.StatisticsQuery{
		From: now.Add(-time.Minute),
		To:   now.Add(time.Minute),
	}

	matching := window
	matching.TenantID = tenant.ID
	if got := runtime.Statistics().Snapshot(matching).WriteFileCount; got != 1 {
		t.Errorf("WriteFileCount for the owning tenant = %d, want 1", got)
	}

	other := window
	other.TenantID = "some-other-tenant"
	if got := runtime.Statistics().Snapshot(other).WriteFileCount; got != 0 {
		t.Errorf("WriteFileCount for another tenant = %d, want 0", got)
	}
}

// TestStatisticsOutputServiceLogsSummaryThroughVenue covers the optional output
// sink: when Statistics.Output is enabled the running runtime logs a summary
// without any caller involvement.
func TestStatisticsOutputServiceLogsSummaryThroughVenue(t *testing.T) {
	handler := &summaryCaptureHandler{}
	cfg := newStatisticsConfig(t, func(statisticsConfig *config.StatisticsConfig) {
		statisticsConfig.Output = config.StatisticsOutputConfig{
			Enabled:     true,
			Sink:        "Logging",
			Interval:    20 * time.Millisecond,
			QueryWindow: time.Minute,
			// An empty runtime still has to emit once the interval elapses, so
			// the summary is observable without producing any traffic.
			IncludeEmptySnapshots: true,
		}
	})
	cfg.WithLogging(&logging.Config{Handler: handler})

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	if err := runtime.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if handler.summaryCount() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no statistics summary was logged within 3s (records = %d)", handler.recordCount())
}

// TestQuotaAdministrationThroughVenue covers W5.3: a caller reaches the limit
// administration capability the runtime documents, and a global limit change
// reaches a tenant that never had its own limit.
func TestQuotaAdministrationThroughVenue(t *testing.T) {
	cfg := newLifecycleConfig(t).
		WithDefaultTenantQuota(2).
		WithTenants(
			config.NewTenantConfig("limited").WithQuota(3),
			config.NewTenantConfig("global-only"),
		)

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	ctx := context.Background()
	administrator, ok := runtime.TenantQuotaAdministrator()
	if !ok {
		t.Fatal("TenantQuotaAdministrator() reported no capability, want the runtime manager to provide it")
	}

	global, err := administrator.GetGlobalLimit(ctx)
	if err != nil {
		t.Fatalf("GetGlobalLimit() error = %v", err)
	}
	if global != 2 {
		t.Fatalf("GetGlobalLimit() = %d, want the configured DefaultTenantQuota 2", global)
	}

	assertEffectiveLimit(t, ctx, administrator, "global-only", 2, "global limit")
	assertEffectiveLimit(t, ctx, administrator, "limited", 3, "explicit tenant limit")

	if err := administrator.SetGlobalLimit(ctx, 5); err != nil {
		t.Fatalf("SetGlobalLimit(5) error = %v", err)
	}
	assertEffectiveLimit(t, ctx, administrator, "global-only", 5, "updated global limit")
	assertEffectiveLimit(t, ctx, administrator, "limited", 3, "explicit tenant limit after a global change")

	if err := administrator.SetTenantLimit(ctx, "global-only", 1); err != nil {
		t.Fatalf("SetTenantLimit() error = %v", err)
	}
	assertEffectiveLimit(t, ctx, administrator, "global-only", 1, "tenant limit override")

	if err := administrator.RemoveTenantLimit(ctx, "global-only"); err != nil {
		t.Fatalf("RemoveTenantLimit() error = %v", err)
	}
	assertEffectiveLimit(t, ctx, administrator, "global-only", 5, "global limit after removing the override")

	if err := administrator.SetGlobalLimit(ctx, -1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("SetGlobalLimit(-1) error = %v, want core.ErrInvalidArgument", err)
	}
	if err := administrator.SetTenantLimit(ctx, "global-only", -1); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("SetTenantLimit(-1) error = %v, want core.ErrInvalidArgument", err)
	}
}

// TestIsTenantEnabledDoesNotMaterializeTenantThroughVenue covers W5.4 through the
// public entry point: probing an unknown tenant must not create tenant state.
func TestIsTenantEnabledDoesNotMaterializeTenantThroughVenue(t *testing.T) {
	cfg := newLifecycleConfig(t)
	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	enabled, err := runtime.TenantManager().IsTenantEnabled(context.Background(), "ghost-tenant")
	if err != nil {
		t.Fatalf("IsTenantEnabled() error = %v", err)
	}
	if enabled {
		t.Error("IsTenantEnabled() for an unknown tenant = true, want false")
	}

	if err := filepath.WalkDir(cfg.MetadataDirectory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if strings.Contains(entry.Name(), "ghost-tenant") {
			t.Errorf("probing an unknown tenant created %s", path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walking the metadata directory returned error = %v", err)
	}
}

// TestBackupAndRestoreMetadataThroughVenue covers W4 end to end: a consistent
// online backup can be restored into a fresh directory, and the runtime that
// opens the restored directory sees the same queue state.
func TestBackupAndRestoreMetadataThroughVenue(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	cfg := newLifecycleConfig(t)
	cfg.BadgerDB.BackupDirectory = filepath.Join(root, "backups")
	cfg.BadgerDB.BackupInterval = time.Hour
	cfg.BadgerDB.BackupRetention = 7 * 24 * time.Hour

	runtime, err := venue.NewVenue(cfg)
	if err != nil {
		t.Fatalf("NewVenue() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Stop(); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	tenant := mustTenant(t, runtime, "backup-tenant")
	fileKey, err := runtime.StoragePool().WriteFile(ctx, tenant, strings.NewReader("payload"), nil)
	if err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	// The direct stream API must work while the runtime serves traffic.
	var stream bytes.Buffer
	since, err := runtime.BackupMetadata(ctx, &stream)
	if err != nil {
		t.Fatalf("BackupMetadata() error = %v", err)
	}
	if since == 0 {
		t.Error("BackupMetadata() since = 0, want the engine sequence number of the snapshot")
	}
	if stream.Len() == 0 {
		t.Fatal("BackupMetadata() wrote an empty stream")
	}

	// The configured runner must write a backup file the status accessor sees.
	backupService := runtime.MetadataBackupService()
	if backupService == nil {
		t.Fatal("MetadataBackupService() = nil, want a runner when BackupDirectory and BackupInterval are set")
	}
	if err := backupService.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	info, err := runtime.MetadataBackupInfo()
	if err != nil {
		t.Fatalf("MetadataBackupInfo() error = %v", err)
	}
	if info.BackupCount != 1 || info.LatestBackupPath == "" || info.TotalBytes == 0 {
		t.Fatalf("MetadataBackupInfo() = %#v, want one readable backup with bytes", info)
	}

	backupBytes, err := os.ReadFile(info.LatestBackupPath)
	if err != nil {
		t.Fatalf("reading the backup file returned error = %v", err)
	}

	restoredRoot := t.TempDir()
	restoredCfg := newLifecycleConfig(t)
	restoredCfg.MetadataDirectory = filepath.Join(restoredRoot, "metadata")
	restoredCfg.QuotaDirectory = filepath.Join(restoredRoot, "quota")

	if err := venue.RestoreMetadata(ctx, restoredCfg, bytes.NewReader(backupBytes)); err != nil {
		t.Fatalf("RestoreMetadata() error = %v", err)
	}

	// Restoring into the same directory must be refused: that is the guard that
	// keeps a mistyped path from overwriting live data.
	if err := venue.RestoreMetadata(ctx, restoredCfg, bytes.NewReader(backupBytes)); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("RestoreMetadata() into a non-empty directory error = %v, want core.ErrInvalidArgument", err)
	}

	restored, err := venue.NewVenue(restoredCfg)
	if err != nil {
		t.Fatalf("NewVenue(restored config) error = %v", err)
	}
	t.Cleanup(func() {
		if err := restored.Stop(); err != nil {
			t.Errorf("restored Stop() error = %v", err)
		}
	})

	restoredTenant := mustTenant(t, restored, "backup-tenant")
	restoredInfo, err := restored.StoragePool().GetFileInfo(ctx, restoredTenant, fileKey)
	if err != nil {
		t.Fatalf("GetFileInfo() after restore error = %v", err)
	}
	if restoredInfo.Status != core.FileStatusPending {
		t.Errorf("restored status = %v, want %v", restoredInfo.Status, core.FileStatusPending)
	}
	if restoredInfo.FileSize != int64(len("payload")) {
		t.Errorf("restored FileSize = %d, want %d", restoredInfo.FileSize, len("payload"))
	}
	if restoredInfo.TenantID != "backup-tenant" {
		t.Errorf("restored TenantID = %q, want %q", restoredInfo.TenantID, "backup-tenant")
	}
}

// TestRestoreMetadataRejectsNilInputs guards the documented argument contract.
func TestRestoreMetadataRejectsNilInputs(t *testing.T) {
	ctx := context.Background()

	if err := venue.RestoreMetadata(ctx, nil, strings.NewReader("")); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("RestoreMetadata(nil config) error = %v, want core.ErrInvalidArgument", err)
	}
	if err := venue.RestoreMetadata(ctx, newLifecycleConfig(t), nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Errorf("RestoreMetadata(nil reader) error = %v, want core.ErrInvalidArgument", err)
	}
}

func assertEffectiveLimit(
	t *testing.T,
	ctx context.Context,
	administrator core.TenantQuotaAdministrator,
	tenantID string,
	want int,
	label string,
) {
	t.Helper()

	got, err := administrator.GetEffectiveLimit(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetEffectiveLimit(%q) error = %v", tenantID, err)
	}
	if got != want {
		t.Errorf("GetEffectiveLimit(%q) = %d, want %d (%s)", tenantID, got, want, label)
	}
}

func measurementByName(measurements []core.StatisticsMeasurement, name string) (core.StatisticsMeasurement, bool) {
	for _, measurement := range measurements {
		if measurement.Name == name {
			return measurement, true
		}
	}
	return core.StatisticsMeasurement{}, false
}

func measurementNames(measurements []core.StatisticsMeasurement) []string {
	names := make([]string, 0, len(measurements))
	for _, measurement := range measurements {
		names = append(names, measurement.Name)
	}
	return names
}

// summaryCaptureHandler records the log records a Venue runtime emits so a test
// can observe the statistics summary without touching slog.Default.
type summaryCaptureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *summaryCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *summaryCaptureHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *summaryCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *summaryCaptureHandler) WithGroup(string) slog.Handler      { return h }

func (h *summaryCaptureHandler) recordCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

// summaryCount counts the records whose Venue event attribute is the statistics
// summary event.
func (h *summaryCaptureHandler) summaryCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	count := 0
	for _, record := range h.records {
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "event" && attr.Value.String() == "statistics_summary" {
				count++
			}
			return true
		})
	}
	return count
}
