package statistics

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// --- test doubles -----------------------------------------------------------

// captureHandler is a minimal slog.Handler that counts emitted records and
// captures their attributes.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record.Clone())

	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.records)
}

// attrsOf returns the captured attributes of the most recent record.
func (h *captureHandler) attrsOf(t *testing.T) map[string]any {
	t.Helper()

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.records) == 0 {
		t.Fatal("expected at least one emitted record")
	}

	record := h.records[len(h.records)-1]

	attrs := make(map[string]any)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()

		return true
	})

	return attrs
}

// --- options and validation -------------------------------------------------

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.WindowSize != 5*time.Minute {
		t.Errorf("WindowSize = %v, want 5m", opts.WindowSize)
	}
	if opts.Retention != time.Hour {
		t.Errorf("Retention = %v, want 1h", opts.Retention)
	}
	if opts.MaxSeries != 16384 {
		t.Errorf("MaxSeries = %d, want 16384", opts.MaxSeries)
	}
	if opts.Dimensions.TenantID {
		t.Error("Dimensions.TenantID = true, want false by default")
	}
	if !opts.Dimensions.VolumeID {
		t.Error("Dimensions.VolumeID = false, want true by default")
	}
	if !opts.Dimensions.WatcherID {
		t.Error("Dimensions.WatcherID = false, want true by default")
	}
	if !opts.Dimensions.Operation {
		t.Error("Dimensions.Operation = false, want true by default")
	}
	if !opts.Output.Enabled {
		t.Error("Output.Enabled = false, want true by default")
	}
	if opts.Output.Sink != "Logging" {
		t.Errorf("Output.Sink = %q, want %q", opts.Output.Sink, "Logging")
	}
	if opts.Output.Interval != time.Minute {
		t.Errorf("Output.Interval = %v, want 1m", opts.Output.Interval)
	}
	if opts.Output.QueryWindow != 15*time.Minute {
		t.Errorf("Output.QueryWindow = %v, want 15m", opts.Output.QueryWindow)
	}
}

func TestNewRecorderValidation(t *testing.T) {
	valid := DefaultOptions()

	tests := []struct {
		name    string
		mutate  func(*Options)
		wantErr bool
	}{
		{name: "defaults are valid", mutate: func(*Options) {}},
		{name: "zero window size", mutate: func(o *Options) { o.WindowSize = 0 }, wantErr: true},
		{name: "negative window size", mutate: func(o *Options) { o.WindowSize = -time.Second }, wantErr: true},
		{name: "zero retention", mutate: func(o *Options) { o.Retention = 0 }, wantErr: true},
		{name: "retention shorter than window", mutate: func(o *Options) { o.Retention = time.Minute }, wantErr: true},
		{
			name: "retention equal to window",
			mutate: func(o *Options) {
				o.WindowSize = time.Minute
				o.Retention = time.Minute
			},
		},
		{name: "max series below floor", mutate: func(o *Options) { o.MaxSeries = 1023 }, wantErr: true},
		{name: "max series at floor", mutate: func(o *Options) { o.MaxSeries = 1024 }},
		{name: "max series at ceiling", mutate: func(o *Options) { o.MaxSeries = 262144 }},
		{name: "max series above ceiling", mutate: func(o *Options) { o.MaxSeries = 262145 }, wantErr: true},
		{name: "sink case insensitive", mutate: func(o *Options) { o.Output.Sink = "logging" }},
		{name: "sink uppercase", mutate: func(o *Options) { o.Output.Sink = "LOGGING" }},
		{name: "unknown sink", mutate: func(o *Options) { o.Output.Sink = "Console" }, wantErr: true},
		{name: "empty sink", mutate: func(o *Options) { o.Output.Sink = "" }, wantErr: true},
		{name: "zero output interval", mutate: func(o *Options) { o.Output.Interval = 0 }, wantErr: true},
		{name: "zero query window", mutate: func(o *Options) { o.Output.QueryWindow = 0 }, wantErr: true},
		{
			name: "disabled output ignores interval",
			mutate: func(o *Options) {
				o.Output.Enabled = false
				o.Output.Interval = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := valid
			tt.mutate(&opts)

			recorder, err := NewRecorder(opts)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewRecorder() error = nil, want ErrInvalidArgument")
				}
				if !errors.Is(err, core.ErrInvalidArgument) {
					t.Fatalf("NewRecorder() error = %v, want it to wrap core.ErrInvalidArgument", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("NewRecorder() error = %v, want nil", err)
			}
			if recorder == nil {
				t.Fatal("NewRecorder() = nil, want a recorder")
			}
		})
	}
}

// --- recording and aggregation ----------------------------------------------

// newTestRecorder builds a recorder over a small fixed window so buckets align
// deterministically, and pins its clock so pruning is evaluated against the
// same instant the test records at.
func newTestRecorder(t *testing.T, mutate func(*Options)) (*Recorder, Options, time.Time) {
	t.Helper()

	window := time.Minute
	base := time.Now().UTC().Truncate(window)

	opts := DefaultOptions()
	opts.WindowSize = window
	opts.Retention = time.Hour
	opts.MaxSeries = 1024
	opts.Dimensions = DimensionOptions{TenantID: true, VolumeID: true, WatcherID: true, Operation: true}

	if mutate != nil {
		mutate(&opts)
	}

	recorder, err := NewRecorder(opts)
	if err != nil {
		t.Fatalf("NewRecorder() error = %v", err)
	}

	recorder.now = func() time.Time { return base }

	return recorder, opts, base
}

func TestRecorderIgnoresEmptyNameAndZeroValue(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	recorder.Record("", 5, base, nil)
	recorder.Record(core.StatisticStorageWriteBytes, 0, base, nil)

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})

	if len(snapshot.Measurements) != 0 {
		t.Fatalf("Measurements = %v, want none for an empty name and a zero value", snapshot.Measurements)
	}
	if snapshot.WriteBytes != 0 || snapshot.WriteFileCount != 0 {
		t.Fatalf("snapshot = %+v, want zero totals", snapshot)
	}
}

func TestRecorderAggregatesAndFillsSnapshot(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	tenantDim := map[string]string{core.StatisticsDimensionTenantID: "tenant-a", core.StatisticsDimensionVolumeID: "vol-1"}

	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base.Add(10*time.Second), tenantDim)
	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base.Add(50*time.Second), tenantDim)
	recorder.Record(core.StatisticStorageWriteBytes, 1024*1024, base.Add(20*time.Second), tenantDim)
	recorder.Record(core.StatisticStorageFileDequeuedCount, 1, base.Add(30*time.Second), tenantDim)
	recorder.Record(core.StatisticStorageFileReadCount, 1, base.Add(30*time.Second), tenantDim)
	recorder.Record(core.StatisticStorageFileCompletedCount, 1, base.Add(30*time.Second), tenantDim)
	recorder.Record(core.StatisticMetadataPersistedOperationCount, 3, base.Add(30*time.Second), tenantDim)
	recorder.Record(core.StatisticWatcherFilesImported, 2, base.Add(30*time.Second), tenantDim)
	recorder.Record(core.StatisticWatcherBytesImported, 2048, base.Add(30*time.Second), tenantDim)

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})

	if snapshot.WriteFileCount != 2 {
		t.Errorf("WriteFileCount = %d, want 2", snapshot.WriteFileCount)
	}
	if snapshot.WriteBytes != 1024*1024 {
		t.Errorf("WriteBytes = %d, want %d", snapshot.WriteBytes, 1024*1024)
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
	if snapshot.MetadataPersistedOperationCount != 3 {
		t.Errorf("MetadataPersistedOperationCount = %d, want 3", snapshot.MetadataPersistedOperationCount)
	}
	if snapshot.WatcherImportedFileCount != 2 {
		t.Errorf("WatcherImportedFileCount = %d, want 2", snapshot.WatcherImportedFileCount)
	}
	if snapshot.WatcherImportedBytes != 2048 {
		t.Errorf("WatcherImportedBytes = %d, want 2048", snapshot.WatcherImportedBytes)
	}

	// 1 MiB over a 60 second bucket is 1/60 MiB per second.
	wantThroughput := float64(1024*1024) / 1024 / 1024 / 60
	if diff := snapshot.WriteMegabytesPerSecond - wantThroughput; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("WriteMegabytesPerSecond = %v, want %v", snapshot.WriteMegabytesPerSecond, wantThroughput)
	}

	// Everything shares one dimension set, so the write byte total collapses into
	// one measurement plus the derived throughput measurement.
	if len(snapshot.Measurements) != len(namesOf(snapshot.Measurements)) {
		t.Fatalf("Measurements = %v, want one entry per distinct name", snapshot.Measurements)
	}

	throughput, ok := findMeasurement(snapshot.Measurements, core.StatisticStorageWriteMegabytesPerSecond)
	if !ok {
		t.Fatalf("Measurements = %v, want the derived throughput measurement", snapshot.Measurements)
	}
	if diff := throughput.Value - wantThroughput; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("derived throughput measurement = %v, want %v", throughput.Value, wantThroughput)
	}
	if throughput.Dimensions[core.StatisticsDimensionVolumeID] != "vol-1" {
		t.Errorf("derived throughput dimensions = %v, want the aggregate dimensions", throughput.Dimensions)
	}

	bytesMeasurement, ok := findMeasurement(snapshot.Measurements, core.StatisticStorageWriteBytes)
	if !ok {
		t.Fatalf("Measurements = %v, want the write byte measurement", snapshot.Measurements)
	}
	if bytesMeasurement.Value != 1024*1024 {
		t.Errorf("write byte measurement = %v, want %d", bytesMeasurement.Value, 1024*1024)
	}
	if bytesMeasurement.Dimensions[core.StatisticsDimensionTenantID] != "tenant-a" {
		t.Errorf("write byte dimensions = %v, want tenant-a", bytesMeasurement.Dimensions)
	}
}

func TestRecorderRetainsOnlyEnabledDimensions(t *testing.T) {
	recorder, _, base := newTestRecorder(t, func(o *Options) {
		o.Dimensions = DimensionOptions{TenantID: true}
	})

	recorder.Record(core.StatisticWatcherFilesImported, 1, base, map[string]string{
		core.StatisticsDimensionTenantID:  "tenant-a",
		core.StatisticsDimensionVolumeID:  "vol-1",
		core.StatisticsDimensionWatcherID: "w1",
		core.StatisticsDimensionOperation: "scan",
	})

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if len(snapshot.Measurements) != 1 {
		t.Fatalf("Measurements = %v, want one measurement", snapshot.Measurements)
	}

	dimensions := snapshot.Measurements[0].Dimensions
	if len(dimensions) != 1 || dimensions[core.StatisticsDimensionTenantID] != "tenant-a" {
		t.Fatalf("Dimensions = %v, want only tenant_id", dimensions)
	}
}

func TestRecorderDropsEmptyDimensionValues(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	recorder.Record(core.StatisticWatcherScanCount, 1, base, map[string]string{
		core.StatisticsDimensionTenantID:  "",
		core.StatisticsDimensionWatcherID: "w1",
	})

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if len(snapshot.Measurements) != 1 {
		t.Fatalf("Measurements = %v, want one measurement", snapshot.Measurements)
	}

	if _, ok := snapshot.Measurements[0].Dimensions[core.StatisticsDimensionTenantID]; ok {
		t.Fatalf("Dimensions = %v, want the empty tenant_id to be dropped", snapshot.Measurements[0].Dimensions)
	}
	if snapshot.Measurements[0].Dimensions[core.StatisticsDimensionWatcherID] != "w1" {
		t.Fatalf("Dimensions = %v, want watcher_id retained", snapshot.Measurements[0].Dimensions)
	}
}

func TestRecorderSeparatesDimensionsAndBuckets(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	recorder.Record(core.StatisticStorageWriteBytes, 100, base.Add(time.Second), map[string]string{
		core.StatisticsDimensionTenantID: "tenant-a",
	})
	recorder.Record(core.StatisticStorageWriteBytes, 200, base.Add(time.Second), map[string]string{
		core.StatisticsDimensionTenantID: "tenant-b",
	})
	// A different window must not merge with the first one.
	recorder.Record(core.StatisticStorageWriteBytes, 400, base.Add(2*time.Minute), map[string]string{
		core.StatisticsDimensionTenantID: "tenant-a",
	})

	all := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(3 * time.Minute)})
	if all.WriteBytes != 700 {
		t.Errorf("WriteBytes over all buckets = %d, want 700", all.WriteBytes)
	}

	first := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if first.WriteBytes != 300 {
		t.Errorf("WriteBytes in the first bucket = %d, want 300", first.WriteBytes)
	}

	filtered := recorder.Snapshot(core.StatisticsQuery{
		From:     base,
		To:       base.Add(3 * time.Minute),
		TenantID: "tenant-b",
	})
	if filtered.WriteBytes != 200 {
		t.Errorf("WriteBytes for tenant-b = %d, want 200", filtered.WriteBytes)
	}
}

func TestRecorderOverlappingBucketIsIncluded(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	// The record lands in the bucket that starts at base; a query that only
	// overlaps the tail of that bucket still includes it.
	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base.Add(59*time.Second), nil)

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base.Add(30 * time.Second), To: base.Add(45 * time.Second)})
	if snapshot.WriteFileCount != 1 {
		t.Fatalf("WriteFileCount = %d, want the overlapping bucket included", snapshot.WriteFileCount)
	}
}

func TestRecorderSnapshotRejectsEmptyRange(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base, nil)

	tests := []struct {
		name  string
		query core.StatisticsQuery
	}{
		{name: "to equals from", query: core.StatisticsQuery{From: base, To: base}},
		{name: "to before from", query: core.StatisticsQuery{From: base.Add(time.Minute), To: base}},
		{name: "zero range", query: core.StatisticsQuery{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := recorder.Snapshot(tt.query)
			if snapshot.Measurements == nil {
				t.Fatal("Measurements = nil, want an empty non-nil slice")
			}
			if len(snapshot.Measurements) != 0 {
				t.Fatalf("Measurements = %v, want empty", snapshot.Measurements)
			}
			if snapshot.WriteFileCount != 0 || snapshot.WriteBytes != 0 {
				t.Fatalf("snapshot = %+v, want zero totals", snapshot)
			}
		})
	}
}

func TestRecorderMeasurementsNeverNil(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if snapshot.Measurements == nil {
		t.Fatal("Measurements = nil, want an empty non-nil slice")
	}
}

func TestRecorderPrunesExpiredBucketsOnSnapshot(t *testing.T) {
	recorder, opts, base := newTestRecorder(t, nil)

	expiredAt := base.Add(-opts.Retention - 2*opts.WindowSize)
	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, expiredAt, nil)
	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base, nil)

	if got := recorder.seriesCount(); got != 2 {
		t.Fatalf("series count before pruning = %d, want 2", got)
	}

	fresh := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if fresh.WriteFileCount != 1 {
		t.Fatalf("WriteFileCount = %d, want only the fresh record", fresh.WriteFileCount)
	}

	if got := recorder.seriesCount(); got != 1 {
		t.Fatalf("series count after snapshot = %d, want the expired bucket pruned", got)
	}

	// A wider query can no longer see the expired bucket either.
	wide := recorder.Snapshot(core.StatisticsQuery{From: expiredAt, To: base.Add(time.Minute)})
	if wide.WriteFileCount != 1 {
		t.Fatalf("WriteFileCount over the whole history = %d, want the expired bucket pruned", wide.WriteFileCount)
	}
}

func TestRecorderBoundsSeriesWithInternalLimit(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	opts := optionsWithMaxSeries(DefaultOptions(), 4)
	recorder, err := newRecorder(opts, 4)
	if err != nil {
		t.Fatalf("newRecorder() error = %v", err)
	}

	for i := 0; i < 50; i++ {
		recorder.Record("custom.counter", int64(i), base, map[string]string{
			core.StatisticsDimensionWatcherID: "watcher-" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
		})
	}

	if got := recorder.seriesCount(); got > 4 {
		t.Fatalf("series count = %d, want at most the configured 4", got)
	}

	// An existing key keeps counting after the bound is reached.
	first := recorder.seriesKeys()[0]
	before := recorder.valueFor(first)
	recorder.Record(first.name, 7, base, dimensionsFor(first))

	if got := recorder.valueFor(first); got != before+7 {
		t.Fatalf("existing series value = %d, want %d (must keep counting after the bound)", got, before+7)
	}
}

func TestRecorderConcurrentRecordIsRaceFree(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	const (
		workers    = 8
		perWorker  = 200
		totalCount = workers * perWorker
	)

	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			dimensions := map[string]string{
				core.StatisticsDimensionTenantID: "tenant-" + string(rune('a'+worker%4)),
				core.StatisticsDimensionVolumeID: "vol-1",
			}

			for i := 0; i < perWorker; i++ {
				recorder.Record(core.StatisticStorageWriteSuccessCount, 1, base.Add(time.Duration(i)*time.Millisecond), dimensions)
			}
		}(worker)
	}

	wg.Wait()

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if snapshot.WriteFileCount != totalCount {
		t.Fatalf("WriteFileCount = %d, want %d", snapshot.WriteFileCount, totalCount)
	}
}

// --- noop recorder ----------------------------------------------------------

func TestNoopRecorder(t *testing.T) {
	var recorder core.StatisticsRecorder = Noop
	var reader core.StatisticsReader = Noop

	recorder.Record(core.StatisticStorageWriteSuccessCount, 1, time.Now(), map[string]string{"tenant_id": "tenant-a"})

	snapshot := reader.Snapshot(core.StatisticsQuery{From: time.Now().Add(-time.Hour), To: time.Now()})
	if snapshot.Measurements == nil {
		t.Fatal("Measurements = nil, want an empty non-nil slice")
	}
	if len(snapshot.Measurements) != 0 {
		t.Fatalf("Measurements = %v, want empty", snapshot.Measurements)
	}
	if snapshot.WriteFileCount != 0 || snapshot.WriteBytes != 0 || snapshot.WriteMegabytesPerSecond != 0 {
		t.Fatalf("snapshot = %+v, want the zero snapshot", snapshot)
	}
}

// --- output service ---------------------------------------------------------

func TestOutputServiceStartIsNoopWhenDisabled(t *testing.T) {
	handler := &captureHandler{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	recorder, _, _ := newTestRecorder(t, nil)

	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "statistics disabled", mutate: func(o *Options) { o.Enabled = false }},
		{name: "output disabled", mutate: func(o *Options) { o.Output.Enabled = false }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.Output.Interval = time.Millisecond
			tt.mutate(&opts)

			service := NewOutputService(recorder, opts, rt)
			service.Start()

			time.Sleep(20 * time.Millisecond)
			service.Stop()

			if handler.count() != 0 {
				t.Fatalf("emitted %d records, want none when the service is disabled", handler.count())
			}
		})
	}
}

func TestOutputServiceEmitsSummary(t *testing.T) {
	handler := &captureHandler{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	recorder, _, _ := newTestRecorder(t, nil)
	recorder.Record(core.StatisticStorageWriteSuccessCount, 3, time.Now().UTC(), nil)
	recorder.Record(core.StatisticStorageWriteBytes, 4096, time.Now().UTC(), nil)

	opts := DefaultOptions()
	opts.Output.Interval = 10 * time.Millisecond

	service := NewOutputService(recorder, opts, rt)
	service.Start()

	deadline := time.Now().Add(2 * time.Second)
	for handler.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	service.Stop()

	if handler.count() == 0 {
		t.Fatal("no statistics summary was emitted")
	}
	if handler.count() > 1 {
		t.Fatalf("emitted %d summaries in one interval, want exactly one", handler.count())
	}

	attrs := handler.attrsOf(t)
	if attrs["query_window"] != opts.Output.QueryWindow {
		t.Errorf("query_window = %v, want %v", attrs["query_window"], opts.Output.QueryWindow)
	}
	if attrs["write_file_count"] != int64(3) {
		t.Errorf("write_file_count = %v, want 3", attrs["write_file_count"])
	}
	if attrs["write_bytes"] != int64(4096) {
		t.Errorf("write_bytes = %v, want 4096", attrs["write_bytes"])
	}
	if _, ok := attrs["component"]; !ok {
		t.Errorf("attrs = %v, want a component attribute", attrs)
	}
	if _, ok := attrs["event"]; !ok {
		t.Errorf("attrs = %v, want an event attribute", attrs)
	}
}

func TestOutputServiceSkipsEmptySummaryUnlessIncluded(t *testing.T) {
	handler := &captureHandler{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	recorder, _, _ := newTestRecorder(t, nil)

	opts := DefaultOptions()
	opts.Output.Interval = time.Millisecond

	service := NewOutputService(recorder, opts, rt)
	service.Start()

	time.Sleep(30 * time.Millisecond)
	service.Stop()

	if handler.count() != 0 {
		t.Fatalf("emitted %d summaries, want none for an all-zero snapshot", handler.count())
	}

	included := &captureHandler{}
	includedRuntime, err := logging.New(logging.Config{Handler: included})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	opts.Output.IncludeEmptySnapshots = true

	service = NewOutputService(recorder, opts, includedRuntime)
	service.Start()

	deadline := time.Now().Add(2 * time.Second)
	for included.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	service.Stop()

	if included.count() == 0 {
		t.Fatal("IncludeEmptySnapshots = true must still emit an all-zero summary")
	}
}

func TestOutputServiceStopIsIdempotentAndRestartable(t *testing.T) {
	handler := &captureHandler{}
	rt, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	recorder, _, _ := newTestRecorder(t, nil)

	opts := DefaultOptions()
	opts.Output.Interval = time.Hour

	service := NewOutputService(recorder, opts, rt)

	// Stop before Start must not block or panic.
	service.Stop()

	service.Start()
	service.Start() // a second Start must not leak a second goroutine
	service.Stop()
	service.Stop()

	// A restart after Stop must not panic.
	service.Start()
	service.Stop()

	if handler.count() != 0 {
		t.Fatalf("emitted %d summaries, want none within the interval", handler.count())
	}
}

func TestOutputServiceNilRuntimeStillStops(t *testing.T) {
	recorder, _, _ := newTestRecorder(t, nil)

	opts := DefaultOptions()
	opts.Output.Interval = time.Millisecond

	service := NewOutputService(recorder, opts, nil)
	service.Start()

	done := make(chan struct{})

	go func() {
		defer close(done)

		time.Sleep(20 * time.Millisecond)
		service.Stop()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() with a nil logging runtime did not return")
	}
}

// --- helpers ----------------------------------------------------------------

func findMeasurement(measurements []core.StatisticsMeasurement, name string) (core.StatisticsMeasurement, bool) {
	for _, measurement := range measurements {
		if measurement.Name == name {
			return measurement, true
		}
	}

	return core.StatisticsMeasurement{}, false
}

func namesOf(measurements []core.StatisticsMeasurement) []string {
	out := make([]string, 0, len(measurements))

	for _, measurement := range measurements {
		out = append(out, measurement.Name)
	}

	return out
}

// dimensionsFor rebuilds the recorded dimension map of a series key.
func dimensionsFor(key seriesKey) map[string]string {
	dimensions := make(map[string]string, 4)

	if key.tenantID != "" {
		dimensions[core.StatisticsDimensionTenantID] = key.tenantID
	}
	if key.volumeID != "" {
		dimensions[core.StatisticsDimensionVolumeID] = key.volumeID
	}
	if key.watcherID != "" {
		dimensions[core.StatisticsDimensionWatcherID] = key.watcherID
	}
	if key.operation != "" {
		dimensions[core.StatisticsDimensionOperation] = key.operation
	}

	return dimensions
}
