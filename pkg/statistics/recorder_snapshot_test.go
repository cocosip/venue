package statistics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// --- snapshot ordering -------------------------------------------------------

// TestSnapshotOrdersMeasurementsAndOmitsEmptyDimensions pins the stable order of
// the aggregated measurements and the compact representation of a measurement
// that carries no dimensions: the series table iterates in an unspecified order,
// so the snapshot must sort, and an undimensioned aggregate must not allocate an
// empty map for every query.
func TestSnapshotOrdersMeasurementsAndOmitsEmptyDimensions(t *testing.T) {
	recorder, _, base := newTestRecorder(t, nil)

	// Recorded in an order that does not match the sorted order.
	recorder.Record("zeta.counter", 1, base, map[string]string{
		core.StatisticsDimensionTenantID: "tenant-b",
	})
	recorder.Record("alpha.counter", 1, base, map[string]string{
		core.StatisticsDimensionTenantID: "tenant-c",
	})
	recorder.Record("alpha.counter", 1, base, map[string]string{
		core.StatisticsDimensionTenantID: "tenant-a",
	})
	recorder.Record("bare.counter", 1, base, nil)

	snapshot := recorder.Snapshot(core.StatisticsQuery{From: base, To: base.Add(time.Minute)})
	if len(snapshot.Measurements) != 4 {
		t.Fatalf("Measurements = %v, want 4 aggregates", snapshot.Measurements)
	}

	type order struct {
		name   string
		tenant string
	}
	want := []order{
		{name: "alpha.counter", tenant: "tenant-a"},
		{name: "alpha.counter", tenant: "tenant-c"},
		{name: "bare.counter"},
		{name: "zeta.counter", tenant: "tenant-b"},
	}
	for i, measurement := range snapshot.Measurements {
		if measurement.Name != want[i].name {
			t.Fatalf("Measurements[%d].Name = %q, want %q (full: %v)",
				i, measurement.Name, want[i].name, snapshot.Measurements)
		}
		if got := measurement.Dimensions[core.StatisticsDimensionTenantID]; got != want[i].tenant {
			t.Fatalf("Measurements[%d] tenant = %q, want %q (full: %v)",
				i, got, want[i].tenant, snapshot.Measurements)
		}
	}

	bare := snapshot.Measurements[2]
	if bare.Name != "bare.counter" {
		t.Fatalf("Measurements[2].Name = %q, want bare.counter", bare.Name)
	}
	if bare.Dimensions != nil {
		t.Fatalf("bare.Dimensions = %v, want nil for a measurement without dimensions", bare.Dimensions)
	}
}

// --- record-path prune -------------------------------------------------------

// TestRecordPruneDoesNotRunOnTheRecordPath is the regression test for the
// synchronous record-path prune: the 1024th delta used to run a full
// O(series) scan on the recording goroutine. Post-fix the scan runs in a
// single-flight background pass, so the recording call returns in
// microseconds even when the table needs a long scan.
func TestRecordPruneDoesNotRunOnTheRecordPath(t *testing.T) {
	const series = 100000
	now := time.Now()

	newFilledRecorder := func(t *testing.T) *Recorder {
		t.Helper()
		recorder, err := NewRecorder(Options{
			Enabled:    true,
			WindowSize: time.Minute,
			Retention:  time.Minute,
			MaxSeries:  MaximumSeries,
		})
		if err != nil {
			t.Fatalf("NewRecorder() error = %v", err)
		}
		recorder.now = func() time.Time { return now }
		fillExpiredSeries(recorder, series, now)
		return recorder
	}

	// Calibration: how long does the whole-table scan take on this host?
	calibration := newFilledRecorder(t)
	scanStart := time.Now()
	calibration.prune(now)
	scanDuration := time.Since(scanStart)
	if scanDuration <= 0 {
		t.Fatal("prune() took no measurable time, the calibration is unusable")
	}

	recorder := newFilledRecorder(t)
	// The next Record crosses the periodic prune probe.
	recorder.recorded.Store(pruneInterval - 1)

	recordStart := time.Now()
	recorder.Record("fresh-series", 1, now, nil)
	recordDuration := time.Since(recordStart)

	if recordDuration > scanDuration/2 {
		t.Fatalf("Record() with a due prune took %s (whole-scan time %s): the prune still runs on the record path",
			recordDuration, scanDuration)
	}

	// The background pass must still do the work.
	deadline := time.Now().Add(5 * time.Second)
	for recorder.seriesCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := recorder.seriesCount(); got != 1 {
		t.Fatalf("seriesCount() after the background prune = %d, want only the fresh series", got)
	}
}

// TestRecordPruneHonoursTheTimeGate verifies the record-time probe is also gated
// on elapsed time, so a burst of record counters cannot start one background
// scan per burst.
func TestRecordPruneHonoursTheTimeGate(t *testing.T) {
	now := time.Now()
	recorder, err := NewRecorder(Options{
		Enabled:    true,
		WindowSize: time.Minute,
		Retention:  time.Minute,
		MaxSeries:  4096,
	})
	if err != nil {
		t.Fatalf("NewRecorder() error = %v", err)
	}
	recorder.now = func() time.Time { return now }

	fillExpiredSeries(recorder, 10, now)
	// A recent prune closes the gate until another window has elapsed.
	recorder.lastPrune.Store(now.UnixNano())
	recorder.recorded.Store(pruneInterval - 1)

	recorder.Record("fresh-series", 1, now, nil)

	// No background pass may be started while the gate is closed, so the
	// expired rows are still present right after the call.
	if got := recorder.seriesCount(); got != 11 {
		t.Fatalf("seriesCount() = %d, want the 10 expired rows and the fresh one (the time gate must block the prune)", got)
	}
}

// --- summary emission --------------------------------------------------------

// countingStatisticsReader counts how often a snapshot is aggregated.
type countingStatisticsReader struct {
	calls atomic.Int64
}

func (r *countingStatisticsReader) Snapshot(core.StatisticsQuery) core.StatisticsSnapshot {
	r.calls.Add(1)
	return core.StatisticsSnapshot{Measurements: []core.StatisticsMeasurement{}}
}

// silentSlogHandler accepts no record at any level.
type silentSlogHandler struct{}

func (silentSlogHandler) Enabled(context.Context, slog.Level) bool { return false }

func (silentSlogHandler) Handle(context.Context, slog.Record) error { return nil }

func (silentSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return silentSlogHandler{} }

func (silentSlogHandler) WithGroup(string) slog.Handler { return silentSlogHandler{} }

// TestSummaryEmissionSkipsAggregationWhenOutputIsDisabled is the regression test
// for the summary that aggregated a snapshot before checking whether it could log
// it: a nil runtime or a handler that rejects the level must short-circuit before
// the Snapshot work.
func TestSummaryEmissionSkipsAggregationWhenOutputIsDisabled(t *testing.T) {
	opts := DefaultOptions()
	opts.Output.Interval = time.Hour

	rejectingRuntime, err := logging.New(logging.Config{Handler: silentSlogHandler{}})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	tests := []struct {
		name string
		rt   *logging.Runtime
	}{
		{name: "nil runtime", rt: nil},
		{name: "handler rejects the level", rt: rejectingRuntime},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &countingStatisticsReader{}
			service := NewOutputService(reader, opts, test.rt)

			service.emitSummary(context.Background())

			if got := reader.calls.Load(); got != 0 {
				t.Fatalf("Snapshot calls = %d, want 0 when the summary cannot be emitted", got)
			}
		})
	}
}

var _ core.StatisticsReader = (*countingStatisticsReader)(nil)
