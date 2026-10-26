package statistics

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fillExpiredSeries stores n expired series directly in the table.
//
// The entries are stored directly rather than through Record: Record itself
// prunes every pruneInterval deltas, which would empty the table before the
// measured prune runs.
func fillExpiredSeries(recorder *Recorder, n int, now time.Time) {
	expiredBucket := bucketStart(now.Add(-24*time.Hour), time.Minute)
	for i := 0; i < n; i++ {
		recorder.series.Store(seriesKey{bucket: expiredBucket, name: fmt.Sprintf("measurement-%d", i)}, &counter{})
	}
	recorder.count = n
}

// TestPruneDoesNotBlockRecordOnAFullSeriesTable is the regression test for the
// record-path stall: prune used to hold r.mu across a full sync.Map range while
// cell() needed the same mutex, so once the table filled up every new series
// creation waited for an O(series) scan.
//
// The assertion is self-calibrating: the time prune needs for the whole scan is
// measured first, then a series creation is timed while a second prune runs over
// an identically sized table. Pre-fix, the creation waits for the scan, so its
// latency tracks the scan time; post-fix it only waits for one lock acquisition,
// so it stays far below it.
func TestPruneDoesNotBlockRecordOnAFullSeriesTable(t *testing.T) {
	// The bound must stay within the recorder's own MaxSeries range.
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

	// Calibration: how long does one prune take over this table?
	calibration := newFilledRecorder(t)
	scanStart := time.Now()
	calibration.prune(now)
	scanDuration := time.Since(scanStart)
	if scanDuration <= 0 {
		t.Fatalf("prune() took no measurable time, the calibration is unusable")
	}

	// Measurement: a series creation racing a prune over the same size table.
	recorder := newFilledRecorder(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		recorder.prune(now)
	}()

	// Give the prune a chance to enter its scan before the creation is timed.
	time.Sleep(scanDuration / 4)

	createStart := time.Now()
	recorder.Record("fresh-series", 1, now, nil)
	createDuration := time.Since(createStart)

	wg.Wait()

	// The creation must not have waited for the scan. Half the scan time is a
	// generous bound that still fails decisively for the lock-holding version,
	// where the latency is the scan time itself.
	if createDuration > scanDuration/2 {
		t.Fatalf("creating a series during prune took %s (whole-scan time %s): the record path is blocked by the prune scan",
			createDuration, scanDuration)
	}

	// The expired buckets must be gone and the fresh one must survive.
	if got := recorder.seriesCount(); got != 1 {
		t.Fatalf("seriesCount() after prune = %d, want only the fresh series", got)
	}
}

// TestPruneCountsEachRemovalOnce covers the concurrent-prune accounting: prunes
// racing over the same expired keys must not decrement the live-series counter
// more than once per removed key.
func TestPruneCountsEachRemovalOnce(t *testing.T) {
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

	fillExpiredSeries(recorder, 500, now)
	for i := 0; i < 20; i++ {
		recorder.Record(fmt.Sprintf("live-%d", i), 1, now, nil)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder.prune(now)
		}()
	}
	wg.Wait()

	if got := recorder.seriesCount(); got != 20 {
		t.Fatalf("seriesCount() = %d, want the 20 live series", got)
	}
}
