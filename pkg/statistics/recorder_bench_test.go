package statistics

import (
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// BenchmarkSnapshot measures one Snapshot over a populated recorder, which is the
// aggregation and ordering path a reader exercises on every query.
//
// The fixture is built outside the timed loop with Record, so the benchmark
// measures the real snapshot path over real series rows.
func BenchmarkSnapshot(b *testing.B) {
	opts := DefaultOptions()
	opts.Dimensions = DimensionOptions{TenantID: true, VolumeID: true, WatcherID: true, Operation: true}

	recorder, err := NewRecorder(opts)
	if err != nil {
		b.Fatalf("NewRecorder() error = %v", err)
	}

	const (
		tenants  = 4
		volumes  = 2
		watchers = 4
		names    = 4
	)
	base := time.Now().UTC()
	for tenant := 0; tenant < tenants; tenant++ {
		for volume := 0; volume < volumes; volume++ {
			for watcher := 0; watcher < watchers; watcher++ {
				for name := 0; name < names; name++ {
					recorder.Record(
						fmt.Sprintf("bench.measurement.%d", name),
						1,
						base,
						map[string]string{
							core.StatisticsDimensionTenantID:  fmt.Sprintf("tenant-%d", tenant),
							core.StatisticsDimensionVolumeID:  fmt.Sprintf("volume-%d", volume),
							core.StatisticsDimensionWatcherID: fmt.Sprintf("watcher-%d", watcher),
							core.StatisticsDimensionOperation: "op",
						},
					)
				}
			}
		}
	}

	query := core.StatisticsQuery{From: base.Add(-time.Minute), To: base.Add(time.Minute)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = recorder.Snapshot(query)
	}
}
