package cleanup

import (
	"sync/atomic"

	"github.com/cocosip/venue/pkg/core"
)

// cumulativeCleanupCounters holds the process-lifetime totals of every cleanup
// operation.
//
// The counters are monotonic: an operation only ever adds its own results, and
// nothing resets them while the cleanup service lives. Every counter is an
// atomic int64, so a snapshot can be read safely while cleanup operations run
// concurrently (including under the race detector).
//
// The struct must not be copied: it embeds atomic values.
type cumulativeCleanupCounters struct {
	emptyDirectoriesRemoved       atomic.Int64
	completedRecordsRemoved       atomic.Int64
	permanentlyFailedFilesRemoved atomic.Int64
	deadLetteredFiles             atomic.Int64
	junkFilesRemoved              atomic.Int64
	invalidDatabaseBackupsRemoved atomic.Int64
	timedOutFilesReset            atomic.Int64
	orphanedMetadataRemoved       atomic.Int64
	metadataDatabasesOptimized    atomic.Int64
	quotaDatabasesOptimized       atomic.Int64
	spaceFreed                    atomic.Int64
}

// add folds one operation's results into the lifetime totals.
//
// A nil statistic is a no-op so callers can record unconditionally. Only the
// fields an operation actually produces are added, so an operation that leaves a
// field at zero cannot distort an unrelated total.
func (c *cumulativeCleanupCounters) add(stats *core.CleanupStatistics) {
	if stats == nil {
		return
	}

	c.emptyDirectoriesRemoved.Add(int64(stats.EmptyDirectoriesRemoved))
	c.completedRecordsRemoved.Add(int64(stats.CompletedRecordsRemoved))
	c.permanentlyFailedFilesRemoved.Add(int64(stats.PermanentlyFailedFilesRemoved))
	c.deadLetteredFiles.Add(int64(stats.DeadLetteredFiles))
	c.junkFilesRemoved.Add(int64(stats.JunkFilesRemoved))
	c.invalidDatabaseBackupsRemoved.Add(int64(stats.InvalidDatabaseBackupsRemoved))
	c.timedOutFilesReset.Add(int64(stats.TimedOutFilesReset))
	c.orphanedMetadataRemoved.Add(int64(stats.OrphanedMetadataRemoved))
	c.metadataDatabasesOptimized.Add(int64(stats.MetadataDatabasesOptimized))
	c.quotaDatabasesOptimized.Add(int64(stats.QuotaDatabasesOptimized))
	c.spaceFreed.Add(stats.SpaceFreed)
}

// snapshot returns an independent copy of the current totals. The caller owns
// the returned struct and may mutate it freely.
func (c *cumulativeCleanupCounters) snapshot() *core.CleanupStatistics {
	return &core.CleanupStatistics{
		EmptyDirectoriesRemoved:       int(c.emptyDirectoriesRemoved.Load()),
		CompletedRecordsRemoved:       int(c.completedRecordsRemoved.Load()),
		PermanentlyFailedFilesRemoved: int(c.permanentlyFailedFilesRemoved.Load()),
		DeadLetteredFiles:             int(c.deadLetteredFiles.Load()),
		JunkFilesRemoved:              int(c.junkFilesRemoved.Load()),
		InvalidDatabaseBackupsRemoved: int(c.invalidDatabaseBackupsRemoved.Load()),
		TimedOutFilesReset:            int(c.timedOutFilesReset.Load()),
		OrphanedMetadataRemoved:       int(c.orphanedMetadataRemoved.Load()),
		MetadataDatabasesOptimized:    int(c.metadataDatabasesOptimized.Load()),
		QuotaDatabasesOptimized:       int(c.quotaDatabasesOptimized.Load()),
		SpaceFreed:                    c.spaceFreed.Load(),
	}
}

// recordCumulative folds one operation's results into the process-lifetime
// totals.
//
// Every cleanup operation records through this helper, so CumulativeStatistics
// reflects the whole lifetime of the service rather than only the last call.
func (s *cleanupService) recordCumulative(stats *core.CleanupStatistics) {
	s.cumulative.add(stats)
}

// CumulativeStatistics returns an independent copy of the process-lifetime
// totals of every cleanup operation.
//
// Counters are monotonic and never reset while the runtime lives, so repeated
// calls never observe a smaller value. The returned struct belongs to the
// caller.
func (s *cleanupService) CumulativeStatistics() *core.CleanupStatistics {
	return s.cumulative.snapshot()
}
