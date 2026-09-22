package volume

import (
	"sync/atomic"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// writePathCounters aggregates the write-path observations of one
// LocalFileSystemVolume.
//
// Every counter is an atomic because the write fast path must never take a
// mutex, and durations are accumulated in nanoseconds since atomics have no
// time.Duration variant. The zero value is ready to use and reports the zero
// snapshot. A value of this type must never be copied: it carries atomics.
type writePathCounters struct {
	totalWrites                  atomic.Int64
	totalBytes                   atomic.Int64
	failedWrites                 atomic.Int64
	directoryPreparationCount    atomic.Int64
	directoryPreparationDuration atomic.Int64
	copyOperationCount           atomic.Int64
	copyDuration                 atomic.Int64
	fsyncCount                   atomic.Int64
	fsyncDuration                atomic.Int64
}

// snapshot returns an independent copy of the current counter values, so a
// caller can mutate or retain it without disturbing the volume.
func (c *writePathCounters) snapshot() core.StorageVolumeWritePathStatistics {
	return core.StorageVolumeWritePathStatistics{
		TotalWrites:                  c.totalWrites.Load(),
		TotalBytes:                   c.totalBytes.Load(),
		FailedWrites:                 c.failedWrites.Load(),
		DirectoryPreparationCount:    c.directoryPreparationCount.Load(),
		DirectoryPreparationDuration: time.Duration(c.directoryPreparationDuration.Load()),
		CopyOperationCount:           c.copyOperationCount.Load(),
		CopyDuration:                 time.Duration(c.copyDuration.Load()),
		FsyncCount:                   c.fsyncCount.Load(),
		FsyncDuration:                time.Duration(c.fsyncDuration.Load()),
	}
}

// observeWritePhase records one attempt of a timed write-path phase: the phase
// was entered (count) and took time.Since(started) (duration). It is called for
// every attempt, successful or not, because the observation describes the work
// this volume actually performed. Durations are measured with the monotonic
// clock rather than the injectable health-cache clock so a frozen test clock
// cannot flatten them.
func observeWritePhase(count *atomic.Int64, duration *atomic.Int64, started time.Time) {
	count.Add(1)
	duration.Add(int64(time.Since(started)))
}
