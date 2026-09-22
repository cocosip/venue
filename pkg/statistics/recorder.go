// Package statistics provides bounded, in-process runtime statistics for
// Venue.
//
// The package exposes two independent pieces:
//
//   - Recorder, a concurrent core.StatisticsRecorder and core.StatisticsReader
//     that aggregates low-overhead counter deltas into time buckets and
//     aggregates them back on demand. It never performs I/O, so it is safe to
//     call from storage, scheduler, and watcher hot paths.
//   - OutputService, an optional background emitter that periodically logs a
//     summary of the recorder through the instance logging runtime.
//
// Everything is allocation-bounded: the recorder keeps at most Options.MaxSeries
// distinct series and prunes buckets older than
// Options.Retention + Options.WindowSize.
package statistics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

const (
	// MinimumSeries is the smallest configurable series bound. The exported
	// validation refuses anything lower so a production recorder cannot be
	// configured into dropping nearly every series.
	MinimumSeries = 1024

	// MaximumSeries is the largest configurable series bound. It caps the
	// recorder's memory footprint.
	MaximumSeries = 262144

	// loggingSinkName is the only supported output sink. It is compared
	// case-insensitively against Options.Output.Sink.
	loggingSinkName = "Logging"

	// pruneInterval is how many recorded deltas may pass before a record-time
	// prune check is attempted. Pruning is also performed by every Snapshot, so
	// an idle recorder is never left holding expired buckets.
	pruneInterval = 1024

	// statisticsComponent is the logging component for statistics events.
	statisticsComponent = "statistics"

	// summaryEvent is the stable event name of the periodic summary.
	summaryEvent = "statistics_summary"

	// summaryMessage is the human-readable summary message. It never contains
	// paths, file names, or tenant-sensitive payloads.
	summaryMessage = "Runtime statistics summary"
)

// Options configures a Recorder and its optional OutputService.
type Options struct {
	// Enabled gates the periodic output service. It does not gate recording:
	// a Recorder always records so a reader can still be served.
	Enabled bool

	// WindowSize is the aggregation bucket width. Buckets are aligned to the
	// Unix epoch, so two recorders with the same window size agree on bucket
	// boundaries regardless of when they started.
	WindowSize time.Duration

	// Retention is how long a bucket stays queryable.
	Retention time.Duration

	// MaxSeries is the maximum number of distinct (bucket, name, dimensions)
	// series kept at once. A new series beyond this bound is dropped; existing
	// series keep counting.
	MaxSeries int

	// Dimensions selects which record dimensions are retained.
	Dimensions DimensionOptions

	// Output configures the periodic summary emitter.
	Output OutputOptions
}

// DimensionOptions selects the record dimensions a Recorder retains.
//
// A dimension is retained only when its flag is enabled and the recorded value
// is non-empty, so an unset dimension never splits an aggregate.
type DimensionOptions struct {
	// TenantID retains the tenant_id dimension.
	TenantID bool

	// VolumeID retains the volume_id dimension.
	VolumeID bool

	// WatcherID retains the watcher_id dimension.
	WatcherID bool

	// Operation retains the operation dimension.
	Operation bool
}

// OutputOptions configures the periodic statistics summary.
type OutputOptions struct {
	// Enabled turns the periodic summary on.
	Enabled bool

	// Sink names the summary destination. Only "Logging" is supported,
	// compared case-insensitively.
	Sink string

	// Interval is the delay between summaries.
	Interval time.Duration

	// QueryWindow is the range each summary aggregates, ending at the emission
	// instant.
	QueryWindow time.Duration

	// IncludeEmptySnapshots emits a summary even when every counter is zero.
	IncludeEmptySnapshots bool
}

// DefaultOptions returns the documented defaults: 5 minute buckets, 1 hour
// retention, 16384 series, volume/watcher/operation dimensions retained, and a
// logging summary every minute over the last 15 minutes.
func DefaultOptions() Options {
	return Options{
		Enabled:    true,
		WindowSize: 5 * time.Minute,
		Retention:  time.Hour,
		MaxSeries:  16384,
		Dimensions: DimensionOptions{
			TenantID:  false,
			VolumeID:  true,
			WatcherID: true,
			Operation: true,
		},
		Output: OutputOptions{
			Enabled:               true,
			Sink:                  loggingSinkName,
			Interval:              time.Minute,
			QueryWindow:           15 * time.Minute,
			IncludeEmptySnapshots: false,
		},
	}
}

// Validate reports whether opts describes a usable configuration.
//
// Interval bounds are validated only when the output service is enabled, so a
// recorder used purely as a reader does not have to carry output settings.
// Every failure wraps core.ErrInvalidArgument.
func (opts Options) Validate() error {
	if opts.WindowSize <= 0 {
		return fmt.Errorf("statistics window size must be positive: %w", core.ErrInvalidArgument)
	}
	if opts.Retention <= 0 {
		return fmt.Errorf("statistics retention must be positive: %w", core.ErrInvalidArgument)
	}
	if opts.Retention < opts.WindowSize {
		return fmt.Errorf("statistics retention must be at least the window size: %w", core.ErrInvalidArgument)
	}
	if opts.MaxSeries < MinimumSeries || opts.MaxSeries > MaximumSeries {
		return fmt.Errorf(
			"statistics max series must be between %d and %d: %w",
			MinimumSeries, MaximumSeries, core.ErrInvalidArgument,
		)
	}

	if !opts.Output.Enabled {
		return nil
	}

	if !strings.EqualFold(strings.TrimSpace(opts.Output.Sink), loggingSinkName) {
		return fmt.Errorf("unsupported statistics output sink %q: %w", opts.Output.Sink, core.ErrInvalidArgument)
	}
	if opts.Output.Interval <= 0 {
		return fmt.Errorf("statistics output interval must be positive: %w", core.ErrInvalidArgument)
	}
	if opts.Output.QueryWindow <= 0 {
		return fmt.Errorf("statistics output query window must be positive: %w", core.ErrInvalidArgument)
	}

	return nil
}

// seriesKey identifies one aggregated series.
//
// Dimensions are stored as discrete fields rather than as a map so the key is
// comparable and can be used as a map key without serialization.
type seriesKey struct {
	bucket    int64
	name      string
	tenantID  string
	volumeID  string
	watcherID string
	operation string
}

// counter is one atomic series cell. Deltas from many goroutines accumulate in
// the cell, so a concurrent Record never has to hold the series lock.
type counter struct {
	value atomic.Int64
}

// aggregateKey identifies one aggregated measurement: a name plus the retained
// dimensions it was recorded under. All series of one aggregate collapse into a
// single measurement.
type aggregateKey struct {
	name      string
	tenantID  string
	volumeID  string
	watcherID string
	operation string
}

// Recorder is a bounded in-process statistics recorder and reader.
//
// Concurrency: Record may be called from any number of goroutines. A
// sync.Map-backed series table keeps the common case (an already existing
// series) lock-free apart from the atomic cell, and a mutex serializes only
// series creation and pruning.
type Recorder struct {
	opts Options

	// series maps every live series key to its counter cell.
	series sync.Map // map[seriesKey]*counter

	// mu guards creation against pruning, so a series cannot be created after
	// the bound was checked and then immediately pruned away.
	mu sync.Mutex

	// count tracks the number of live series for the bound check.
	count int

	// recorded counts Record calls so pruning runs periodically without a
	// separate timer.
	recorded atomic.Int64

	// now is the clock seam used by record-time pruning. Nil means time.Now.
	now func() time.Time
}

// NewRecorder creates a statistics recorder.
//
// It returns an error wrapping core.ErrInvalidArgument when opts fails
// Options.Validate.
func NewRecorder(opts Options) (*Recorder, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return newRecorder(opts, opts.MaxSeries)
}

// newRecorder builds a recorder with an explicit series bound.
//
// maxSeries is the internal test seam for the series bound: the exported
// validation enforces a production floor that a unit test cannot reach, so the
// bound tests construct the recorder directly instead of weakening validation.
func newRecorder(opts Options, maxSeries int) (*Recorder, error) {
	if opts.WindowSize <= 0 || opts.Retention <= 0 || opts.Retention < opts.WindowSize {
		return nil, fmt.Errorf("invalid statistics window or retention: %w", core.ErrInvalidArgument)
	}
	if maxSeries < 1 {
		return nil, fmt.Errorf("statistics series bound must be positive: %w", core.ErrInvalidArgument)
	}

	opts.MaxSeries = maxSeries

	return &Recorder{opts: opts, now: time.Now}, nil
}

// Record adds value to the named measurement for the bucket containing
// timestamp.
//
// An empty name and a zero value are ignored. Only the dimensions enabled in
// Options.Dimensions are retained, and only when their recorded value is
// non-empty. A series beyond the configured bound is dropped silently; the
// recorder never grows past MaxSeries.
func (r *Recorder) Record(name string, value int64, timestamp time.Time, dimensions map[string]string) {
	if r == nil || name == "" || value == 0 {
		return
	}

	key := r.keyFor(name, timestamp, dimensions)

	cell := r.cell(key)
	if cell == nil {
		return
	}

	cell.value.Add(value)

	if r.recorded.Add(1)%pruneInterval == 0 {
		r.prune(r.timeNow())
	}
}

// cell returns the counter cell for key, creating it while the series bound
// allows. It returns nil when the bound is already reached.
func (r *Recorder) cell(key seriesKey) *counter {
	if existing, ok := r.series.Load(key); ok {
		if cell, ok := existing.(*counter); ok {
			return cell
		}

		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.series.Load(key); ok {
		if cell, ok := existing.(*counter); ok {
			return cell
		}

		return nil
	}

	if r.count >= r.opts.MaxSeries {
		return nil
	}

	cell := &counter{}
	r.series.Store(key, cell)
	r.count++

	return cell
}

// keyFor builds the series key of one record, retaining only enabled
// non-empty dimensions.
func (r *Recorder) keyFor(name string, timestamp time.Time, dimensions map[string]string) seriesKey {
	key := seriesKey{
		bucket: bucketStart(timestamp, r.opts.WindowSize),
		name:   name,
	}

	if len(dimensions) == 0 {
		return key
	}

	if r.opts.Dimensions.TenantID {
		key.tenantID = dimensions[core.StatisticsDimensionTenantID]
	}
	if r.opts.Dimensions.VolumeID {
		key.volumeID = dimensions[core.StatisticsDimensionVolumeID]
	}
	if r.opts.Dimensions.WatcherID {
		key.watcherID = dimensions[core.StatisticsDimensionWatcherID]
	}
	if r.opts.Dimensions.Operation {
		key.operation = dimensions[core.StatisticsDimensionOperation]
	}

	return key
}

// Snapshot aggregates every bucket that overlaps [query.From, query.To) and
// returns the totals of the queried range.
//
// A query whose To is not after From returns an empty snapshot. Non-empty
// filter fields must match the retained dimension exactly; an empty filter
// matches every value. Snapshot also prunes expired buckets.
func (r *Recorder) Snapshot(query core.StatisticsQuery) core.StatisticsSnapshot {
	if r == nil {
		return emptySnapshot()
	}

	if !query.To.After(query.From) {
		return emptySnapshot()
	}

	r.prune(r.timeNow())

	totals := make(map[aggregateKey]float64)
	writeBytes := int64(0)

	r.series.Range(func(rawKey, rawValue any) bool {
		key, ok := rawKey.(seriesKey)
		if !ok {
			return true
		}

		cell, ok := rawValue.(*counter)
		if !ok {
			return true
		}

		bucketStartTime := time.Unix(0, key.bucket)
		bucketEndTime := bucketStartTime.Add(r.opts.WindowSize)

		// Overlap of [bucketStart, bucketEnd) with [From, To).
		if !bucketEndTime.After(query.From) || !bucketStartTime.Before(query.To) {
			return true
		}

		if query.TenantID != "" && query.TenantID != key.tenantID {
			return true
		}
		if query.VolumeID != "" && query.VolumeID != key.volumeID {
			return true
		}
		if query.WatcherID != "" && query.WatcherID != key.watcherID {
			return true
		}
		if query.Operation != "" && query.Operation != key.operation {
			return true
		}

		value := cell.value.Load()
		aggregate := aggregateKey{
			name:      key.name,
			tenantID:  key.tenantID,
			volumeID:  key.volumeID,
			watcherID: key.watcherID,
			operation: key.operation,
		}
		totals[aggregate] += float64(value)

		if key.name == core.StatisticStorageWriteBytes {
			writeBytes += value
		}

		return true
	})

	measurements := make([]core.StatisticsMeasurement, 0, len(totals)+1)

	// Deterministic order: the series table iterates in an unspecified order,
	// so measurements are sorted by name and then by dimension value.
	aggregates := make([]aggregateKey, 0, len(totals))
	for aggregate := range totals {
		aggregates = append(aggregates, aggregate)
	}

	sort.Slice(aggregates, func(i, j int) bool {
		return aggregateOrderKey(aggregates[i]) < aggregateOrderKey(aggregates[j])
	})

	for _, aggregate := range aggregates {
		measurements = append(measurements, core.StatisticsMeasurement{
			Name:       aggregate.name,
			Value:      totals[aggregate],
			Dimensions: aggregate.dimensions(),
		})
	}

	snapshot := core.StatisticsSnapshot{}

	for _, measurement := range measurements {
		applyTotal(&snapshot, measurement)
	}

	// The throughput is derived from the aggregated write bytes over the queried
	// range, never recorded directly. Locus uses the same divisor
	// (InMemoryLocusStatisticsRecorder.GetSnapshot: bytes / 1024 / 1024 /
	// (query.To - query.From).TotalSeconds), so a longer query window reports a
	// correspondingly lower rate instead of being normalized per bucket.
	throughputSeconds := query.To.Sub(query.From).Seconds()

	snapshot.WriteMegabytesPerSecond = megabytesPerSecond(writeBytes, throughputSeconds)

	for _, aggregate := range aggregates {
		if aggregate.name != core.StatisticStorageWriteBytes {
			continue
		}

		measurements = append(measurements, core.StatisticsMeasurement{
			Name:       core.StatisticStorageWriteMegabytesPerSecond,
			Value:      megabytesPerSecond(int64(totals[aggregate]), throughputSeconds),
			Dimensions: aggregate.dimensions(),
		})
	}

	snapshot.Measurements = measurements

	return snapshot
}

// dimensions returns the aggregate's dimensions as a fresh map.
func (a aggregateKey) dimensions() map[string]string {
	dimensions := make(map[string]string, 4)

	if a.tenantID != "" {
		dimensions[core.StatisticsDimensionTenantID] = a.tenantID
	}
	if a.volumeID != "" {
		dimensions[core.StatisticsDimensionVolumeID] = a.volumeID
	}
	if a.watcherID != "" {
		dimensions[core.StatisticsDimensionWatcherID] = a.watcherID
	}
	if a.operation != "" {
		dimensions[core.StatisticsDimensionOperation] = a.operation
	}

	return dimensions
}

// aggregateOrderKey is the stable sort key of an aggregate.
func aggregateOrderKey(a aggregateKey) string {
	return strings.Join(
		[]string{a.name, a.tenantID, a.volumeID, a.watcherID, a.operation},
		"\x00",
	)
}

// prune removes every bucket older than now - Retention - WindowSize.
func (r *Recorder) prune(now time.Time) {
	cutoff := now.Add(-r.opts.Retention - r.opts.WindowSize).UnixNano()

	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0

	r.series.Range(func(rawKey, _ any) bool {
		key, ok := rawKey.(seriesKey)
		if !ok {
			return true
		}

		if key.bucket < cutoff {
			r.series.Delete(rawKey)
			removed++
		}

		return true
	})

	r.count -= removed
	if r.count < 0 {
		r.count = 0
	}
}

// timeNow returns the recorder clock.
func (r *Recorder) timeNow() time.Time {
	if r.now != nil {
		return r.now()
	}

	return time.Now()
}

// seriesCount returns the number of live series. It exists for the internal
// series-bound tests.
func (r *Recorder) seriesCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.count
}

// seriesKeys returns a copy of the live series keys. It exists for the internal
// series-bound tests.
func (r *Recorder) seriesKeys() []seriesKey {
	keys := make([]seriesKey, 0, r.seriesCount())

	r.series.Range(func(rawKey, _ any) bool {
		if key, ok := rawKey.(seriesKey); ok {
			keys = append(keys, key)
		}

		return true
	})

	sort.Slice(keys, func(i, j int) bool {
		return aggregateOrderKey(aggregateKey{
			name:      keys[i].name,
			tenantID:  keys[i].tenantID,
			volumeID:  keys[i].volumeID,
			watcherID: keys[i].watcherID,
			operation: keys[i].operation,
		}) < aggregateOrderKey(aggregateKey{
			name:      keys[j].name,
			tenantID:  keys[j].tenantID,
			volumeID:  keys[j].volumeID,
			watcherID: keys[j].watcherID,
			operation: keys[j].operation,
		})
	})

	return keys
}

// valueFor returns the current value of a series. It exists for the internal
// series-bound tests.
func (r *Recorder) valueFor(key seriesKey) int64 {
	value, ok := r.series.Load(key)
	if !ok {
		return 0
	}

	cell, ok := value.(*counter)
	if !ok {
		return 0
	}

	return cell.value.Load()
}

// bucketStart aligns timestamp down to the start of its WindowSize bucket,
// measured from the Unix epoch.
func bucketStart(timestamp time.Time, window time.Duration) int64 {
	if window <= 0 {
		return timestamp.UnixNano()
	}

	return timestamp.UnixNano() / int64(window) * int64(window)
}

// megabytesPerSecond converts bytes over the given number of seconds into MiB/s.
// A non-positive duration reports zero rather than an infinite rate.
func megabytesPerSecond(bytes int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}

	return float64(bytes) / 1024 / 1024 / seconds
}

// applyTotal fills the known snapshot field of a measurement name.
func applyTotal(snapshot *core.StatisticsSnapshot, measurement core.StatisticsMeasurement) {
	value := int64(measurement.Value)

	switch measurement.Name {
	case core.StatisticStorageWriteSuccessCount:
		snapshot.WriteFileCount += value
	case core.StatisticStorageWriteBytes:
		snapshot.WriteBytes += value
	case core.StatisticStorageFileDequeuedCount:
		snapshot.DequeuedFileCount += value
	case core.StatisticStorageFileReadCount:
		snapshot.ReadFileCount += value
	case core.StatisticStorageFileCompletedCount:
		snapshot.CompletedFileCount += value
	case core.StatisticMetadataPersistedOperationCount:
		snapshot.MetadataPersistedOperationCount += value
	case core.StatisticWatcherFilesImported:
		snapshot.WatcherImportedFileCount += value
	case core.StatisticWatcherBytesImported:
		snapshot.WatcherImportedBytes += value
	default:
	}
}

// emptySnapshot returns a snapshot with a non-nil empty measurement slice.
func emptySnapshot() core.StatisticsSnapshot {
	return core.StatisticsSnapshot{Measurements: []core.StatisticsMeasurement{}}
}

// optionsWithMaxSeries returns a copy of opts with the series bound replaced.
// It exists for the internal series-bound tests.
func optionsWithMaxSeries(opts Options, maxSeries int) Options {
	opts.MaxSeries = maxSeries

	return opts
}

// NoopRecorder is a disabled statistics recorder.
//
// It drops every record without allocating and always reports an empty
// snapshot, so a component can accept a recorder unconditionally.
type NoopRecorder struct{}

// Noop is the shared disabled recorder. It implements both
// core.StatisticsRecorder and core.StatisticsReader.
var Noop NoopRecorder

// Record drops the delta. It allocates nothing.
func (NoopRecorder) Record(string, int64, time.Time, map[string]string) {}

// Snapshot returns an empty snapshot with a non-nil measurement slice.
func (NoopRecorder) Snapshot(core.StatisticsQuery) core.StatisticsSnapshot {
	return emptySnapshot()
}

// OutputService periodically logs a statistics summary through the instance
// logging runtime.
//
// Lifecycle: Start launches at most one goroutine, which sleeps Interval and
// then emits one summary of the last Output.QueryWindow. A second Start while
// running is a no-op. Stop cancels the goroutine and waits for it; Stop is
// idempotent and safe before Start. Start after Stop restarts the service, so
// a restart never panics. A nil logging runtime emits nothing but is otherwise
// a fully functional service.
type OutputService struct {
	reader core.StatisticsReader
	opts   Options
	rt     *logging.Runtime

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewOutputService creates a statistics summary service.
//
// reader supplies the snapshots; a nil reader is treated as an empty reader.
// rt may be nil, which makes the service silent: the goroutine and its timer
// still start and stop cleanly. Invalid options disable the service instead of
// failing, because Start cannot report an error.
func NewOutputService(reader core.StatisticsReader, opts Options, rt *logging.Runtime) *OutputService {
	if reader == nil {
		reader = Noop
	}

	return &OutputService{reader: reader, opts: opts, rt: rt}
}

// Start launches the summary goroutine.
//
// It is a no-op unless both Options.Enabled and Options.Output.Enabled are set
// and the options validate. Start is safe to call repeatedly: only the first
// call while stopped launches a goroutine.
func (s *OutputService) Start() {
	if s == nil {
		return
	}
	if !s.opts.Enabled || !s.opts.Output.Enabled {
		return
	}
	if err := s.opts.Validate(); err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.cancel = cancel
	s.done = done
	s.running = true

	go s.run(ctx, done)
}

// Stop cancels the summary goroutine and waits for it to finish.
//
// Stop is idempotent and safe to call before Start; it never panics and never
// blocks longer than the goroutine's current sleep.
func (s *OutputService) Stop() {
	if s == nil {
		return
	}

	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.cancel = nil
	s.done = nil
	s.running = false
	s.mu.Unlock()

	if cancel == nil {
		return
	}

	cancel()

	if done != nil {
		<-done
	}
}

// run emits one summary per interval until ctx is cancelled. done is closed
// exactly once as the goroutine exits, so Stop always joins it.
func (s *OutputService) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	timer := time.NewTimer(s.opts.Output.Interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.emitSummary(ctx)

			timer.Reset(s.opts.Output.Interval)
		}
	}
}

// emitSummary aggregates the configured query window and logs it.
func (s *OutputService) emitSummary(ctx context.Context) {
	now := time.Now().UTC()

	snapshot := s.reader.Snapshot(core.StatisticsQuery{
		From: now.Add(-s.opts.Output.QueryWindow),
		To:   now,
	})

	if !s.opts.Output.IncludeEmptySnapshots && isEmpty(snapshot) {
		return
	}

	if s.rt == nil {
		return
	}

	s.rt.Emit(ctx, logging.Record{
		Level:     slog.LevelInfo,
		Component: statisticsComponent,
		Event:     summaryEvent,
		Message:   summaryMessage,
		Attrs: []slog.Attr{
			slog.Int64("write_file_count", snapshot.WriteFileCount),
			slog.Int64("write_bytes", snapshot.WriteBytes),
			slog.Float64("write_megabytes_per_second", snapshot.WriteMegabytesPerSecond),
			slog.Int64("dequeued_file_count", snapshot.DequeuedFileCount),
			slog.Int64("read_file_count", snapshot.ReadFileCount),
			slog.Int64("completed_file_count", snapshot.CompletedFileCount),
			slog.Int64("metadata_persisted_operation_count", snapshot.MetadataPersistedOperationCount),
			slog.Int64("watcher_imported_file_count", snapshot.WatcherImportedFileCount),
			slog.Int64("watcher_imported_bytes", snapshot.WatcherImportedBytes),
			slog.Duration("query_window", s.opts.Output.QueryWindow),
		},
	})
}

// isEmpty reports whether every known snapshot counter is zero.
func isEmpty(snapshot core.StatisticsSnapshot) bool {
	return snapshot.WriteFileCount == 0 &&
		snapshot.WriteBytes == 0 &&
		snapshot.DequeuedFileCount == 0 &&
		snapshot.ReadFileCount == 0 &&
		snapshot.CompletedFileCount == 0 &&
		snapshot.MetadataPersistedOperationCount == 0 &&
		snapshot.WatcherImportedFileCount == 0 &&
		snapshot.WatcherImportedBytes == 0
}
