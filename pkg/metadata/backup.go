package metadata

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"google.golang.org/protobuf/proto"
)

const (
	// backupFilePrefix and backupFileSuffix frame the frozen backup file name
	// "metadata.<yyyyMMddTHHmmssZ>.bak". A backup set scanned by an operator is
	// a glob on the same shape.
	backupFilePrefix = "metadata."
	backupFileSuffix = ".bak"

	// backupTempFileSuffix marks the staging file of an in-flight backup. A
	// reader that globs *.bak never observes a partial backup.
	backupTempFileSuffix = ".tmp"

	// backupTimestampLayout renders the UTC stamp inside a backup file name. It
	// is deliberately colon-free because a colon cannot appear in a Windows
	// path segment.
	backupTimestampLayout = "20060102T150405Z"

	// backupPathCollisionLimit bounds the search for a free backup name when two
	// backups land inside the same second.
	backupPathCollisionLimit = 1000

	// backupRestoreSuffix marks the staging sibling an automatic restore loads a
	// backup into before it replaces the database directory.
	backupRestoreSuffix = ".restore."

	// backupLogComponent identifies the backup runner in structured logs.
	backupLogComponent = "metadata_backup"

	// backupRestorePendingWrites bounds how many load batches one restore keeps
	// in flight.
	//
	// It must be positive. BadgerDB sizes its load throttle with
	// y.NewThrottle(maxPendingWrites), and a zero limit builds an unbuffered
	// channel that throttles the very first batch forever, so db.Load(r, 0)
	// deadlocks until the process is killed. The upstream backup tests pass a
	// positive budget for the same reason.
	backupRestorePendingWrites = 16

	// backupMaxEntryBytes bounds one length-prefixed backup entry. It is far
	// above the engine's own flush threshold, so a legitimate metadata backup
	// never reaches it, while a damaged stream is rejected instead of driving an
	// allocation from an unchecked length.
	backupMaxEntryBytes = 256 << 20
)

// BackupRepository writes a consistent online backup of repo to w and returns
// the engine sequence number the snapshot was taken at.
//
// repo is used through the optional core.MetadataBackupService capability, so
// any repository implementation may support backups without widening
// core.MetadataRepository. A repository that does not implement it is reported
// with an error wrapping core.ErrInvalidArgument, because the caller asked for a
// capability that this repository does not have.
//
// Venue's recoverability granularity is the backup interval rather than Locus's
// per-event queue journal: everything committed before the returned sequence
// number is in the stream, and everything after it belongs to the next backup.
func BackupRepository(ctx context.Context, repo core.MetadataRepository, w io.Writer) (uint64, error) {
	if repo == nil {
		return 0, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}
	if w == nil {
		return 0, fmt.Errorf("backup writer cannot be nil: %w", core.ErrInvalidArgument)
	}

	service, ok := repo.(core.MetadataBackupService)
	if !ok {
		return 0, fmt.Errorf("metadata repository %T does not support backups: %w", repo, core.ErrInvalidArgument)
	}
	return service.Backup(ctx, w)
}

// backupFile is one entry of a backup directory.
type backupFile struct {
	path    string
	size    int64
	modTime time.Time
}

// isBackupFileName reports whether name has the frozen backup file shape
// metadata.<stamp>.bak, including the collision suffix two backups inside one
// second can add.
func isBackupFileName(name string) bool {
	if !strings.HasPrefix(name, backupFilePrefix) || !strings.HasSuffix(name, backupFileSuffix) {
		return false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(name, backupFilePrefix), backupFileSuffix)
	if middle == "" {
		return false
	}
	if _, err := parseBackupTimestamp(name); err == nil {
		return true
	}
	// Accept the collision form metadata.<stamp>.<n>.bak as well.
	if index := strings.LastIndex(middle, "."); index > 0 {
		_, err := parseBackupTimestamp(backupFilePrefix + middle[:index] + backupFileSuffix)
		return err == nil
	}
	return false
}

// parseBackupTimestamp extracts the UTC instant encoded in a backup file name.
// A name that does not carry the frozen stamp yields an empty time and an error,
// so a caller can fall back to the file's modification time.
func parseBackupTimestamp(name string) (time.Time, error) {
	middle := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(name), backupFilePrefix), backupFileSuffix)
	stamp, err := time.Parse(backupTimestampLayout, middle)
	if err != nil {
		return time.Time{}, err
	}
	return stamp.UTC(), nil
}

// listBackupsNewestFirst returns every backup file directly inside directory,
// newest first.
//
// Ordering uses the modification time, with the timestamp encoded in the file
// name breaking ties and settling equal modification times, so the order is
// deterministic even when a file system reports coarse timestamps. A missing or
// unreadable directory yields no backups rather than an error: a backup
// directory is an operational artifact and its absence is not a failure of the
// caller's own state.
func listBackupsNewestFirst(directory string) ([]backupFile, error) {
	if directory == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read backup directory %s: %w", directory, err)
	}

	backups := make([]backupFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isBackupFileName(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		backups = append(backups, backupFile{
			path:    filepath.Join(directory, entry.Name()),
			size:    info.Size(),
			modTime: info.ModTime(),
		})
	}

	sort.SliceStable(backups, func(i, j int) bool {
		if !backups[i].modTime.Equal(backups[j].modTime) {
			return backups[i].modTime.After(backups[j].modTime)
		}
		leftStamp, leftErr := parseBackupTimestamp(filepath.Base(backups[i].path))
		rightStamp, rightErr := parseBackupTimestamp(filepath.Base(backups[j].path))
		if leftErr == nil && rightErr == nil && !leftStamp.Equal(rightStamp) {
			return leftStamp.After(rightStamp)
		}
		return backups[i].path > backups[j].path
	})

	return backups, nil
}

// LatestBackup describes the newest backup in directory.
//
// A missing directory, and a directory without any backup, are reported as an
// empty result (BackupCount == 0 with an empty path and a zero time) and are
// never an error, because "no backup yet" is a valid operational state. The
// returned value always names the directory that was inspected.
func LatestBackup(directory string) (*core.MetadataBackupInfo, error) {
	info := &core.MetadataBackupInfo{BackupDirectory: directory}
	if directory == "" {
		return info, nil
	}

	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		return nil, err
	}
	if len(backups) == 0 {
		return info, nil
	}

	info.BackupCount = len(backups)
	info.LatestBackupPath = backups[0].path
	info.LatestBackupAt = backups[0].modTime
	for _, backup := range backups {
		info.TotalBytes += backup.size
	}
	if stamp, err := parseBackupTimestamp(filepath.Base(backups[0].path)); err == nil {
		info.LatestBackupAt = stamp
	}
	return info, nil
}

// PruneBackups removes the backups in directory whose modification time is
// older than now-retention and returns how many were removed.
//
// A non-positive retention removes nothing: zero means "keep everything", which
// is why the retention is not defaulted here. A missing directory is not an
// error, and each removal is best-effort but reported: a file that cannot be
// removed stops the sweep and is returned as an error, so an operator learns
// about a backup directory that is filling up instead of losing the signal.
func PruneBackups(directory string, retention time.Duration, now time.Time) (int, error) {
	if retention <= 0 || directory == "" {
		return 0, nil
	}

	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		return 0, err
	}

	cutoff := now.Add(-retention)
	removed := 0
	for _, backup := range backups {
		if !backup.modTime.Before(cutoff) {
			continue
		}
		if err := os.Remove(backup.path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, fmt.Errorf("failed to remove expired backup %s: %w", backup.path, err)
		}
		removed++
	}
	return removed, nil
}

// RestoreDatabase loads the backup stream r into a fresh metadata database
// described by opts.
//
// This is an offline restore with a narrow, deliberate contract:
//
//   - It never touches a database that is already there. A target directory that
//     exists and is not empty is rejected with an error wrapping
//     core.ErrInvalidArgument, so a mistyped path cannot overwrite live data.
//   - It opens the database with exactly the options the repository would use,
//     replays the backup stream into it, and closes the handle again. The replay
//     validates the stream instead of trusting the engine's entry point, which
//     can panic on a damaged backup (see loadBackupStream).
//   - On any failure it removes the directory it created, so a failed restore
//     never leaves a half-loaded database behind for the next startup to
//     quarantine.
//
// The caller owns the restored directory afterwards and must open it before
// serving traffic. Restoring is a repair step, not a substitute for an operator:
// the restored state is only as new as the backup stream.
func RestoreDatabase(ctx context.Context, opts *BadgerRepositoryOptions, r io.Reader) error {
	if opts == nil {
		return fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}
	if r == nil {
		return fmt.Errorf("backup reader cannot be nil: %w", core.ErrInvalidArgument)
	}

	dbPath, _, err := newBadgerOptions(opts)
	if err != nil {
		return err
	}
	return restoreDatabaseFromStream(ctx, dbPath, badger.DefaultOptions(dbPath).WithLogger(nil), r)
}

// loadBackupStream replays a BadgerDB backup stream into an empty database.
//
// It deliberately does not call db.Load. BadgerDB v4.9.6 derives a value length
// straight from the stream and sizes a slice with it before reading, so a
// damaged or hostile backup file panics the process with "makeslice: len out of
// range" instead of returning an error, and the same entry point sized its load
// throttle with a zero pending-write budget, which deadlocks on the first batch.
// This loader reads the same length-prefixed protobuf frames, validates each
// value length before allocating, and reports a malformed stream as an error so
// a failed restore can remove its directory and leave the runtime serving.
//
// The caller owns db and must Close it.
func loadBackupStream(db *badger.DB, r io.Reader) error {
	reader := bufio.NewReaderSize(r, 16<<10)
	loader := db.NewKVLoader(backupRestorePendingWrites)
	unmarshalBuf := make([]byte, 0, 1<<10)

	for {
		var valueLength uint64
		err := binary.Read(reader, binary.LittleEndian, &valueLength)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read the backup stream header: %w", err)
		}

		// The engine sizes a slice straight from this length before reading it,
		// so an implausible value is rejected here rather than becoming a panic
		// or an unbounded allocation.
		if valueLength == 0 || valueLength > backupMaxEntryBytes {
			return fmt.Errorf("malformed backup stream: entry length %d is outside the supported range: %w", valueLength, core.ErrInvalidArgument)
		}
		if uint64(cap(unmarshalBuf)) < valueLength {
			unmarshalBuf = make([]byte, valueLength)
		}
		unmarshalBuf = unmarshalBuf[:valueLength]

		if _, err := io.ReadFull(reader, unmarshalBuf); err != nil {
			return fmt.Errorf("failed to read a backup entry: %w", err)
		}

		list := &pb.KVList{}
		if err := proto.Unmarshal(unmarshalBuf, list); err != nil {
			return fmt.Errorf("failed to decode a backup entry: %w", err)
		}
		for _, kv := range list.Kv {
			if err := loader.Set(kv); err != nil {
				return fmt.Errorf("failed to load a backup entry: %w", err)
			}
		}
	}

	if err := loader.Finish(); err != nil {
		return fmt.Errorf("failed to finish loading the backup: %w", err)
	}
	return nil
}

// restoreDatabaseFromStream loads the backup stream r into the fresh database
// directory dbPath, using the engine options dbOpts. The directory is created
// here and removed again when anything fails, which is the guarantee both the
// public restore and the automatic recovery path rely on.
func restoreDatabaseFromStream(ctx context.Context, dbPath string, dbOpts badger.Options, r io.Reader) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	created, err := prepareRestoreDirectory(dbPath)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("database directory %s already exists and is not empty: %w", dbPath, core.ErrInvalidArgument)
	}

	// From here on the directory belongs to this restore, so every failure must
	// take it away again.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dbPath)
		}
	}()

	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("failed to open the restore target %s: %w", dbPath, err)
	}

	loadErr := loadBackupStream(db, r)
	closeErr := db.Close()
	if loadErr != nil {
		return fmt.Errorf("failed to load the metadata backup: %w", loadErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close the restored database: %w", closeErr)
	}
	return nil
}

// prepareRestoreDirectory makes dbPath usable as a restore target. It reports
// created=false when the path already holds something, and created=true when
// the caller now owns a fresh directory.
func prepareRestoreDirectory(dbPath string) (created bool, err error) {
	entries, readErr := os.ReadDir(dbPath)
	switch {
	case readErr == nil:
		if len(entries) > 0 {
			return false, nil
		}
		// An existing empty directory is a usable target.
		return true, nil
	case errors.Is(readErr, os.ErrNotExist):
		if err := os.MkdirAll(dbPath, 0o755); err != nil {
			return false, fmt.Errorf("failed to create the restore target %s: %w", dbPath, err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("failed to inspect the restore target %s: %w", dbPath, readErr)
	}
}

// writeBackupFile streams the backup produced by write into directory under the
// frozen name metadata.<yyyyMMddTHHmmssZ>.bak.
//
// The stream is written to a temporary sibling, flushed to the device, and then
// renamed onto the final name. A reader that globs *.bak therefore either sees a
// complete backup or does not see the file at all, and a crash mid-write leaves
// only a removable *.tmp file behind.
func writeBackupFile(directory string, write func(w io.Writer) error, now time.Time) (path string, err error) {
	if directory == "" {
		return "", fmt.Errorf("backup directory cannot be empty: %w", core.ErrInvalidArgument)
	}
	if write == nil {
		return "", fmt.Errorf("backup writer function cannot be nil: %w", core.ErrInvalidArgument)
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", fmt.Errorf("failed to create backup directory %s: %w", directory, err)
	}

	finalName, err := freeBackupFileName(directory, now.UTC())
	if err != nil {
		return "", err
	}
	finalPath := filepath.Join(directory, finalName)
	tempPath := finalPath + backupTempFileSuffix

	// A leftover staging file from an interrupted run must not block this one.
	_ = os.Remove(tempPath)

	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("failed to create the backup staging file: %w", err)
	}

	err = write(file)
	if err == nil {
		err = file.Sync()
	} else {
		err = fmt.Errorf("failed to write the backup: %w", err)
	}
	if closeErr := file.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("failed to close the backup staging file: %w", closeErr)
	}
	if err == nil {
		if renameErr := os.Rename(tempPath, finalPath); renameErr != nil {
			err = fmt.Errorf("failed to publish the backup: %w", renameErr)
		}
	}

	if err != nil {
		// Best-effort cleanup: the staging file is not a backup and must not
		// survive a failed run as a candidate for a restore.
		_ = os.Remove(tempPath)
		return "", err
	}
	return finalPath, nil
}

// freeBackupFileName picks the frozen backup name for now, adding a numeric
// suffix when a backup already exists for that second so concurrent cycles
// cannot overwrite each other.
func freeBackupFileName(directory string, now time.Time) (string, error) {
	base := backupFilePrefix + now.Format(backupTimestampLayout)
	candidate := base + backupFileSuffix
	for suffix := 1; ; suffix++ {
		full := filepath.Join(directory, candidate)
		_, err := os.Lstat(full)
		if errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("failed to inspect the backup name: %w", err)
		}
		if suffix > backupPathCollisionLimit {
			return "", fmt.Errorf("cannot find a free backup file name in %s", directory)
		}
		candidate = fmt.Sprintf("%s.%d%s", base, suffix, backupFileSuffix)
	}
}

// BackupServiceOptions configures a periodic metadata backup runner.
type BackupServiceOptions struct {
	// Repository is the metadata repository to back up. It must implement
	// core.MetadataBackupService whenever the service is enabled.
	Repository core.MetadataRepository

	// Directory is where backup files are written. An empty value disables the
	// service. The directory is created on the first backup.
	Directory string

	// Interval is the delay between two backup cycles. A non-positive value
	// disables the service.
	Interval time.Duration

	// Retention is how long a backup file is kept. Backups older than this are
	// removed after a successful cycle. A non-positive value disables pruning.
	Retention time.Duration

	// Logging receives structured backup diagnostics. A nil runtime means
	// silent.
	Logging *logging.Runtime
}

// BackupService writes periodic consistent backups of one metadata repository
// and prunes backups that outlived the retention window.
//
// Lifecycle: NewBackupService validates the configuration and Start owns the
// single background goroutine that performs the cycles. Stop cancels that
// goroutine and waits for it to return; both Start and Stop are idempotent, and
// the service may be restarted after Stop. RunOnce performs exactly one cycle on
// the calling goroutine and is safe to call while the background loop runs or
// while the service is disabled.
//
// The backed-up repository is used only through core.MetadataBackupService, so
// the runner never stops or serializes the write path: backups happen online and
// the recoverable point is the newest backup file, not the newest write.
type BackupService struct {
	repo      core.MetadataRepository
	service   core.MetadataBackupService
	directory string
	interval  time.Duration
	retention time.Duration
	log       *logging.Runtime
	enabled   bool
	mu        sync.Mutex
	stopCh    chan struct{}
	wg        sync.WaitGroup
	stopOnce  sync.Once
	running   bool
}

// NewBackupService builds a periodic backup runner.
//
// It returns an error wrapping core.ErrInvalidArgument when no repository is
// configured, and when the service would be enabled (a directory and a positive
// interval) but the repository does not implement core.MetadataBackupService.
// A disabled service accepts any repository because it never touches it.
func NewBackupService(opts *BackupServiceOptions) (*BackupService, error) {
	if opts == nil {
		return nil, fmt.Errorf("backup service options cannot be nil: %w", core.ErrInvalidArgument)
	}
	if opts.Repository == nil {
		return nil, fmt.Errorf("backup service repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	service := &BackupService{
		repo:      opts.Repository,
		directory: opts.Directory,
		interval:  opts.Interval,
		retention: opts.Retention,
		log:       opts.Logging,
		stopCh:    make(chan struct{}),
		enabled:   opts.Directory != "" && opts.Interval > 0,
	}

	if backup, ok := opts.Repository.(core.MetadataBackupService); ok {
		service.service = backup
	} else if service.enabled {
		return nil, fmt.Errorf("metadata repository %T does not support backups: %w", opts.Repository, core.ErrInvalidArgument)
	}

	return service, nil
}

// IsEnabled reports whether the service is configured to back up at all.
func (s *BackupService) IsEnabled() bool {
	return s != nil && s.enabled
}

// Start begins the periodic backup loop. It is idempotent and a no-op when the
// service is disabled; after Stop it may be called again to restart the loop.
func (s *BackupService) Start() {
	if s == nil || !s.enabled || s.service == nil {
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	stopCh := make(chan struct{})
	s.stopCh = stopCh
	s.stopOnce = sync.Once{}
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(stopCh)
}

// Stop cancels the periodic backup loop and waits for it to return. It is
// idempotent and safe to call on a service that was never started.
func (s *BackupService) Stop() {
	if s == nil {
		return
	}

	s.mu.Lock()
	stopCh := s.stopCh
	running := s.running
	s.running = false
	s.mu.Unlock()

	if !running || stopCh == nil {
		return
	}
	s.stopOnce.Do(func() { close(stopCh) })
	s.wg.Wait()
}

// RunOnce performs one backup cycle: it writes a backup and then prunes expired
// backups.
//
// A disabled service performs no I/O and reports success. The returned error
// describes the backup attempt; the logging runtime, when configured, already
// received the same failure as a structured warning, so a caller that only polls
// the service can ignore it.
func (s *BackupService) RunOnce(ctx context.Context) error {
	if s == nil || !s.enabled || s.service == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	_, err := writeBackupFile(s.directory, func(w io.Writer) error {
		_, backupErr := s.service.Backup(ctx, w)
		return backupErr
	}, time.Now().UTC())
	if err != nil {
		s.warn(ctx, "backup_failed", err)
		return err
	}

	if s.retention > 0 {
		if _, pruneErr := PruneBackups(s.directory, s.retention, time.Now().UTC()); pruneErr != nil {
			s.warn(ctx, "prune_failed", pruneErr)
		}
	}
	return nil
}

// LatestBackup describes the newest backup this service has written.
//
// A disabled service, and a service whose directory does not exist yet, report
// an empty result rather than an error, so a status call never fails merely
// because no backup has been taken.
func (s *BackupService) LatestBackup() (*core.MetadataBackupInfo, error) {
	if s == nil {
		return &core.MetadataBackupInfo{}, nil
	}
	return LatestBackup(s.directory)
}

// run performs one backup cycle per interval until stopCh is closed. Every
// failure is contained here: a cycle that fails leaves the repository serving
// and is retried on the next tick.
func (s *BackupService) run(stopCh <-chan struct{}) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// A failed cycle is reported to the logging runtime by RunOnce, and
			// the loop keeps its cadence: backing off would silently widen the
			// recoverability window.
			_ = s.RunOnce(context.Background())
		case <-stopCh:
			return
		}
	}
}

// warn emits one structured warning for a failed backup cycle.
//
// The record deliberately carries no raw error and no path: a file-system error
// embeds the configured backup directory, and the logging rules keep full
// physical paths out of logs. The stable reason and the error classification are
// enough to alert an operator.
func (s *BackupService) warn(ctx context.Context, reason string, err error) {
	if s.log == nil || !s.log.Enabled(ctx, slog.LevelWarn) {
		return
	}

	s.log.Emit(ctx, logging.Record{
		Level:     slog.LevelWarn,
		Component: backupLogComponent,
		Event:     "metadata_backup_failed",
		Message:   "metadata backup cycle failed",
		Attrs: []slog.Attr{
			slog.String("reason", reason),
			slog.String("error_kind", classifyBackupError(err)),
		},
	})
}

// classifyBackupError reduces an error to a stable, path-free label.
func classifyBackupError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, core.ErrInvalidArgument):
		return "invalid_argument"
	case errors.Is(err, core.ErrDatabaseError):
		return "database_error"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	default:
		return "error"
	}
}
