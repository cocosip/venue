// Package recovery rebuilds metadata for physical files that lost it, so a
// crash inside the write window cannot orphan a durable file permanently.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// defaultBatchSize bounds how many files one scan recovers, so a large orphan
// backlog is drained gradually instead of in one unbounded pass.
const defaultBatchSize = 1000

// orphanLookupBatchSize bounds how many candidate file keys one batched metadata
// existence query carries, so a full-volume scan buffers a bounded batch instead
// of the whole walk.
const orphanLookupBatchSize = 256

// existingFileKeyChecker is the optional metadata repository capability that
// checks a batch of file keys in one query.
//
// It is the batch counterpart of Get for a volume scan: one bounded query per
// batch instead of one point select per physical file. Callers type-assert it and
// keep the per-key Get path otherwise.
type existingFileKeyChecker interface {
	ExistingFileKeys(ctx context.Context, tenantID string, fileKeys []string) (map[string]bool, error)
}

// pendingOrphan is one physically present candidate file whose metadata has not
// been checked yet.
type pendingOrphan struct {
	fullPath     string
	relativePath string
	tenantID     string
	fileKey      string
	info         fs.FileInfo
}

// errorTypeName reduces an error to its Go type. Recovery report errors are
// part of the public report surface, and filesystem errors carry full paths,
// so the raw text must never be copied into the report.
func errorTypeName(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%T", err)
}

// Optional core capabilities implemented by this service. Callers type-assert
// them, so an implementation drift must fail the build here rather than silently
// degrade a caller to its fallback path.
var _ core.TenantOrphanRecoveryService = (*OrphanRecoveryService)(nil)

// OrphanRecoveryServiceOptions configures the orphan recovery service.
type OrphanRecoveryServiceOptions struct {
	// MetadataRepository stores the rebuilt metadata records.
	MetadataRepository core.MetadataRepository

	// Volumes are the storage volumes scanned for orphaned files.
	Volumes map[string]core.StorageVolume

	// TenantManager verifies tenant existence for the tenant-scoped entry point
	// (RecoverOrphanedFilesForTenant).
	//
	// It is optional: RecoverNow scans every tenant and works without it. When it
	// is nil, the tenant-scoped entry point fails closed instead of scanning a
	// tenant whose existence could not be verified.
	TenantManager core.TenantManager

	// TenantQuotaManager and DirectoryQuotaManager keep quotas consistent with
	// recovered records. They may be nil, in which case quotas are not updated.
	TenantQuotaManager    core.TenantQuotaManager
	DirectoryQuotaManager core.DirectoryQuotaManager

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// RecoveryInterval is the delay between periodic scans.
	// Default: 6 hours.
	RecoveryInterval time.Duration

	// InitialDelay delays the first scan so volumes can finish mounting.
	// Default: 10 seconds.
	InitialDelay time.Duration

	// RunOnStartup runs one scan after InitialDelay when the service starts.
	RunOnStartup bool

	// BatchSize is the maximum number of files recovered per scan.
	// Zero selects the default; a negative value means unlimited.
	BatchSize int

	// MinimumFileAge skips files modified more recently than this window.
	// It is an extra guard against recovering a file another writer is still
	// filling; the size/mtime stability check runs regardless.
	// Zero disables the age guard.
	MinimumFileAge time.Duration
}

// OrphanRecoveryService periodically rebuilds metadata for physical files that
// have no metadata record.
//
// The service is intentionally conservative: it only recovers a file when the
// stored layout yields a valid tenant ID and a valid 32-hex-character fileKey,
// and when no metadata exists for that pair. Everything else is counted as
// skipped, never guessed.
type OrphanRecoveryService struct {
	metadataRepo  core.MetadataRepository
	volumes       map[string]core.StorageVolume
	tenantManager core.TenantManager
	tenantQuota   core.TenantQuotaManager
	dirQuota      core.DirectoryQuotaManager
	logger        *logging.Runtime

	recoveryInterval time.Duration
	initialDelay     time.Duration
	runOnStartup     bool
	batchSize        int
	minimumFileAge   time.Duration

	// scanMu serializes scans so a manual RecoverNow cannot race the periodic
	// loop into two metadata writes for the same file.
	scanMu sync.Mutex

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool

	// lifecycleMu serializes Start against the cancel-and-join sequence of
	// Stop. Without it, a Start concurrent with Stop could add to the WaitGroup
	// while Wait is in flight and overwrite the context the old loop watches,
	// leaving an unstoppable second loop behind.
	lifecycleMu sync.Mutex
}

// NewOrphanRecoveryService creates an orphan recovery service.
func NewOrphanRecoveryService(opts *OrphanRecoveryServiceOptions) (*OrphanRecoveryService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}
	if opts.MetadataRepository == nil {
		return nil, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}
	if len(opts.Volumes) == 0 {
		return nil, fmt.Errorf("at least one storage volume is required: %w", core.ErrInvalidArgument)
	}

	recoveryInterval := opts.RecoveryInterval
	if recoveryInterval <= 0 {
		recoveryInterval = 6 * time.Hour
	}

	initialDelay := opts.InitialDelay
	if initialDelay < 0 {
		initialDelay = 0
	}

	batchSize := opts.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	return &OrphanRecoveryService{
		metadataRepo:     opts.MetadataRepository,
		volumes:          opts.Volumes,
		tenantManager:    opts.TenantManager,
		tenantQuota:      opts.TenantQuotaManager,
		dirQuota:         opts.DirectoryQuotaManager,
		logger:           logger,
		recoveryInterval: recoveryInterval,
		initialDelay:     initialDelay,
		runOnStartup:     opts.RunOnStartup,
		batchSize:        batchSize,
		minimumFileAge:   opts.MinimumFileAge,
	}, nil
}

// Start starts the periodic recovery loop.
func (s *OrphanRecoveryService) Start() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("orphan recovery service is already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(ctx)
	}()

	s.emit(ctx, slog.LevelInfo, "started", "Orphan recovery service started")
	return nil
}

// Stop stops the recovery loop and joins it before returning.
//
// Stop does not hold the service mutex while waiting, so it cannot deadlock
// against a scan that needs the same lock; the lifecycle mutex instead delays a
// concurrent Start until the join has finished, which is what keeps the
// WaitGroup and the context handoff race-free.
func (s *OrphanRecoveryService) Stop() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return fmt.Errorf("orphan recovery service is not running")
	}
	cancel := s.cancel
	s.running = false
	s.mu.Unlock()

	s.emit(context.Background(), slog.LevelInfo, "stopping", "Stopping orphan recovery service")

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()

	s.emit(context.Background(), slog.LevelInfo, "stopped", "Orphan recovery service stopped")
	return nil
}

// IsRunning reports whether the periodic recovery loop is active.
func (s *OrphanRecoveryService) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// run executes the recovery loop. The context is captured by the caller and
// never re-read from the service, so a Start/Stop/Start cycle cannot leave an
// old loop watching a newer context.
func (s *OrphanRecoveryService) run(ctx context.Context) {
	ticker := time.NewTicker(s.recoveryInterval)
	defer ticker.Stop()

	if s.runOnStartup {
		select {
		case <-time.After(s.initialDelay):
			s.recoverSafely(ctx)
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ticker.C:
			s.recoverSafely(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// recoverSafely runs one scan without letting an error or a panic stop the
// loop: the containment is per scan, so one panicking scan costs only that
// scan and the schedule keeps ticking.
func (s *OrphanRecoveryService) recoverSafely(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.emit(context.Background(), slog.LevelError, "panic", "Orphan recovery scan panicked",
				slog.String("error_type", fmt.Sprintf("%T", recovered)))
		}
	}()

	report, err := s.RecoverNow(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.emit(context.Background(), slog.LevelInfo, "scan_cancelled", "Orphan recovery scan cancelled")
			return
		}
		s.emit(ctx, slog.LevelError, "scan_failed", "Orphan recovery scan failed",
			slog.String("error_type", fmt.Sprintf("%T", err)))
		return
	}
	if report.FilesRecovered > 0 || report.FilesFailed > 0 {
		s.emit(ctx, slog.LevelInfo, "scan_completed", "Orphan recovery scan completed",
			slog.Int("scanned", report.FilesScanned),
			slog.Int("recovered", report.FilesRecovered),
			slog.Int("skipped", report.FilesSkipped),
			slog.Int("failed", report.FilesFailed),
			slog.Int64("bytes", report.BytesRecovered))
	}
}

// RecoverNow scans every configured volume and rebuilds metadata for files that
// have none. Concurrent calls are serialized.
func (s *OrphanRecoveryService) RecoverNow(ctx context.Context) (*core.OrphanRecoveryReport, error) {
	report := core.NewOrphanRecoveryReport()

	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	for _, volumeID := range sortedVolumeIDs(s.volumes) {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		volume := s.volumes[volumeID]
		if err := s.recoverVolume(ctx, volumeID, volume, "", report); err != nil {
			report.AddError("volume %s: %v", volumeID, err)
		}
	}

	return report, nil
}

// RecoverOrphanedFilesForTenant scans every configured volume for physical files
// that belong to tenantID and rebuilds their missing metadata as Pending.
//
// Only the named tenant's own storage directories are inspected and only files
// whose stored layout resolves to that tenant are registered, so the scan can
// never attribute another tenant's file. The rebuild reuses the same logic as
// RecoverNow, including the size/mtime stability check and the tenant and
// directory quota safeguards.
//
// Scans are serialized with RecoverNow through the same lock, so the periodic
// loop and a tenant-scoped call cannot both register the same file (and charge
// its quota twice). Recovery only ever adds a metadata record for a file that
// has none; it never moves or rewrites a tenant's payload, so it is safe to run
// while the runtime serves normal storage operations.
//
// Errors:
//   - ErrInvalidArgument when tenantID is empty or unsafe as a path segment
//   - ErrTenantNotFound when no tenant with that ID exists; the lookup is
//     read-only and never creates a tenant
//   - a configuration error when no TenantManager was configured, because an
//     unverified tenant must never be scanned
func (s *OrphanRecoveryService) RecoverOrphanedFilesForTenant(ctx context.Context, tenantID string) (*core.OrphanRecoveryReport, error) {
	report := core.NewOrphanRecoveryReport()

	// The tenant identifier becomes a path segment, so it is validated before it
	// can be joined onto a volume root.
	if err := core.ValidateTenantID(tenantID); err != nil {
		return report, fmt.Errorf("invalid tenant identifier for orphan recovery: %w", err)
	}

	if s.tenantManager == nil {
		return report, fmt.Errorf("cannot verify tenant %s because no tenant manager is configured", tenantID)
	}

	if _, ok, err := s.tenantManager.TryGetTenant(ctx, tenantID); err != nil {
		return report, fmt.Errorf("failed to read tenant %s: %w", tenantID, err)
	} else if !ok {
		return report, fmt.Errorf("tenant %s does not exist: %w", tenantID, core.ErrTenantNotFound)
	}

	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	for _, volumeID := range sortedVolumeIDs(s.volumes) {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		volume := s.volumes[volumeID]
		if err := s.recoverVolume(ctx, volumeID, volume, tenantID, report); err != nil {
			report.AddError("volume %s: %v", volumeID, err)
		}
	}

	return report, nil
}

// recoverVolume scans one volume and rebuilds missing metadata.
//
// tenantScope restricts the walk to that tenant's directory below the volume
// mount and requires every recovered file to resolve to that tenant; an empty
// scope scans the whole volume. Paths are always interpreted relative to the
// volume mount, so the tenant segment stays the first segment of the layout
// ParsePhysicalPath expects.
func (s *OrphanRecoveryService) recoverVolume(
	ctx context.Context,
	volumeID string,
	volume core.StorageVolume,
	tenantScope string,
	report *core.OrphanRecoveryReport,
) error {
	mountPath := volume.MountPath()
	if mountPath == "" {
		return fmt.Errorf("volume %s has no mount path", volumeID)
	}

	walkRoot := mountPath
	if tenantScope != "" {
		walkRoot = filepath.Join(mountPath, tenantScope)

		// A tenant that has not materialized a directory on this volume has no
		// orphaned file here. Any other stat failure means the subtree could not
		// be inspected, which is reported rather than reported as success.
		info, statErr := os.Stat(walkRoot)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return nil
			}
			return fmt.Errorf("failed to inspect tenant %s: %w", tenantScope, statErr)
		}
		if !info.IsDir() {
			return nil
		}
	}

	limit := s.batchSize
	pending := make(map[string][]pendingOrphan)

	// flush resolves one tenant's buffered candidates and reports whether the
	// recovery limit was reached.
	flush := func(tenantID string) (bool, error) {
		if err := s.recoverPendingBatch(ctx, volumeID, pending, tenantID, limit, report); err != nil {
			return false, err
		}
		return limit > 0 && report.FilesRecovered >= limit, nil
	}

	walkErr := filepath.WalkDir(walkRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		// Cancellation outranks entry errors: a cancelled scan must stop
		// immediately instead of being recorded as skipped entries.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if walkErr != nil {
			// An unreadable walk root means the scan saw nothing at all, which
			// must not look like a clean pass; deeper unreadable entries are
			// skipped, and the next scan retries them.
			if path == walkRoot {
				return fmt.Errorf("the scan root is not readable: %s", errorTypeName(walkErr))
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if limit > 0 && report.FilesRecovered >= limit {
			return filepath.SkipAll
		}

		// WalkDir does not stat per entry (that is the point of the switch from
		// Walk), so the size and modification time the rebuild needs are read
		// only for a candidate file.
		info, infoErr := entry.Info()
		if infoErr != nil {
			// A file that cannot be inspected is retried on the next scan, and
			// is not counted as scanned, matching the Walk-based behaviour.
			return nil
		}

		report.FilesScanned++

		relativePath, err := filepath.Rel(mountPath, path)
		if err != nil {
			report.FilesSkipped++
			return nil
		}
		relativePath = filepath.ToSlash(relativePath)

		tenantID, fileKey, ok := ParsePhysicalPath(relativePath)
		if !ok {
			report.FilesSkipped++
			return nil
		}
		// Defense in depth: a tenant-scoped scan must never attribute another
		// tenant's file, whatever the walked subtree contains.
		if tenantScope != "" && tenantID != tenantScope {
			report.FilesSkipped++
			return nil
		}

		pending[tenantID] = append(pending[tenantID], pendingOrphan{
			fullPath:     path,
			relativePath: relativePath,
			tenantID:     tenantID,
			fileKey:      fileKey,
			info:         info,
		})

		if len(pending[tenantID]) < s.lookupBatchCapacity(limit, report.FilesRecovered) {
			return nil
		}

		reachedLimit, flushErr := flush(tenantID)
		if flushErr != nil {
			return flushErr
		}
		if reachedLimit {
			return filepath.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	// Flush every remaining batch in a deterministic tenant order.
	tenantIDs := make([]string, 0, len(pending))
	for tenantID := range pending {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	for _, tenantID := range tenantIDs {
		if _, err := flush(tenantID); err != nil {
			return err
		}
	}

	return nil
}

// lookupBatchCapacity is how many candidates one existence batch may hold.
//
// It never lets a batch span more files than the recovery limit still allows, so
// a capped scan stops at exactly the file it stopped at file-by-file: if the
// limit is large the batch is bounded by orphanLookupBatchSize, and if the limit
// is small the batch is exactly the remaining allowance.
func (s *OrphanRecoveryService) lookupBatchCapacity(limit int, recovered int) int {
	capacity := orphanLookupBatchSize
	if limit > 0 {
		if remaining := limit - recovered; remaining < capacity {
			capacity = remaining
		}
	}
	if capacity < 1 {
		capacity = 1
	}
	return capacity
}

// recoverPendingBatch resolves the buffered candidates of one tenant and rebuilds
// the missing metadata.
//
// The existence check is one batched query when the repository provides it. A
// failed batch falls back to the per-key Get path, so every file keeps the
// failure accounting the point-query scan produced.
func (s *OrphanRecoveryService) recoverPendingBatch(
	ctx context.Context,
	volumeID string,
	pending map[string][]pendingOrphan,
	tenantID string,
	limit int,
	report *core.OrphanRecoveryReport,
) error {
	batch := pending[tenantID]
	delete(pending, tenantID)
	if len(batch) == 0 {
		return nil
	}

	if checker, ok := s.metadataRepo.(existingFileKeyChecker); ok && len(batch) > 1 {
		keys := make([]string, 0, len(batch))
		for _, entry := range batch {
			keys = append(keys, entry.fileKey)
		}

		existing, batchErr := checker.ExistingFileKeys(ctx, tenantID, keys)
		if batchErr != nil {
			// A cancelled context must stop the scan instead of being replayed
			// as one failing point query per file.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			// Another batch failure falls through to the per-key path below.
		} else {
			for _, entry := range batch {
				if limit > 0 && report.FilesRecovered >= limit {
					return nil
				}
				if existing[entry.fileKey] {
					report.FilesSkipped++
					continue
				}
				if err := s.rebuildPending(ctx, volumeID, entry, report); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	for _, entry := range batch {
		if limit > 0 && report.FilesRecovered >= limit {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		existing, err := s.metadataRepo.Get(ctx, entry.tenantID, entry.fileKey)
		switch {
		case err == nil && existing != nil:
			report.FilesSkipped++
			continue
		case err != nil && !errors.Is(err, core.ErrFileNotFound):
			report.FilesFailed++
			report.AddError("metadata lookup failed for tenant %s", entry.tenantID)
			continue
		}

		if err := s.rebuildPending(ctx, volumeID, entry, report); err != nil {
			return err
		}
	}

	return nil
}

// rebuildPending rebuilds one candidate's metadata and folds the outcome into the
// report. A non-nil error is a cancellation only: rebuild failures are counted.
func (s *OrphanRecoveryService) rebuildPending(
	ctx context.Context,
	volumeID string,
	entry pendingOrphan,
	report *core.OrphanRecoveryReport,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	recovered, err := s.rebuild(ctx, volumeID, entry.fullPath, entry.relativePath, entry.tenantID, entry.fileKey, entry.info)
	if err != nil {
		report.FilesFailed++
		report.AddError("recover failed for tenant %s: %v", entry.tenantID, err)
		return nil
	}
	if !recovered {
		report.FilesSkipped++
		return nil
	}

	report.FilesRecovered++
	report.BytesRecovered += entry.info.Size()
	return nil
}

// stableFile re-stats path and reports whether the file is unchanged since it
// was discovered.
//
// A physical file that is still being filled must never be recovered: doing so
// would register truncated bytes as Pending and hand a worker a partial
// payload. The file is re-checked immediately before the metadata is persisted,
// so a writer that is still active is skipped and retried on the next scan.
func (s *OrphanRecoveryService) stableFile(path string, discovered os.FileInfo) (os.FileInfo, bool) {
	fresh, err := os.Stat(path)
	if err != nil {
		return nil, false
	}
	if fresh.Size() != discovered.Size() || !fresh.ModTime().Equal(discovered.ModTime()) {
		return nil, false
	}
	if s.minimumFileAge > 0 && time.Since(fresh.ModTime()) < s.minimumFileAge {
		return nil, false
	}
	return fresh, true
}

// rebuild persists metadata for one orphaned file and compensates quotas when
// persistence fails. It returns false without an error when the file is not
// eligible (quota, or freshly written bytes).
func (s *OrphanRecoveryService) rebuild(
	ctx context.Context,
	volumeID string,
	fullPath string,
	relativePath string,
	tenantID string,
	fileKey string,
	info os.FileInfo,
) (bool, error) {
	// Only a file whose bytes stopped changing is recoverable.
	if _, stable := s.stableFile(fullPath, info); !stable {
		return false, nil
	}

	tenantQuotaTaken := false
	if s.tenantQuota != nil {
		if err := s.tenantQuota.IncrementFileCount(ctx, tenantID); err != nil {
			// Recovering must not exceed the tenant quota.
			return false, nil
		}
		tenantQuotaTaken = true
	}

	dirQuotaTaken := false
	if s.dirQuota != nil {
		if err := s.dirQuota.IncrementFileCount(ctx, tenantID, recoveredDirectoryPath); err != nil {
			if tenantQuotaTaken {
				if decErr := s.tenantQuota.DecrementFileCount(ctx, tenantID); decErr != nil {
					// The startup reconciliation can repair a leaked count on
					// the next restart, but only if it is visible. A silently
					// discarded compensation would leave the tenant quota
					// over-counted until then.
					s.emit(context.Background(), slog.LevelWarn, "orphan_quota_compensation_failed",
						"Failed to release the tenant quota reservation of a failed orphan recovery",
						slog.String("tenant_id", tenantID),
						slog.String("quota", "tenant"),
						slog.String("error_type", errorTypeName(decErr)))
				}
			}
			return false, nil
		}
		dirQuotaTaken = true
	}

	now := time.Now()
	metadata := &core.FileMetadata{
		FileKey:       fileKey,
		TenantID:      tenantID,
		VolumeID:      volumeID,
		PhysicalPath:  relativePath,
		DirectoryPath: recoveredDirectoryPath,
		FileSize:      info.Size(),
		FileExtension: filepath.Ext(relativePath),
		Status:        core.FileStatusPending,
		RetryCount:    0,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.metadataRepo.AddOrUpdate(ctx, metadata); err != nil {
		if dirQuotaTaken {
			if decErr := s.dirQuota.DecrementFileCount(ctx, tenantID, recoveredDirectoryPath); decErr != nil {
				s.emit(context.Background(), slog.LevelWarn, "orphan_quota_compensation_failed",
					"Failed to release the directory quota reservation of a failed orphan recovery",
					slog.String("tenant_id", tenantID),
					slog.String("quota", "directory"),
					slog.String("error_type", errorTypeName(decErr)))
			}
		}
		if tenantQuotaTaken {
			if decErr := s.tenantQuota.DecrementFileCount(ctx, tenantID); decErr != nil {
				s.emit(context.Background(), slog.LevelWarn, "orphan_quota_compensation_failed",
					"Failed to release the tenant quota reservation of a failed orphan recovery",
					slog.String("tenant_id", tenantID),
					slog.String("quota", "tenant"),
					slog.String("error_type", errorTypeName(decErr)))
			}
		}
		return false, err
	}

	s.emit(ctx, slog.LevelWarn, "file_recovered", "Orphaned file recovered and re-queued",
		slog.String("tenant_id", tenantID),
		slog.String("volume_id", volumeID))
	return true, nil
}

// recoveredDirectoryPath is the logical directory assigned to recovered files.
// The logical directory of the original write cannot be reconstructed from the
// physical layout, so recovered files are accounted against the root directory,
// as a plain WriteFile would be.
const recoveredDirectoryPath = "/"

// ParsePhysicalPath extracts the tenant ID and fileKey from a volume-relative
// physical path.
//
// The supported layout is "{tenantID}/[{shard}/...]/{fileKey}[.ext]", where the
// shards come from the volume's sharding settings and fileKey is the
// 32-hex-character key generated by the storage pool. Paths that do not match
// are rejected so recovery never guesses ownership.
func ParsePhysicalPath(relativePath string) (tenantID string, fileKey string, ok bool) {
	cleaned := filepath.ToSlash(relativePath)

	// The first segment is the tenant. Slicing at the separators avoids the
	// []string a split allocates for every walked file.
	firstSeparator := strings.IndexByte(cleaned, '/')
	if firstSeparator <= 0 {
		return "", "", false
	}

	tenantID = cleaned[:firstSeparator]
	if core.ValidateTenantID(tenantID) != nil {
		return "", "", false
	}

	fileName := cleaned[strings.LastIndexByte(cleaned, '/')+1:]
	if extension := strings.IndexByte(fileName, '.'); extension >= 0 {
		fileName = fileName[:extension]
	}
	if !isFileKey(fileName) {
		return "", "", false
	}

	return tenantID, fileName, true
}

// isFileKey reports whether value is a 32-character lowercase hexadecimal key,
// the exact shape produced by the storage pool.
//
// The validation is a byte range check: it rejects uppercase (and every
// non-ASCII byte) without the string copy ToLower and the decoding allocation
// hex.DecodeString paid per walked file.
func isFileKey(value string) bool {
	if len(value) != 32 {
		return false
	}

	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}

	return true
}

func sortedVolumeIDs(volumes map[string]core.StorageVolume) []string {
	ids := make([]string, 0, len(volumes))
	for id := range volumes {
		ids = append(ids, id)
	}
	// Deterministic scan order keeps tests and operational logs stable.
	sort.Strings(ids)
	return ids
}

func (s *OrphanRecoveryService) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	s.logger.Emit(ctx, logging.Record{
		Level: level, Component: "recovery.orphan", Event: event, Message: message, Attrs: attrs,
	})
}
