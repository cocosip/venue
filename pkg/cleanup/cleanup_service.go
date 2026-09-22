package cleanup

import (
	"context"
	"fmt"
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

// cleanupPageSize bounds how many metadata records a single status page holds,
// so a large store is scanned in bounded chunks instead of being materialised at
// once.
const cleanupPageSize = 500

// Optional core capabilities implemented by this service. Callers type-assert
// them, so an implementation drift must fail the build here rather than silently
// degrade a caller to its fallback path.
var (
	_ core.DatabaseOptimizationService = (*cleanupService)(nil)
	_ core.TenantCleanupService        = (*cleanupService)(nil)
)

// CleanupServiceOptions configures the cleanup service.
type CleanupServiceOptions struct {
	// TenantManager provides the tenant scope for maintenance scans.
	TenantManager core.TenantManager

	// MetadataRepository stores file metadata.
	MetadataRepository core.MetadataRepository

	// FileScheduler manages file processing queue.
	FileScheduler core.FileScheduler

	// Volumes is the collection of storage volumes.
	Volumes map[string]core.StorageVolume

	// TenantQuotaManager manages tenant-level quotas.
	// Optional: used to decrement counts when deleting files.
	TenantQuotaManager core.TenantQuotaManager

	// DirectoryQuotaManager manages directory-level quotas.
	// Optional: used to decrement counts when deleting files.
	DirectoryQuotaManager core.DirectoryQuotaManager

	// DirectoryQuotaRepository stores directory quota data.
	// Optional: used for database optimization.
	DirectoryQuotaRepository core.DirectoryQuotaRepository

	// Logging is the instance-scoped logging runtime. Nil disables logging.
	Logging *logging.Runtime

	// DefaultProcessingTimeout is the default timeout for processing files.
	// Default: 30 minutes
	DefaultProcessingTimeout time.Duration

	// PermanentlyFailedDisposition selects what happens to a permanently failed
	// file once its retention period elapses: keep it, move it to the dead-letter
	// area, or delete it.
	//
	// The Go zero value is core.PermanentlyFailedKeep. The Locus default
	// (core.PermanentlyFailedMoveToDeadLetter) is supplied by the configuration
	// model and by core.ParsePermanentlyFailedDisposition(""), so every
	// disposition stays reachable through this option.
	PermanentlyFailedDisposition core.PermanentlyFailedDisposition

	// DeadLetter configures the dead-letter layout used by
	// core.PermanentlyFailedMoveToDeadLetter. The zero value selects the Locus
	// defaults; see DeadLetterOptions.
	DeadLetter DeadLetterOptions

	// RetiredVolumes maps a retired volume identifier to how metadata that still
	// references it is handled.
	//
	// A volume that is neither registered in Volumes nor listed here keeps the
	// safe default (core.RetiredVolumeKeep): the record is retained.
	RetiredVolumes map[string]core.RetiredVolumeDisposition

	// MetadataDirectory is the root of the metadata database tree. The expired
	// quarantined-database sweep scans it for "<dbDir>.corrupted.<stamp>"
	// entries. Empty disables that half of the sweep.
	MetadataDirectory string

	// QuotaDirectory is the root of the quota database tree, scanned the same way
	// as MetadataDirectory. Empty disables that half of the sweep.
	QuotaDirectory string

	// CorruptedDatabaseRetention is how long a quarantined database directory is
	// kept before the sweep removes it. Zero selects the default of 72 hours; a
	// negative value disables the sweep.
	CorruptedDatabaseRetention time.Duration
}

// cleanupService implements the CleanupService interface.
type cleanupService struct {
	tenantManager            core.TenantManager
	metadataRepo             core.MetadataRepository
	scheduler                core.FileScheduler
	volumes                  map[string]core.StorageVolume
	tenantQuotaMgr           core.TenantQuotaManager
	dirQuotaMgr              core.DirectoryQuotaManager
	dirQuotaRepo             core.DirectoryQuotaRepository
	logger                   *logging.Runtime
	defaultProcessingTimeout time.Duration

	// permanentlyFailedDisposition is the configured disposition for retention-
	// elapsed permanently failed files.
	permanentlyFailedDisposition core.PermanentlyFailedDisposition

	// deadLetter is the resolved dead-letter layout (defaults applied).
	deadLetter DeadLetterOptions

	// retiredVolumes maps a retired volume identifier to its disposition. The map
	// is copied on construction, so the service never observes a caller mutation.
	retiredVolumes map[string]core.RetiredVolumeDisposition

	// metadataDirectory and quotaDirectory are the quarantine sweep roots.
	metadataDirectory string
	quotaDirectory    string

	// corruptedDatabaseRetention is the configured quarantine retention; zero and
	// negative are resolved by the sweep itself.
	corruptedDatabaseRetention time.Duration

	// cumulative holds the process-lifetime cleanup totals. The counters are
	// monotonic and are only ever added to.
	cumulative cumulativeCleanupCounters

	mu sync.RWMutex
}

// NewCleanupService creates a new cleanup service.
func NewCleanupService(opts *CleanupServiceOptions) (core.CleanupService, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.MetadataRepository == nil {
		return nil, fmt.Errorf("metadata repository cannot be nil: %w", core.ErrInvalidArgument)
	}
	if opts.TenantManager == nil {
		return nil, fmt.Errorf("tenant manager cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.FileScheduler == nil {
		return nil, fmt.Errorf("file scheduler cannot be nil: %w", core.ErrInvalidArgument)
	}

	if len(opts.Volumes) == 0 {
		return nil, fmt.Errorf("at least one storage volume is required: %w", core.ErrInvalidArgument)
	}

	defaultTimeout := opts.DefaultProcessingTimeout
	if defaultTimeout == 0 {
		defaultTimeout = 30 * time.Minute
	}

	logger := opts.Logging
	if logger == nil {
		logger = logging.Disabled()
	}

	retiredVolumes := make(map[string]core.RetiredVolumeDisposition, len(opts.RetiredVolumes))
	for volumeID, disposition := range opts.RetiredVolumes {
		if volumeID == "" {
			continue
		}
		retiredVolumes[volumeID] = disposition
	}

	return &cleanupService{
		tenantManager:                opts.TenantManager,
		metadataRepo:                 opts.MetadataRepository,
		scheduler:                    opts.FileScheduler,
		volumes:                      opts.Volumes,
		tenantQuotaMgr:               opts.TenantQuotaManager,
		dirQuotaMgr:                  opts.DirectoryQuotaManager,
		dirQuotaRepo:                 opts.DirectoryQuotaRepository,
		logger:                       logger,
		defaultProcessingTimeout:     defaultTimeout,
		permanentlyFailedDisposition: opts.PermanentlyFailedDisposition,
		deadLetter:                   opts.DeadLetter.withDefaults(),
		retiredVolumes:               retiredVolumes,
		metadataDirectory:            opts.MetadataDirectory,
		quotaDirectory:               opts.QuotaDirectory,
		corruptedDatabaseRetention:   opts.CorruptedDatabaseRetention,
	}, nil
}

// volumeSnapshot returns a stable copy of the registered volumes, so a cleanup
// sweep iterates a consistent set even if the service is reconfigured
// concurrently.
func (s *cleanupService) volumeSnapshot() map[string]core.StorageVolume {
	s.mu.RLock()
	defer s.mu.RUnlock()

	volumes := make(map[string]core.StorageVolume, len(s.volumes))
	for id, volume := range s.volumes {
		volumes[id] = volume
	}
	return volumes
}

// forEachStatusRecord visits every record with the given status in bounded
// pages, so a large store is never materialised at once. It uses the optional
// paging capability when the repository provides it and falls back to the
// unbounded query otherwise.
//
// The scan is forward-only over the repository's key-ordered status index, so a
// visitor that deletes the record it was given cannot make the scan skip a
// later record or revisit an earlier one. Context cancellation is honoured
// before the first page, between pages and between records. Repository and
// visitor errors stop the scan and are returned unwrapped, so each caller can
// add its own tenant/status context.
func (s *cleanupService) forEachStatusRecord(
	ctx context.Context,
	tenantID string,
	status core.FileProcessingStatus,
	visit func(*core.FileMetadata) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	reader, paged := s.metadataRepo.(core.StatusPageReader)
	if !paged {
		records, err := s.metadataRepo.GetByStatus(ctx, tenantID, status, 0)
		if err != nil {
			return err
		}
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(record); err != nil {
				return err
			}
		}
		return nil
	}

	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		page, err := reader.GetByStatusPage(ctx, tenantID, status, cursor, cleanupPageSize)
		if err != nil {
			return err
		}
		if page == nil {
			return nil
		}

		for _, record := range page.Records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(record); err != nil {
				return err
			}
		}

		// An empty cursor means the scan is complete. A cursor that does not
		// move would repeat the same page forever (and visit its records
		// twice), so the contract violation is reported instead.
		if page.NextCursor == "" {
			return nil
		}
		if page.NextCursor == cursor {
			return fmt.Errorf("metadata repository returned a non-advancing status page cursor for tenant %s with status %s", tenantID, status)
		}
		cursor = page.NextCursor
	}
}

// emit logs one structured cleanup event through the injected runtime.
func (s *cleanupService) emit(ctx context.Context, level slog.Level, event, message string, attrs ...slog.Attr) {
	s.logger.Emit(ctx, logging.Record{
		Level: level, Component: "cleanup.service", Event: event, Message: message, Attrs: attrs,
	})
}

// CleanupEmptyDirectories removes empty directories recursively, for every
// tenant on every configured volume.
//
// Directory removal is best-effort per directory, but the sweep itself is not:
// walk failures and context cancellation are reported so a failed sweep is never
// reported as success.
func (s *cleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if ctx == nil {
		ctx = context.Background()
	}

	// For each volume, scan and remove empty directories
	for _, volume := range s.volumeSnapshot() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		removed, err := s.cleanupEmptyDirsInVolume(ctx, volume, "")
		stats.EmptyDirectoriesRemoved += removed
		if err != nil {
			return stats, err
		}
	}

	return stats, nil
}

// CleanupEmptyDirectoriesForTenant removes empty directories for one tenant
// only, on every configured volume.
//
// The sweep is bounded to the tenant's own directory on each volume, so it never
// inspects or removes another tenant's directories. Every safety rule of the
// all-tenant sweep applies unchanged: the volume root, the tenant directory (the
// minimum protection depth of a tenant-scoped sweep) and the volume's shard
// directories are never removed. A tenant that has not materialized a directory
// on a volume is skipped; directory removal stays best-effort per directory, and
// walk failures and context cancellation are reported so a failed sweep is never
// reported as success.
//
// The tenant lookup is read-only: an unknown tenant is rejected instead of being
// created as a side effect.
//
// Errors:
//   - ErrInvalidArgument when tenantID is empty or unsafe as a path segment
//   - ErrTenantNotFound when no tenant with that ID exists
func (s *cleanupService) CleanupEmptyDirectoriesForTenant(ctx context.Context, tenantID string) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if ctx == nil {
		ctx = context.Background()
	}

	// The tenant identifier becomes a path segment, so it is validated before it
	// can be joined onto a volume root.
	if err := core.ValidateTenantID(tenantID); err != nil {
		return stats, fmt.Errorf("invalid tenant identifier for directory cleanup: %w", err)
	}

	if _, ok, err := s.tenantManager.TryGetTenant(ctx, tenantID); err != nil {
		return stats, fmt.Errorf("failed to read tenant %s: %w", tenantID, err)
	} else if !ok {
		return stats, fmt.Errorf("tenant %s does not exist: %w", tenantID, core.ErrTenantNotFound)
	}

	for _, volume := range s.volumeSnapshot() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		removed, err := s.cleanupEmptyDirsInVolume(ctx, volume, tenantID)
		stats.EmptyDirectoriesRemoved += removed
		if err != nil {
			return stats, err
		}
	}

	return stats, nil
}

// cleanupEmptyDirsInVolume removes empty directories in a specific volume.
//
// scopeTenantID restricts the sweep to that tenant's directory below the volume
// mount; an empty value sweeps the whole volume. The caller must have validated
// the tenant identifier before it reaches this helper.
//
// Removal repeats until no further directory can be removed, so a nested chain of
// now-empty parents is reclaimed within a single cycle. Directories are re-checked
// for emptiness at removal time because removing a child makes its parent empty.
func (s *cleanupService) cleanupEmptyDirsInVolume(ctx context.Context, volume core.StorageVolume, scopeTenantID string) (int, error) {
	mountPath := volume.MountPath()
	rootPath := mountPath

	if scopeTenantID != "" {
		rootPath = filepath.Join(mountPath, scopeTenantID)

		// A tenant that has not materialized a directory on this volume has
		// nothing to clean. Any other stat failure means the subtree could not be
		// inspected, which is reported rather than reported as success.
		info, err := os.Stat(rootPath)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, nil
			}
			return 0, fmt.Errorf("failed to inspect tenant directory %s on volume %s: %w", scopeTenantID, volume.VolumeID(), err)
		}
		if !info.IsDir() {
			return 0, nil
		}
	}

	removed := 0

	for {
		candidates, err := s.emptyDirCandidates(ctx, volume, rootPath)
		if err != nil {
			if removed == 0 {
				return 0, err
			}
			return removed, err
		}
		if len(candidates) == 0 {
			return removed, nil
		}

		// Deepest paths first: removing a child can make its parent empty.
		sort.Slice(candidates, func(i, j int) bool {
			return pathDepth(candidates[i]) > pathDepth(candidates[j])
		})

		removedThisPass := 0
		for _, path := range candidates {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return removed, ctxErr
			}

			entries, readErr := os.ReadDir(path)
			if readErr != nil || len(entries) > 0 {
				continue
			}
			if removeErr := os.Remove(path); removeErr == nil {
				removed++
				removedThisPass++
			}
			// Best-effort: a directory that is not removable right now is retried
			// on a later pass.
		}

		if removedThisPass == 0 {
			// The sweep cannot make further progress.
			return removed, nil
		}
	}
}

// emptyDirCandidates walks one subtree of a volume and returns every unprotected
// directory that is empty right now.
//
// rootPath is the volume mount for an all-tenant sweep, or one tenant's
// directory for a tenant-scoped sweep. Protection is always evaluated relative
// to the volume mount, so neither the volume root, nor the tenant level, nor a
// shard directory is ever a candidate.
func (s *cleanupService) emptyDirCandidates(ctx context.Context, volume core.StorageVolume, rootPath string) ([]string, error) {
	mountPath := volume.MountPath()
	shards := volumeShardDepth(volume)
	candidates := make([]string, 0)

	walkErr := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			// The walk root itself is not optional: report it so callers can tell
			// an unavailable volume apart from an empty one.
			if path == rootPath {
				return err
			}
			return nil
		}
		if info == nil || !info.IsDir() {
			return nil
		}
		if isProtectedSystemDirectory(mountPath, path, shards) {
			return nil
		}

		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			// Best-effort: an unreadable directory is not a cleanup failure.
			return nil
		}
		if len(entries) == 0 {
			candidates = append(candidates, path)
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	return candidates, nil
}

func pathDepth(path string) int {
	cleaned := filepath.Clean(path)
	if cleaned == "." || cleaned == string(filepath.Separator) {
		return 0
	}
	return strings.Count(cleaned, string(filepath.Separator))
}

// shardDepthProtection describes how the shard chain below a volume's tenant
// directory is protected from empty-directory removal.
type shardDepthProtection struct {
	// depth is the number of two-character shard segments directly below the
	// tenant directory.
	depth int
	// exact reports whether depth is the volume's own reported value. When it is
	// false, the structural heuristic is used instead.
	exact bool
}

// volumeShardDepth resolves shard protection for one volume.
//
// A volume that implements core.ShardingDepthProvider reports its configured
// sharding depth, which is then used exactly: the tenant directory and every
// segment down to the last shard level are protected, and nothing deeper is. The
// structural heuristic is only a fallback for volumes that cannot report a
// depth, where the layout has to be inferred from the path segments.
func volumeShardDepth(volume core.StorageVolume) shardDepthProtection {
	provider, ok := volume.(core.ShardingDepthProvider)
	if !ok {
		return shardDepthProtection{}
	}

	depth := provider.ShardingDepth()
	if depth < 0 {
		depth = 0
	}

	return shardDepthProtection{depth: depth, exact: true}
}

func isProtectedSystemDirectory(mountPath string, path string, shards shardDepthProtection) bool {
	cleanMountPath := filepath.Clean(mountPath)
	cleanPath := filepath.Clean(path)

	if cleanPath == cleanMountPath {
		return true
	}

	relativePath, err := filepath.Rel(cleanMountPath, cleanPath)
	if err != nil {
		return true
	}

	if relativePath == "." {
		return true
	}

	parts := strings.Split(filepath.ToSlash(relativePath), "/")
	if len(parts) == 0 {
		return true
	}

	// The first level under the volume mount is system-managed in Venue.
	// With the default generators this is the tenant directory, and with
	// sharded storage it can also be the first shard directory.
	if len(parts) == 1 {
		return true
	}

	// Date-partitioned hierarchies are a separate system layout, so they stay
	// protected however the shard depth is known.
	if isProtectedDateHierarchy(parts) {
		return true
	}

	if shards.exact {
		// parts[0] is the tenant directory, so the shard chain occupies
		// parts[1:1+depth]. The reported depth is used exactly: it protects every
		// directory a concurrent WriteFile could be creating with MkdirAll, and
		// it protects nothing deeper, so directories that merely look like shards
		// far below the chain are still reclaimed.
		return len(parts) <= 1+shards.depth
	}

	return isProtectedShardHierarchy(parts)
}

func isProtectedDateHierarchy(parts []string) bool {
	if len(parts) < 2 || len(parts) > 5 {
		return false
	}

	if !isNDigits(parts[1], 4) {
		return false
	}

	if len(parts) >= 3 && !isNumericRange(parts[2], 2, 1, 12) {
		return false
	}

	if len(parts) >= 4 && !isNumericRange(parts[3], 2, 1, 31) {
		return false
	}

	if len(parts) == 5 && !isNumericRange(parts[4], 2, 0, 23) {
		return false
	}

	return true
}

func isProtectedShardHierarchy(parts []string) bool {
	if len(parts) < 2 {
		return false
	}

	// parts[0] is the tenant directory (see LocalFileSystemVolume.BuildPhysicalPath),
	// so only the segments after it can be shard bytes. With the default
	// ShardingDepth of 2 the real layout is {tenant}/{xx}/{yy}/{fileKey}; deleting
	// an empty {xx} or {xx}/{yy} would race WriteFile's MkdirAll -> Create window.
	for _, part := range parts[1:] {
		if !isLowerHexByte(part) {
			return false
		}
	}

	return true
}

func isNDigits(value string, width int) bool {
	if len(value) != width {
		return false
	}

	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

func isNumericRange(value string, width int, min int, max int) bool {
	if !isNDigits(value, width) {
		return false
	}

	number := 0
	for _, r := range value {
		number = number*10 + int(r-'0')
	}

	return number >= min && number <= max
}

func isLowerHexByte(value string) bool {
	if len(value) != 2 {
		return false
	}

	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}

	return true
}

// CleanupTimedOutProcessingFiles resets files that have been in Processing status too long.
func (s *cleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	// Use configured timeout if not specified
	if timeout == 0 {
		timeout = s.defaultProcessingTimeout
	}

	tenants, err := s.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to list tenants: %w", err)
	}
	for _, tenant := range tenants {
		count, err := s.scheduler.ResetTimedOutFiles(ctx, tenant, timeout)
		if err != nil {
			return stats, fmt.Errorf("failed to reset timed out files for tenant %s: %w", tenant.ID, err)
		}
		stats.TimedOutFilesReset += count
	}

	return stats, nil
}

// CleanupPermanentlyFailedFiles applies the configured disposition to
// permanently failed files whose retention period has elapsed.
//
//   - core.PermanentlyFailedKeep leaves the payload, the metadata and the quota
//     counts untouched and performs no work at all (Locus skips the sweep, see
//     StorageCleanupService.cs:286-290).
//   - core.PermanentlyFailedDelete deletes the physical file first, then releases
//     the directory and tenant counts, and finally deletes the metadata. Every
//     failure compensates the steps that already succeeded so metadata and quota
//     accounting never drift apart.
//   - core.PermanentlyFailedMoveToDeadLetter moves the payload into the
//     dead-letter area of the same volume, records the transition and then
//     releases the quota counts.
//
// A record whose volume is not registered is handled by the retired-volume
// policy; a record that is already DeadLettered is skipped so quota can never be
// released twice.
func (s *cleanupService) CleanupPermanentlyFailedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if retention < 0 {
		return stats, fmt.Errorf("permanently failed file retention cannot be negative: %w", core.ErrInvalidArgument)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	switch s.permanentlyFailedDisposition {
	case core.PermanentlyFailedKeep:
		s.emit(ctx, slog.LevelDebug, "permanently_failed_kept",
			"Skipped permanently failed cleanup because the configured disposition is Keep")
		return stats, nil
	case core.PermanentlyFailedMoveToDeadLetter, core.PermanentlyFailedDelete:
	default:
		// An unrecognized disposition is never interpreted as "delete": the safe
		// outcome is to leave the records for manual intervention.
		s.emit(ctx, slog.LevelWarn, "permanently_failed_disposition_unknown",
			"Skipped permanently failed cleanup because the configured disposition is not recognized",
			slog.Int("disposition", int(s.permanentlyFailedDisposition)))
		return stats, nil
	}

	tenants, err := s.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to list tenants: %w", err)
	}

	cutoff := time.Now().Add(-retention)
	for _, tenant := range tenants {
		if err := s.forEachStatusRecord(ctx, tenant.ID, core.FileStatusPermanentlyFailed, func(file *core.FileMetadata) error {
			// A row that is already dead-lettered was never scanned by status, but
			// guard anyway: its quota was released by the transition, so applying a
			// disposition again would release it twice.
			if file.Status == core.FileStatusDeadLettered {
				return nil
			}
			// A row without a failure timestamp is not eligible yet: the retention
			// period cannot be evaluated, and Locus blocks such rows too
			// (StorageCleanupService.cs:1435-1439).
			if file.LastFailedAt == nil || file.LastFailedAt.After(cutoff) {
				return nil
			}

			volume, exists := s.volumes[file.VolumeID]
			if !exists {
				if s.purgeRecordForMissingVolume(ctx, file, "failed_file_volume_missing", slog.LevelWarn) {
					stats.PermanentlyFailedFilesRemoved++
				}
				return nil
			}

			if s.permanentlyFailedDisposition == core.PermanentlyFailedMoveToDeadLetter {
				s.applyDeadLetterDisposition(ctx, volume, file, stats)
				return nil
			}

			s.deletePermanentlyFailedRecord(ctx, tenant.ID, volume, file, stats)
			return nil
		}); err != nil {
			return stats, fmt.Errorf("failed to get permanently failed files for tenant %s: %w", tenant.ID, err)
		}
	}

	return stats, nil
}

// deletePermanentlyFailedRecord physically deletes one permanently failed file,
// releases its quota counts and removes its metadata, compensating every step
// that already succeeded when a later step fails.
func (s *cleanupService) deletePermanentlyFailedRecord(
	ctx context.Context,
	tenantID string,
	volume core.StorageVolume,
	file *core.FileMetadata,
	stats *core.CleanupStatistics,
) {
	if err := volume.DeleteFile(ctx, file.PhysicalPath); err != nil {
		s.emit(ctx, slog.LevelWarn, "failed_file_delete_failed",
			"Failed to delete physical file for permanently failed file; metadata retained",
			slog.String("tenant_id", tenantID), slog.String("volume_id", file.VolumeID))
		return
	}

	directoryPath := file.DirectoryPath
	directoryQuotaDecremented := false
	if s.dirQuotaMgr != nil && directoryPath != "" {
		if err := s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, directoryPath); err != nil {
			// The physical file is already gone; leave metadata in place so a
			// later cycle retries the accounting.
			s.emit(ctx, slog.LevelError, "failed_file_directory_quota_failed",
				"Failed to decrement directory quota for permanently failed file",
				slog.String("tenant_id", tenantID), errorTypeAttr(err))
			return
		}
		directoryQuotaDecremented = true
	}

	tenantQuotaDecremented := false
	if s.tenantQuotaMgr != nil {
		if err := s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID); err != nil {
			if directoryQuotaDecremented {
				_ = s.dirQuotaMgr.IncrementFileCount(ctx, file.TenantID, directoryPath)
			}
			s.emit(ctx, slog.LevelError, "failed_file_tenant_quota_failed",
				"Failed to decrement tenant quota for permanently failed file",
				slog.String("tenant_id", tenantID), errorTypeAttr(err))
			return
		}
		tenantQuotaDecremented = true
	}

	// Snapshot identity before deleting: the row must not be referenced after.
	fileTenantID := file.TenantID
	fileKey := file.FileKey
	fileSize := file.FileSize

	if err := s.metadataRepo.Delete(ctx, fileTenantID, fileKey); err != nil {
		if tenantQuotaDecremented {
			_ = s.tenantQuotaMgr.IncrementFileCount(ctx, fileTenantID)
		}
		if directoryQuotaDecremented {
			_ = s.dirQuotaMgr.IncrementFileCount(ctx, fileTenantID, directoryPath)
		}
		s.emit(ctx, slog.LevelError, "failed_file_metadata_delete_failed",
			"Failed to delete metadata for permanently failed file; quota counts restored",
			slog.String("tenant_id", fileTenantID), errorTypeAttr(err))
		return
	}

	stats.PermanentlyFailedFilesRemoved++
	stats.SpaceFreed += fileSize
}

// CleanupCompletedFiles deletes completed physical files and finalizes their metadata.
func (s *cleanupService) CleanupCompletedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if retention < 0 {
		return stats, fmt.Errorf("completed file retention cannot be negative: %w", core.ErrInvalidArgument)
	}

	tenants, err := s.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to list tenants: %w", err)
	}
	cutoff := time.Now().Add(-retention)
	for _, tenant := range tenants {
		if err := s.forEachStatusRecord(ctx, tenant.ID, core.FileStatusCompleted, func(file *core.FileMetadata) error {
			if file.CompletedAt == nil || file.CompletedAt.After(cutoff) {
				return nil
			}
			volume, exists := s.volumes[file.VolumeID]
			if !exists {
				if s.purgeRecordForMissingVolume(ctx, file, "completed_file_volume_missing", slog.LevelDebug) {
					stats.CompletedRecordsRemoved++
				}
				return nil
			}
			if err := volume.DeleteFile(ctx, file.PhysicalPath); err != nil {
				return nil
			}

			directoryPath := file.DirectoryPath
			directoryQuotaDecremented := false
			if s.dirQuotaMgr != nil {
				if err := s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, directoryPath); err != nil {
					return nil
				}
				directoryQuotaDecremented = true
			}

			tenantQuotaDecremented := false
			if s.tenantQuotaMgr != nil {
				if err := s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID); err != nil {
					if directoryQuotaDecremented {
						_ = s.dirQuotaMgr.IncrementFileCount(ctx, file.TenantID, directoryPath)
					}
					return nil
				}
				tenantQuotaDecremented = true
			}

			if err := s.metadataRepo.Delete(ctx, file.TenantID, file.FileKey); err != nil {
				if tenantQuotaDecremented {
					_ = s.tenantQuotaMgr.IncrementFileCount(ctx, file.TenantID)
				}
				if directoryQuotaDecremented {
					_ = s.dirQuotaMgr.IncrementFileCount(ctx, file.TenantID, directoryPath)
				}
				return nil
			}

			stats.CompletedRecordsRemoved++
			stats.SpaceFreed += file.FileSize
			return nil
		}); err != nil {
			return stats, fmt.Errorf("failed to get completed files for tenant %s: %w", tenant.ID, err)
		}
	}

	return stats, nil
}

// CleanupOrphanedMetadata removes metadata for files that no longer exist physically.
func (s *cleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	// Get all file metadata (we'll scan by status)
	allStatuses := []core.FileProcessingStatus{
		core.FileStatusPending,
		core.FileStatusProcessing,
		core.FileStatusFailed,
		core.FileStatusPermanentlyFailed,
	}

	tenants, err := s.tenantManager.GetAllTenants(ctx)
	if err != nil {
		return stats, fmt.Errorf("failed to list tenants: %w", err)
	}
	for _, tenant := range tenants {
		for _, status := range allStatuses {
			if err := s.forEachStatusRecord(ctx, tenant.ID, status, func(file *core.FileMetadata) error {
				// Only act on a confirmed absence. An unknown volume, an empty
				// physical path and an inconclusive existence check all mean the
				// metadata cannot be proven orphaned, so it is retained: deleting
				// live metadata loses the only reference to a real file.
				confirmedMissing := false

				if volume, exists := s.volumes[file.VolumeID]; !exists {
					// The volume is not registered with this service, so the physical
					// file cannot be checked. The retired-volume policy decides
					// whether the metadata is purged or retained; the accounting
					// stays paired with a row that is really gone.
					if s.purgeRecordForMissingVolume(ctx, file, "orphaned_metadata_volume_missing", slog.LevelWarn) {
						stats.OrphanedMetadataRemoved++
					}
					return nil
				} else if file.PhysicalPath == "" {
					s.emit(ctx, slog.LevelWarn, "orphaned_metadata_empty_path",
						"Skipped orphaned metadata check because PhysicalPath is empty",
						slog.String("tenant_id", tenant.ID), slog.String("volume_id", file.VolumeID))
				} else {
					fileExists, existsErr := volume.FileExists(ctx, file.PhysicalPath)
					if existsErr != nil {
						s.emit(ctx, slog.LevelWarn, "orphaned_metadata_check_failed",
							"Skipped orphaned metadata check after a failed existence check",
							slog.String("tenant_id", tenant.ID), slog.String("volume_id", file.VolumeID),
							errorTypeAttr(existsErr))
					} else {
						confirmedMissing = !fileExists
					}
				}

				if !confirmedMissing {
					return nil
				}

				// Metadata deletion and quota decrements stay paired: the delete is
				// attempted first, and quota counts are only adjusted for a metadata
				// row that is actually gone.
				if err := s.metadataRepo.Delete(ctx, tenant.ID, file.FileKey); err != nil {
					s.emit(ctx, slog.LevelError, "orphaned_metadata_delete_failed",
						"Failed to delete orphaned metadata",
						slog.String("tenant_id", tenant.ID), errorTypeAttr(err))
					return nil
				}

				if s.dirQuotaMgr != nil && file.DirectoryPath != "" {
					if err := s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, file.DirectoryPath); err != nil {
						s.emit(ctx, slog.LevelError, "orphaned_metadata_directory_quota_failed",
							"Failed to decrement directory quota after deleting orphaned metadata",
							slog.String("tenant_id", tenant.ID), errorTypeAttr(err))
					}
				}
				if s.tenantQuotaMgr != nil {
					if err := s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID); err != nil {
						s.emit(ctx, slog.LevelError, "orphaned_metadata_tenant_quota_failed",
							"Failed to decrement tenant quota after deleting orphaned metadata",
							slog.String("tenant_id", tenant.ID), errorTypeAttr(err))
					}
				}

				stats.OrphanedMetadataRemoved++
				return nil
			}); err != nil {
				return stats, fmt.Errorf("failed to get orphaned metadata for tenant %s with status %s: %w", tenant.ID, status, err)
			}
		}
	}

	return stats, nil
}

// OptimizeDatabases triggers repository-level garbage collection / compaction work.
func (s *cleanupService) OptimizeDatabases(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if err := s.metadataRepo.Optimize(ctx); err != nil {
		return stats, fmt.Errorf("failed to optimize metadata repository: %w", err)
	}
	stats.MetadataDatabasesOptimized++

	if s.dirQuotaRepo != nil {
		if err := s.dirQuotaRepo.Optimize(ctx); err != nil {
			return stats, fmt.Errorf("failed to optimize directory quota repository: %w", err)
		}
		stats.QuotaDatabasesOptimized++
	}

	return stats, nil
}
