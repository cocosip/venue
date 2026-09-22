package cleanup

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// deadLetterDateLayout renders the dead-letter date partition.
const deadLetterDateLayout = "20060102"

// DeadLetterOptions configures where a permanently failed payload is moved when
// core.PermanentlyFailedMoveToDeadLetter is configured.
//
// The layout below the volume mount path is:
//
//	<RootPath>/[<tenantID>/][<yyyyMMdd>/][<shard pairs>/]<fileKey><ext>
//
// A zero-valued DeadLetterOptions selects the Locus defaults: root
// ".deadletter", the tenant and date partitions enabled, and a shard depth of 2.
// Because Go cannot tell an unset bool from an explicit false, a non-zero value
// is honoured literally: callers that want to change a single field should start
// from DefaultDeadLetterOptions().
type DeadLetterOptions struct {
	// RootPath is the dead-letter root. It must be relative and stays inside the
	// owning volume's mount path. Default: ".deadletter".
	RootPath string

	// IncludeTenantInPath inserts the owning tenant identifier below the root.
	// Default: true.
	IncludeTenantInPath bool

	// IncludeDatePartition inserts a yyyyMMdd partition below the tenant.
	// Default: true.
	IncludeDatePartition bool

	// ShardingDepth is the number of two-character lowercase hexadecimal shard
	// segments derived from the file key. Default: 2.
	ShardingDepth int
}

// DefaultDeadLetterOptions returns the Locus default dead-letter layout.
func DefaultDeadLetterOptions() DeadLetterOptions {
	return DeadLetterOptions{
		RootPath:             ".deadletter",
		IncludeTenantInPath:  true,
		IncludeDatePartition: true,
		ShardingDepth:        2,
	}
}

// withDefaults resolves the documented defaults for an option value the caller
// left entirely unset.
func (o DeadLetterOptions) withDefaults() DeadLetterOptions {
	if o == (DeadLetterOptions{}) {
		return DefaultDeadLetterOptions()
	}
	if o.RootPath == "" {
		o.RootPath = DefaultDeadLetterOptions().RootPath
	}
	return o
}

// buildDeadLetterPath derives the volume-relative dead-letter path for a record:
//
//	<RootPath>/[<tenantID>/][<yyyyMMdd>/][<shard pairs>/]<fileKey><ext>
//
// The result is always relative and always stays inside the volume mount path:
// the root is validated, the tenant identifier is validated, shard segments are
// only emitted for lowercase hexadecimal pairs, and the final file name must be a
// single path segment. A record that cannot produce such a path is reported as an
// error and left untouched by the caller.
func (s *cleanupService) buildDeadLetterPath(record *core.FileMetadata, deadLetteredAt time.Time) (string, error) {
	if record == nil {
		return "", fmt.Errorf("dead-letter record cannot be nil: %w", core.ErrInvalidArgument)
	}
	if record.FileKey == "" {
		return "", fmt.Errorf("dead-letter file key cannot be empty: %w", core.ErrInvalidArgument)
	}

	options := s.deadLetter.withDefaults()

	root, err := sanitizeDeadLetterRoot(options.RootPath)
	if err != nil {
		return "", err
	}

	parts := []string{root}
	if options.IncludeTenantInPath {
		if err := core.ValidateTenantID(record.TenantID); err != nil {
			return "", fmt.Errorf("invalid tenant identifier for a dead-letter path: %w", err)
		}
		parts = append(parts, record.TenantID)
	}
	if options.IncludeDatePartition {
		parts = append(parts, deadLetteredAt.UTC().Format(deadLetterDateLayout))
	}
	parts = append(parts, deadLetterShardSegments(record.FileKey, options.ShardingDepth)...)

	fileName := record.FileKey + record.FileExtension
	if err := validateSinglePathSegment(fileName); err != nil {
		return "", fmt.Errorf("unsafe dead-letter file name: %w", err)
	}
	parts = append(parts, fileName)

	// The stored physical path uses forward slashes on every platform, matching
	// the storage pool's relative paths, so operator-visible paths and recovery
	// parsing are consistent. The volume sanitizer accepts both separators.
	return filepath.ToSlash(filepath.Join(parts...)), nil
}

// deadLetterShardSegments renders the two-character lowercase hexadecimal shard
// segments of a file key.
//
// This is the same derivation the volume uses (pkg/volume/path_sanitizer.go
// BuildShardedPath): the key is sliced into consecutive two-character pairs. The
// depth is capped by the key length, and a pair that is not lowercase hexadecimal
// stops the sharding, so an unexpected key can never inject a path segment.
func deadLetterShardSegments(fileKey string, depth int) []string {
	if depth <= 0 {
		return nil
	}

	segments := make([]string, 0, depth)
	for index := 0; index < depth; index++ {
		start := index * 2
		if start+2 > len(fileKey) {
			break
		}
		segment := fileKey[start : start+2]
		if !isLowerHexByte(segment) {
			break
		}
		segments = append(segments, segment)
	}

	return segments
}

// sanitizeDeadLetterRoot validates a configured dead-letter root and returns it
// cleaned. The root must be relative and must not escape the volume mount path.
func sanitizeDeadLetterRoot(root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("dead-letter root path cannot be empty: %w", core.ErrInvalidArgument)
	}
	if filepath.IsAbs(root) || filepath.VolumeName(root) != "" {
		return "", fmt.Errorf("dead-letter root path must be relative to the volume mount path: %w", core.ErrInvalidArgument)
	}

	clean := filepath.Clean(root)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("dead-letter root path must not escape the volume mount path: %w", core.ErrPathTraversalAttempt)
	}
	for _, segment := range strings.Split(filepath.ToSlash(clean), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("dead-letter root path must not contain empty or parent segments: %w", core.ErrPathTraversalAttempt)
		}
	}

	return clean, nil
}

// validateSinglePathSegment rejects a name that is not one safe path segment.
func validateSinglePathSegment(name string) error {
	if name == "" {
		return fmt.Errorf("path segment cannot be empty: %w", core.ErrInvalidArgument)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("path segment cannot be %q: %w", name, core.ErrInvalidArgument)
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("path segment must not contain path separators: %w", core.ErrPathTraversalAttempt)
	}
	if strings.ContainsRune(name, 0) {
		return fmt.Errorf("path segment must not contain a null byte: %w", core.ErrPathTraversalAttempt)
	}
	if strings.Contains(name, "...") {
		return fmt.Errorf("path segment contains a suspicious pattern: %w", core.ErrPathTraversalAttempt)
	}
	return nil
}

// moveWithinVolume moves a payload between two volume-relative paths.
//
// The optional core.FileMover capability is preferred because it moves the bytes
// without a copy. A volume that does not provide it is served by a streaming
// read + write + delete copy: the source is removed only after the destination is
// fully written, so a failed move never loses the payload.
func moveWithinVolume(ctx context.Context, volume core.StorageVolume, fromRelativePath string, toRelativePath string) error {
	if mover, ok := volume.(core.FileMover); ok {
		return mover.MoveFile(ctx, fromRelativePath, toRelativePath)
	}

	reader, err := volume.ReadFile(ctx, fromRelativePath)
	if err != nil {
		return fmt.Errorf("failed to read the payload for a dead-letter move: %w", err)
	}
	_, writeErr := volume.WriteFile(ctx, toRelativePath, reader)
	closeErr := reader.Close()
	if writeErr != nil {
		return fmt.Errorf("failed to write the dead-letter payload: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close the dead-letter source: %w", closeErr)
	}
	if err := volume.DeleteFile(ctx, fromRelativePath); err != nil {
		return fmt.Errorf("failed to remove the source payload after the dead-letter copy: %w", err)
	}

	return nil
}

// purgeRecordForMissingVolume applies the configured retired-volume policy to a
// record whose volume is not registered with this service.
//
// It returns true when the metadata row was removed. With
// core.RetiredVolumePurgeMetadataOnly the record is deleted and the directory and
// tenant counts are released without touching physical storage, because the
// volume is gone and its payloads are no longer addressable from here. Every
// other disposition keeps today's safe behaviour: the record is retained and the
// skip is logged at the caller's level.
func (s *cleanupService) purgeRecordForMissingVolume(
	ctx context.Context,
	file *core.FileMetadata,
	skipEvent string,
	skipLevel slog.Level,
) bool {
	disposition, _ := s.retiredVolumeDisposition(file.VolumeID)
	if disposition != core.RetiredVolumePurgeMetadataOnly {
		// Never delete metadata for a volume this service cannot address.
		s.emit(ctx, skipLevel, skipEvent,
			"Skipped because the record's volume is not registered",
			slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID))
		return false
	}

	if err := s.metadataRepo.Delete(ctx, file.TenantID, file.FileKey); err != nil {
		s.emit(ctx, slog.LevelError, "retired_volume_metadata_delete_failed",
			"Failed to purge metadata for a record on a retired volume",
			slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID), errorTypeAttr(err))
		return false
	}

	// Quota counts are only released for a metadata row that is actually gone, so
	// the accounting stays paired with the record.
	if s.dirQuotaMgr != nil && file.DirectoryPath != "" {
		if err := s.dirQuotaMgr.DecrementFileCount(ctx, file.TenantID, file.DirectoryPath); err != nil {
			s.emit(ctx, slog.LevelError, "retired_volume_directory_quota_failed",
				"Failed to decrement the directory quota after purging metadata on a retired volume",
				slog.String("tenant_id", file.TenantID), errorTypeAttr(err))
		}
	}
	if s.tenantQuotaMgr != nil {
		if err := s.tenantQuotaMgr.DecrementFileCount(ctx, file.TenantID); err != nil {
			s.emit(ctx, slog.LevelError, "retired_volume_tenant_quota_failed",
				"Failed to decrement the tenant quota after purging metadata on a retired volume",
				slog.String("tenant_id", file.TenantID), errorTypeAttr(err))
		}
	}

	return true
}

// retiredVolumeDisposition resolves the configured disposition of a volume that
// is not registered with this service.
func (s *cleanupService) retiredVolumeDisposition(volumeID string) (core.RetiredVolumeDisposition, bool) {
	if volumeID == "" {
		return core.RetiredVolumeKeep, false
	}
	disposition, listed := s.retiredVolumes[volumeID]
	if !listed {
		return core.RetiredVolumeKeep, false
	}
	return disposition, true
}

// applyDeadLetterDisposition moves one eligible payload into the dead-letter area
// and releases its quota counts.
//
// Ordering follows the delete path: the physical payload moves first, the record
// is then persisted in its new state, and only then are the directory and tenant
// counts released. A failure in a later step compensates the earlier ones
// (best-effort), so the record, its payload and the quota counts cannot drift
// apart and a later cycle can retry the transition.
func (s *cleanupService) applyDeadLetterDisposition(
	ctx context.Context,
	volume core.StorageVolume,
	file *core.FileMetadata,
	stats *core.CleanupStatistics,
) {
	deadLetteredAt := time.Now()
	deadLetterPath, err := s.buildDeadLetterPath(file, deadLetteredAt)
	if err != nil {
		s.emit(ctx, slog.LevelWarn, "dead_letter_target_rejected",
			"Skipped dead-lettering because no safe target path could be derived",
			slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID), errorTypeAttr(err))
		return
	}

	original := *file

	payloadExists := original.PhysicalPath != ""
	if payloadExists {
		exists, existsErr := volume.FileExists(ctx, original.PhysicalPath)
		if existsErr != nil {
			s.emit(ctx, slog.LevelWarn, "dead_letter_payload_check_failed",
				"Skipped dead-lettering after a failed payload existence check",
				slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID), errorTypeAttr(existsErr))
			return
		}
		payloadExists = exists
	}

	moved := false
	switch {
	case !payloadExists:
		// The absence is confirmed (the volume reported it without an error), so
		// the record is transitioned without a move, which releases quota that
		// would otherwise leak forever. Locus does the same
		// (StorageCleanupService.cs:1612-1617).
		s.emit(ctx, slog.LevelWarn, "dead_letter_payload_missing",
			"Dead-lettering a record whose payload is already missing",
			slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID))
	case original.PhysicalPath == deadLetterPath:
		// Already at the target: only the state transition is left.
	default:
		if moveErr := moveWithinVolume(ctx, volume, original.PhysicalPath, deadLetterPath); moveErr != nil {
			s.emit(ctx, slog.LevelWarn, "dead_letter_move_failed",
				"Failed to move a payload into the dead-letter area; the record is retained",
				slog.String("tenant_id", file.TenantID), slog.String("volume_id", file.VolumeID), errorTypeAttr(moveErr))
			return
		}
		moved = true
	}

	file.Status = core.FileStatusDeadLettered
	file.DeadLetteredAt = &deadLetteredAt
	file.PhysicalPath = deadLetterPath
	if err := s.metadataRepo.AddOrUpdate(ctx, file); err != nil {
		s.rollbackDeadLetterTransition(ctx, volume, file, original, moved, deadLetterPath)
		s.emit(ctx, slog.LevelError, "dead_letter_state_update_failed",
			"Failed to persist the dead-letter state; the transition was rolled back",
			slog.String("tenant_id", original.TenantID), slog.String("volume_id", original.VolumeID), errorTypeAttr(err))
		return
	}

	directoryPath := original.DirectoryPath
	directoryReleased := false
	if s.dirQuotaMgr != nil && directoryPath != "" {
		if err := s.dirQuotaMgr.DecrementFileCount(ctx, original.TenantID, directoryPath); err != nil {
			s.rollbackDeadLetterTransition(ctx, volume, file, original, moved, deadLetterPath)
			s.emit(ctx, slog.LevelError, "dead_letter_directory_quota_failed",
				"Failed to decrement the directory quota for a dead-lettered file; the transition was rolled back",
				slog.String("tenant_id", original.TenantID), errorTypeAttr(err))
			return
		}
		directoryReleased = true
	}

	if s.tenantQuotaMgr != nil {
		if err := s.tenantQuotaMgr.DecrementFileCount(ctx, original.TenantID); err != nil {
			if directoryReleased {
				_ = s.dirQuotaMgr.IncrementFileCount(ctx, original.TenantID, directoryPath)
			}
			s.rollbackDeadLetterTransition(ctx, volume, file, original, moved, deadLetterPath)
			s.emit(ctx, slog.LevelError, "dead_letter_tenant_quota_failed",
				"Failed to decrement the tenant quota for a dead-lettered file; the transition was rolled back",
				slog.String("tenant_id", original.TenantID), errorTypeAttr(err))
			return
		}
	}

	stats.DeadLetteredFiles++
}

// rollbackDeadLetterTransition restores the pre-transition record and, when the
// payload was already moved, moves it back.
//
// Both steps are best-effort. A failure here leaves the record and the payload
// where the last successful step put them and the quota counts unreleased, so the
// accounting stays conservative and a later cycle can retry the transition.
func (s *cleanupService) rollbackDeadLetterTransition(
	ctx context.Context,
	volume core.StorageVolume,
	file *core.FileMetadata,
	original core.FileMetadata,
	moved bool,
	deadLetterPath string,
) {
	*file = original
	if err := s.metadataRepo.AddOrUpdate(ctx, file); err != nil {
		s.emit(ctx, slog.LevelError, "dead_letter_state_rollback_failed",
			"Failed to restore a record after a partial dead-letter transition",
			slog.String("tenant_id", original.TenantID), slog.String("volume_id", original.VolumeID), errorTypeAttr(err))
	}
	if !moved {
		return
	}
	if err := moveWithinVolume(ctx, volume, deadLetterPath, original.PhysicalPath); err != nil {
		s.emit(ctx, slog.LevelError, "dead_letter_move_rollback_failed",
			"Failed to move a payload back after a partial dead-letter transition",
			slog.String("tenant_id", original.TenantID), slog.String("volume_id", original.VolumeID), errorTypeAttr(err))
	}
}
