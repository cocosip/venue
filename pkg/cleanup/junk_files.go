package cleanup

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocosip/venue/pkg/core"
)

// junkFileNames are the OS-generated metadata files cleanup removes, matching
// case-insensitively. The list mirrors Locus's _ignoredFilenames set
// (StorageCleanupService.cs:71-76).
var junkFileNames = map[string]struct{}{
	"thumbs.db":   {},
	".ds_store":   {},
	"desktop.ini": {},
}

// isJunkFileName reports whether a base name is an OS junk file.
//
// Managed payloads are excluded explicitly. A payload name is
// "<32 lowercase hex><ext>", which no junk name can match, and the guard keeps
// that invariant true even if the junk list ever grows.
func isJunkFileName(name string) bool {
	if isManagedPayloadName(name) {
		return false
	}

	_, junk := junkFileNames[strings.ToLower(name)]
	return junk
}

// isManagedPayloadName reports whether name has the managed payload shape
// "<32 lowercase hex><ext>".
func isManagedPayloadName(name string) bool {
	key := name
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		key = name[:dot]
	}
	if len(key) != 32 {
		return false
	}

	for index := 0; index < len(key); index++ {
		character := key[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// CleanupJunkFiles removes OS junk files (Thumbs.db, .DS_Store, desktop.ini)
// from every configured storage volume.
//
// The sweep walks each volume mount path depth-first, deletes matching files
// once, counts them in CleanupStatistics.JunkFilesRemoved, and adds the removed
// bytes to SpaceFreed. Context cancellation is honoured inside the walk and
// between volumes; per-file removal is best-effort, so a locked or in-use junk
// file is skipped and retried by a later sweep.
//
// Managed payloads, whose base name is "<32 lowercase hex><ext>", can never
// match a junk file name and are never removed.
func (s *cleanupService) CleanupJunkFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	stats := &core.CleanupStatistics{}
	defer s.recordCumulative(stats)

	if ctx == nil {
		ctx = context.Background()
	}

	for volumeID, volume := range s.volumeSnapshot() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		removed, freed, err := s.cleanupJunkFilesInVolume(ctx, volumeID, volume.MountPath())
		stats.JunkFilesRemoved += removed
		stats.SpaceFreed += freed
		if err != nil {
			return stats, err
		}
	}

	return stats, nil
}

// cleanupJunkFilesInVolume removes every junk file below one volume mount path.
//
// The walk root is not optional: an unavailable volume is reported so a broken
// mount is distinguishable from a clean one. Every deeper failure (an unreadable
// directory, a file that disappeared mid-walk) is best-effort.
func (s *cleanupService) cleanupJunkFilesInVolume(ctx context.Context, volumeID string, mountPath string) (int, int64, error) {
	removed := 0
	var freed int64

	walkErr := filepath.Walk(mountPath, func(path string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			if path == mountPath {
				return err
			}
			return nil
		}
		if info == nil || info.IsDir() {
			return nil
		}
		if !isJunkFileName(info.Name()) {
			return nil
		}

		size := info.Size()
		if removeErr := os.Remove(path); removeErr != nil {
			// Best-effort: a locked junk file is retried on a later sweep.
			s.emit(ctx, slog.LevelWarn, "junk_file_delete_failed",
				"Failed to remove a junk file",
				slog.String("volume_id", volumeID), errorTypeAttr(removeErr))
			return nil
		}

		removed++
		freed += size
		return nil
	})
	if walkErr != nil {
		return removed, freed, walkErr
	}

	return removed, freed, nil
}
