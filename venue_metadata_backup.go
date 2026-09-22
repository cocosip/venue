package venue

import (
	"context"
	"fmt"
	"io"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
)

// BackupMetadata writes a consistent online backup of the metadata database to w
// and returns the engine sequence number the snapshot was taken at.
//
// The backup runs while the runtime keeps serving reads and writes: it neither
// stops the queue nor blocks writers. The stream is self-describing, so
// RestoreMetadata can load it into an empty metadata directory later.
//
// The returned sequence number is an engine cursor for diagnostics. This
// runtime provides backup-period recoverability, not Locus's per-event queue
// journal, so a restore returns every record committed before the backup
// instant and nothing committed after it.
//
// Errors:
// - ErrInvalidArgument when w is nil
// - ErrDatabaseError when the runtime is stopped or the repository cannot be read
func (v *Venue) BackupMetadata(ctx context.Context, w io.Writer) (uint64, error) {
	if v.metadataRepo == nil {
		return 0, fmt.Errorf("metadata repository is not available: %w", core.ErrDatabaseError)
	}
	return metadata.BackupRepository(ctx, v.metadataRepo, w)
}

// MetadataBackupService returns the periodic backup runner, or nil when
// BadgerDB.BackupDirectory is empty or BadgerDB.BackupInterval is not positive.
//
// The runner is started and stopped with the Venue lifecycle. Its LatestBackup
// accessor is the observable "closest recoverable point" of this runtime.
func (v *Venue) MetadataBackupService() *metadata.BackupService {
	return v.metadataBackupCore
}

// MetadataBackupInfo returns the newest metadata backup on disk, or a zero value
// info when no backup directory is configured.
//
// It reads the filesystem rather than the running service, so it also works when
// backup scheduling is disabled.
func (v *Venue) MetadataBackupInfo() (*core.MetadataBackupInfo, error) {
	return metadata.LatestBackup(v.config.BadgerDB.BackupDirectory)
}

// RestoreMetadata restores a metadata database from a backup stream produced by
// (*Venue).BackupMetadata or (*metadata.BackupService).
//
// RestoreMetadata is an offline, destructive-aware repair step:
//
//   - It writes to the metadata database directory that cfg describes, so the
//     directory must be absent or empty. A non-empty directory is rejected and
//     left untouched, which prevents a mistyped path from overwriting live data.
//   - The caller must not have a Venue instance running against that directory.
//     Restore first, then construct the runtime with NewVenue.
//   - A failed restore removes the database directory it created, so the next
//     startup cannot mistake a half-loaded database for a usable one.
//
// cfg is cloned and defaulted like NewVenue does; only the metadata directory and
// the BadgerDB tuning values are used.
func RestoreMetadata(ctx context.Context, cfg *config.Config, r io.Reader) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil: %w", core.ErrInvalidArgument)
	}
	if r == nil {
		return fmt.Errorf("backup reader cannot be nil: %w", core.ErrInvalidArgument)
	}

	runtimeConfig := cfg.Clone()
	runtimeConfig.ApplyDefaults()

	options := badgerRepositoryOptions(runtimeConfig)
	if err := metadata.RestoreDatabase(ctx, options, r); err != nil {
		return fmt.Errorf("failed to restore metadata database: %w", err)
	}

	return nil
}

// badgerRepositoryOptions maps the public configuration onto the metadata
// repository options, so restore and normal open agree on every engine setting.
func badgerRepositoryOptions(runtimeConfig *config.Config) *metadata.BadgerRepositoryOptions {
	return &metadata.BadgerRepositoryOptions{
		TenantID:                   sharedMetadataTenantID,
		DataPath:                   runtimeConfig.MetadataDirectory,
		CacheTTL:                   runtimeConfig.Metadata.CacheTTL,
		MaxCacheEntries:            runtimeConfig.Metadata.MaxCacheEntries,
		GCInterval:                 runtimeConfig.BadgerDB.GCInterval,
		GCDiscardRatio:             runtimeConfig.BadgerDB.GCDiscardRatio,
		MemTableSize:               int64(runtimeConfig.BadgerDB.MemTableSize) << 20,
		ValueLogFileSize:           int64(runtimeConfig.BadgerDB.ValueLogFileSize) << 20,
		BlockCacheSize:             int64(runtimeConfig.BadgerDB.BlockCacheSize) << 20,
		SyncWrites:                 runtimeConfig.BadgerDB.SyncWrites,
		RecoverCorruptedDatabase:   runtimeConfig.BadgerDB.RecoverCorruptedDatabase,
		CorruptedDatabaseRetention: runtimeConfig.BadgerDB.CorruptedDatabaseRetention,
		BackupDirectory:            runtimeConfig.BadgerDB.BackupDirectory,
		BackupRetention:            runtimeConfig.BadgerDB.BackupRetention,
		AutoRestoreFromBackup:      runtimeConfig.BadgerDB.AutoRestoreFromBackup,
	}
}
