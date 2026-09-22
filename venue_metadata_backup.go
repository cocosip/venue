package venue

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/sqlite"
)

// metadataDatabaseFileName is the SQLite database file name inside one tenant
// directory. It mirrors the layout the repository owns.
const metadataDatabaseFileName = "metadata.db"

// metadataBackupSuffix is the extension of a written backup file.
const metadataBackupSuffix = ".bak"

// sqliteOptions maps the public SQLite configuration onto the engine options.
func sqliteOptions(cfg config.SqliteConfig) sqlite.Options {
	return sqlite.Options{
		JournalMode:          cfg.JournalMode,
		SynchronousMode:      cfg.SynchronousMode,
		CacheSizeKb:          cfg.CacheSizeKb,
		BusyTimeoutMs:        cfg.BusyTimeoutMs,
		CheckpointAfterBatch: cfg.CheckpointAfterBatch,
	}
}

// sqliteRepositoryOptions maps the public configuration onto the metadata
// repository options, so the runtime, its health checks and an offline restore
// all agree on the layout and the engine policy.
func sqliteRepositoryOptions(runtimeConfig *config.Config) *metadata.SQLiteRepositoryOptions {
	return &metadata.SQLiteRepositoryOptions{
		DataPath:                    runtimeConfig.MetadataDirectory,
		CacheTTL:                    runtimeConfig.Metadata.CacheTTL,
		MaxCacheEntries:             runtimeConfig.Metadata.MaxCacheEntries,
		Sqlite:                      sqliteOptions(runtimeConfig.Sqlite),
		MaxOpenDatabases:            runtimeConfig.Sqlite.MaxOpenDatabases,
		OpenDatabaseIdleTimeout:     runtimeConfig.Sqlite.OpenDatabaseIdleTimeout,
		RecoverCorruptedDatabase:    runtimeConfig.Sqlite.RecoverCorruptedDatabase,
		CorruptedDatabaseRetention:  runtimeConfig.Sqlite.CorruptedDatabaseRetention,
		BackupDirectory:             runtimeConfig.Sqlite.BackupDirectory,
		BackupRetention:             runtimeConfig.Sqlite.BackupRetention,
		AutoRestoreFromBackup:       runtimeConfig.Sqlite.AutoRestoreFromBackup,
		SkipBackupVerification:      runtimeConfig.Sqlite.SkipBackupVerification,
		OptimizeIdleTenantDatabases: runtimeConfig.Sqlite.OptimizeIdleTenantDatabases,
	}
}

// BackupMetadata writes a consistent online backup of the metadata store to w and
// returns 0.
//
// The stream is a zip container with one entry per tenant, named
// "<tenantId>/metadata.db", where each entry is a complete SQLite database
// produced with VACUUM INTO. The backup runs while the runtime keeps serving
// reads and writes: it neither stops the queue nor blocks writers.
//
// The returned sequence number is always zero: the SQLite engine has no
// incremental backup cursor, so callers must treat it as opaque and must not use
// it for ordering or for incremental-backup decisions. Each tenant's entry
// reflects that tenant's backup instant, so tenants may differ within one cycle.
//
// Errors:
// - ErrInvalidArgument when w is nil
// - ErrDatabaseError when the runtime is stopped or the repository cannot be read
func (v *Venue) BackupMetadata(ctx context.Context, w io.Writer) (uint64, error) {
	if v.metadataRepo == nil {
		return 0, fmt.Errorf("metadata repository is not available: %w", core.ErrDatabaseError)
	}

	service, ok := v.metadataRepo.(core.MetadataBackupService)
	if !ok {
		return 0, fmt.Errorf("metadata repository does not support backups: %w", core.ErrInvalidArgument)
	}

	return service.Backup(ctx, w)
}

// MetadataBackupService returns the periodic backup runner, or nil when
// Sqlite.BackupDirectory is empty or Sqlite.BackupInterval is not positive.
//
// The runner writes one consistent SQLite backup per tenant into
// {BackupDirectory}/{tenantId}/metadata.<stamp>.bak, which is also the tree the
// repository reads when it restores a quarantined tenant. It is started and
// stopped with the Venue lifecycle.
func (v *Venue) MetadataBackupService() *MetadataBackupService {
	return v.metadataBackupCore
}

// tenantBackupRepository is the optional repository capability the per-tenant
// backup runner needs: the tenant list plus a consistent copy of one tenant.
//
// It is an interface declared by this package so the runner does not depend on
// the concrete SQLite repository type.
type tenantBackupRepository interface {
	// KnownTenantIDs lists the tenants that currently have a metadata directory.
	KnownTenantIDs(ctx context.Context) ([]string, error)

	// BackupTenant writes a consistent copy of one tenant's database to destPath.
	BackupTenant(ctx context.Context, tenantID string, destPath string) error
}

// MetadataBackupService writes periodic consistent per-tenant metadata backups
// and prunes backups that outlived the retention window.
//
// Lifecycle: newMetadataBackupService validates the configuration and Start owns
// the single background goroutine that performs the cycles. Stop cancels that
// goroutine and waits for it to return; both are idempotent and the service may
// be restarted. RunOnce performs exactly one cycle on the calling goroutine and
// is safe to call while the loop runs or while the service is disabled.
//
// The repository is used only through tenantBackupRepository, so VACUUM INTO
// copies happen while the runtime keeps serving reads and writes. The recoverable
// point of a tenant is its own newest backup file, so tenants may differ within
// one cycle.
type MetadataBackupService struct {
	repo      core.MetadataRepository
	backups   tenantBackupRepository
	directory string
	interval  time.Duration
	retention time.Duration
	log       *logging.Runtime
	enabled   bool

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// newMetadataBackupService builds the per-tenant backup runner.
//
// It returns an error wrapping core.ErrInvalidArgument when the repository is nil
// or does not support per-tenant backups while the service is enabled.
func newMetadataBackupService(
	repo core.MetadataRepository,
	directory string,
	interval time.Duration,
	retention time.Duration,
	log *logging.Runtime,
) (*MetadataBackupService, error) {
	if repo == nil {
		return nil, fmt.Errorf("metadata backup service repository cannot be nil: %w", core.ErrInvalidArgument)
	}

	service := &MetadataBackupService{
		repo:      repo,
		directory: directory,
		interval:  interval,
		retention: retention,
		log:       log,
		enabled:   directory != "" && interval > 0,
	}

	if !service.enabled {
		return service, nil
	}

	backups, ok := repo.(tenantBackupRepository)
	if !ok {
		return nil, fmt.Errorf("metadata repository does not support per-tenant backups: %w", core.ErrInvalidArgument)
	}
	service.backups = backups

	return service, nil
}

// IsEnabled reports whether the runner has a directory and a positive interval.
func (s *MetadataBackupService) IsEnabled() bool {
	return s != nil && s.enabled
}

// Directory returns the configured backup root, or an empty string when backups
// are disabled.
func (s *MetadataBackupService) Directory() string {
	if s == nil {
		return ""
	}
	return s.directory
}

// Start launches the background backup loop. It is a no-op when the service is
// disabled and is idempotent while running.
func (s *MetadataBackupService) Start() {
	if s == nil || !s.enabled {
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.stopCh = make(chan struct{})
	stopCh := s.stopCh
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(stopCh)
}

// Stop cancels the background loop and waits for it to return. It is idempotent
// and safe to call without Start.
func (s *MetadataBackupService) Stop() {
	if s == nil {
		return
	}

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	stopCh := s.stopCh
	s.running = false
	s.stopCh = nil
	s.mu.Unlock()

	close(stopCh)
	s.wg.Wait()
}

// RunOnce performs one backup cycle: a backup of every tenant that has a
// database, followed by retention pruning. A disabled service performs no I/O and
// returns nil so a caller can invoke it unconditionally.
func (s *MetadataBackupService) RunOnce(ctx context.Context) error {
	if s == nil || !s.enabled {
		return nil
	}

	tenantIDs, err := s.backups.KnownTenantIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to enumerate tenants for backup: %w", err)
	}

	for _, tenantID := range tenantIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.backupTenant(ctx, tenantID); err != nil {
			s.warn(ctx, "tenant_backup_failed", err)
		}
	}

	s.prune(ctx)

	return nil
}

// LatestBackup reports the newest backup file anywhere in the backup tree.
func (s *MetadataBackupService) LatestBackup() (*core.MetadataBackupInfo, error) {
	if s == nil {
		return &core.MetadataBackupInfo{}, nil
	}
	return latestMetadataBackup(s.directory)
}

// backupTenant writes one tenant's backup file atomically.
func (s *MetadataBackupService) backupTenant(ctx context.Context, tenantID string) error {
	tenantDir := filepath.Join(s.directory, tenantID)
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		return fmt.Errorf("failed to create the tenant backup directory: %w", err)
	}

	instant := time.Now().UTC()
	targetPath := filepath.Join(tenantDir, metadataBackupFileName(instant))

	// VACUUM INTO refuses an existing target, and a second backup inside one
	// second must not overwrite the first one.
	targetPath, err := freeBackupPath(targetPath)
	if err != nil {
		return err
	}

	if err := s.backups.BackupTenant(ctx, tenantID, targetPath); err != nil {
		return err
	}

	return nil
}

// prune removes backup files older than the retention window. A non-positive
// retention disables pruning.
func (s *MetadataBackupService) prune(ctx context.Context) {
	if s.retention <= 0 {
		return
	}

	cutoff := time.Now().UTC().Add(-s.retention)
	_ = filepath.WalkDir(s.directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || ctx.Err() != nil {
			return nil
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), metadataBackupSuffix) {
			return nil
		}

		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			s.warn(ctx, "backup_prune_failed", err)
		}
		return nil
	})
}

// run performs the periodic backup cycles until the stop channel is closed.
func (s *MetadataBackupService) run(stopCh <-chan struct{}) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if err := s.RunOnce(context.Background()); err != nil {
				s.warn(context.Background(), "backup_cycle_failed", err)
			}
		}
	}
}

// warn emits one path-free warning about a contained backup failure.
func (s *MetadataBackupService) warn(ctx context.Context, event string, err error) {
	if s.log == nil {
		return
	}
	s.log.Emit(ctx, logging.Record{
		Level:     slog.LevelWarn,
		Component: "metadata_backup",
		Event:     event,
		Message:   "Metadata backup cycle did not complete",
		Attrs:     []slog.Attr{slog.String("error_type", fmt.Sprintf("%T", err))},
	})
}

// metadataBackupTimeFormat is the timestamp layout inside a backup file name.
const metadataBackupTimeFormat = "20060102T150405Z"

// metadataBackupFileName builds the backup file name for one tenant at instant.
func metadataBackupFileName(instant time.Time) string {
	return "metadata." + instant.UTC().Format(metadataBackupTimeFormat) + metadataBackupSuffix
}

// freeBackupPath returns a path that does not exist yet, appending a numeric
// suffix when two backups land in the same second.
func freeBackupPath(base string) (string, error) {
	if _, err := os.Lstat(base); errors.Is(err, os.ErrNotExist) {
		return base, nil
	} else if err != nil {
		return "", fmt.Errorf("failed to inspect the backup target: %w", err)
	}

	trimmed := strings.TrimSuffix(base, metadataBackupSuffix)
	for suffix := 1; suffix < 1000; suffix++ {
		candidate := fmt.Sprintf("%s.%d%s", trimmed, suffix, metadataBackupSuffix)
		if _, err := os.Lstat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", fmt.Errorf("failed to inspect the backup target: %w", err)
		}
	}

	return "", fmt.Errorf("cannot find a free backup file name below %s", filepath.Dir(base))
}

// MetadataBackupInfo returns the newest metadata backup on disk, or zero-valued
// info when no backup directory is configured or no backup exists.
//
// It reads the filesystem rather than the running service, so it also works when
// backup scheduling is disabled.
func (v *Venue) MetadataBackupInfo() (*core.MetadataBackupInfo, error) {
	return latestMetadataBackup(v.config.Sqlite.BackupDirectory)
}

// RestoreMetadata restores a metadata store from a backup stream produced by
// (*Venue).BackupMetadata.
//
// RestoreMetadata is an offline, destructive-aware repair step:
//
//   - The stream is a zip container of "<tenantId>/metadata.db" entries, which is
//     exactly the per-tenant layout the runtime uses.
//   - It writes into the metadata directory described by cfg, so that directory
//     must be absent or empty. A non-empty directory is rejected and left
//     untouched, which prevents a mistyped path from overwriting live data.
//   - The caller must not have a Venue instance running against that directory.
//     Restore first, then construct the runtime with NewVenue.
//   - A failed restore removes the files it created, so the next startup cannot
//     mistake a half-written database for a usable one.
//
// cfg is cloned and defaulted the way NewVenue does.
func RestoreMetadata(ctx context.Context, cfg *config.Config, r io.Reader) error {
	if cfg == nil {
		return fmt.Errorf("config cannot be nil: %w", core.ErrInvalidArgument)
	}
	if r == nil {
		return fmt.Errorf("backup reader cannot be nil: %w", core.ErrInvalidArgument)
	}

	runtimeConfig := cfg.Clone()
	runtimeConfig.ApplyDefaults()

	if err := restoreMetadataArchive(ctx, runtimeConfig.MetadataDirectory, r); err != nil {
		return fmt.Errorf("failed to restore metadata: %w", err)
	}

	return nil
}

// restoreMetadataArchive expands a metadata backup archive into dataPath.
//
// The target must be absent or empty, every entry must be a
// "<tenantId>/metadata.db" path with a valid tenant ID, and any failure removes
// the files this call created so a partial restore never looks usable.
func restoreMetadataArchive(ctx context.Context, dataPath string, r io.Reader) error {
	if dataPath == "" {
		return fmt.Errorf("metadata directory cannot be empty: %w", core.ErrInvalidArgument)
	}

	if err := ensureRestorableDirectory(dataPath); err != nil {
		return err
	}

	// archive/zip needs random access, so the stream is staged in memory-bounded
	// chunks on disk first: the caller owns the stream and may not be seekable.
	staging, err := os.CreateTemp(filepath.Dir(dataPath), "metadata-restore-*.zip")
	if err != nil {
		return fmt.Errorf("failed to create the restore staging file: %w: %w", err, core.ErrDatabaseError)
	}
	stagingPath := staging.Name()
	defer func() { _ = os.Remove(stagingPath) }()

	if _, err := io.Copy(staging, r); err != nil {
		_ = staging.Close()
		return fmt.Errorf("failed to read the metadata backup: %w", err)
	}
	if err := staging.Close(); err != nil {
		return fmt.Errorf("failed to close the restore staging file: %w", err)
	}

	created, err := expandMetadataArchive(ctx, stagingPath, dataPath)
	if err != nil {
		for _, path := range created {
			_ = os.RemoveAll(path)
		}
		return err
	}

	return nil
}

// expandMetadataArchive extracts every tenant database from archivePath into
// dataPath and returns the paths it created.
func expandMetadataArchive(ctx context.Context, archivePath string, dataPath string) ([]string, error) {
	archive, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open the metadata backup archive: %w", err)
	}
	defer func() { _ = archive.Close() }()

	created := make([]string, 0, len(archive.File))

	for _, entry := range archive.File {
		if err := ctx.Err(); err != nil {
			return created, err
		}

		tenantID, ok := metadataArchiveTenant(entry.Name)
		if !ok {
			return created, fmt.Errorf("backup entry %q is not a tenant metadata database: %w", entry.Name, core.ErrInvalidArgument)
		}
		if err := core.ValidateTenantID(tenantID); err != nil {
			return created, fmt.Errorf("backup entry %q has an invalid tenant ID: %w", entry.Name, err)
		}

		tenantDir := filepath.Join(dataPath, tenantID)
		if err := os.MkdirAll(tenantDir, 0o755); err != nil {
			return created, fmt.Errorf("failed to create the tenant directory: %w", err)
		}
		created = append(created, tenantDir)

		targetPath := filepath.Join(tenantDir, metadataDatabaseFileName)
		if err := writeArchiveEntry(ctx, entry, targetPath); err != nil {
			return created, err
		}
	}

	if len(created) == 0 {
		return created, fmt.Errorf("the metadata backup contains no tenant database: %w", core.ErrInvalidArgument)
	}

	return created, nil
}

// writeArchiveEntry writes one archive entry to targetPath and verifies that the
// result is a readable SQLite database.
func writeArchiveEntry(ctx context.Context, entry *zip.File, targetPath string) error {
	source, err := entry.Open()
	if err != nil {
		return fmt.Errorf("failed to open the backup entry: %w", err)
	}
	defer func() { _ = source.Close() }()

	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create the restored database: %w", err)
	}

	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return fmt.Errorf("failed to write the restored database: %w", err)
	}
	if err := target.Close(); err != nil {
		return fmt.Errorf("failed to close the restored database: %w", err)
	}

	return verifyRestoredDatabase(ctx, targetPath)
}

// verifyRestoredDatabase opens the restored file and runs an integrity check, so
// a truncated or damaged backup is rejected before a runtime can serve it.
func verifyRestoredDatabase(ctx context.Context, path string) error {
	db, err := sqlite.Open(path, sqlite.DefaultOptions())
	if err != nil {
		return fmt.Errorf("the restored database cannot be opened: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := sqlite.IntegrityCheck(ctx, db); err != nil {
		return fmt.Errorf("the restored database failed its integrity check: %w", err)
	}

	return nil
}

// ensureRestorableDirectory rejects a target that already holds data.
func ensureRestorableDirectory(dataPath string) error {
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.MkdirAll(dataPath, 0o755)
		}
		return fmt.Errorf("failed to inspect the metadata directory: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("metadata directory %s already exists and is not empty: %w", dataPath, core.ErrInvalidArgument)
	}
	return nil
}

// metadataArchiveTenant reports the tenant ID of a "<tenantId>/metadata.db" entry.
func metadataArchiveTenant(name string) (string, bool) {
	trimmed := strings.TrimPrefix(filepath.ToSlash(name), "./")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[1] != metadataDatabaseFileName || parts[0] == "" {
		return "", false
	}
	return parts[0], true
}

// latestMetadataBackup scans the per-tenant backup tree and reports the newest
// backup file, mirroring the naming the periodic runner writes.
func latestMetadataBackup(root string) (*core.MetadataBackupInfo, error) {
	info := &core.MetadataBackupInfo{BackupDirectory: root}
	if root == "" {
		return info, nil
	}

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), metadataBackupSuffix) {
			return nil
		}

		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}

		info.BackupCount++
		info.TotalBytes += fileInfo.Size()
		if fileInfo.ModTime().After(info.LatestBackupAt) {
			info.LatestBackupAt = fileInfo.ModTime().UTC()
			info.LatestBackupPath = path
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return info, nil
		}
		return nil, fmt.Errorf("failed to scan the backup directory: %w", err)
	}

	return info, nil
}
