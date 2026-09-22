// Package recovery rebuilds metadata for physical files that lost it, so a
// crash inside the write window cannot orphan a durable file permanently.
package recovery

import (
	"context"
	"encoding/hex"
	"errors"
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

// defaultBatchSize bounds how many files one scan recovers, so a large orphan
// backlog is drained gradually instead of in one unbounded pass.
const defaultBatchSize = 1000

// OrphanRecoveryServiceOptions configures the orphan recovery service.
type OrphanRecoveryServiceOptions struct {
	// MetadataRepository stores the rebuilt metadata records.
	MetadataRepository core.MetadataRepository

	// Volumes are the storage volumes scanned for orphaned files.
	Volumes map[string]core.StorageVolume

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
	metadataRepo core.MetadataRepository
	volumes      map[string]core.StorageVolume
	tenantQuota  core.TenantQuotaManager
	dirQuota     core.DirectoryQuotaManager
	logger       *logging.Runtime

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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("orphan recovery service is already running")
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.running = true

	s.wg.Add(1)
	go s.run()

	s.emit(s.ctx, slog.LevelInfo, "started", "Orphan recovery service started")
	return nil
}

// Stop stops the recovery loop. Stop does not hold the service mutex while
// waiting, so it cannot deadlock against a scan that needs the same lock.
func (s *OrphanRecoveryService) Stop() error {
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

// run executes the recovery loop.
func (s *OrphanRecoveryService) run() {
	defer s.wg.Done()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.emit(context.Background(), slog.LevelError, "panic", "Orphan recovery loop panicked",
				slog.String("error_type", fmt.Sprintf("%T", recovered)))
		}
	}()

	ticker := time.NewTicker(s.recoveryInterval)
	defer ticker.Stop()

	if s.runOnStartup {
		select {
		case <-time.After(s.initialDelay):
			s.recoverSafely()
		case <-s.ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ticker.C:
			s.recoverSafely()
		case <-s.ctx.Done():
			return
		}
	}
}

// recoverSafely runs one scan without letting an error stop the loop.
func (s *OrphanRecoveryService) recoverSafely() {
	report, err := s.RecoverNow(s.ctx)
	if err != nil {
		s.emit(s.ctx, slog.LevelError, "scan_failed", "Orphan recovery scan failed",
			slog.String("error_type", fmt.Sprintf("%T", err)))
		return
	}
	if report.FilesRecovered > 0 || report.FilesFailed > 0 {
		s.emit(s.ctx, slog.LevelInfo, "scan_completed", "Orphan recovery scan completed",
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
		if err := s.recoverVolume(ctx, volumeID, volume, report); err != nil {
			report.AddError("volume %s: %v", volumeID, err)
		}
	}

	return report, nil
}

func (s *OrphanRecoveryService) recoverVolume(
	ctx context.Context,
	volumeID string,
	volume core.StorageVolume,
	report *core.OrphanRecoveryReport,
) error {
	mountPath := volume.MountPath()
	if mountPath == "" {
		return fmt.Errorf("volume %s has no mount path", volumeID)
	}

	limit := s.batchSize
	return filepath.Walk(mountPath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil // Skip unreadable entries; the next scan retries.
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if info.IsDir() {
			return nil
		}
		if limit > 0 && report.FilesRecovered >= limit {
			return filepath.SkipAll
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

		existing, err := s.metadataRepo.Get(ctx, tenantID, fileKey)
		switch {
		case err == nil && existing != nil:
			report.FilesSkipped++
			return nil
		case err != nil && !errors.Is(err, core.ErrFileNotFound):
			report.FilesFailed++
			report.AddError("metadata lookup failed for tenant %s", tenantID)
			return nil
		}

		recovered, err := s.rebuild(ctx, volumeID, path, relativePath, tenantID, fileKey, info)
		if err != nil {
			report.FilesFailed++
			report.AddError("recover failed for tenant %s: %v", tenantID, err)
			return nil
		}
		if !recovered {
			report.FilesSkipped++
			return nil
		}

		report.FilesRecovered++
		report.BytesRecovered += info.Size()
		return nil
	})
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
				_ = s.tenantQuota.DecrementFileCount(ctx, tenantID)
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
			_ = s.dirQuota.DecrementFileCount(ctx, tenantID, recoveredDirectoryPath)
		}
		if tenantQuotaTaken {
			_ = s.tenantQuota.DecrementFileCount(ctx, tenantID)
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
	segments := strings.Split(cleaned, "/")
	if len(segments) < 2 {
		return "", "", false
	}

	tenantID = segments[0]
	if core.ValidateTenantID(tenantID) != nil {
		return "", "", false
	}

	fileName := segments[len(segments)-1]
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
func isFileKey(value string) bool {
	if len(value) != 32 {
		return false
	}
	if strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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
