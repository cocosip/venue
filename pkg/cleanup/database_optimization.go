package cleanup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/cocosip/venue/pkg/core"
)

// OptimizeDatabasesDetailed runs the same repository optimization work as
// OptimizeDatabases and reports how much on-disk space it reclaimed.
//
// The metadata and quota database trees are measured before and after their
// optimization, and the two trees are summed separately. A tree is identified by
// the configured CleanupServiceOptions.MetadataDirectory / QuotaDirectory roots;
// its size is the sum of the sizes of the files below that root, which is the
// closest platform-independent reading of "on disk size" available to a portable
// runtime. SizeAfter is always the measured post-pass size. Because the databases
// keep serving reads and writes during the pass a tree can also grow, so
// SpaceReclaimed is SizeBefore-SizeAfter clamped at zero rather than a negative
// reclaim.
//
// A database tree whose size cannot be measured is skipped: it is neither
// optimized nor counted, so MetadataDatabasesOptimized and
// QuotaDatabasesOptimized always describe exactly the trees that contributed to
// SizeBefore and SizeAfter. An unmeasurable tree never fails the pass, and a pass
// with nothing measurable returns a zero result and a nil error.
//
// The optimized database counts are also folded into CumulativeStatistics, the
// same way OptimizeDatabases reports them.
//
// This is the detailed counterpart of OptimizeDatabases, which is unchanged and
// still reports only the aggregate counters.
//
// Errors:
//   - the repository's optimization error, wrapped with the failing component
func (s *cleanupService) OptimizeDatabasesDetailed(ctx context.Context) (*core.DatabaseOptimizationResult, error) {
	result := &core.DatabaseOptimizationResult{}
	cumulative := &core.CleanupStatistics{}
	defer s.recordCumulative(cumulative)

	if ctx == nil {
		ctx = context.Background()
	}

	// Metadata databases.
	if before, measurable := databaseDirectorySize(s.metadataDirectory); measurable {
		if err := s.metadataRepo.Optimize(ctx); err != nil {
			return result, fmt.Errorf("failed to optimize metadata repository: %w", err)
		}

		result.MetadataDatabasesOptimized++
		cumulative.MetadataDatabasesOptimized++
		result.SizeBefore += before
		result.SizeAfter += s.measureAfter(ctx, s.metadataDirectory, before, "metadata")
	} else {
		s.emit(ctx, slog.LevelDebug, "metadata_optimization_skipped",
			"Skipped metadata database optimization because its size cannot be measured")
	}

	// Quota databases.
	if s.dirQuotaRepo != nil {
		if before, measurable := databaseDirectorySize(s.quotaDirectory); measurable {
			if err := s.dirQuotaRepo.Optimize(ctx); err != nil {
				return result, fmt.Errorf("failed to optimize directory quota repository: %w", err)
			}

			result.QuotaDatabasesOptimized++
			cumulative.QuotaDatabasesOptimized++
			result.SizeBefore += before
			result.SizeAfter += s.measureAfter(ctx, s.quotaDirectory, before, "quota")
		} else {
			s.emit(ctx, slog.LevelDebug, "quota_optimization_skipped",
				"Skipped quota database optimization because its size cannot be measured")
		}
	}

	if result.SizeAfter < result.SizeBefore {
		result.SpaceReclaimed = result.SizeBefore - result.SizeAfter
	}

	return result, nil
}

// measureAfter measures a database tree after its optimization.
//
// The post-optimization size is the reported SizeAfter contribution. When the
// tree cannot be measured after the pass, the pre-optimization size is reported
// instead: a database that was optimized must not disappear from the accounting,
// and claiming space it never freed would be worse than claiming none.
func (s *cleanupService) measureAfter(ctx context.Context, path string, before int64, database string) int64 {
	after, measurable := databaseDirectorySize(path)
	if !measurable {
		s.emit(ctx, slog.LevelDebug, "database_size_after_unavailable",
			"Reported the pre-optimization size because the database tree cannot be measured after optimization",
			slog.String("database", database))
		return before
	}
	return after
}

// databaseDirectorySize returns the combined size of the files below path.
//
// measurable is false when path is empty, is not an existing directory, or
// cannot be walked. Callers treat that as "this database has no size report"
// rather than as an error, because a size report is diagnostic and must never
// turn a successful optimization pass into a failure.
func databaseDirectorySize(path string) (size int64, measurable bool) {
	if path == "" {
		return 0, false
	}

	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return 0, false
	}

	var total int64
	walkErr := filepath.Walk(path, func(_ string, entry os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if entry == nil || entry.IsDir() {
			return nil
		}
		total += entry.Size()
		return nil
	})
	if walkErr != nil {
		return 0, false
	}

	return total, true
}
