package core

import (
	"context"
	"fmt"
)

// OrphanRecoveryReport summarizes one orphan-recovery scan.
type OrphanRecoveryReport struct {
	// FilesScanned is the number of physical files inspected on the volumes.
	FilesScanned int

	// FilesRecovered is the number of files whose metadata was rebuilt and
	// re-queued as Pending.
	FilesRecovered int

	// FilesSkipped is the number of files that could not be attributed to a
	// tenant/fileKey pair, were already tracked, or exceeded quota.
	FilesSkipped int

	// FilesFailed is the number of files whose metadata could not be persisted.
	FilesFailed int

	// BytesRecovered is the total size of the recovered files.
	BytesRecovered int64

	// Errors holds safe, non-sensitive failure descriptions.
	//
	// The slice is capped: a scan can fail once per physical file, and a volume
	// with a persistent fault would otherwise accumulate one string per file for
	// the whole run. FilesFailed already carries the exact count, so the detail
	// list is capped at OrphanRecoveryReportErrorLimit entries and drops further
	// descriptions instead of growing without bound.
	Errors []string
}

// OrphanRecoveryReportErrorLimit bounds how many failure descriptions one report
// retains. The counters stay exact; only the per-file detail is capped.
const OrphanRecoveryReportErrorLimit = 100

// OrphanRecoveryService rebuilds metadata for physical files that have no
// metadata record and re-queues them as Pending.
//
// Recovery exists because the write path creates the physical file before the
// metadata record; a process crash, power loss, or storage failure in that
// window leaves a durable file that no fileKey references. Cleanup only handles
// the opposite direction (metadata without a physical file), so without
// recovery such a file is unreachable and permanently consumes disk space.
type OrphanRecoveryService interface {
	// RecoverNow scans the configured storage volumes and recovers orphaned
	// files. It is safe to call concurrently with normal storage operations;
	// each file is only re-registered when no metadata exists for it.
	RecoverNow(ctx context.Context) (*OrphanRecoveryReport, error)

	// IsRunning reports whether the periodic recovery loop is active.
	IsRunning() bool
}

// NewOrphanRecoveryReport returns an initialized report.
func NewOrphanRecoveryReport() *OrphanRecoveryReport {
	return &OrphanRecoveryReport{Errors: make([]string, 0)}
}

// AddError records a safe failure description.
//
// Descriptions beyond OrphanRecoveryReportErrorLimit are dropped, so a scan over
// a persistently failing volume cannot grow the report without bound. Callers
// that need the exact failure count read FilesFailed.
func (r *OrphanRecoveryReport) AddError(format string, args ...any) {
	if len(r.Errors) >= OrphanRecoveryReportErrorLimit {
		return
	}

	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}
