package metadata

import (
	"context"
	"fmt"
	"strings"

	"github.com/cocosip/venue/pkg/core"
)

// CountByStatuses aggregates one tenant's record count for the given statuses in
// SQLite. It is the startup quota reconciliation's replacement for paging every
// record into Go memory.
func (r *SQLiteMetadataRepository) CountByStatuses(
	ctx context.Context,
	tenantID string,
	statuses []core.FileProcessingStatus,
) (int64, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	statuses = uniqueStatuses(statuses)
	if len(statuses) == 0 {
		return 0, nil
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	defer r.end(handle)

	query := `SELECT COUNT(*) FROM files WHERE tenant_id = ? AND status IN (` +
		statusPlaceholders(len(statuses)) + `)`
	args := make([]any, 0, len(statuses)+1)
	args = append(args, tenantID)
	for _, status := range statuses {
		args = append(args, int(status))
	}

	var count int64
	if err := handle.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, fmt.Errorf("failed to count files by status: %w: %w", err, core.ErrDatabaseError)
	}
	return count, nil
}

// CountByDirectory aggregates one tenant's per-directory record count for the
// given statuses in SQLite.
func (r *SQLiteMetadataRepository) CountByDirectory(
	ctx context.Context,
	tenantID string,
	statuses []core.FileProcessingStatus,
) (map[string]int, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	statuses = uniqueStatuses(statuses)
	counts := make(map[string]int)
	if len(statuses) == 0 {
		return counts, nil
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	query := `SELECT directory_path, COUNT(*) FROM files WHERE tenant_id = ? AND status IN (` +
		statusPlaceholders(len(statuses)) + `) GROUP BY directory_path`
	args := make([]any, 0, len(statuses)+1)
	args = append(args, tenantID)
	for _, status := range statuses {
		args = append(args, int(status))
	}

	rows, err := handle.db.QueryContext(ctx, query, args...)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to count files per directory: %w: %w", err, core.ErrDatabaseError)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var directoryPath string
		var count int
		if err := rows.Scan(&directoryPath, &count); err != nil {
			return nil, fmt.Errorf("failed to scan directory usage: %w: %w", err, core.ErrDatabaseError)
		}
		counts[directoryPath] += count
	}
	if err := rows.Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to read directory usage: %w: %w", err, core.ErrDatabaseError)
	}
	return counts, nil
}

// statusPlaceholders returns "?, ?, ..." for an IN list.
func statusPlaceholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

// uniqueStatuses removes duplicate statuses so an IN list never counts a record
// twice and the query stays minimal.
func uniqueStatuses(statuses []core.FileProcessingStatus) []core.FileProcessingStatus {
	if len(statuses) == 0 {
		return nil
	}
	seen := make(map[core.FileProcessingStatus]struct{}, len(statuses))
	unique := make([]core.FileProcessingStatus, 0, len(statuses))
	for _, status := range statuses {
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		unique = append(unique, status)
	}
	return unique
}
