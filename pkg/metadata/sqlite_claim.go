package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/cocosip/venue/pkg/core"
)

// sqliteClaimPendingSQL claims pending records in one statement.
//
// Candidate selection runs inside the UPDATE, so a claim opens one
// BEGIN IMMEDIATE transaction and one statement instead of materializing a
// candidate window and re-reading every candidate in its own transaction. The
// outer status predicate is the claim guard: SQLite re-evaluates it while it
// writes, and the write lock BEGIN IMMEDIATE already holds makes the selection
// atomic with respect to other writers, so a file is claimed at most once.
const sqliteClaimPendingSQL = `
UPDATE files
SET status = @processing,
    processing_start_time = @lease,
    updated_at = @updated
WHERE file_key IN (
        SELECT file_key
        FROM files
        WHERE tenant_id = @tenant
          AND status = @pending
          AND available_for_processing_at <= @availableBefore
        ORDER BY available_for_processing_at ASC, created_at ASC, file_key ASC
        LIMIT @limit)
  AND status = @pending
RETURNING ` + sqliteFileMetadataColumns

// ClaimPendingFiles atomically transitions up to limit claimable Pending records
// to Processing and returns them in queue order.
//
// The whole claim is one write transaction, so a caller that needs several files
// pays one commit instead of one commit per candidate. A claim that finds nothing
// claimable returns an empty slice and no error; a claim that loses the write
// race to another process retries the transaction and reports an empty slice
// when every attempt was lost, so contention is never reported as a failure.
//
// errors:
//   - ErrInvalidArgument when tenantID is empty or limit is not positive
//   - ErrDatabaseError when the store cannot be written
func (r *SQLiteMetadataRepository) ClaimPendingFiles(
	ctx context.Context,
	tenantID string,
	limit int,
) ([]*core.FileMetadata, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("claim limit must be positive: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	now := r.now().UTC()
	var claimed []*core.FileMetadata
	txErr := retryOnConflict(ctx, defaultConflictRetryAttempts, func() error {
		claimed = nil
		return handle.inTx(ctx, func(tx *sql.Tx) error {
			rows, queryErr := tx.QueryContext(ctx, sqliteClaimPendingSQL,
				sql.Named("processing", int(core.FileStatusProcessing)),
				sql.Named("lease", now.UnixNano()),
				sql.Named("updated", now.UnixNano()),
				sql.Named("tenant", tenantID),
				sql.Named("pending", int(core.FileStatusPending)),
				sql.Named("availableBefore", now.UnixNano()),
				sql.Named("limit", limit))
			if queryErr != nil {
				return queryErr
			}
			records, scanErr := sqliteScanMetadataRows(ctx, rows)
			closeErr := rows.Close()
			if scanErr != nil {
				return scanErr
			}
			if closeErr != nil {
				return closeErr
			}
			claimed = records
			return nil
		})
	})
	if txErr != nil {
		return nil, fmt.Errorf("failed to claim pending files: %w: %w", txErr, core.ErrDatabaseError)
	}

	// RETURNING does not promise a row order, so the queue order the previous
	// per-candidate claim produced is restored here.
	sort.Slice(claimed, func(i, j int) bool {
		left, right := claimed[i], claimed[j]
		leftAvailable := optionalTimeToNanos(left.AvailableForProcessingAt)
		rightAvailable := optionalTimeToNanos(right.AvailableForProcessingAt)
		if leftAvailable != rightAvailable {
			return leftAvailable < rightAvailable
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.FileKey < right.FileKey
	})

	for _, record := range claimed {
		r.cacheRecord(record)
	}
	return claimed, nil
}
