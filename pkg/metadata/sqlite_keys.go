package metadata

import (
	"context"
	"fmt"
	"strings"

	"github.com/cocosip/venue/pkg/core"
)

// fileKeyExistenceChunkSize bounds how many keys one existence query carries, so
// a caller that checks a whole walk stays well below SQLite's default parameter
// limit while never issuing one statement per file.
const fileKeyExistenceChunkSize = 500

// ExistingFileKeys reports which of fileKeys already have a metadata record for
// tenantID.
//
// It is the batch counterpart of Get: a full-volume scan (orphan recovery) can
// check one bounded batch of keys per query instead of issuing one point select
// per physical file. The result contains an entry only for keys that exist, so a
// missing key is simply absent from the map.
//
// errors:
//   - ErrInvalidArgument when tenantID is empty or any key is empty
//   - ErrDatabaseError when the repository is closed or the read fails
func (r *SQLiteMetadataRepository) ExistingFileKeys(
	ctx context.Context,
	tenantID string,
	fileKeys []string,
) (map[string]bool, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if len(fileKeys) == 0 {
		return map[string]bool{}, nil
	}
	for i, fileKey := range fileKeys {
		if fileKey == "" {
			return nil, fmt.Errorf("fileKeys[%d] cannot be empty: %w", i, core.ErrInvalidArgument)
		}
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	existing := make(map[string]bool, len(fileKeys))
	for start := 0; start < len(fileKeys); start += fileKeyExistenceChunkSize {
		end := start + fileKeyExistenceChunkSize
		if end > len(fileKeys) {
			end = len(fileKeys)
		}
		chunk := fileKeys[start:end]

		args := make([]any, 0, len(chunk)+1)
		args = append(args, tenantID)
		for _, fileKey := range chunk {
			args = append(args, fileKey)
		}
		query := `SELECT file_key FROM files WHERE tenant_id = ? AND file_key IN (` +
			fileKeyPlaceholders(len(chunk)) + `)`

		rows, queryErr := handle.db.QueryContext(ctx, query, args...)
		if queryErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("failed to check metadata existence: %w: %w", queryErr, core.ErrDatabaseError)
		}
		for rows.Next() {
			var fileKey string
			if scanErr := rows.Scan(&fileKey); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("failed to read metadata existence: %w: %w", scanErr, core.ErrDatabaseError)
			}
			existing[fileKey] = true
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, fmt.Errorf("failed to check metadata existence: %w: %w", rowsErr, core.ErrDatabaseError)
		}
		if closeErr := rows.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to close metadata existence read: %w: %w", closeErr, core.ErrDatabaseError)
		}
	}
	return existing, nil
}

// fileKeyPlaceholders returns "?, ?, ..." for an IN list.
func fileKeyPlaceholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}
