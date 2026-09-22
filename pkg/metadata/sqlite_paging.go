package metadata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// statusPageCursorVersion is the only cursor payload revision this
// implementation mints. A token that declares any other version is rejected
// instead of being interpreted with today's field layout.
const statusPageCursorVersion = byte(0x01)

// statusPageCursorHeaderSize is the fixed part of the payload:
// version(1) + available(8) + created(8) + key length(4).
const statusPageCursorHeaderSize = 21

// statusPageCursorInitialCapacity bounds the allocation one page pre-reserves,
// so a caller that passes a very large limit to mean "everything" cannot make
// the repository allocate that much before it knows the page is full.
const statusPageCursorInitialCapacity = 1024

// sqliteSelectStatusPageSQL is the keyset page query.
//
// The three-part predicate is a strict total order because file_key is the
// primary key, so pages neither overlap nor skip records. The first page binds
// the smallest possible triple (available = -1, created = -1, file_key = ""),
// which makes every part trivially true and keeps the query a plain index-ordered
// range scan. A branch such as "OR @cursorEmpty = 1" is deliberately avoided: it
// would stop the planner from using the index's ordering.
const sqliteSelectStatusPageSQL = `
SELECT ` + sqliteFileMetadataColumns + `
FROM files
WHERE tenant_id = @tenant
  AND status = @status
  AND (
        available_for_processing_at > @cursorAvailable
     OR (available_for_processing_at = @cursorAvailable AND created_at > @cursorCreated)
     OR (available_for_processing_at = @cursorAvailable AND created_at = @cursorCreated AND file_key > @cursorFileKey)
      )
ORDER BY available_for_processing_at ASC, created_at ASC, file_key ASC
LIMIT @limit`

// statusPageCursor is the decoded position of one page boundary.
type statusPageCursor struct {
	available int64
	created   int64
	fileKey   string
}

// statusPageCursorStart is the position that precedes every real record.
func statusPageCursorStart() statusPageCursor {
	return statusPageCursor{available: -1, created: -1, fileKey: ""}
}

// GetByStatusPage returns up to limit records with the given status, ordered like
// GetPendingFiles (availability, then arrival, then file key).
//
// cursor is an opaque continuation token: an empty value starts at the head, and
// a non-empty value must be the NextCursor of the previous page, passed back
// unchanged. NextCursor is empty exactly when the scan reached the end; a full
// page always returns a token, so a caller simply keeps calling until it receives
// an empty one.
//
// Errors:
//   - ErrInvalidArgument when tenantID is empty, limit is not positive, or the
//     cursor was not produced by this method.
//   - ErrDatabaseError when the repository is closed.
func (r *SQLiteMetadataRepository) GetByStatusPage(ctx context.Context, tenantID string, status core.FileProcessingStatus, cursor string, limit int) (*core.MetadataPage, error) {
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errRepositoryClosed()
	}

	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive: %w", core.ErrInvalidArgument)
	}

	position, err := decodeSQLiteStatusPageCursor(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid status page cursor: %w: %v", core.ErrInvalidArgument, err)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer r.end(handle)

	capacity := limit
	if capacity > statusPageCursorInitialCapacity {
		capacity = statusPageCursorInitialCapacity
	}
	page := &core.MetadataPage{Records: make([]*core.FileMetadata, 0, capacity)}

	rows, err := handle.db.QueryContext(ctx, sqliteSelectStatusPageSQL,
		sql.Named("tenant", tenantID),
		sql.Named("status", int(status)),
		sql.Named("cursorAvailable", position.available),
		sql.Named("cursorCreated", position.created),
		sql.Named("cursorFileKey", position.fileKey),
		sql.Named("limit", limit))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status page: %w", err)
	}
	defer func() { _ = rows.Close() }()

	records, err := sqliteScanMetadataRows(ctx, rows)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status page: %w", err)
	}
	page.Records = records

	// A short page exhausts the scan, so the caller stops. A full page must
	// return a token to continue from.
	if len(records) == limit {
		last := records[len(records)-1]
		page.NextCursor = encodeSQLiteStatusPageCursor(statusPageCursor{
			available: availableNanos(last.AvailableForProcessingAt),
			created:   last.CreatedAt.UTC().UnixNano(),
			fileKey:   last.FileKey,
		})
	}
	return page, nil
}

// encodeSQLiteStatusPageCursor renders the opaque continuation token: base64url
// (no padding) of a fixed-width big-endian payload.
func encodeSQLiteStatusPageCursor(cursor statusPageCursor) string {
	payload := make([]byte, 0, statusPageCursorHeaderSize+len(cursor.fileKey))
	payload = append(payload, statusPageCursorVersion)
	payload = binary.BigEndian.AppendUint64(payload, uint64(cursor.available))
	payload = binary.BigEndian.AppendUint64(payload, uint64(cursor.created))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(cursor.fileKey)))
	payload = append(payload, cursor.fileKey...)
	return base64.RawURLEncoding.EncodeToString(payload)
}

// decodeSQLiteStatusPageCursor parses a token produced by
// encodeSQLiteStatusPageCursor. An empty token means "start at the head".
//
// The payload is self-describing, so a damaged or foreign token is rejected
// rather than silently reinterpreted as a position in this scan.
func decodeSQLiteStatusPageCursor(token string) (statusPageCursor, error) {
	if token == "" {
		return statusPageCursorStart(), nil
	}

	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return statusPageCursor{}, err
	}
	if len(payload) < statusPageCursorHeaderSize {
		return statusPageCursor{}, fmt.Errorf("cursor payload is %d bytes, shorter than the %d-byte header",
			len(payload), statusPageCursorHeaderSize)
	}
	if payload[0] != statusPageCursorVersion {
		return statusPageCursor{}, fmt.Errorf("unsupported cursor version %d", payload[0])
	}

	declaredLength := int(binary.BigEndian.Uint32(payload[17:21]))
	if declaredLength != len(payload)-statusPageCursorHeaderSize {
		return statusPageCursor{}, fmt.Errorf("cursor declares a %d-byte file key but carries %d bytes",
			declaredLength, len(payload)-statusPageCursorHeaderSize)
	}

	return statusPageCursor{
		available: int64(binary.BigEndian.Uint64(payload[1:9])),
		created:   int64(binary.BigEndian.Uint64(payload[9:17])),
		fileKey:   string(payload[statusPageCursorHeaderSize:]),
	}, nil
}

// availableNanos renders the availability sentinel for a cursor: a nil pointer
// is the sentinel 0, which sorts before every real instant.
func availableNanos(available *time.Time) int64 {
	if available == nil {
		return 0
	}
	return available.UTC().UnixNano()
}
