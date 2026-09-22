package metadata

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// scanStaleLimit bounds how many consecutive stale secondary-index entries a
// single paged status scan tolerates before it stops and hands the cursor back.
//
// A stale entry is an index key whose record is gone or no longer carries the
// requested status. Skipping them is required for correctness, but a long run of
// stale entries must not make a caller loop forever over empty pages, so the
// reader stops early and lets the caller resume past them.
const scanStaleLimit = 1000

// Compile-time proof that the repository provides the optional paged-reader
// capability callers are told to type-assert for.
var _ core.StatusPageReader = (*BadgerMetadataRepository)(nil)

// GetByStatusPage returns up to limit records with the given status, in the same
// order GetPendingFiles uses (availability, then arrival, then file key).
//
// The scan walks the status index and stops as soon as limit records have been
// produced, so a large status backlog is never materialized in memory.
//
// cursor is an opaque continuation token: an empty value starts at the
// beginning, and a non-empty value must be the NextCursor of the previous page,
// passed back unchanged. NextCursor is empty exactly when the prefix was
// exhausted before the limit was reached; a full page always returns a token, so
// the caller simply keeps calling until it receives an empty one.
//
// Records whose index entry is stale (the record is missing, or it no longer
// carries the requested status) are skipped without counting toward the limit.
// If more than scanStaleLimit entries are skipped in a row, the scan stops and
// returns a cursor so the caller can make progress.
//
// Errors:
//   - ErrInvalidArgument when tenantID is empty, limit is not positive, or the
//     cursor is not a token produced by this method.
//   - ErrDatabaseError when the repository is closed.
func (r *BadgerMetadataRepository) GetByStatusPage(ctx context.Context, tenantID string, status core.FileProcessingStatus, cursor string, limit int) (*core.MetadataPage, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errRepositoryClosed()
	}
	r.mu.RUnlock()

	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive: %w", core.ErrInvalidArgument)
	}

	resumeKey, err := decodeStatusPageCursor(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid status page cursor: %w: %v", core.ErrInvalidArgument, err)
	}

	// Preallocate conservatively. A caller may pass a very large limit to mean
	// "everything", and allocating that up front would defeat the point of a
	// bounded page; append grows the slice if the page really is that big.
	initialCapacity := limit
	if initialCapacity > scanStaleLimit {
		initialCapacity = scanStaleLimit
	}
	page := &core.MetadataPage{Records: make([]*core.FileMetadata, 0, initialCapacity)}

	err = r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // The index stores the file key as its value.
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan status index: v3:idx:status:{tenant}:{status}:{availableUTC}:{createdUTC}:{fileKey}
		prefix := r.buildStatusIndexPrefix(tenantID, status)

		// Resume strictly after the previous page's last key: appending a zero
		// byte makes the target the smallest key that can follow it, so the
		// previous page's last record is never repeated.
		seek := prefix
		if len(resumeKey) > 0 {
			seek = make([]byte, 0, len(resumeKey)+1)
			seek = append(seek, resumeKey...)
			seek = append(seek, 0)
		}

		staleRun := 0
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}

			// KeyCopy is required: the item is only valid until the next
			// iteration, and the key is handed back to the caller as a cursor.
			indexKey := it.Item().KeyCopy(nil)
			fileKeyBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				// Treat an unreadable index value like a stale entry rather than
				// failing a whole maintenance scan on one bad key.
				staleRun++
				if staleRun >= scanStaleLimit {
					page.NextCursor = encodeStatusPageCursor(indexKey)
					return nil
				}
				continue
			}
			fileKey := string(fileKeyBytes)

			metadata, metadataErr := r.getMetadataInTxn(txn, tenantID, fileKey)
			if fileKey == "" || metadataErr != nil || metadata == nil || metadata.Status != status {
				staleRun++
				if staleRun >= scanStaleLimit {
					page.NextCursor = encodeStatusPageCursor(indexKey)
					return nil
				}
				continue
			}
			staleRun = 0

			page.Records = append(page.Records, metadata)
			if len(page.Records) >= limit {
				page.NextCursor = encodeStatusPageCursor(indexKey)
				return nil
			}
		}

		return nil
	})

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("failed to get files by status page: %w", err)
	}

	return page, nil
}

// encodeStatusPageCursor renders the opaque continuation token for a page: the
// base64url encoding of the last index key the page consumed.
func encodeStatusPageCursor(indexKey []byte) string {
	return base64.RawURLEncoding.EncodeToString(indexKey)
}

// decodeStatusPageCursor parses a token produced by encodeStatusPageCursor. An
// empty token means "start at the beginning".
func decodeStatusPageCursor(cursor string) ([]byte, error) {
	if cursor == "" {
		return nil, nil
	}

	key, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, errors.New("cursor decodes to an empty key")
	}
	return key, nil
}
