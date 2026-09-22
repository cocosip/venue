package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// metadataSchemaVersion is the stored metadata schema version.
//
// Version 3 sorts the status index by availability and then by arrival time
// instead of falling back to the random file key, which restores FIFO claim
// order for files that are immediately available.
const metadataSchemaVersion = "3"

// timestampLayout is the fixed-width UTC layout used inside secondary index
// keys. Fixed width matters: variable-width or zoned timestamps (for example a
// ".999999999" format that drops trailing zeros, or a local offset) compare
// lexicographically in an order that differs from chronological order.
const timestampLayout = "2006-01-02T15:04:05.000000000Z"

// unavailableTimestamp is the availability segment for files that are
// immediately available. It must stay lexicographically smaller than any real
// timestamp rendered with timestampLayout.
const unavailableTimestamp = "0000-00-00T00:00:00.000000000Z"

const (
	legacyMetadataBatchSize = 128
	indexRebuildBatchSize   = 256
	deleteBatchSize         = 256
)

var (
	metadataSchemaKey       = []byte("schema:metadata")
	metadataPrimaryPrefix   = []byte("v2:file:")
	statusIndexPrefix       = []byte("v3:idx:status:")
	legacyMetadataPrefix    = []byte("file:")
	legacyStatusIndexPrefix = []byte("idx:status:")
	// staleStatusIndexPrefix is the pre-version-3 status index namespace. It is
	// dropped once the v3 indexes have been rebuilt.
	staleStatusIndexPrefix = []byte("v2:idx:status:")
)

type legacyMetadataRecord struct {
	key      []byte
	metadata *core.FileMetadata
}

// migrateLegacyMetadata brings an opened database up to metadataSchemaVersion.
// Every step is restartable and idempotent: work is applied in batched
// transactions and the schema version is written last, so an interrupted run
// simply repeats the remaining work on the next start.
func migrateLegacyMetadata(db *badger.DB) error {
	if err := migrateLegacyRecords(db); err != nil {
		return err
	}
	if err := deletePrefixInBatches(db, legacyStatusIndexPrefix, deleteBatchSize); err != nil {
		return err
	}
	return ensureStatusIndexSchema(db)
}

// migrateLegacyRecords incrementally rekeys global legacy records. Each primary
// record is copied and removed in one transaction, making restart after
// interruption idempotent.
func migrateLegacyRecords(db *badger.DB) error {
	for {
		records, err := readLegacyMetadataBatch(db, legacyMetadataBatchSize)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if err := migrateLegacyRecord(db, record); err != nil {
				return err
			}
		}
	}
}

// ensureStatusIndexSchema rebuilds every secondary status index when the stored
// schema version is not current, removes the previous index namespace, and only
// then records the new version. Reading the version is what makes the rebuild
// run once instead of on every open.
func ensureStatusIndexSchema(db *badger.DB) error {
	version, err := readMetadataSchemaVersion(db)
	if err != nil {
		return err
	}
	if version == metadataSchemaVersion {
		return nil
	}
	if err := rebuildStatusIndexes(db, indexRebuildBatchSize); err != nil {
		return fmt.Errorf("rebuild status indexes for schema %q: %w", version, err)
	}
	if err := deletePrefixInBatches(db, staleStatusIndexPrefix, deleteBatchSize); err != nil {
		return err
	}
	// The version is written last so that an interrupted rebuild runs again.
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(metadataSchemaKey, []byte(metadataSchemaVersion))
	})
}

// readMetadataSchemaVersion returns the stored schema version, or an empty
// string when the database predates schema versioning.
func readMetadataSchemaVersion(db *badger.DB) (string, error) {
	var version string
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metadataSchemaKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			version = string(val)
			return nil
		})
	})
	if err != nil {
		return "", fmt.Errorf("read metadata schema version: %w", err)
	}
	return version, nil
}

// rebuildStatusIndexes rewrites every secondary status index entry from the
// stored primary records, so the indexes always reflect tenant ownership and
// current record content. It is safe to run repeatedly.
func rebuildStatusIndexes(db *badger.DB, batchSize int) error {
	var after []byte
	for {
		records, last, err := readPrimaryMetadataBatch(db, after, batchSize)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		if err := writeStatusIndexes(db, records); err != nil {
			return err
		}
		after = last
	}
}

// readPrimaryMetadataBatch reads up to limit primary records that sort after
// the exclusive lower bound `after`, returning the last key read so the caller
// can resume without re-reading a batch.
func readPrimaryMetadataBatch(db *badger.DB, after []byte, limit int) ([]core.FileMetadata, []byte, error) {
	records := make([]core.FileMetadata, 0, limit)
	var last []byte
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		start := metadataPrimaryPrefix
		if len(after) > 0 {
			start = after
		}
		for it.Seek(start); it.ValidForPrefix(metadataPrimaryPrefix) && len(records) < limit; it.Next() {
			if len(after) > 0 && bytes.Equal(it.Item().Key(), after) {
				continue // Exclusive lower bound.
			}
			key := it.Item().KeyCopy(nil)
			var metadata core.FileMetadata
			if err := it.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &metadata)
			}); err != nil {
				return fmt.Errorf("decode metadata %q: %w", key, err)
			}
			if metadata.TenantID == "" || metadata.FileKey == "" {
				return fmt.Errorf("metadata %q has no tenant ID or file key", key)
			}
			records = append(records, metadata)
			last = key
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return records, last, nil
}

// writeStatusIndexes writes the status index entries for one batch of records.
func writeStatusIndexes(db *badger.DB, records []core.FileMetadata) error {
	return db.Update(func(txn *badger.Txn) error {
		for i := range records {
			if err := txn.Set(buildStatusIndexKey(&records[i]), []byte(records[i].FileKey)); err != nil {
				return err
			}
		}
		return nil
	})
}

func readLegacyMetadataBatch(db *badger.DB, limit int) ([]legacyMetadataRecord, error) {
	records := make([]legacyMetadataRecord, 0, limit)
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(legacyMetadataPrefix); it.ValidForPrefix(legacyMetadataPrefix) && len(records) < limit; it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			var metadata core.FileMetadata
			if err := item.Value(func(value []byte) error { return json.Unmarshal(value, &metadata) }); err != nil {
				return fmt.Errorf("decode legacy metadata %q: %w", key, err)
			}
			if metadata.TenantID == "" || metadata.FileKey == "" {
				return fmt.Errorf("legacy metadata %q has no tenant ID or file key", key)
			}
			records = append(records, legacyMetadataRecord{key: key, metadata: &metadata})
		}
		return nil
	})
	return records, err
}

func migrateLegacyRecord(db *badger.DB, record legacyMetadataRecord) error {
	data, err := json.Marshal(record.metadata)
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(buildMetadataKey(record.metadata.TenantID, record.metadata.FileKey), data); err != nil {
			return err
		}
		indexKey := buildStatusIndexKey(record.metadata)
		if err := txn.Set(indexKey, []byte(record.metadata.FileKey)); err != nil {
			return err
		}
		return txn.Delete(record.key)
	})
}

func deletePrefixInBatches(db *badger.DB, prefix []byte, limit int) error {
	for {
		keys := make([][]byte, 0, limit)
		if err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keys) < limit; it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			return nil
		}); err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := db.Update(func(txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
}

// buildStatusIndexKey builds the tenant-scoped secondary index key:
//
//	v3:idx:status:{base64url tenant}:{status}:{availableUTC}:{createdUTC}:{fileKey}
//
// The tenant segment is base64url, so it cannot contain ":" and no tenant can
// alias another tenant's prefix. Both timestamps use fixed-width UTC so that
// index iteration order is queue order: files that are immediately available
// sort first (unavailableTimestamp), oldest arrival first.
func buildStatusIndexKey(metadata *core.FileMetadata) []byte {
	availableUTC := unavailableTimestamp
	if metadata.AvailableForProcessingAt != nil {
		availableUTC = metadata.AvailableForProcessingAt.UTC().Format(timestampLayout)
	}
	return []byte(fmt.Sprintf("%s%s:%d:%s:%s:%s",
		statusIndexPrefix,
		encodeTenantID(metadata.TenantID),
		metadata.Status,
		availableUTC,
		metadata.CreatedAt.UTC().Format(timestampLayout),
		metadata.FileKey,
	))
}
