package metadata

import (
	"encoding/json"
	"fmt"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

const metadataSchemaVersion = "2"

var (
	metadataSchemaKey       = []byte("schema:metadata")
	legacyMetadataPrefix    = []byte("file:")
	legacyStatusIndexPrefix = []byte("idx:status:")
)

type legacyMetadataRecord struct {
	key      []byte
	metadata *core.FileMetadata
}

// migrateLegacyMetadata incrementally rekeys global legacy records. Each
// primary record is copied and removed in one transaction, making restart
// after interruption idempotent.
func migrateLegacyMetadata(db *badger.DB) error {
	for {
		records, err := readLegacyMetadataBatch(db, 128)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if err := migrateLegacyRecord(db, record); err != nil {
				return err
			}
		}
	}
	if err := deletePrefixInBatches(db, legacyStatusIndexPrefix, 256); err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		return txn.Set(metadataSchemaKey, []byte(metadataSchemaVersion))
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

func buildStatusIndexKey(metadata *core.FileMetadata) []byte {
	availableTime := "0000-00-00T00:00:00Z"
	if metadata.AvailableForProcessingAt != nil {
		availableTime = metadata.AvailableForProcessingAt.Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	return []byte(fmt.Sprintf("v2:idx:status:%s:%d:%s:%s", encodeTenantID(metadata.TenantID), metadata.Status, availableTime, metadata.FileKey))
}
