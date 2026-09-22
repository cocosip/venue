package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	venuesqlite "github.com/cocosip/venue/pkg/sqlite"
)

// Example demonstrates the whole connection lifecycle of one tenant database:
// open with the shared policy, write, checkpoint, verify, back it up online and
// close it.
func Example() {
	// The caller owns the directory layout; Open never creates it.
	dir, err := os.MkdirTemp("", "venue-sqlite-example")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}()

	path := filepath.Join(dir, "tenant-1", "metadata.db")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}

	// DefaultOptions mirrors the Locus SqliteOptions defaults: WAL journaling,
	// NORMAL synchronous mode, a 4000 KiB cache and a 5000 ms busy timeout.
	opts := venuesqlite.DefaultOptions()
	db, err := venuesqlite.Open(path, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// A clean shutdown checkpoints the write-ahead log and releases the
		// file, which is what lets the directory be removed on Windows.
		if err := venuesqlite.TruncateCheckpoint(context.Background(), db); err != nil {
			log.Fatal(err)
		}
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (file_key TEXT PRIMARY KEY NOT NULL)`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO files (file_key) VALUES (?)`, "file-1"); err != nil {
		log.Fatal(err)
	}

	// CheckpointAfterBatch governs this call: the caller decides when a
	// committed batch is followed by a passive checkpoint.
	if err := venuesqlite.Checkpoint(context.Background(), db); err != nil {
		log.Fatal(err)
	}
	if err := venuesqlite.IntegrityCheck(context.Background(), db); err != nil {
		log.Fatal(err)
	}

	backup := filepath.Join(dir, "tenant-1", "metadata.bak")
	if err := venuesqlite.VacuumInto(context.Background(), db, backup); err != nil {
		log.Fatal(err)
	}

	var count int
	if err := db.QueryRow(`SELECT count(*) FROM files`).Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Println("rows:", count)

	// A copy that has already been produced is never overwritten in place.
	if err := venuesqlite.VacuumInto(context.Background(), db, backup); err == nil {
		fmt.Println("unexpected: existing backup was overwritten")
	} else {
		fmt.Println("existing backup refused")
	}

	// Output:
	// rows: 1
	// existing backup refused
}

// ExampleIsCorruptionError shows the boundary that makes quarantine safe: only
// a damaged file is classified as corruption, never a lock or a constraint.
func ExampleIsCorruptionError() {
	damaged := fmt.Errorf("database disk image is malformed (11)")
	locked := fmt.Errorf("database is locked (5) (SQLITE_BUSY)")
	missing := sql.ErrNoRows

	fmt.Println(venuesqlite.IsCorruptionError(damaged))
	fmt.Println(venuesqlite.IsCorruptionError(locked))
	fmt.Println(venuesqlite.IsCorruptionError(missing))

	// Output:
	// true
	// false
	// false
}
