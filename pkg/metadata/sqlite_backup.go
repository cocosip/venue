package metadata

import (
	"archive/zip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
)

// BackupTenant writes a consistent snapshot of one tenant's metadata to
// destPath as a complete, openable SQLite database file.
//
// destPath must not exist; SQLite refuses to overwrite a file and this method
// rejects the case up front with a wrapped core.ErrInvalidArgument. Its parent
// directory must already exist. When verification is enabled (the default) the
// produced file is checked with PRAGMA integrity_check(1) through a read-only
// connection and is deleted again when the check fails, so a caller never keeps
// an unusable backup.
//
// Errors wrap core.ErrDatabaseError when the tenant has no database.
func (r *SQLiteMetadataRepository) BackupTenant(ctx context.Context, tenantID string, destPath string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", core.ErrInvalidArgument)
	}
	if err := core.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("invalid tenant ID: %w", err)
	}
	if strings.TrimSpace(destPath) == "" {
		return fmt.Errorf("backup destination cannot be empty: %w", core.ErrInvalidArgument)
	}

	handle, err := r.begin(ctx, tenantID)
	if err != nil {
		return err
	}
	defer r.end(handle)

	if !r.hasDatabase(handle) {
		return fmt.Errorf("tenant metadata database is not open: %w", core.ErrDatabaseError)
	}

	if err := r.backupTenantInto(ctx, handle.path, destPath); err != nil {
		return err
	}
	if r.skipBackupVerify {
		return nil
	}
	if err := verifyBackupFile(ctx, destPath); err != nil {
		_ = os.Remove(destPath)
		return err
	}
	return nil
}

// KnownTenantIDs returns the tenant identifiers this repository can back up, in
// stable sorted order.
//
// The set is the open handles merged with a cached snapshot of the tenant
// directories below DataPath. Enumeration reads directory names only and never
// opens a database file, so it stays cheap for a whole-repository backup.
func (r *SQLiteMetadataRepository) KnownTenantIDs(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errRepositoryClosed()
	}
	return r.knownTenantIDs(ctx)
}

// hasDatabase reports whether a handle has an open connection.
func (r *SQLiteMetadataRepository) hasDatabase(handle *sqliteTenantDatabase) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return handle != nil && handle.db != nil
}

// backupTenantInto writes one tenant's database to destPath with VACUUM INTO.
//
// VACUUM INTO reads the source inside a read transaction and rewrites it into a
// new complete file, so the tenant keeps serving reads and writes while the
// backup is taken. The translation runs on a separate read-only connection, so
// the tenant's own single connection is never occupied by the copy.
func (r *SQLiteMetadataRepository) backupTenantInto(ctx context.Context, sourcePath string, destPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch _, err := os.Lstat(destPath); {
	case err == nil:
		return fmt.Errorf("backup target already exists: %w", core.ErrInvalidArgument)
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("failed to inspect the backup target: %w", err)
	}

	db, err := openReadOnlyDatabase(sourcePath, r.sqlite.BusyTimeoutMs)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if err := sqlite.VacuumInto(ctx, db, destPath); err != nil {
		_ = os.Remove(destPath)
		return err
	}
	return nil
}

// verifyBackupFile runs PRAGMA integrity_check(1) on a produced backup through a
// read-only connection.
//
// A healthy database answers with exactly one "ok" row; anything else, including
// a multi-row answer, is a failed verification. The artifact is opened read-only
// on purpose: verification must not change the bytes that are about to be
// archived.
func verifyBackupFile(ctx context.Context, path string) error {
	db, err := openReadOnlyDatabase(path, 0)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return sqlite.IntegrityCheck(ctx, db)
}

// openReadOnlyDatabase opens path with the driver's read-only mode.
//
// The shared foundation always opens read-write-create, because that is what a
// live tenant database needs; verification and online backup need the opposite,
// so the read-only DSN is built here. It deliberately adds no shared-cache
// pragma: a read-only connection that joined a process-wide shared cache would
// make concurrent writers see the read-only flag and fail with SQLITE_READONLY.
func openReadOnlyDatabase(path string, busyTimeoutMs int) (*sql.DB, error) {
	name := filepath.ToSlash(path)
	if isWindowsDrivePath(name) {
		name = "/" + name
	}
	builder := &strings.Builder{}
	builder.WriteString("file:")
	builder.WriteString(name)
	builder.WriteString("?mode=ro")
	fmt.Fprintf(builder, "&_busy_timeout=%d", busyTimeoutMs)

	db, err := sql.Open(sqlite.DriverName, builder.String())
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata database read-only: %w: %w", err, core.ErrDatabaseError)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// isWindowsDrivePath reports whether name starts with a drive designator such as
// "C:/". A file: URI without a leading slash would otherwise read that drive as
// a URI scheme or as a drive-relative path.
func isWindowsDrivePath(name string) bool {
	if len(name) < 2 || name[1] != ':' {
		return false
	}
	character := name[0]
	return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
}

// writeZipBackup writes one zip entry per tenant into w.
//
// The entry name is "{tenantID}/metadata.db", which maps one-to-one onto the
// per-tenant database layout, and each entry body is a complete SQLite backup
// file produced with VACUUM INTO. Entries are deflated: the container is a
// long-lived archive, and a SQLite file compresses well, so the storage saving
// outweighs the copy-side CPU cost.
func (r *SQLiteMetadataRepository) writeZipBackup(ctx context.Context, w io.Writer, tenantIDs []string) error {
	archive := zip.NewWriter(w)
	for _, tenantID := range tenantIDs {
		if err := ctx.Err(); err != nil {
			_ = archive.Close()
			return err
		}
		if err := r.writeTenantZipEntry(ctx, archive, tenantID); err != nil {
			_ = archive.Close()
			return err
		}
	}
	if err := archive.Close(); err != nil {
		return fmt.Errorf("failed to finish the metadata backup: %w", err)
	}
	return nil
}

// writeTenantZipEntry copies one tenant's database into the archive.
func (r *SQLiteMetadataRepository) writeTenantZipEntry(ctx context.Context, archive *zip.Writer, tenantID string) error {
	sourcePath := filepath.Join(r.dataPath, tenantID, metadataDatabaseFileName)
	if _, err := os.Stat(sourcePath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A tenant directory without a database has nothing to back up.
			return nil
		}
		return fmt.Errorf("failed to inspect the tenant metadata database: %w: %w", err, core.ErrDatabaseError)
	}

	entry, err := archive.CreateHeader(&zip.FileHeader{
		Name:   tenantID + "/" + metadataDatabaseFileName,
		Method: zip.Deflate,
	})
	if err != nil {
		return fmt.Errorf("failed to create the backup entry: %w", err)
	}

	// VACUUM INTO needs a real file, so the consistent copy is produced into a
	// staging file first and streamed into the archive afterwards.
	staging, err := os.CreateTemp(r.dataPath, "metadata-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create the backup staging file: %w: %w", err, core.ErrDatabaseError)
	}
	stagingPath := staging.Name()
	if err := staging.Close(); err != nil {
		_ = os.Remove(stagingPath)
		return fmt.Errorf("failed to close the backup staging file: %w: %w", err, core.ErrDatabaseError)
	}
	if err := os.Remove(stagingPath); err != nil {
		return fmt.Errorf("failed to prepare the backup staging path: %w: %w", err, core.ErrDatabaseError)
	}
	defer func() { _ = os.Remove(stagingPath) }()

	if err := r.backupTenantInto(ctx, sourcePath, stagingPath); err != nil {
		return err
	}
	if !r.skipBackupVerify {
		if err := verifyBackupFile(ctx, stagingPath); err != nil {
			return err
		}
	}

	file, err := os.Open(stagingPath)
	if err != nil {
		return fmt.Errorf("failed to open the produced backup: %w: %w", err, core.ErrDatabaseError)
	}
	defer func() { _ = file.Close() }()

	if _, err := io.Copy(entry, file); err != nil {
		return fmt.Errorf("failed to write the backup entry: %w", err)
	}
	return nil
}
