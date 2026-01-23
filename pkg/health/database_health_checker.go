package health

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// DatabaseHealthCheckerOptions configures the database health checker.
type DatabaseHealthCheckerOptions struct {
	// MetadataDataPath is the root path for metadata databases.
	MetadataDataPath string

	// DirectoryQuotaDataPath is the path for directory quota database.
	DirectoryQuotaDataPath string

	// VolumePaths are the storage volume paths to check for orphaned files.
	VolumePaths []string
}

// databaseHealthChecker implements DatabaseHealthChecker interface.
type databaseHealthChecker struct {
	metadataDataPath       string
	directoryQuotaDataPath string
	volumePaths            []string
}

// NewDatabaseHealthChecker creates a new database health checker.
func NewDatabaseHealthChecker(opts *DatabaseHealthCheckerOptions) (core.DatabaseHealthChecker, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil: %w", core.ErrInvalidArgument)
	}

	if opts.MetadataDataPath == "" {
		return nil, fmt.Errorf("metadata data path cannot be empty: %w", core.ErrInvalidArgument)
	}

	return &databaseHealthChecker{
		metadataDataPath:       opts.MetadataDataPath,
		directoryQuotaDataPath: opts.DirectoryQuotaDataPath,
		volumePaths:            opts.VolumePaths,
	}, nil
}

// CheckAllDatabases checks the health of all databases.
func (c *databaseHealthChecker) CheckAllDatabases(ctx context.Context) (*core.DatabaseHealthReport, error) {
	report := &core.DatabaseHealthReport{
		CorruptedDatabases: make([]*core.DatabaseHealthStatus, 0),
		OrphanedTenants:    make([]string, 0),
		AllHealthy:         true,
	}

	// Check metadata databases
	if err := c.checkMetadataDatabases(ctx, report); err != nil {
		slog.Warn("Error checking metadata databases", "error", err)
	}

	// Check directory quota database
	if c.directoryQuotaDataPath != "" {
		status := c.checkDirectoryQuotaDatabaseInternal(ctx)
		if status.IsHealthy {
			report.HealthyDatabases++
		} else {
			report.CorruptedDatabases = append(report.CorruptedDatabases, status)
			report.AllHealthy = false
		}
	}

	// Detect orphaned files if no databases exist
	if report.HealthyDatabases == 0 && len(report.CorruptedDatabases) == 0 {
		orphaned, err := c.DetectOrphanedFiles(ctx)
		if err != nil {
			slog.Warn("Error detecting orphaned files", "error", err)
		} else {
			report.OrphanedTenants = orphaned
		}
	}

	return report, nil
}

// checkMetadataDatabases checks all metadata databases.
func (c *databaseHealthChecker) checkMetadataDatabases(ctx context.Context, report *core.DatabaseHealthReport) error {
	// Check if metadata directory exists
	if _, err := os.Stat(c.metadataDataPath); os.IsNotExist(err) {
		return nil // No databases yet, this is normal for first startup
	}

	// List all tenant directories
	entries, err := os.ReadDir(c.metadataDataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		tenantID := entry.Name()
		status, err := c.CheckMetadataDatabase(ctx, tenantID)
		if err != nil {
			slog.Warn("Error checking metadata database", "tenant", tenantID, "error", err)
			continue
		}

		if status.IsHealthy {
			report.HealthyDatabases++
		} else {
			report.CorruptedDatabases = append(report.CorruptedDatabases, status)
			report.AllHealthy = false
		}
	}

	return nil
}

// CheckMetadataDatabase checks a specific metadata database.
func (c *databaseHealthChecker) CheckMetadataDatabase(ctx context.Context, tenantID string) (*core.DatabaseHealthStatus, error) {
	dbPath := filepath.Join(c.metadataDataPath, tenantID)

	status := &core.DatabaseHealthStatus{
		DatabaseType: core.DatabaseTypeMetadata,
		TenantID:     tenantID,
		DatabasePath: dbPath,
		IsHealthy:    false,
	}

	// Check if database directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		status.Error = "database directory does not exist"
		return status, nil
	}

	// Try to open the database
	opts := badger.DefaultOptions(dbPath).WithReadOnly(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		status.Error = fmt.Sprintf("failed to open database: %v", err)
		return status, nil
	}
	defer db.Close()

	// Try a simple read operation
	err = db.View(func(txn *badger.Txn) error {
		// Just iterate to check if database is readable
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Read first key to verify database is accessible
		it.Rewind()
		return nil
	})

	if err != nil {
		status.Error = fmt.Sprintf("database read test failed: %v", err)
		return status, nil
	}

	// Database is healthy
	status.IsHealthy = true
	return status, nil
}

// CheckDirectoryQuotaDatabase checks the directory quota database.
func (c *databaseHealthChecker) CheckDirectoryQuotaDatabase(ctx context.Context) (*core.DatabaseHealthStatus, error) {
	return c.checkDirectoryQuotaDatabaseInternal(ctx), nil
}

// checkDirectoryQuotaDatabaseInternal checks the directory quota database (internal).
func (c *databaseHealthChecker) checkDirectoryQuotaDatabaseInternal(ctx context.Context) *core.DatabaseHealthStatus {
	status := &core.DatabaseHealthStatus{
		DatabaseType: core.DatabaseTypeDirectoryQuota,
		TenantID:     "",
		DatabasePath: c.directoryQuotaDataPath,
		IsHealthy:    false,
	}

	if c.directoryQuotaDataPath == "" {
		status.Error = "directory quota database path not configured"
		return status
	}

	// Check if database directory exists
	if _, err := os.Stat(c.directoryQuotaDataPath); os.IsNotExist(err) {
		status.Error = "database directory does not exist"
		return status
	}

	// Try to open the database
	opts := badger.DefaultOptions(c.directoryQuotaDataPath).WithReadOnly(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		status.Error = fmt.Sprintf("failed to open database: %v", err)
		return status
	}
	defer db.Close()

	// Try a simple read operation
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		return nil
	})

	if err != nil {
		status.Error = fmt.Sprintf("database read test failed: %v", err)
		return status
	}

	// Database is healthy
	status.IsHealthy = true
	return status
}

// DetectOrphanedFiles detects tenants with physical files but no metadata.
func (c *databaseHealthChecker) DetectOrphanedFiles(ctx context.Context) ([]string, error) {
	orphanedTenants := make([]string, 0)

	if len(c.volumePaths) == 0 {
		return orphanedTenants, nil
	}

	// Get existing tenant IDs from metadata databases
	existingTenants := make(map[string]bool)

	if _, err := os.Stat(c.metadataDataPath); !os.IsNotExist(err) {
		entries, err := os.ReadDir(c.metadataDataPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata directory: %w", err)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				existingTenants[entry.Name()] = true
			}
		}
	}

	// Check each volume for tenant directories with files
	for _, volumePath := range c.volumePaths {
		if _, err := os.Stat(volumePath); os.IsNotExist(err) {
			continue
		}

		entries, err := os.ReadDir(volumePath)
		if err != nil {
			slog.Warn("Failed to read volume directory", "path", volumePath, "error", err)
			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			tenantID := entry.Name()
			tenantPath := filepath.Join(volumePath, tenantID)

			// Check if this tenant directory has any files
			if hasFiles(tenantPath) {
				// Check if metadata database exists
				if !existingTenants[tenantID] {
					orphanedTenants = append(orphanedTenants, tenantID)
				}
			}
		}
	}

	return orphanedTenants, nil
}

// hasFiles checks if a directory has any files (recursively).
func hasFiles(dirPath string) bool {
	hasAnyFile := false

	_ = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if !info.IsDir() {
			hasAnyFile = true
			return filepath.SkipDir // Stop walking
		}

		return nil
	})

	return hasAnyFile
}

// GetDatabaseSize returns the size of a database directory in bytes.
func GetDatabaseSize(dbPath string) (int64, error) {
	var size int64

	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if !info.IsDir() {
			size += info.Size()
		}

		return nil
	})

	return size, err
}

// IsDatabaseCorrupted checks if a database directory appears corrupted.
// This is a fast check that looks for common corruption indicators.
func IsDatabaseCorrupted(dbPath string) bool {
	// Check if directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return true
	}

	// Try to open database in read-only mode
	opts := badger.DefaultOptions(dbPath).WithReadOnly(true).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		// Cannot open = corrupted
		return true
	}
	defer db.Close()

	// Try a simple operation
	err = db.View(func(txn *badger.Txn) error {
		return nil
	})

	return err != nil
}

// GetTenantIDsFromMetadata returns all tenant IDs that have metadata databases.
func GetTenantIDsFromMetadata(metadataDataPath string) ([]string, error) {
	tenantIDs := make([]string, 0)

	if _, err := os.Stat(metadataDataPath); os.IsNotExist(err) {
		return tenantIDs, nil
	}

	entries, err := os.ReadDir(metadataDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Exclude system directories
			name := entry.Name()
			if !strings.HasPrefix(name, ".") && !strings.HasPrefix(name, "_") {
				tenantIDs = append(tenantIDs, name)
			}
		}
	}

	return tenantIDs, nil
}
