package tenant

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// MetadataStore handles persistent storage of tenant metadata using JSON files.
type MetadataStore struct {
	rootPath string
	mu       sync.RWMutex
}

// NewMetadataStore creates a new metadata store.
func NewMetadataStore(rootPath string) (*MetadataStore, error) {
	// Ensure root directory exists
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	return &MetadataStore{
		rootPath: rootPath,
	}, nil
}

// Save saves tenant metadata to disk.
func (s *MetadataStore) Save(metadata *core.TenantMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.getFilePath(metadata.TenantID)

	// Marshal to JSON
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal tenant metadata: %w", err)
	}

	// Write to temporary file first
	tmpPath := filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write tenant metadata: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath) // Cleanup
		return fmt.Errorf("failed to rename tenant metadata: %w", err)
	}

	return nil
}

// Load loads tenant metadata from disk.
func (s *MetadataStore) Load(tenantID string) (*core.TenantMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := s.getFilePath(tenantID)

	// Check if file exists
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return nil, core.ErrTenantNotFound
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read tenant metadata: %w", err)
	}

	// Unmarshal JSON
	var metadata core.TenantMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tenant metadata: %w", err)
	}

	return &metadata, nil
}

// Delete deletes tenant metadata from disk.
func (s *MetadataStore) Delete(tenantID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filePath := s.getFilePath(tenantID)

	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to delete tenant metadata: %w", err)
	}

	return nil
}

// Exists checks if tenant metadata exists.
func (s *MetadataStore) Exists(tenantID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := s.getFilePath(tenantID)
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// ListAll returns all tenant IDs.
func (s *MetadataStore) ListAll() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata directory: %w", err)
	}

	var tenantIDs []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			// Remove .json extension to get tenant ID
			tenantID := entry.Name()[:len(entry.Name())-5]
			tenantIDs = append(tenantIDs, tenantID)
		}
	}

	return tenantIDs, nil
}

// getFilePath returns the file path for a tenant's metadata.
func (s *MetadataStore) getFilePath(tenantID string) string {
	return filepath.Join(s.rootPath, tenantID+".json")
}

// createDefaultMetadata creates default tenant metadata.
func createDefaultMetadata(tenantID string, storagePath string) *core.TenantMetadata {
	now := time.Now()
	return &core.TenantMetadata{
		TenantID:    tenantID,
		Status:      core.TenantStatusEnabled,
		StoragePath: storagePath,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
