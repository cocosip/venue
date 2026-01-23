package pool

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/metadata"
	"github.com/cocosip/venue/pkg/scheduler"
	"github.com/cocosip/venue/pkg/volume"
)

// TestNewStoragePool tests creating a new storage pool.
func TestNewStoragePool(t *testing.T) {
	t.Run("Valid configuration", func(t *testing.T) {
		tenantMgr := &mockTenantManager{}
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

		opts := &StoragePoolOptions{
			TenantManager:      tenantMgr,
			MetadataRepository: repo,
			FileScheduler:      sched,
			Volumes:            volumes,
		}

		pool, err := NewStoragePool(opts)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if pool == nil {
			t.Fatal("Expected pool to be created")
		}
	})

	t.Run("Nil options", func(t *testing.T) {
		_, err := NewStoragePool(nil)
		if err == nil {
			t.Fatal("Expected error for nil options")
		}
	})

	t.Run("Nil tenant manager", func(t *testing.T) {
		repo, tmpDir := createTestRepository(t)
		defer os.RemoveAll(tmpDir)

		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

		opts := &StoragePoolOptions{
			TenantManager:      nil,
			MetadataRepository: repo,
			FileScheduler:      sched,
			Volumes:            volumes,
		}

		_, err := NewStoragePool(opts)
		if err == nil {
			t.Fatal("Expected error for nil tenant manager")
		}
	})

	t.Run("Nil metadata repository", func(t *testing.T) {
		tenantMgr := &mockTenantManager{}
		volumes := createTestVolumes(t)
		defer cleanupVolumes(volumes)

		opts := &StoragePoolOptions{
			TenantManager:      tenantMgr,
			MetadataRepository: nil,
			FileScheduler:      nil,
			Volumes:            volumes,
		}

		_, err := NewStoragePool(opts)
		if err == nil {
			t.Fatal("Expected error for nil metadata repository")
		}
	})
}

// TestWriteFile tests writing files to the pool.
func TestWriteFile(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	t.Run("Write file successfully", func(t *testing.T) {
		content := bytes.NewReader([]byte("test content"))
		fileName := "test.txt"

		fileKey, err := pool.WriteFile(ctx, tenant, content, &fileName)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if fileKey == "" {
			t.Fatal("Expected file key to be generated")
		}

		// Verify metadata was saved
		metadata, err := repo.Get(ctx, fileKey)
		if err != nil {
			t.Fatalf("Expected metadata to exist, got error: %v", err)
		}

		if metadata.TenantID != tenant.ID {
			t.Errorf("Expected tenant ID %s, got %s", tenant.ID, metadata.TenantID)
		}

		if metadata.Status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", metadata.Status)
		}

		if metadata.FileSize != 12 {
			t.Errorf("Expected file size 12, got %d", metadata.FileSize)
		}

		if metadata.FileExtension != ".txt" {
			t.Errorf("Expected extension .txt, got %s", metadata.FileExtension)
		}
	})

	t.Run("Write file without extension", func(t *testing.T) {
		content := bytes.NewReader([]byte("test"))

		fileKey, err := pool.WriteFile(ctx, tenant, content, nil)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		metadata, _ := repo.Get(ctx, fileKey)
		if metadata.FileExtension != "" {
			t.Errorf("Expected empty extension, got %s", metadata.FileExtension)
		}
	})

	t.Run("Disabled tenant", func(t *testing.T) {
		disabledTenant := core.TenantContext{
			ID:     "disabled-tenant",
			Status: core.TenantStatusDisabled,
		}

		content := bytes.NewReader([]byte("test"))

		_, err := pool.WriteFile(ctx, disabledTenant, content, nil)
		if err != core.ErrTenantDisabled {
			t.Errorf("Expected ErrTenantDisabled, got %v", err)
		}
	})
}

// TestReadFile tests reading files from the pool.
func TestReadFile(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	t.Run("Read existing file", func(t *testing.T) {
		// Write a file first
		testContent := "test content for reading"
		content := bytes.NewReader([]byte(testContent))
		fileName := "read-test.txt"

		fileKey, err := pool.WriteFile(ctx, tenant, content, &fileName)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Read the file
		reader, err := pool.ReadFile(ctx, tenant, fileKey)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		defer reader.Close()

		// Verify content
		readContent, _ := io.ReadAll(reader)
		if string(readContent) != testContent {
			t.Errorf("Expected content '%s', got '%s'", testContent, string(readContent))
		}
	})

	t.Run("Read non-existent file", func(t *testing.T) {
		_, err := pool.ReadFile(ctx, tenant, "non-existent-key")
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})

	t.Run("Empty file key", func(t *testing.T) {
		_, err := pool.ReadFile(ctx, tenant, "")
		if err == nil {
			t.Fatal("Expected error for empty file key")
		}
	})

	t.Run("Wrong tenant", func(t *testing.T) {
		// Write file with one tenant
		content := bytes.NewReader([]byte("test"))
		fileKey, _ := pool.WriteFile(ctx, tenant, content, nil)

		// Try to read with different tenant
		otherTenant := core.TenantContext{
			ID:     "other-tenant",
			Status: core.TenantStatusEnabled,
		}

		_, err := pool.ReadFile(ctx, otherTenant, fileKey)
		if err != core.ErrFileNotFound {
			t.Errorf("Expected ErrFileNotFound, got %v", err)
		}
	})
}

// TestGetFileInfo tests getting file information.
func TestGetFileInfo(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	t.Run("Get file info", func(t *testing.T) {
		// Write a file
		content := bytes.NewReader([]byte("test content"))
		fileName := "info-test.txt"
		fileKey, _ := pool.WriteFile(ctx, tenant, content, &fileName)

		// Get file info
		info, err := pool.GetFileInfo(ctx, tenant, fileKey)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if info.FileKey != fileKey {
			t.Errorf("Expected file key %s, got %s", fileKey, info.FileKey)
		}

		if info.Status != core.FileStatusPending {
			t.Errorf("Expected status Pending, got %v", info.Status)
		}

		if info.FileSize != 12 {
			t.Errorf("Expected file size 12, got %d", info.FileSize)
		}
	})
}

// TestGetFileLocation tests getting file location.
func TestGetFileLocation(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)
	tenant := createTestTenant()

	t.Run("Get file location", func(t *testing.T) {
		// Write a file
		content := bytes.NewReader([]byte("test content"))
		fileName := "location-test.txt"
		fileKey, _ := pool.WriteFile(ctx, tenant, content, &fileName)

		// Get file location
		location, err := pool.GetFileLocation(ctx, tenant, fileKey)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if location.FileKey != fileKey {
			t.Errorf("Expected file key %s, got %s", fileKey, location.FileKey)
		}

		if location.TenantID != tenant.ID {
			t.Errorf("Expected tenant ID %s, got %s", tenant.ID, location.TenantID)
		}

		if location.FileExtension != ".txt" {
			t.Errorf("Expected extension .txt, got %s", location.FileExtension)
		}

		if location.OriginalFileName != fileName {
			t.Errorf("Expected original filename %s, got %s", fileName, location.OriginalFileName)
		}
	})
}

// TestCapacity tests capacity calculation.
func TestCapacity(t *testing.T) {
	ctx := context.Background()

	tenantMgr := &mockTenantManager{}
	repo, tmpDir := createTestRepository(t)
	defer os.RemoveAll(tmpDir)

	volumes := createTestVolumes(t)
	defer cleanupVolumes(volumes)

	sched, _ := scheduler.NewFileScheduler(repo, volumes, nil)

	opts := &StoragePoolOptions{
		TenantManager:      tenantMgr,
		MetadataRepository: repo,
		FileScheduler:      sched,
		Volumes:            volumes,
	}

	pool, _ := NewStoragePool(opts)

	t.Run("Get total capacity", func(t *testing.T) {
		capacity, err := pool.GetTotalCapacity(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if capacity <= 0 {
			t.Errorf("Expected positive capacity, got %d", capacity)
		}
	})

	t.Run("Get available space", func(t *testing.T) {
		space, err := pool.GetAvailableSpace(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if space <= 0 {
			t.Errorf("Expected positive space, got %d", space)
		}
	})
}

// Helper functions and mocks

type mockTenantManager struct{}

func (m *mockTenantManager) GetTenant(ctx context.Context, tenantID string) (core.TenantContext, error) {
	return core.TenantContext{
		ID:        tenantID,
		Status:    core.TenantStatusEnabled,
		CreatedAt: time.Now(),
	}, nil
}

func (m *mockTenantManager) IsTenantEnabled(ctx context.Context, tenantID string) (bool, error) {
	return true, nil
}

func (m *mockTenantManager) CreateTenant(ctx context.Context, tenantID string) error {
	return nil
}

func (m *mockTenantManager) EnableTenant(ctx context.Context, tenantID string) error {
	return nil
}

func (m *mockTenantManager) DisableTenant(ctx context.Context, tenantID string) error {
	return nil
}

func (m *mockTenantManager) GetAllTenants(ctx context.Context) ([]core.TenantContext, error) {
	return []core.TenantContext{}, nil
}

func createTestRepository(t *testing.T) (core.MetadataRepository, string) {
	tmpDir, err := os.MkdirTemp("", "pool-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &metadata.BadgerRepositoryOptions{
		TenantID:       "test-tenant",
		DataPath:       tmpDir,
		CacheTTL:       5 * time.Minute,
		GCInterval:     10 * time.Minute,
		GCDiscardRatio: 0.5,
	}

	repo, err := metadata.NewBadgerMetadataRepository(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create repository: %v", err)
	}

	return repo, tmpDir
}

func createTestVolumes(t *testing.T) map[string]core.StorageVolume {
	tmpDir, err := os.MkdirTemp("", "volume-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &volume.LocalFileSystemVolumeOptions{
		VolumeID:  "test-volume",
		MountPath: tmpDir,
	}

	vol, err := volume.NewLocalFileSystemVolume(opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create volume: %v", err)
	}

	return map[string]core.StorageVolume{
		"test-volume": vol,
	}
}

func cleanupVolumes(volumes map[string]core.StorageVolume) {
	for _, vol := range volumes {
		os.RemoveAll(vol.MountPath())
	}
}

func createTestTenant() core.TenantContext {
	return core.TenantContext{
		ID:        "test-tenant",
		Status:    core.TenantStatusEnabled,
		CreatedAt: time.Now(),
	}
}
