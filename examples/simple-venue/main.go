package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
)

func main() {
	// Clean up old data
	dataDir := "./venue-data"
	_ = os.RemoveAll(dataDir)
	defer func() { _ = os.RemoveAll(dataDir) }()

	fmt.Println("=== Simple Venue Usage Example ===")
	fmt.Println()

	// Method 1: Use default configuration
	fmt.Println("1. Creating Venue with default configuration")
	cfg := config.DefaultConfig()

	// Override some settings
	cfg.MetadataDirectory = dataDir + "/metadata"
	cfg.QuotaDirectory = dataDir + "/quotas"
	cfg.Volumes[0].MountPath = dataDir + "/storage/default"
	cfg.EnableBackgroundCleanup = true
	cfg.EnableDatabaseHealthCheck = true

	// Add a tenant configuration
	cfg.Tenants = []config.TenantConfig{
		{
			TenantId: "demo-tenant",
			Enabled:  true,
			Quota:    nil, // unlimited
		},
	}

	// Create Venue instance
	v, err := venue.NewVenue(&venue.VenueOptions{
		Config: cfg,
	})
	if err != nil {
		log.Fatalf("Failed to create venue: %v", err)
	}

	// Start Venue services
	if err := v.Start(); err != nil {
		log.Fatalf("Failed to start venue: %v", err)
	}
	defer func() { _ = v.Stop() }()

	fmt.Println("✓ Venue services started")
	fmt.Println()

	// 2. Get component instances from Venue (dependency injection pattern)
	fmt.Println("2. Getting component instances")
	storagePool := v.StoragePool()
	tenantManager := v.TenantManager()
	fileScheduler := v.FileScheduler()

	fmt.Printf("   - StoragePool: %T\n", storagePool)
	fmt.Printf("   - TenantManager: %T\n", tenantManager)
	fmt.Printf("   - FileScheduler: %T\n", fileScheduler)
	fmt.Println()

	// 3. Write file to storage pool
	fmt.Println("3. Writing file to storage pool")
	ctx := context.Background()
	tenantCtx, err := tenantManager.GetTenant(ctx, "demo-tenant")
	if err != nil {
		log.Fatalf("Failed to get tenant: %v", err)
	}

	fileName := "test-file.txt"
	content := strings.NewReader("Hello, Venue! This is a test file.")

	fileKey, err := storagePool.WriteFile(ctx, tenantCtx, content, &fileName)
	if err != nil {
		log.Fatalf("Failed to write file: %v", err)
	}

	fmt.Printf("   ✓ File written, FileKey: %s\n", fileKey)
	fmt.Println()

	// 4. Read file
	fmt.Println("4. Reading file from storage pool")
	reader, err := storagePool.ReadFile(ctx, tenantCtx, fileKey)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	defer func() { _ = reader.Close() }()

	fmt.Println("   ✓ File read successfully")
	fmt.Println()

	// 5. Get file status
	fmt.Println("5. Getting file processing status")
	status, err := storagePool.GetFileStatus(ctx, fileKey)
	if err != nil {
		log.Fatalf("Failed to get file status: %v", err)
	}

	fmt.Printf("   - File status: %v\n", status)
	fmt.Println()

	// 6. Get system capacity
	fmt.Println("6. Getting system capacity information")
	totalCapacity, _ := storagePool.GetTotalCapacity(ctx)
	availableSpace, _ := storagePool.GetAvailableSpace(ctx)

	fmt.Printf("   - Total capacity: %d GB\n", totalCapacity/(1024*1024*1024))
	fmt.Printf("   - Available space: %d GB\n", availableSpace/(1024*1024*1024))
	fmt.Println()

	// 7. Print configuration info
	fmt.Println("7. Configuration information")
	fmt.Printf("   - MetadataDirectory: %s\n", cfg.MetadataDirectory)
	fmt.Printf("   - QuotaDirectory: %s\n", cfg.QuotaDirectory)
	fmt.Printf("   - AutoCreateTenants: %v\n", cfg.AutoCreateTenants)
	fmt.Printf("   - Tenants: %d configured\n", len(cfg.Tenants))
	fmt.Printf("   - Volumes: %d configured\n", len(cfg.Volumes))
	fmt.Printf("   - EnableBackgroundCleanup: %v\n", cfg.EnableBackgroundCleanup)
	fmt.Printf("   - EnableDatabaseHealthCheck: %v\n", cfg.EnableDatabaseHealthCheck)
	fmt.Println()

	fmt.Println("✨ Example completed!")
	fmt.Println()
	fmt.Println("This example demonstrates:")
	fmt.Println("  1. Creating Venue instance with configuration (Locus-style)")
	fmt.Println("  2. Starting/stopping Venue services")
	fmt.Println("  3. Getting component instances from Venue (like dependency injection)")
	fmt.Println("  4. Performing file operations using StoragePool")
	fmt.Println("  5. Configuration follows Locus structure with Tenants, Volumes, etc.")
}
