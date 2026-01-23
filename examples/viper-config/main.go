package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cocosip/venue"
	"github.com/cocosip/venue/config"
	"github.com/spf13/viper"
)

func main() {
	// Clean up old data
	dataDir := "./venue-data"
	_ = os.RemoveAll(dataDir)
	defer func() { _ = os.RemoveAll(dataDir) }()

	fmt.Println("=== Venue with Viper Configuration Example ===")
	fmt.Println()

	// Method 1: Load from configuration file (YAML)
	fmt.Println("Method 1: Load from YAML file")
	cfg1, err := config.LoadFromFileWithViper("venue-config.yaml")
	if err != nil {
		fmt.Println("   Config file not found, using defaults")
		cfg1 = config.DefaultConfig()
	} else {
		fmt.Println("   ✓ Loaded from config file successfully")
	}
	fmt.Println()

	// Method 2: Load from JSON file
	fmt.Println("Method 2: Load from JSON file")
	cfg2, err := config.LoadFromFileWithViper("venue-config.json")
	if err != nil {
		fmt.Println("   Config file not found, using defaults")
		cfg2 = config.DefaultConfig()
	} else {
		fmt.Println("   ✓ Loaded from JSON config file successfully")
	}
	fmt.Println()

	// Method 3: Manual configuration using Viper
	fmt.Println("Method 3: Manual Viper configuration")
	v := config.NewViperWithDefaults()

	// Override some settings
	v.Set("metadataDirectory", dataDir+"/metadata")
	v.Set("quotaDirectory", dataDir+"/quota")
	v.Set("autoCreateTenants", true)
	v.Set("enableBackgroundCleanup", true)
	v.Set("enableDatabaseHealthCheck", true)

	// Set volumes
	v.Set("volumes", []map[string]interface{}{
		{
			"volumeId":      "default-volume",
			"mountPath":     dataDir + "/storage/default",
			"volumeType":    "LocalFileSystem",
			"shardingDepth": 2,
		},
	})

	// Set tenants
	v.Set("tenants", []map[string]interface{}{
		{
			"tenantId": "demo-tenant",
			"enabled":  true,
			"quota":    nil,
		},
	})

	// Bind environment variables (optional)
	// Example: VENUE_METADATADIRECTORY will override metadataDirectory
	v.SetEnvPrefix("VENUE")
	v.AutomaticEnv()

	_, err = config.LoadWithViper(v)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fmt.Println("   ✓ Viper configuration loaded successfully")
	fmt.Println()

	// Method 4: Load from embedded config section
	fmt.Println("Method 4: Load from embedded config section")
	appViper := viper.New()
	appViper.SetConfigType("yaml")

	// Simulate a larger application config file
	appConfig := `
app:
  name: MyApp
  port: 8080

venue:
  metadataDirectory: ` + dataDir + `/metadata
  quotaDirectory: ` + dataDir + `/quota
  autoCreateTenants: true
  enableDatabaseHealthCheck: true
  enableBackgroundCleanup: true
  retryPolicy:
    maxRetryCount: 3
    initialRetryDelay: 5s
    useExponentialBackoff: true
    maxRetryDelay: 5m
  volumes:
    - volumeId: default-volume
      mountPath: ` + dataDir + `/storage/default
      volumeType: LocalFileSystem
      shardingDepth: 2
  tenants:
    - tenantId: demo-tenant
      enabled: true
      quota: null
  cleanupOptions:
    cleanupInterval: 1h
    initialDelay: 1m
    cleanupEmptyDirectories: true
    cleanupTimedOutFiles: true
    processingTimeout: 30m
    cleanupPermanentlyFailedFiles: true
    failedFileRetentionPeriod: 168h
    cleanupCompletedRecords: false
    completedRecordRetentionPeriod: 720h
    optimizeDatabases: true
    databaseOptimizationInterval: 24h
  databaseHealthCheckOptions:
    initialDelay: 2s
    maxRetries: 3
    retryDelay: 1s
    checkOnStartupOnly: true
    periodicCheckInterval: 1h
`

	_ = appViper.ReadConfig(strings.NewReader(appConfig))

	cfg4, err := config.LoadVenueSectionWithViper(appViper, "venue")
	if err != nil {
		log.Fatalf("Failed to load venue section: %v", err)
	}
	fmt.Println("   ✓ Loaded from embedded config section successfully")
	fmt.Println()

	// Create Venue instance using the embedded config
	fmt.Println("Creating Venue instance")
	venueInstance, err := venue.NewVenue(&venue.VenueOptions{
		Config: cfg4,
	})
	if err != nil {
		log.Fatalf("Failed to create venue: %v", err)
	}

	// Start services
	if err := venueInstance.Start(); err != nil {
		log.Fatalf("Failed to start venue: %v", err)
	}
	defer func() { _ = venueInstance.Stop() }()

	fmt.Println("✓ Venue services started")
	fmt.Println()

	// Use the service
	fmt.Println("Using Venue services")
	ctx := context.Background()
	storagePool := venueInstance.StoragePool()
	tenantManager := venueInstance.TenantManager()

	tenantCtx, err := tenantManager.GetTenant(ctx, "demo-tenant")
	if err != nil {
		log.Fatalf("Failed to get tenant: %v", err)
	}

	// Write test file
	fileName := "viper-test.txt"
	fileContent := strings.NewReader("Hello from Viper configuration!")
	fileKey, err := storagePool.WriteFile(ctx, tenantCtx, fileContent, &fileName)
	if err != nil {
		log.Fatalf("Failed to write file: %v", err)
	}

	fmt.Printf("   ✓ File written, FileKey: %s\n", fileKey)
	fmt.Println()

	// Print configuration info
	fmt.Println("Configuration info:")
	fmt.Printf("   - MetadataDirectory: %s\n", cfg4.MetadataDirectory)
	fmt.Printf("   - QuotaDirectory: %s\n", cfg4.QuotaDirectory)
	fmt.Printf("   - AutoCreateTenants: %v\n", cfg4.AutoCreateTenants)
	fmt.Printf("   - Tenants: %d configured\n", len(cfg4.Tenants))
	fmt.Printf("   - Volumes: %d configured\n", len(cfg4.Volumes))
	fmt.Printf("   - FileWatchers: %d configured\n", len(cfg4.FileWatchers))
	fmt.Printf("   - EnableBackgroundCleanup: %v\n", cfg4.EnableBackgroundCleanup)
	fmt.Printf("   - EnableDatabaseHealthCheck: %v\n", cfg4.EnableDatabaseHealthCheck)
	fmt.Println()

	fmt.Println("✨ Viper configuration example completed!")
	fmt.Println()
	fmt.Println("Advantages:")
	fmt.Println("  ✓ Multiple config formats (JSON, YAML, TOML, HCL, etc.)")
	fmt.Println("  ✓ Environment variable overrides")
	fmt.Println("  ✓ Configuration hot-reload support")
	fmt.Println("  ✓ Embedded config section support")
	fmt.Println("  ✓ Missing config nodes use defaults automatically")
	fmt.Println("  ✓ Matches Locus configuration structure")

	// Save config examples
	fmt.Println()
	fmt.Println("Saving configuration to files:")
	if err := cfg1.SaveToFile("venue-saved.json"); err == nil {
		fmt.Println("   ✓ Saved to venue-saved.json")
	}
	if err := cfg2.SaveToFile("venue-saved.yaml"); err == nil {
		fmt.Println("   ✓ Saved to venue-saved.yaml")
	}
}
