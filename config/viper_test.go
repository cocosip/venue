package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestLoadFromFileWithViperNode(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "viper-node-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("Load YAML from venue node", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "viper-venue-node.yaml")

		// Create YAML config with venue node
		yamlContent := `venue:
  metadataDirectory: ./viper-metadata
  quotaDirectory: ./viper-quota
  autoCreateTenants: true
  defaultTenantQuota: 6000000
  enableDatabaseHealthCheck: true
  retryPolicy:
    maxRetryCount: 3
    initialRetryDelay: 5s
    useExponentialBackoff: true
    maxRetryDelay: 5m
  tenantManagerOptions:
    metadataPath: ""
    cacheTTL: 5m
  metadataOptions:
    cacheTTL: 5m
  badgerDBOptions:
    gcInterval: 10m
    gcDiscardRatio: 0.5
  volumes:
    - volumeId: viper-vol
      mountPath: ./viper-storage
      volumeType: LocalFileSystem
      shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config (should use venue node by default)
		config, err := LoadFromFileWithViper(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./viper-metadata" {
			t.Errorf("Expected MetadataDirectory './viper-metadata', got %s", config.MetadataDirectory)
		}
		if config.QuotaDirectory != "./viper-quota" {
			t.Errorf("Expected QuotaDirectory './viper-quota', got %s", config.QuotaDirectory)
		}
		if config.DefaultTenantQuota != 6000000 {
			t.Errorf("Expected DefaultTenantQuota 6000000, got %d", config.DefaultTenantQuota)
		}
	})

	t.Run("Load YAML from root level (backward compatibility)", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "viper-root.yaml")

		// Create YAML config at root level (no venue node)
		yamlContent := `metadataDirectory: ./viper-root-metadata
quotaDirectory: ./viper-root-quota
autoCreateTenants: false
defaultTenantQuota: 0
enableDatabaseHealthCheck: true
retryPolicy:
  maxRetryCount: 5
  initialRetryDelay: 3s
  useExponentialBackoff: true
  maxRetryDelay: 3m
tenantManagerOptions:
  metadataPath: ""
  cacheTTL: 5m
metadataOptions:
  cacheTTL: 5m
badgerDBOptions:
  gcInterval: 10m
  gcDiscardRatio: 0.5
volumes:
  - volumeId: viper-root-vol
    mountPath: ./viper-root-storage
    volumeType: LocalFileSystem
    shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config (should fallback to root level)
		config, err := LoadFromFileWithViper(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./viper-root-metadata" {
			t.Errorf("Expected MetadataDirectory './viper-root-metadata', got %s", config.MetadataDirectory)
		}
		if config.AutoCreateTenants {
			t.Error("Expected AutoCreateTenants to be false")
		}
	})

	t.Run("Load with custom node name", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "viper-custom-node.yaml")

		// Create YAML config with custom node name
		yamlContent := `storage:
  metadataDirectory: ./custom-metadata
  quotaDirectory: ./custom-quota
  autoCreateTenants: true
  defaultTenantQuota: 0
  enableDatabaseHealthCheck: true
  retryPolicy:
    maxRetryCount: 3
    initialRetryDelay: 5s
    useExponentialBackoff: true
    maxRetryDelay: 5m
  tenantManagerOptions:
    metadataPath: ""
    cacheTTL: 5m
  metadataOptions:
    cacheTTL: 5m
  badgerDBOptions:
    gcInterval: 10m
    gcDiscardRatio: 0.5
  volumes:
    - volumeId: custom-vol
      mountPath: ./custom-storage
      volumeType: LocalFileSystem
      shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config with custom node name
		config, err := LoadFromFileWithViperNode(configPath, "storage")
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./custom-metadata" {
			t.Errorf("Expected MetadataDirectory './custom-metadata', got %s", config.MetadataDirectory)
		}
	})

	t.Run("Load from root level with empty node name", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "viper-empty-node.yaml")

		// Create YAML config at root level
		yamlContent := `metadataDirectory: ./empty-node-metadata
quotaDirectory: ./empty-node-quota
autoCreateTenants: true
defaultTenantQuota: 0
enableDatabaseHealthCheck: true
retryPolicy:
  maxRetryCount: 3
  initialRetryDelay: 5s
  useExponentialBackoff: true
  maxRetryDelay: 5m
tenantManagerOptions:
  metadataPath: ""
  cacheTTL: 5m
metadataOptions:
  cacheTTL: 5m
badgerDBOptions:
  gcInterval: 10m
  gcDiscardRatio: 0.5
volumes:
  - volumeId: empty-vol
    mountPath: ./empty-storage
    volumeType: LocalFileSystem
    shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config with empty node name (should load from root)
		config, err := LoadFromFileWithViperNode(configPath, "")
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./empty-node-metadata" {
			t.Errorf("Expected MetadataDirectory './empty-node-metadata', got %s", config.MetadataDirectory)
		}
	})
}

func TestLoadVenueSectionWithViper(t *testing.T) {
	t.Run("Load venue section from application config", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")

		// Simulate a larger application config with venue section
		appConfig := `
app:
  name: TestApp
  port: 8080

venue:
  metadataDirectory: ./app-metadata
  quotaDirectory: ./app-quota
  autoCreateTenants: true
  defaultTenantQuota: 7000000
  enableDatabaseHealthCheck: true
  retryPolicy:
    maxRetryCount: 3
    initialRetryDelay: 5s
    useExponentialBackoff: true
    maxRetryDelay: 5m
  tenantManagerOptions:
    metadataPath: ""
    cacheTTL: 5m
  metadataOptions:
    cacheTTL: 5m
  badgerDBOptions:
    gcInterval: 10m
    gcDiscardRatio: 0.5
  volumes:
    - volumeId: app-vol
      mountPath: ./app-storage
      volumeType: LocalFileSystem
      shardingDepth: 2
`
		v.ReadConfig(strings.NewReader(appConfig))

		// Load venue section
		config, err := LoadVenueSectionWithViper(v, "venue")
		if err != nil {
			t.Fatalf("Failed to load venue section: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./app-metadata" {
			t.Errorf("Expected MetadataDirectory './app-metadata', got %s", config.MetadataDirectory)
		}
		if config.DefaultTenantQuota != 7000000 {
			t.Errorf("Expected DefaultTenantQuota 7000000, got %d", config.DefaultTenantQuota)
		}
	})

	t.Run("Section not found", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")

		appConfig := `
app:
  name: TestApp
  port: 8080
`
		v.ReadConfig(strings.NewReader(appConfig))

		// Try to load non-existent section
		_, err := LoadVenueSectionWithViper(v, "venue")
		if err == nil {
			t.Error("Expected error for non-existent section")
		}
	})
}

func TestNewViperWithDefaults(t *testing.T) {
	v := NewViperWithDefaults()

	// Verify some default values
	if v.GetString("metadataDirectory") == "" {
		t.Error("Expected metadataDirectory to be set")
	}

	if v.GetString("quotaDirectory") == "" {
		t.Error("Expected quotaDirectory to be set")
	}

	if v.GetInt("retryPolicy.maxRetryCount") == 0 {
		t.Error("Expected retryPolicy.maxRetryCount to be set")
	}

	if v.GetDuration("cleanupOptions.cleanupInterval") == 0 {
		t.Error("Expected cleanupOptions.cleanupInterval to be set")
	}
}

func TestLoadWithViper(t *testing.T) {
	v := viper.New()

	// Set configuration values
	v.Set("metadataDirectory", "./test-metadata")
	v.Set("quotaDirectory", "./test-quota")
	v.Set("autoCreateTenants", true)
	v.Set("defaultTenantQuota", 5000000)
	v.Set("enableDatabaseHealthCheck", true)
	v.Set("retryPolicy.maxRetryCount", 3)
	v.Set("retryPolicy.initialRetryDelay", 5*time.Second)
	v.Set("retryPolicy.useExponentialBackoff", true)
	v.Set("retryPolicy.maxRetryDelay", 5*time.Minute)
	v.Set("tenantManagerOptions.metadataPath", "")
	v.Set("tenantManagerOptions.cacheTTL", 5*time.Minute)
	v.Set("metadataOptions.cacheTTL", 5*time.Minute)
	v.Set("badgerDBOptions.gcInterval", 10*time.Minute)
	v.Set("badgerDBOptions.gcDiscardRatio", 0.5)
	v.Set("volumes", []map[string]interface{}{
		{
			"volumeId":      "test-vol",
			"mountPath":     "./test-storage",
			"volumeType":    "LocalFileSystem",
			"shardingDepth": 2,
		},
	})

	// Load configuration
	config, err := LoadWithViper(v)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify loaded config
	if config.MetadataDirectory != "./test-metadata" {
		t.Errorf("Expected MetadataDirectory './test-metadata', got %s", config.MetadataDirectory)
	}

	if config.DefaultTenantQuota != 5000000 {
		t.Errorf("Expected DefaultTenantQuota 5000000, got %d", config.DefaultTenantQuota)
	}

	if !config.AutoCreateTenants {
		t.Error("Expected AutoCreateTenants to be true")
	}

	if len(config.Volumes) != 1 {
		t.Errorf("Expected 1 volume, got %d", len(config.Volumes))
	}

	if config.Volumes[0].VolumeId != "test-vol" {
		t.Errorf("Expected VolumeId 'test-vol', got %s", config.Volumes[0].VolumeId)
	}
}
