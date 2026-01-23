package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.MetadataDirectory == "" {
		t.Error("Expected MetadataDirectory to be set")
	}

	if config.QuotaDirectory == "" {
		t.Error("Expected QuotaDirectory to be set")
	}

	if len(config.Volumes) == 0 {
		t.Error("Expected at least one volume")
	}

	if config.RetryPolicy.MaxRetryCount <= 0 {
		t.Error("Expected MaxRetryCount to be positive")
	}

	if config.CleanupOptions.CleanupInterval <= 0 {
		t.Error("Expected CleanupInterval to be positive")
	}
}

func TestConfigValidation(t *testing.T) {
	t.Run("Valid configuration", func(t *testing.T) {
		config := DefaultConfig()
		if err := config.Validate(); err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("Empty metadata directory", func(t *testing.T) {
		config := DefaultConfig()
		config.MetadataDirectory = ""
		if err := config.Validate(); err == nil {
			t.Error("Expected error for empty metadata directory")
		}
	})

	t.Run("Empty quota directory", func(t *testing.T) {
		config := DefaultConfig()
		config.QuotaDirectory = ""
		if err := config.Validate(); err == nil {
			t.Error("Expected error for empty quota directory")
		}
	})

	t.Run("No volumes", func(t *testing.T) {
		config := DefaultConfig()
		config.Volumes = []VolumeConfig{}
		if err := config.Validate(); err == nil {
			t.Error("Expected error for no volumes")
		}
	})

	t.Run("Duplicate volume IDs", func(t *testing.T) {
		config := DefaultConfig()
		config.Volumes = []VolumeConfig{
			{VolumeId: "vol-001", MountPath: "/path1", ShardingDepth: 2},
			{VolumeId: "vol-001", MountPath: "/path2", ShardingDepth: 2},
		}
		if err := config.Validate(); err == nil {
			t.Error("Expected error for duplicate volume IDs")
		}
	})

	t.Run("Duplicate tenant IDs", func(t *testing.T) {
		config := DefaultConfig()
		config.Tenants = []TenantConfig{
			{TenantId: "tenant-001", Enabled: true},
			{TenantId: "tenant-001", Enabled: true},
		}
		if err := config.Validate(); err == nil {
			t.Error("Expected error for duplicate tenant IDs")
		}
	})

	t.Run("File watcher without tenant in single-tenant mode", func(t *testing.T) {
		config := DefaultConfig()
		config.FileWatchers = []FileWatcherConfig{
			{
				WatcherId:       "watcher-001",
				WatchPath:       "/watch",
				MultiTenantMode: false,
				TenantId:        "", // Missing tenant ID
				Enabled:         true,
			},
		}
		if err := config.Validate(); err == nil {
			t.Error("Expected error for file watcher without tenant ID in single-tenant mode")
		}
	})

	t.Run("Invalid post import action", func(t *testing.T) {
		config := DefaultConfig()
		config.FileWatchers = []FileWatcherConfig{
			{
				WatcherId:        "watcher-001",
				WatchPath:        "/watch",
				TenantId:         "tenant-001",
				PostImportAction: "InvalidAction",
				Enabled:          true,
			},
		}
		if err := config.Validate(); err == nil {
			t.Error("Expected error for invalid post import action")
		}
	})
}

func TestConfigLoadSave(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	t.Run("JSON format", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config.json")

		// Create and save config
		config := DefaultConfig()
		config.MetadataDirectory = "./test-metadata"
		config.QuotaDirectory = "./test-quota"
		config.AutoCreateTenants = true
		config.Tenants = []TenantConfig{
			{TenantId: "test-tenant", Enabled: true, Quota: nil},
		}

		if err := config.SaveToFile(configPath); err != nil {
			t.Fatalf("Failed to save config: %v", err)
		}

		// Load config
		loadedConfig, err := LoadFromFile(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if loadedConfig.MetadataDirectory != "./test-metadata" {
			t.Errorf("Expected MetadataDirectory './test-metadata', got %s", loadedConfig.MetadataDirectory)
		}

		if loadedConfig.QuotaDirectory != "./test-quota" {
			t.Errorf("Expected QuotaDirectory './test-quota', got %s", loadedConfig.QuotaDirectory)
		}

		if !loadedConfig.AutoCreateTenants {
			t.Error("Expected AutoCreateTenants to be true")
		}

		if len(loadedConfig.Tenants) != 1 {
			t.Errorf("Expected 1 tenant, got %d", len(loadedConfig.Tenants))
		}
	})

	t.Run("YAML format", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config.yaml")

		// Create and save config
		config := DefaultConfig()
		config.MetadataDirectory = "./test-metadata"
		config.EnableBackgroundCleanup = true
		config.CleanupOptions.CleanupInterval = 2 * time.Hour

		if err := config.SaveToFile(configPath); err != nil {
			t.Fatalf("Failed to save config: %v", err)
		}

		// Load config
		loadedConfig, err := LoadFromFile(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if !loadedConfig.EnableBackgroundCleanup {
			t.Error("Expected EnableBackgroundCleanup to be true")
		}

		if loadedConfig.CleanupOptions.CleanupInterval != 2*time.Hour {
			t.Errorf("Expected CleanupInterval 2h, got %v", loadedConfig.CleanupOptions.CleanupInterval)
		}
	})
}

func TestApplyDefaults(t *testing.T) {
	t.Run("Apply defaults to empty config", func(t *testing.T) {
		config := &Config{}
		config.ApplyDefaults()

		if config.MetadataDirectory == "" {
			t.Error("Expected MetadataDirectory to be set by defaults")
		}

		if config.RetryPolicy.MaxRetryCount == 0 {
			t.Error("Expected RetryPolicy.MaxRetryCount to be set by defaults")
		}

		if config.CleanupOptions.CleanupInterval == 0 {
			t.Error("Expected CleanupOptions.CleanupInterval to be set by defaults")
		}
	})

	t.Run("Preserve existing values", func(t *testing.T) {
		config := &Config{
			MetadataDirectory: "./custom-metadata",
			RetryPolicy: RetryPolicyConfig{
				MaxRetryCount: 10,
			},
		}
		config.ApplyDefaults()

		if config.MetadataDirectory != "./custom-metadata" {
			t.Error("Expected custom MetadataDirectory to be preserved")
		}

		if config.RetryPolicy.MaxRetryCount != 10 {
			t.Error("Expected custom MaxRetryCount to be preserved")
		}

		// But other fields should get defaults
		if config.QuotaDirectory == "" {
			t.Error("Expected QuotaDirectory to be set by defaults")
		}
	})
}

func TestTenantConfig(t *testing.T) {
	t.Run("Tenant with quota", func(t *testing.T) {
		quota := int64(10000000)
		tenant := TenantConfig{
			TenantId: "tenant-001",
			Enabled:  true,
			Quota:    &quota,
		}

		if tenant.Quota == nil {
			t.Error("Expected Quota to be set")
		}

		if *tenant.Quota != 10000000 {
			t.Errorf("Expected Quota 10000000, got %d", *tenant.Quota)
		}
	})

	t.Run("Tenant with nil quota (unlimited)", func(t *testing.T) {
		tenant := TenantConfig{
			TenantId: "tenant-002",
			Enabled:  true,
			Quota:    nil,
		}

		if tenant.Quota != nil {
			t.Error("Expected Quota to be nil (unlimited)")
		}
	})
}

func TestFileWatcherConfig(t *testing.T) {
	t.Run("Single-tenant file watcher", func(t *testing.T) {
		watcher := FileWatcherConfig{
			WatcherId:            "watcher-001",
			TenantId:             "tenant-001",
			MultiTenantMode:      false,
			WatchPath:            "/watch/path",
			Enabled:              true,
			FilePatterns:         []string{"*.pdf", "*.docx"},
			PostImportAction:     "Delete",
			MaxConcurrentImports: 16,
		}

		if watcher.MultiTenantMode {
			t.Error("Expected MultiTenantMode to be false")
		}

		if watcher.TenantId != "tenant-001" {
			t.Error("Expected TenantId to be set")
		}
	})

	t.Run("Multi-tenant file watcher", func(t *testing.T) {
		watcher := FileWatcherConfig{
			WatcherId:                   "watcher-shared",
			TenantId:                    "",
			MultiTenantMode:             true,
			AutoCreateTenantDirectories: true,
			WatchPath:                   "/watch/shared",
			Enabled:                     true,
		}

		if !watcher.MultiTenantMode {
			t.Error("Expected MultiTenantMode to be true")
		}

		if !watcher.AutoCreateTenantDirectories {
			t.Error("Expected AutoCreateTenantDirectories to be true")
		}
	})
}

func TestLoadFromFileWithNode(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "config-node-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	t.Run("Load YAML from venue node", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config-with-node.yaml")

		// Create YAML config with venue node
		yamlContent := `venue:
  metadataDirectory: ./test-metadata
  quotaDirectory: ./test-quota
  autoCreateTenants: true
  defaultTenantQuota: 5000000
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
    - volumeId: vol-001
      mountPath: ./storage
      volumeType: LocalFileSystem
      shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config (should use venue node by default)
		config, err := LoadFromFile(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./test-metadata" {
			t.Errorf("Expected MetadataDirectory './test-metadata', got %s", config.MetadataDirectory)
		}
		if config.QuotaDirectory != "./test-quota" {
			t.Errorf("Expected QuotaDirectory './test-quota', got %s", config.QuotaDirectory)
		}
		if !config.AutoCreateTenants {
			t.Error("Expected AutoCreateTenants to be true")
		}
		if config.DefaultTenantQuota != 5000000 {
			t.Errorf("Expected DefaultTenantQuota 5000000, got %d", config.DefaultTenantQuota)
		}
	})

	t.Run("Load YAML from root level (backward compatibility)", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config-root.yaml")

		// Create YAML config at root level (no venue node)
		yamlContent := `metadataDirectory: ./root-metadata
quotaDirectory: ./root-quota
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
  - volumeId: vol-root
    mountPath: ./root-storage
    volumeType: LocalFileSystem
    shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config (should fallback to root level)
		config, err := LoadFromFile(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./root-metadata" {
			t.Errorf("Expected MetadataDirectory './root-metadata', got %s", config.MetadataDirectory)
		}
		if config.QuotaDirectory != "./root-quota" {
			t.Errorf("Expected QuotaDirectory './root-quota', got %s", config.QuotaDirectory)
		}
		if config.AutoCreateTenants {
			t.Error("Expected AutoCreateTenants to be false")
		}
		if config.RetryPolicy.MaxRetryCount != 5 {
			t.Errorf("Expected MaxRetryCount 5, got %d", config.RetryPolicy.MaxRetryCount)
		}
	})

	t.Run("Load JSON from venue node", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config-with-node.json")

		// Create JSON config with venue node
		// Note: JSON requires time.Duration in nanoseconds (int64), not string format
		jsonContent := `{
  "venue": {
    "metadataDirectory": "./json-metadata",
    "quotaDirectory": "./json-quota",
    "autoCreateTenants": true,
    "defaultTenantQuota": 8000000,
    "enableDatabaseHealthCheck": true,
    "retryPolicy": {
      "maxRetryCount": 3,
      "initialRetryDelay": 5000000000,
      "useExponentialBackoff": true,
      "maxRetryDelay": 300000000000
    },
    "tenantManagerOptions": {
      "metadataPath": "",
      "cacheTTL": 300000000000
    },
    "metadataOptions": {
      "cacheTTL": 300000000000
    },
    "badgerDBOptions": {
      "gcInterval": 600000000000,
      "gcDiscardRatio": 0.5
    },
    "volumes": [
      {
        "volumeId": "vol-json",
        "mountPath": "./json-storage",
        "volumeType": "LocalFileSystem",
        "shardingDepth": 2
      }
    ]
  }
}`
		if err := os.WriteFile(configPath, []byte(jsonContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config (should use venue node by default)
		config, err := LoadFromFile(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./json-metadata" {
			t.Errorf("Expected MetadataDirectory './json-metadata', got %s", config.MetadataDirectory)
		}
		if config.DefaultTenantQuota != 8000000 {
			t.Errorf("Expected DefaultTenantQuota 8000000, got %d", config.DefaultTenantQuota)
		}
	})

	t.Run("Load with custom node name", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config-custom-node.yaml")

		// Create YAML config with custom node name
		yamlContent := `myapp:
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
    - volumeId: vol-custom
      mountPath: ./custom-storage
      volumeType: LocalFileSystem
      shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config with custom node name
		config, err := LoadFromFileWithNode(configPath, "myapp")
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./custom-metadata" {
			t.Errorf("Expected MetadataDirectory './custom-metadata', got %s", config.MetadataDirectory)
		}
	})

	t.Run("Load from root level with empty node name", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "config-empty-node.yaml")

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
  - volumeId: vol-empty
    mountPath: ./empty-storage
    volumeType: LocalFileSystem
    shardingDepth: 2
`
		if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write config: %v", err)
		}

		// Load config with empty node name (should load from root)
		config, err := LoadFromFileWithNode(configPath, "")
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		// Verify loaded config
		if config.MetadataDirectory != "./empty-node-metadata" {
			t.Errorf("Expected MetadataDirectory './empty-node-metadata', got %s", config.MetadataDirectory)
		}
	})
}
