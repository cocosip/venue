package config

import (
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.MetadataDirectory == "" || cfg.QuotaDirectory == "" || len(cfg.Volumes) == 0 {
		t.Fatalf("incomplete defaults: %#v", cfg)
	}
}

func TestApplyDefaultsPreservesExplicitValues(t *testing.T) {
	cfg := &Config{
		MetadataDirectory: "custom-metadata",
		RetryPolicy: RetryPolicyConfig{
			MaxRetryCount: 10,
		},
	}
	cfg.ApplyDefaults()

	if cfg.MetadataDirectory != "custom-metadata" || cfg.RetryPolicy.MaxRetryCount != 10 {
		t.Fatalf("explicit values changed: %#v", cfg)
	}
	if cfg.QuotaDirectory == "" || cfg.Cleanup.CleanupInterval == 0 {
		t.Fatalf("defaults not applied: %#v", cfg)
	}
}

func TestDefaultCleanupRemovesCompletedRecordsWithoutRetention(t *testing.T) {
	cfg := New()

	if !cfg.Cleanup.CleanupCompletedRecords {
		t.Fatal("CleanupCompletedRecords = false, want true")
	}
	if cfg.Cleanup.CompletedRecordRetentionPeriod != 0 {
		t.Fatalf("CompletedRecordRetentionPeriod = %v, want 0", cfg.Cleanup.CompletedRecordRetentionPeriod)
	}
	if cfg.Cleanup.FailedFileRetentionPeriod != 3*24*time.Hour {
		t.Fatalf("FailedFileRetentionPeriod = %v, want 72h", cfg.Cleanup.FailedFileRetentionPeriod)
	}
}

func TestDurabilityAndLifecycleDefaultsAreEnabled(t *testing.T) {
	cfg := New()

	if !cfg.AutoCreateTenants {
		t.Fatal("AutoCreateTenants = false, want true")
	}
	if !cfg.EnableBackgroundCleanup {
		t.Fatal("EnableBackgroundCleanup = false, want true")
	}
	if len(cfg.Volumes) != 1 || !cfg.Volumes[0].EnableFsync {
		t.Fatalf("default volume fsync = %v, want true", cfg.Volumes)
	}
	if !NewVolumeConfig().EnableFsync {
		t.Fatal("NewVolumeConfig().EnableFsync = false, want true")
	}
}

func TestValidateRejectsDuplicateAndIncompleteEntries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty metadata directory", func(c *Config) { c.MetadataDirectory = "" }},
		{"empty quota directory", func(c *Config) { c.QuotaDirectory = "" }},
		{"no volumes", func(c *Config) { c.Volumes = nil }},
		{"duplicate volume", func(c *Config) {
			c.Volumes = []VolumeConfig{{VolumeID: "v1", MountPath: "a"}, {VolumeID: "v1", MountPath: "b"}}
		}},
		{"duplicate tenant", func(c *Config) {
			c.Tenants = []TenantConfig{{TenantID: "t1"}, {TenantID: "t1"}}
		}},
		{"negative default tenant quota", func(c *Config) { c.DefaultTenantQuota = -1 }},
		{"negative tenant quota", func(c *Config) {
			quota := int64(-1)
			c.Tenants = []TenantConfig{{TenantID: "t1", Quota: &quota}}
		}},
		{"watcher missing tenant", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{WatcherID: "w1", WatchPath: "in", PostImportAction: "Delete"}}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			test.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestCloneDeepCopiesMutableConfiguration(t *testing.T) {
	quota := int64(10)
	cfg := DefaultConfig().
		WithTenants(NewTenantConfig("tenant-1").WithQuota(quota)).
		WithFileWatchers(NewFileWatcherConfig().
			WithWatcherID("watcher-1").
			WithTenantID("tenant-1").
			WithWatchPath("incoming").
			WithFilePatterns("*.dcm"))

	clone := cfg.Clone()
	*cfg.Tenants[0].Quota = 20
	cfg.FileWatchers[0].FilePatterns[0] = "*.txt"

	if *clone.Tenants[0].Quota != 10 || clone.FileWatchers[0].FilePatterns[0] != "*.dcm" {
		t.Fatalf("clone changed with source: %#v", clone)
	}
}

func TestEveryConfigModuleSupportsFluentConstruction(t *testing.T) {
	volume := NewVolumeConfig().
		WithVolumeID("volume-1").
		WithMountPath("storage").
		WithVolumeType("LocalFileSystem").
		WithShardingDepth(3).
		WithFsync(true)
	tenant := NewTenantConfig("tenant-1").WithEnabled(false).WithQuota(99)
	watcher := NewFileWatcherConfig().
		WithWatcherID("watcher-1").
		WithTenantID("tenant-1").
		WithWatchPath("incoming").
		WithEnabled(true).
		WithSubdirectories(true).
		WithFilePatterns("*.dcm").
		WithPostImportAction("Keep").
		WithPollingInterval(time.Second).
		WithMaxFileSizeBytes(1024).
		WithMinFileAge(2 * time.Second).
		WithMaxConcurrentImports(2)

	cfg := New().
		WithMetadataDirectory("metadata").
		WithQuotaDirectory("quota").
		WithAutoCreateTenants(true).
		WithRetryPolicy(NewRetryPolicyConfig().WithMaxRetryCount(5).WithInitialRetryDelay(time.Second)).
		WithTenantManager(NewTenantManagerConfig().WithMetadataPath("tenants").WithCacheTTL(time.Minute)).
		WithMetadata(NewMetadataConfig().WithCacheTTL(time.Minute).WithMaxCacheEntries(50)).
		WithBadgerDB(NewBadgerDBConfig().WithGCInterval(time.Hour).WithSyncWrites(true)).
		WithVolumes(volume).
		WithTenants(tenant).
		WithFileWatchers(watcher).
		WithBackgroundCleanupEnabled(true).
		WithCleanup(NewCleanupConfig().WithProcessingTimeout(time.Hour)).
		WithDatabaseHealthCheck(NewDatabaseHealthCheckConfig().WithMaxRetries(9))

	if cfg.Volumes[0].ShardingDepth != 3 || cfg.Tenants[0].Quota == nil || cfg.FileWatchers[0].MaxConcurrentImports != 2 {
		t.Fatalf("nested fluent config not retained: %#v", cfg)
	}
	if cfg.RetryPolicy.MaxRetryCount != 5 || cfg.Metadata.MaxCacheEntries != 50 || cfg.DatabaseHealthCheck.MaxRetries != 9 {
		t.Fatalf("module fluent config not retained: %#v", cfg)
	}
}
