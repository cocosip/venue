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
		{"unsafe tenant id", func(c *Config) {
			c.Tenants = []TenantConfig{{TenantID: "../../evil", Enabled: true}}
		}},
		{"tenant id with separator", func(c *Config) {
			c.Tenants = []TenantConfig{{TenantID: "org/tenant", Enabled: true}}
		}},
		{"unsafe watcher tenant id", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{WatcherID: "w1", TenantID: "../evil", WatchPath: "in", PostImportAction: "Delete"}}
		}},
		{"negative watcher concurrency", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{
				WatcherID: "w1", TenantID: "t1", WatchPath: "in",
				PostImportAction: "Delete", MaxConcurrentImports: -1,
			}}
		}},
		{"blank watcher pattern", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{
				WatcherID: "w1", TenantID: "t1", WatchPath: "in",
				PostImportAction: "Delete", FilePatterns: []string{""},
			}}
		}},
		{"invalid watcher pattern", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{
				WatcherID: "w1", TenantID: "t1", WatchPath: "in",
				PostImportAction: "Delete", FilePatterns: []string{"["},
			}}
		}},
		{"watcher tenant with multi-tenant mode", func(c *Config) {
			c.FileWatchers = []FileWatcherConfig{{
				WatcherID: "w1", TenantID: "t1", WatchPath: "in",
				MultiTenantMode: true, PostImportAction: "Delete",
			}}
		}},
		{"badger discard ratio out of range", func(c *Config) { c.BadgerDB.GCDiscardRatio = 1.0 }},
		{"negative badger sizing", func(c *Config) { c.BadgerDB.MemTableSize = -1 }},
		{"negative cleanup interval", func(c *Config) { c.Cleanup.CleanupInterval = -time.Second }},
		{"negative timed out reclaim cooldown", func(c *Config) { c.Cleanup.TimedOutReclaimCooldown = -time.Second }},
		{"negative corrupted database retention", func(c *Config) { c.BadgerDB.CorruptedDatabaseRetention = -time.Second }},
		{"negative failed retention", func(c *Config) { c.Cleanup.FailedFileRetentionPeriod = -time.Second }},
		{"negative retry count", func(c *Config) { c.RetryPolicy.MaxRetryCount = -1 }},
		{"negative orphan recovery interval", func(c *Config) { c.OrphanRecovery.RecoveryInterval = -time.Second }},
		{"negative health check retries", func(c *Config) { c.DatabaseHealthCheck.MaxRetries = -1 }},
		{"negative cache entries", func(c *Config) { c.Metadata.MaxCacheEntries = -1 }},
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

func TestOrphanRecoveryDefaultsAndFluentConstruction(t *testing.T) {
	cfg := New().WithOrphanRecovery(
		NewOrphanRecoveryConfig().
			WithEnabled(true).
			WithRunOnStartup(true).
			WithRecoveryInterval(2 * time.Hour).
			WithInitialDelay(30 * time.Second),
	)

	if !cfg.OrphanRecovery.Enabled || !cfg.OrphanRecovery.RunOnStartup {
		t.Fatalf("orphan recovery flags not retained: %#v", cfg.OrphanRecovery)
	}
	if cfg.OrphanRecovery.RecoveryInterval != 2*time.Hour || cfg.OrphanRecovery.InitialDelay != 30*time.Second {
		t.Fatalf("orphan recovery intervals not retained: %#v", cfg.OrphanRecovery)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	defaults := DefaultConfig().OrphanRecovery
	if defaults.Enabled || defaults.RunOnStartup {
		t.Fatalf("orphan recovery must be opt-in by default: %#v", defaults)
	}
	if defaults.RecoveryInterval != 6*time.Hour || defaults.InitialDelay != 10*time.Second {
		t.Fatalf("orphan recovery defaults = %#v", defaults)
	}
}

func TestLocusAlignedWatcherDefaults(t *testing.T) {
	watcher := NewFileWatcherConfig()
	if !watcher.Enabled {
		t.Error("NewFileWatcherConfig().Enabled = false, want true")
	}
	if !watcher.IncludeSubdirectories {
		t.Error("NewFileWatcherConfig().IncludeSubdirectories = false, want true")
	}
	if watcher.MinFileAge != 5*time.Second {
		t.Errorf("NewFileWatcherConfig().MinFileAge = %v, want 5s", watcher.MinFileAge)
	}

	cfg := &Config{}
	cfg.ApplyDefaults()
	cfg.FileWatchers = append(cfg.FileWatchers, FileWatcherConfig{
		WatcherID: "w1", TenantID: "t1", WatchPath: "in", PostImportAction: "Delete",
	})
	cfg.ApplyDefaults()
	if cfg.FileWatchers[len(cfg.FileWatchers)-1].MinFileAge != 5*time.Second {
		t.Errorf("ApplyDefaults MinFileAge = %v, want 5s", cfg.FileWatchers[len(cfg.FileWatchers)-1].MinFileAge)
	}
}

// TestFollowUpOptimizationDefaultsAndFluent covers the reclaim, recovery and
// volume-health-cache options added after the review.
func TestFollowUpOptimizationDefaultsAndFluent(t *testing.T) {
	defaults := DefaultConfig()
	if !defaults.Cleanup.RecoverTimedOutOnEmptyQueue {
		t.Error("RecoverTimedOutOnEmptyQueue = false, want true")
	}
	if defaults.Cleanup.TimedOutReclaimCooldown != 30*time.Second {
		t.Errorf("TimedOutReclaimCooldown = %v, want 30s", defaults.Cleanup.TimedOutReclaimCooldown)
	}
	if defaults.BadgerDB.RecoverCorruptedDatabase {
		t.Error("RecoverCorruptedDatabase = true, want false (destructive repair is opt-in)")
	}
	if defaults.BadgerDB.CorruptedDatabaseRetention != 72*time.Hour {
		t.Errorf("CorruptedDatabaseRetention = %v, want 72h", defaults.BadgerDB.CorruptedDatabaseRetention)
	}

	// ApplyDefaults fills durations but never overrides an explicit boolean.
	applied := &Config{}
	applied.ApplyDefaults()
	if applied.Cleanup.TimedOutReclaimCooldown != 30*time.Second {
		t.Errorf("ApplyDefaults TimedOutReclaimCooldown = %v, want 30s", applied.Cleanup.TimedOutReclaimCooldown)
	}
	if applied.BadgerDB.CorruptedDatabaseRetention != 72*time.Hour {
		t.Errorf("ApplyDefaults CorruptedDatabaseRetention = %v, want 72h", applied.BadgerDB.CorruptedDatabaseRetention)
	}

	cfg := New().
		WithCleanup(NewCleanupConfig().
			WithTimedOutReclaimOnEmptyQueue(false).
			WithTimedOutReclaimCooldown(5 * time.Second)).
		WithBadgerDB(NewBadgerDBConfig().
			WithCorruptedDatabaseRecovery(true).
			WithCorruptedDatabaseRetention(time.Hour)).
		WithVolumes(NewVolumeConfig().
			WithVolumeID("v1").
			WithMountPath("storage").
			WithHealthCheckCacheTTL(-1))

	if cfg.Cleanup.RecoverTimedOutOnEmptyQueue || cfg.Cleanup.TimedOutReclaimCooldown != 5*time.Second {
		t.Errorf("cleanup reclaim options not retained: %#v", cfg.Cleanup)
	}
	if !cfg.BadgerDB.RecoverCorruptedDatabase || cfg.BadgerDB.CorruptedDatabaseRetention != time.Hour {
		t.Errorf("badger recovery options not retained: %#v", cfg.BadgerDB)
	}
	if cfg.Volumes[0].HealthCheckCacheTTL != -1 {
		t.Errorf("HealthCheckCacheTTL = %v, want -1", cfg.Volumes[0].HealthCheckCacheTTL)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
