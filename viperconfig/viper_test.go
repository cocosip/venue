package viperconfig

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/config"
	"github.com/spf13/viper"
)

func TestLoadSectionProducesConfig(t *testing.T) {
	source := viper.New()
	source.SetConfigType("yaml")
	if err := source.ReadConfig(strings.NewReader(`
venue:
  metadataDirectory: ./metadata
  quotaDirectory: ./quota
  cleanupOptions:
    processingTimeout: 45m
  volumes:
    - volumeId: volume-1
      mountPath: ./storage
      shardingDepth: 2
`)); err != nil {
		t.Fatalf("ReadConfig() error = %v", err)
	}

	cfg, err := LoadSection(source, "venue")
	if err != nil {
		t.Fatalf("LoadSection() error = %v", err)
	}
	if cfg.MetadataDirectory != "./metadata" || len(cfg.Volumes) != 1 || cfg.Volumes[0].VolumeID != "volume-1" {
		t.Fatalf("config = %#v", cfg)
	}
	if cfg.Cleanup.ProcessingTimeout.String() != "45m0s" {
		t.Fatalf("processing timeout = %s", cfg.Cleanup.ProcessingTimeout)
	}
	if cfg.Logging != nil {
		t.Fatal("Viper must not bind logging")
	}
}

// TestLoadOnAnEmptySourceProducesTheDocumentedDefaults pins the reason
// NewWithDefaults is unnecessary: Load seeds the result from
// config.DefaultConfig(), so an empty source already yields every documented
// default, including the sections that a hand-maintained SetDefault list used to
// drift away from.
func TestLoadOnAnEmptySourceProducesTheDocumentedDefaults(t *testing.T) {
	cfg, err := Load(viper.New())
	if err != nil {
		t.Fatalf("Load(empty source) error = %v", err)
	}

	defaults := config.DefaultConfig()
	if cfg.SourceCleanup.Enabled != defaults.SourceCleanup.Enabled ||
		cfg.SourceCleanup.DatabasePath != defaults.SourceCleanup.DatabasePath ||
		cfg.SourceCleanup.MaxActiveJobs != defaults.SourceCleanup.MaxActiveJobs ||
		cfg.SourceCleanup.TerminalPruneBatchSize != defaults.SourceCleanup.TerminalPruneBatchSize {
		t.Fatalf("source cleanup defaults = %#v, want %#v", cfg.SourceCleanup, defaults.SourceCleanup)
	}
	if cfg.Cleanup.ProcessingTimeout != defaults.Cleanup.ProcessingTimeout {
		t.Fatalf("processing timeout = %s, want the default %s", cfg.Cleanup.ProcessingTimeout, defaults.Cleanup.ProcessingTimeout)
	}
	if cfg.Sqlite.JournalMode != defaults.Sqlite.JournalMode {
		t.Fatalf("journal mode = %q, want the default %q", cfg.Sqlite.JournalMode, defaults.Sqlite.JournalMode)
	}
}

// TestLoadFromFileSectionRejectsAMissingSection covers the silent-fallback bug:
// an absent section used to bind the whole document, so an unrelated application
// config became Venue configuration and surfaced as a confusing validation error.
func TestLoadFromFileSectionRejectsAMissingSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	content := []byte("app:\n  name: MyApp\n  port: 8080\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadFromFile(path); !errors.Is(err, ErrSectionNotFound) {
		t.Fatalf("LoadFromFile() error = %v, want it to wrap ErrSectionNotFound", err)
	}
	if _, err := LoadFromFileSection(path, "venue"); !errors.Is(err, ErrSectionNotFound) {
		t.Fatalf("LoadFromFileSection() error = %v, want it to wrap ErrSectionNotFound", err)
	}

	// An explicitly empty section name still reads the root document, which is
	// how a caller loads a file whose keys sit at the top level.
	if _, err := LoadFromFileSection(path, ""); err != nil {
		t.Fatalf("LoadFromFileSection(\"\") error = %v, want the root document to load", err)
	}
}

func TestLoadFromFileAdaptsFileToConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "venue.yaml")
	content := []byte("venue:\n  metadataDirectory: ./metadata\n  quotaDirectory: ./quota\n  volumes:\n    - volumeId: v1\n      mountPath: ./storage\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() error = %v", err)
	}
	if cfg.Volumes[0].VolumeID != "v1" {
		t.Fatalf("volume ID = %q", cfg.Volumes[0].VolumeID)
	}
}

func TestShippedViperExampleFilesUseVenueSection(t *testing.T) {
	for _, fileName := range []string{"venue-config.yaml", "venue-config.json"} {
		t.Run(fileName, func(t *testing.T) {
			path := filepath.Join("..", "examples", "viper-config", fileName)
			source := viper.New()
			source.SetConfigFile(path)
			if err := source.ReadInConfig(); err != nil {
				t.Fatalf("ReadInConfig(%s) error = %v", path, err)
			}
			if source.Sub("venue") == nil {
				t.Fatalf("%s does not define a top-level venue section", path)
			}

			cfg, err := LoadFromFile(path)
			if err != nil {
				t.Fatalf("LoadFromFile(%s) error = %v", path, err)
			}
			if len(cfg.Volumes) != 2 || len(cfg.Tenants) != 3 || len(cfg.FileWatchers) != 2 {
				t.Fatalf("configuration shape = %d volumes, %d tenants, %d watchers",
					len(cfg.Volumes), len(cfg.Tenants), len(cfg.FileWatchers))
			}
		})
	}
}

func TestLoadSectionUsesBoundEnvironmentOverride(t *testing.T) {
	t.Setenv("APP_VENUE_RETRYPOLICY_MAXRETRYCOUNT", "9")

	source := viper.New()
	source.SetConfigType("yaml")
	source.SetEnvPrefix("APP")
	source.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	source.AutomaticEnv()
	if err := source.BindEnv("venue.retryPolicy.maxRetryCount"); err != nil {
		t.Fatalf("BindEnv() error = %v", err)
	}
	if err := source.ReadConfig(strings.NewReader(`
venue:
  retryPolicy:
    maxRetryCount: 5
  volumes:
    - volumeId: volume-1
      mountPath: ./storage
`)); err != nil {
		t.Fatalf("ReadConfig() error = %v", err)
	}

	cfg, err := LoadSection(source, "venue")
	if err != nil {
		t.Fatalf("LoadSection() error = %v", err)
	}
	if cfg.RetryPolicy.MaxRetryCount != 9 {
		t.Fatalf("MaxRetryCount = %d, want environment override 9", cfg.RetryPolicy.MaxRetryCount)
	}
}

func TestLoadRejectsNilSource(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Fatal("Load(nil) error = nil")
	}
}

// TestShippedExampleConfigurationLoads guards the documented example file from
// drifting away from the code: it must bind and pass validation.
func TestShippedExampleConfigurationLoads(t *testing.T) {
	path := filepath.Join("..", "venue-config-example.yaml")

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile(%s) error = %v", path, err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("example configuration failed validation: %v", err)
	}
	if len(cfg.Volumes) != 2 || len(cfg.Tenants) != 3 || len(cfg.FileWatchers) != 2 {
		t.Fatalf("example configuration shape = %d volumes, %d tenants, %d watchers",
			len(cfg.Volumes), len(cfg.Tenants), len(cfg.FileWatchers))
	}
	if cfg.OrphanRecovery.Enabled {
		t.Error("example enables orphan recovery; the shipped default is opt-in")
	}
	if cfg.OrphanRecovery.RecoveryInterval != 6*time.Hour {
		t.Errorf("OrphanRecovery.RecoveryInterval = %v, want 6h", cfg.OrphanRecovery.RecoveryInterval)
	}
	if cfg.Cleanup.FailedFileRetentionPeriod != 72*time.Hour {
		t.Errorf("Cleanup.FailedFileRetentionPeriod = %v, want 72h", cfg.Cleanup.FailedFileRetentionPeriod)
	}
	for i, watcher := range cfg.FileWatchers {
		if !watcher.Enabled {
			t.Errorf("fileWatchers[%d].Enabled = false; boolean keys must be explicit in the example", i)
		}
		if watcher.MinFileAge <= 0 {
			t.Errorf("fileWatchers[%d].MinFileAge = %v", i, watcher.MinFileAge)
		}
	}
}
