package viperconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestLoadSectionProducesConfig(t *testing.T) {
	source := NewWithDefaults()
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
