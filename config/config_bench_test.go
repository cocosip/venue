package config

import (
	"runtime"
	"testing"
	"time"
)

func BenchmarkConfigFluentConstruction(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		cfg := New().
			WithMetadataDirectory("metadata").
			WithQuotaDirectory("quota").
			WithRetryPolicy(NewRetryPolicyConfig().
				WithMaxRetryCount(5).
				WithInitialRetryDelay(time.Second)).
			WithVolumes(NewVolumeConfig().
				WithVolumeID("volume-1").
				WithMountPath("storage")).
			WithTenants(NewTenantConfig("tenant-1"))
		runtime.KeepAlive(cfg)
	}
}

func BenchmarkConfigClone(b *testing.B) {
	cfg := New().
		WithVolumes(NewVolumeConfig().WithVolumeID("volume-1").WithMountPath("storage")).
		WithTenants(NewTenantConfig("tenant-1").WithQuota(1024)).
		WithFileWatchers(NewFileWatcherConfig().
			WithWatcherID("watcher-1").
			WithTenantID("tenant-1").
			WithWatchPath("incoming").
			WithFilePatterns("*.dcm", "*.jpg"))

	b.ReportAllocs()
	for b.Loop() {
		clone := cfg.Clone()
		runtime.KeepAlive(clone)
	}
}
