package config_test

import (
	"log/slog"
	"testing"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/logging"
)

// TestWithCollectionSettersDoNotAliasAShallowCopy covers the aliasing hazard of
// the collection setters: `c.X = c.X[:0]` followed by appends writes into the
// backing array a shallow copy still shares, so mutating one Config would mutate
// the other.
func TestWithCollectionSettersDoNotAliasAShallowCopy(t *testing.T) {
	original := config.New().
		WithVolumes(config.NewVolumeConfig().WithVolumeID("original-volume").WithMountPath("original-path")).
		WithTenants(config.NewTenantConfig("original-tenant")).
		WithFileWatchers(config.NewFileWatcherConfig().WithWatcherID("original-watcher")).
		WithFileWatcherRoots(config.NewFileWatcherRootConfig("original-root"))

	shallow := *original

	// Replace every collection on the copy.
	shallow.WithVolumes(config.NewVolumeConfig().WithVolumeID("new-volume").WithMountPath("new-path"))
	shallow.WithTenants(config.NewTenantConfig("new-tenant"))
	shallow.WithFileWatchers(config.NewFileWatcherConfig().WithWatcherID("new-watcher"))
	shallow.WithFileWatcherRoots(config.NewFileWatcherRootConfig("new-root"))

	if len(original.Volumes) != 1 || original.Volumes[0].VolumeID != "original-volume" {
		t.Fatalf("original volumes were mutated through the shared backing array: %#v", original.Volumes)
	}
	if len(original.Tenants) != 1 || original.Tenants[0].TenantID != "original-tenant" {
		t.Fatalf("original tenants were mutated through the shared backing array: %#v", original.Tenants)
	}
	if len(original.FileWatchers) != 1 || original.FileWatchers[0].WatcherID != "original-watcher" {
		t.Fatalf("original file watchers were mutated through the shared backing array: %#v", original.FileWatchers)
	}
	if len(original.FileWatcherRoots) != 1 || original.FileWatcherRoots[0].RootPath != "original-root" {
		t.Fatalf("original watcher roots were mutated through the shared backing array: %#v", original.FileWatcherRoots)
	}
}

// TestWithLoggingCopiesTheCallerValue covers the only setter that used to store
// the caller's pointer verbatim.
func TestWithLoggingCopiesTheCallerValue(t *testing.T) {
	handler := slog.NewTextHandler(discardWriter{}, nil)
	source := &logging.Config{Handler: handler}

	cfg := config.New().WithLogging(source)

	// Mutating the caller's struct must not reach the configuration.
	source.Handler = nil

	if cfg.Logging == nil {
		t.Fatal("WithLogging() left Logging nil")
	}
	if cfg.Logging.Handler == nil {
		t.Fatal("WithLogging() aliased the caller's struct instead of copying it")
	}
	if cfg.Logging == source {
		t.Fatal("WithLogging() stored the caller's pointer")
	}

	// An explicit nil still clears it.
	if cleared := config.New().WithLogging(source).WithLogging(nil); cleared.Logging != nil {
		t.Fatal("WithLogging(nil) did not clear the configuration")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
