# Venue Examples

The repository ships two runnable examples. Both go through the public entry
point `venue.NewVenue(*config.Config)`.

## Programmatic Configuration

`simple-venue` demonstrates fluent configuration, the instance lifecycle,
tenant lookup, and file write and read:

```powershell
go run ./examples/simple-venue
```

The core configuration shape:

```go
cfg := config.New().
    WithMetadataDirectory("./venue-data/metadata").
    WithQuotaDirectory("./venue-data/quota").
    WithVolumes(
        config.NewVolumeConfig().
            WithVolumeID("default-volume").
            WithMountPath("./venue-data/storage").
            WithShardingDepth(2),
    ).
    WithTenants(config.NewTenantConfig("demo-tenant"))

runtime, err := venue.NewVenue(cfg)
```

`Config` and the nested types `VolumeConfig`, `TenantConfig`,
`RetryPolicyConfig`, `TenantManagerConfig`, `MetadataConfig`, `SqliteConfig`,
`FileWatcherConfig`, `FileWatcherRootConfig`, `FileWatcherServiceConfig`,
`CleanupConfig`, `DeadLetterConfig`, `RetiredVolumeConfig`,
`OrphanRecoveryConfig`, `DatabaseHealthCheckConfig`, and `StatisticsConfig`
(plus `StatisticsDimensionConfig` and `StatisticsOutputConfig`) all support
fluent construction. The top-level `WithVolumes`, `WithTenants`, and
`WithFileWatchers` replace the whole collection; use `AddVolume`, `AddTenant`,
and `AddFileWatcher` to append.

A few settings have no fluent setter and are set by direct assignment, for
example `VolumeConfig.InitialDelay` and `VolumeConfig.HealthCheckDelay`.

Tenant IDs become metadata file names and physical storage directory segments,
so every entry point validates them first: an ID containing `/`, `\`, `:`,
control characters, a leading or trailing dot, surrounding whitespace, a Windows
device name, or more than 128 bytes is rejected with
`core.ErrInvalidArgument`.

## Viper Adapter

`viper-config` demonstrates loading YAML or JSON through the separate adapter, or
binding from a Viper instance the application already owns:

```powershell
go run ./examples/viper-config
```

```go
cfg, err := viperconfig.LoadFromFile("venue-config-example.yaml")
runtime, err := venue.NewVenue(cfg)
```

The dependency direction is fixed at `viperconfig -> config`. The base `config`
package does not import Viper and does not read files, so an application that
does not use the adapter is not forced to depend on the Viper API.

Every public configuration field carries `json`, `yaml`, and `mapstructure`
tags, so an application may bind `config.Config` with another decoder.
`Logging` is a runtime object whose three tags are all `-`, and it must be
injected by the application after decoding.

## Configuration Files

- [`../venue-config-example.yaml`](../venue-config-example.yaml): the complete
  example with a `venue` root node, including the `sqliteOptions` engine
  settings, statistics, per-tenant metadata-backup, per-volume startup, advanced
  watcher, and timed-out reclaim options.
- [`viper-config/venue-config.yaml`](viper-config/venue-config.yaml): a YAML
  configuration example with a top-level `venue` section.
- [`viper-config/venue-config.json`](viper-config/venue-config.json): a JSON
  configuration example with a top-level `venue` section.

When binding from a file, boolean keys such as `enabled` and
`includeSubdirectories` must be written explicitly: a boolean key that is absent
from the source cannot be distinguished from an explicit `false`. The Go
constructor `config.NewFileWatcherConfig()` defaults them to `true`. The same
applies to the advanced watcher switches
`disableImportedFilesPruneThrottle` and
`disableImportedFilesHistoryFlushDebounce`, which are deliberately inverted: the
throttle and the debounce are on by default, so the zero value keeps them on and
only an explicit `true` turns them off.

The examples create temporary local data directories for demonstration only;
they are not a production directory layout. The generated databases and
configuration dumps are excluded by `.gitignore`.
