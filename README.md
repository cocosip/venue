# Venue

Venue is a multi-tenant file storage queue for Go. Applications write a stream
and receive a system-generated `fileKey`; Venue owns volume selection, physical
paths, queue state, retries, and cleanup.

## Features

- Tenant-scoped metadata, queue operations, and quotas.
- Multiple local filesystem volumes with configurable directory sharding.
- Atomic `Pending` to `Processing` allocation for concurrent workers.
- Retry scheduling with optional exponential backoff.
- Background cleanup, directory watchers, and database health checks.
- A public configuration model that does not depend on a configuration-file library.
- Optional Viper integration through the separate `viperconfig` package.
- Instance-scoped structured logging without reading or replacing `slog.Default()`.

## Requirements

- Go 1.26 or newer.
- A local or mounted filesystem writable by the application process.

```powershell
go get github.com/cocosip/venue
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "strings"

    "github.com/cocosip/venue"
    "github.com/cocosip/venue/config"
)

func main() {
    cfg := config.New().
        WithMetadataDirectory("./data/metadata").
        WithQuotaDirectory("./data/quota").
        WithVolumes(
            config.NewVolumeConfig().
                WithVolumeID("primary").
                WithMountPath("./data/storage").
                WithShardingDepth(2),
        ).
        WithTenants(config.NewTenantConfig("tenant-001"))

    runtime, err := venue.NewVenue(cfg)
    if err != nil {
        log.Fatal(err)
    }
    if err := runtime.Start(); err != nil {
        log.Fatal(err)
    }
    defer func() { _ = runtime.Stop() }()

    ctx := context.Background()
    tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-001")
    if err != nil {
        log.Fatal(err)
    }

    originalName := "sample.txt"
    fileKey, err := runtime.StoragePool().WriteFile(
        ctx,
        tenant,
        strings.NewReader("hello venue"),
        &originalName,
    )
    if err != nil {
        log.Fatal(err)
    }

    file, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
    if err != nil {
        log.Fatal(err)
    }
    if file == nil {
        log.Fatal("no pending file")
    }
    if file.Lease == nil {
        log.Fatal("processing file has no lease")
    }

    reader, err := runtime.StoragePool().ReadFile(ctx, tenant, file.FileKey)
    if err != nil {
        log.Fatal(err)
    }
    _, copyErr := io.Copy(io.Discard, reader)
    closeErr := reader.Close()
    if copyErr != nil || closeErr != nil {
        log.Fatal("read file failed")
    }

    if err := runtime.StoragePool().MarkAsCompleted(ctx, *file.Lease); err != nil {
        log.Fatal(err)
    }

    fmt.Println(fileKey)
}
```

`Start` starts the enabled background services. `Stop` stops those services in
reverse order and closes the repositories. Every `io.ReadCloser` returned by
`ReadFile` must be closed by the caller.

## Configuration Model

Venue has one public runtime configuration type:
`github.com/cocosip/venue/config.Config`. `venue.NewVenue` accepts
`*config.Config` directly and clones it before initialization. Mutating the
caller's configuration after `NewVenue` returns does not change the running
instance.

The configuration package provides:

- `config.New()` and `config.DefaultConfig()` for initialized defaults.
- Public fields for direct assignment.
- Chainable `With...` methods for Go-friendly construction.
- `ApplyDefaults`, `Validate`, and `Clone`.
- `json`, `yaml`, and `mapstructure` tags on every bindable field.

The base `config` package does not import Viper, YAML, JSON, or another source
library. It does not read files, bind environment variables, watch for changes,
or own reload policy. Those decisions belong to the application entry point or
a dedicated integration package.

### Configuration Without Viper

Most library consumers can construct configuration directly and do not need a
configuration-file dependency.

```go
cfg := config.New().
    WithMetadataDirectory("./data/metadata").
    WithQuotaDirectory("./data/quota").
    WithAutoCreateTenants(false).
    WithRetryPolicy(
        config.NewRetryPolicyConfig().
            WithMaxRetryCount(5).
            WithInitialRetryDelay(2 * time.Second).
            WithExponentialBackoff(true).
            WithMaxRetryDelay(time.Minute),
    ).
    WithMetadata(
        config.NewMetadataConfig().
            WithCacheTTL(10 * time.Minute).
            WithMaxCacheEntries(20_000),
    ).
    WithBadgerDB(
        config.NewBadgerDBConfig().
            WithGCInterval(15 * time.Minute).
            WithGCDiscardRatio(0.5).
            WithSyncWrites(true),
    ).
    WithVolumes(
        config.NewVolumeConfig().
            WithVolumeID("primary").
            WithMountPath("./data/storage-primary").
            WithShardingDepth(2).
            WithFsync(true),
        config.NewVolumeConfig().
            WithVolumeID("secondary").
            WithMountPath("./data/storage-secondary").
            WithShardingDepth(2),
    ).
    WithTenants(
        config.NewTenantConfig("tenant-001").WithQuota(100_000),
        config.NewTenantConfig("tenant-002").WithoutQuota(),
    ).
    WithBackgroundCleanupEnabled(true).
    WithCleanup(
        config.NewCleanupConfig().
            WithCleanupInterval(30 * time.Minute).
            WithProcessingTimeout(10 * time.Minute).
            WithPermanentlyFailedFileCleanup(true).
            WithCompletedRecordCleanup(true).
            WithCompletedRecordRetention(0),
    )

runtime, err := venue.NewVenue(cfg)
```

Direct field assignment is also supported:

```go
cfg := config.New()
cfg.MetadataDirectory = "./data/metadata"
cfg.QuotaDirectory = "./data/quota"
cfg.Volumes = []config.VolumeConfig{
    {
        VolumeID:      "primary",
        MountPath:     "./data/storage",
        VolumeType:    "LocalFileSystem",
        ShardingDepth: 2,
    },
}
```

For a direct JSON decoder, initialize defaults first and decode over them:

```go
cfg := config.New()

data, err := os.ReadFile("venue.json")
if err != nil {
    return err
}
if err := json.Unmarshal(data, cfg); err != nil {
    return err
}

cfg.ApplyDefaults()
if err := cfg.Validate(); err != nil {
    return err
}

runtime, err := venue.NewVenue(cfg)
```

The same ownership model applies to a YAML decoder: the application reads the
source, decodes into `config.Config`, validates it, and passes the result to
`venue.NewVenue`. Venue never opens or watches the configuration source.

Prefer starting with `config.New()` instead of a zero-value `config.Config`.
`ApplyDefaults` fills missing paths, durations, and capacities, but it cannot
distinguish an omitted boolean from an explicitly configured `false`. Starting
with initialized defaults preserves boolean defaults such as database health
checks.

Applications may bind another configuration system directly to `config.Config`
using its `json`, `yaml`, and `mapstructure` tags. When doing so, the application
must configure duration conversion, call `ApplyDefaults`, call `Validate`, and
decide how reloads replace Venue instances.

The configuration lifecycle is:

1. Start with `config.New()` or `config.DefaultConfig()`.
2. Apply direct assignments, fluent methods, or a source decoder.
3. Call `ApplyDefaults()` when using an external decoder that may leave nested
   numeric, duration, or path values unset.
4. Call `Validate()` when configuration errors must be reported before runtime
   construction. `venue.NewVenue` also applies defaults and validates.
5. Pass the config to `venue.NewVenue`. The runtime clones it, so later caller
   mutations do not reconfigure the running instance.

### Top-Level Configuration

| Go field | JSON/YAML/Viper key | Purpose |
| --- | --- | --- |
| `MetadataDirectory` | `metadataDirectory` | BadgerDB file metadata root |
| `QuotaDirectory` | `quotaDirectory` | Directory-quota database root |
| `FileWatcherConfigurationDirectory` | `fileWatcherConfigurationDirectory` | Persisted watcher configuration root |
| `AutoCreateTenants` | `autoCreateTenants` | Allow unknown tenants to be created on demand |
| `DefaultTenantQuota` | `defaultTenantQuota` | Default maximum file count; `0` is unlimited |
| `EnableDatabaseHealthCheck` | `enableDatabaseHealthCheck` | Start database and volume health checks |
| `RetryPolicy` | `retryPolicy` | Retry count, delay, and backoff behavior |
| `TenantManager` | `tenantManagerOptions` | Tenant metadata path and cache TTL |
| `Metadata` | `metadataOptions` | Active metadata cache settings |
| `BadgerDB` | `badgerDBOptions` | BadgerDB GC, cache, sizing, and sync-write settings |
| `Volumes` | `volumes` | Storage volumes available for writes |
| `Tenants` | `tenants` | Statically configured tenants |
| `FileWatchers` | `fileWatchers` | Directory import watchers |
| `EnableBackgroundCleanup` | `enableBackgroundCleanup` | Start the periodic cleanup service |
| `Cleanup` | `cleanupOptions` | Cleanup intervals, retention, and processing timeout |
| `DatabaseHealthCheck` | `databaseHealthCheckOptions` | Health-check retry and scheduling settings |
| `Logging` | not bindable | Instance-scoped `slog.Handler`; inject in Go code only |

Tenant and directory quotas count managed files, not bytes. Pending,
processing, completed-but-not-yet-cleaned, retryable-failed, and permanently
failed records continue to consume quota. Venue reconstructs both tenant and
directory counts from persisted metadata during startup, while retaining
explicit directory limits. A tenant quota of `0` is unlimited; a nil tenant
quota inherits `DefaultTenantQuota`.

### Nested Fluent Configuration

Each configuration module has its own constructor and chainable methods:

| Type | Constructor | Purpose |
| --- | --- | --- |
| `Config` | `config.New()` | Top-level runtime configuration |
| `VolumeConfig` | `config.NewVolumeConfig()` | Volume ID, mount path, sharding, fsync |
| `TenantConfig` | `config.NewTenantConfig(id)` | Tenant status and quota |
| `RetryPolicyConfig` | `config.NewRetryPolicyConfig()` | Retry count and backoff |
| `TenantManagerConfig` | `config.NewTenantManagerConfig()` | Tenant metadata and cache |
| `MetadataConfig` | `config.NewMetadataConfig()` | Active metadata cache |
| `BadgerDBConfig` | `config.NewBadgerDBConfig()` | BadgerDB sizing, GC, and sync writes |
| `FileWatcherConfig` | `config.NewFileWatcherConfig()` | Watched-directory imports |
| `CleanupConfig` | `config.NewCleanupConfig()` | Cleanup and processing timeouts |
| `DatabaseHealthCheckConfig` | `config.NewDatabaseHealthCheckConfig()` | Startup and periodic checks |

Collection methods have explicit semantics:

- `WithVolumes`, `WithTenants`, and `WithFileWatchers` replace the collection.
- `AddVolume`, `AddTenant`, and `AddFileWatcher` append one entry.
- Values passed through those methods are copied into the top-level configuration.

### Default Values

The important runtime defaults are:

| Setting | Default |
| --- | --- |
| Metadata directory | `./venue-metadata` |
| Quota directory | `./venue-quota` |
| Watcher configuration directory | `./venue-watchers` |
| Automatic tenant creation | `true` |
| Default tenant quota | `0` (unlimited) |
| Database health check | enabled |
| Maximum retry count | `3` |
| Initial retry delay | `5s` |
| Exponential backoff | enabled |
| Maximum retry delay | `5m` |
| Tenant cache TTL | `5m` |
| Metadata cache TTL | `5m` |
| Maximum metadata cache entries | `10,000` |
| BadgerDB GC interval | `10m` |
| BadgerDB GC discard ratio | `0.5` |
| BadgerDB memtable size | `32 MiB` |
| BadgerDB value-log file size | `64 MiB` |
| BadgerDB block cache size | `64 MiB` |
| BadgerDB synchronous writes | disabled |
| Default volume sharding depth | `2` |
| Default volume fsync | enabled |
| Background cleanup | enabled |
| Cleanup interval | `1h` |
| Processing timeout | `30m` |
| Permanently failed retention | `3d` |
| Completed-file cleanup | enabled |
| Completed-file retention | `0` (next cleanup cycle) |

See [`venue-config-example.yaml`](venue-config-example.yaml) for all bindable
settings.

## Configuration With Viper

Viper support is optional and lives in the top-level
`github.com/cocosip/venue/viperconfig` package. The dependency direction is
`viperconfig -> config`; the Venue runtime and the base configuration package do
not import Viper.

### Load a File

`LoadFromFile` supports formats understood by Viper and prefers a top-level
`venue` section. If that section is absent, it loads the root object.

```go
import (
    "github.com/cocosip/venue"
    "github.com/cocosip/venue/viperconfig"
)

cfg, err := viperconfig.LoadFromFile("venue.yaml")
if err != nil {
    return err
}

runtime, err := venue.NewVenue(cfg)
```

Example YAML with a `venue` section:

```yaml
venue:
  metadataDirectory: ./data/metadata
  quotaDirectory: ./data/quota
  autoCreateTenants: false

  retryPolicy:
    maxRetryCount: 5
    initialRetryDelay: 2s
    useExponentialBackoff: true
    maxRetryDelay: 1m

  volumes:
    - volumeId: primary
      mountPath: ./data/storage
      volumeType: LocalFileSystem
      shardingDepth: 2
      enableFsync: true

  tenants:
    - tenantId: tenant-001
      enabled: true
      quota: 100000

  enableBackgroundCleanup: true
  cleanupOptions:
    cleanupInterval: 30m
    processingTimeout: 10m
    cleanupTimedOutFiles: true
    cleanupCompletedRecords: true
    completedRecordRetentionPeriod: 0s
```

Use `LoadFromFileSection(path, "")` to force root-level loading, or pass another
section name when Venue is nested under a different application key.

### Use an Existing Viper Instance

Applications that already own a Viper instance can apply files, environment
variables, command-line overrides, or remote sources before adapting it:

```go
import (
    "strings"

    "github.com/cocosip/venue"
    "github.com/cocosip/venue/viperconfig"
    "github.com/spf13/viper"
)

source := viper.New()
source.SetConfigFile("application.yaml")
source.SetEnvPrefix("APP")
source.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
source.AutomaticEnv()
if err := source.BindEnv("venue.retryPolicy.maxRetryCount"); err != nil {
    return err
}
if err := source.ReadInConfig(); err != nil {
    return err
}

cfg, err := viperconfig.LoadSection(source, "venue")
if err != nil {
    return err
}
runtime, err := venue.NewVenue(cfg)
```

With this setup, `APP_VENUE_RETRYPOLICY_MAXRETRYCOUNT=9` overrides the file
value. Register environment-only keys with `BindEnv`; Viper cannot unmarshal a
key that exists only in the environment unless the key is known to it.

Use `viperconfig.Load(source)` when the supplied Viper instance already points
at Venue's root configuration. Use `LoadSection(source, "venue")` when Venue is
nested inside a larger application configuration. Both functions decode into a
new `config.Config`, apply missing defaults, validate the result, and return an
error instead of a partially valid configuration.

`viperconfig.NewWithDefaults()` returns a Viper instance with Venue scalar
defaults registered. The application still owns source precedence, environment
key mapping, file watching, reload timing, and instance replacement.

`WatchConfig` does not mutate an existing Venue runtime. On a Viper change
event, load and validate a new `config.Config`, then replace the Venue instance
using application-controlled lifecycle coordination. This avoids partially
applying configuration while file operations are active.

### Logging After File Binding

Logging contains a live `slog.Handler` and is deliberately excluded from every
configuration-file binding tag:

```go
json:"-" yaml:"-" mapstructure:"-"
```

Inject logging after loading the file:

```go
import (
    "log/slog"
    "os"

    "github.com/cocosip/venue"
    "github.com/cocosip/venue/pkg/logging"
    "github.com/cocosip/venue/viperconfig"
)

cfg, err := viperconfig.LoadFromFile("venue.yaml")
if err != nil {
    return err
}

cfg.WithLogging(&logging.Config{
    Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelInfo,
    }),
})

runtime, err := venue.NewVenue(cfg)
```

The logging import path is `github.com/cocosip/venue/pkg/logging`.

## Logging

Venue is silent when `Config.Logging` is nil. A configured handler belongs to
one Venue instance and is never installed as the process default.

- Venue never reads, replaces, or writes through `slog.Default()`.
- The caller owns the handler and its underlying writer.
- Stopping Venue does not close application-owned writers.
- Handler panics are isolated from storage and queue operations.
- Handler level filtering controls which records are emitted.

This allows multiple Venue instances in one process to use different handlers,
levels, and destinations.

## Queue Workflow

```text
WriteFile
   |
   v
Pending -> Processing -> MarkAsCompleted -> Completed
              |                              |
              |                              +-> completed-file cleanup
              |                                  -> physical file and metadata removed
              |
              +-> MarkAsFailed -> Pending after retry delay
                                   |
                                   +-> PermanentlyFailed after retry limit
```

Completion is a two-phase operation. `MarkAsCompleted` first commits the
`Completed` state and releases the processing lease. The physical file and its
metadata remain available until completed-file cleanup runs after
`CompletedRecordRetentionPeriod`. The default retention is zero, so an enabled
background cleanup service removes them on its next cycle. If background
cleanup is disabled, applications can invoke
`Venue.CleanupService().CleanupCompletedFiles` explicitly.

File reads, queue allocation, and status queries use an explicit
`core.TenantContext`; completion and failure use the tenant identity embedded in
the processing lease. Metadata and cache entries are isolated by
`(tenantID, fileKey)`.

Queue allocation returns a `FileLocation` with a `FileProcessingLease`. The
lease contains the tenant ID, file key, and processing start time for that exact
claim. Workers must pass the same lease to `MarkAsCompleted` or `MarkAsFailed`.
If a timeout recovery or another worker has already replaced the claim, Venue
returns `core.ErrProcessingLeaseMismatch` and leaves the active processing
attempt unchanged. Use `errors.Is` for classification and `errors.As` with
`*core.FileProcessingLeaseMismatchError` when the current status and lease time
are needed for diagnostics.

The main storage-pool operations are:

| Operation | Behavior |
| --- | --- |
| `WriteFile` | Stores a stream and returns a generated file key |
| `ReadFile` | Returns an `io.ReadCloser` for an existing file |
| `GetFileInfo` | Returns caller-safe metadata |
| `GetFileLocation` | Returns storage diagnostics including volume and path |
| `GetNextFileForProcessing` | Atomically claims the next pending file and returns its lease |
| `GetNextBatchForProcessing` | Atomically claims up to the requested batch size with leases |
| `MarkAsCompleted` | Commits `Completed` for the matching lease; cleanup removes the managed file later |
| `MarkAsFailed` | Applies retry or permanent-failure state to the matching lease |
| `GetFileStatus` | Returns the current queue state |

## Build and Verification

```powershell
go build ./...
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
```

On a restricted Windows environment, repository-local ignored caches can be
used:

```powershell
$env:GOCACHE='D:\Code\go\venue\tmp\go-build'
$env:GOLANGCI_LINT_CACHE='D:\Code\go\venue\tmp\golangci-lint'
```

## Benchmarks

Configuration benchmarks cover fluent construction and cloning. System
benchmarks initialize through `venue.NewVenue(*config.Config)` so they exercise
the same composition path used by applications.

The published cases measure:

- `BenchmarkConfigFluentConstruction`: building a complete nested configuration
  with fluent methods.
- `BenchmarkConfigClone`: cloning configuration, including slices and nested
  quota pointers.
- `BenchmarkWriteFile`: writing a small file plus metadata and quota updates.
- `BenchmarkReadFile`: opening and closing an existing small file.
- `BenchmarkCompleteWorkflow`: write, queue claim, and durable transition to
  `Completed`. Deferred completed-file cleanup is outside the timed loop.

Run all benchmarks:

```powershell
go test -run '^$' -bench=. -benchmem ./...
```

Commands used for the published snapshot:

```powershell
go test -run '^$' -bench '^Benchmark(ConfigFluentConstruction|ConfigClone)$' -benchmem -benchtime=1s -count=3 ./config
go test -run '^$' -bench '^Benchmark(WriteFile|ReadFile|CompleteWorkflow)$' -benchmem -benchtime=1s -count=3 ./test/benchmark
```

Median results from three local runs on 2026-09-17:

- Go 1.27.1
- Windows/amd64
- Intel Core Ultra 9 185H

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkConfigFluentConstruction-22` | 230.2 | 992 | 5 |
| `BenchmarkConfigClone-22` | 253.9 | 712 | 6 |
| `BenchmarkWriteFile-22` | 3,718,046 | 11,828 | 182 |
| `BenchmarkReadFile-22` | 54,621 | 1,465 | 9 |
| `BenchmarkCompleteWorkflow-22` | 3,327,882 | 27,576 | 435 |

The file benchmarks use small byte readers, temporary local directories, and
the durable default with volume fsync enabled.
Results depend on storage hardware, antivirus scanning, power policy, Go
version, and other host activity. They are regression evidence for this
implementation, not a cross-machine performance guarantee.

## Repository Layout

```text
config/              Public source-independent configuration model
viperconfig/         Optional Viper adapter
pkg/core/            Interfaces, models, statuses, and domain errors
pkg/metadata/        BadgerDB metadata and active-data cache
pkg/pool/            Storage-pool facade
pkg/scheduler/       Queue scheduling and retries
pkg/tenant/          Tenant management
pkg/quota/           Tenant and directory quotas
pkg/volume/          Storage volumes and path safety
pkg/cleanup/         Cleanup services
pkg/watcher/         Watched-directory imports
pkg/health/          Database and volume health checks
pkg/logging/         Instance-scoped slog runtime
examples/            Runnable examples
test/benchmark/      Public-entry system benchmarks
```

See [`examples/README.md`](examples/README.md) for runnable examples and
[`venue-config-example.yaml`](venue-config-example.yaml) for the complete file
configuration shape.

## License

MIT
