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

Choose one of the two configuration examples below, then create and start a
Venue instance. The runtime owns storage paths and queue state; callers only
provide tenant context, file content, and the generated `fileKey`.

- [Load configuration from YAML](#load-configuration-from-yaml)
- [Configure directly in Go](#configure-directly-in-go)

`Start` starts the enabled background services. `Stop` stops those services in
reverse order and closes the repositories. Every `io.ReadCloser` returned by
`ReadFile` must be closed by the caller.

## Configuration

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
or own reload policy. Those decisions belong to the application entry point.

### Load Configuration From YAML

The optional `viperconfig` adapter reads a Viper-supported file, prefers a
top-level `venue` section, applies defaults, validates the result, and returns
`*config.Config`. Venue never watches or reloads the source by itself.

```go
cfg, err := viperconfig.LoadFromFile("venue.yaml")
if err != nil {
    return err
}

runtime, err := venue.NewVenue(cfg)
if err != nil {
    return err
}
if err := runtime.Start(); err != nil {
    return err
}
defer runtime.Stop()
```

```yaml
venue:
  metadataDirectory: ./data/metadata
  quotaDirectory: ./data/quota
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
```

The complete bindable file shape is in
[`venue-config-example.yaml`](venue-config-example.yaml). Use
`LoadFromFileSection(path, "")` for a root-level file or
`LoadSection(source, "venue")` when the application already owns a Viper
instance. Environment variables, file watching, and replacement of a running
instance remain application responsibilities.

### Configure Directly In Go

Use `config.New()` to retain boolean and nested defaults, then override only
the values needed by the application. `WithVolumes`, `WithTenants`, and
`WithFileWatchers` replace collections; the corresponding `Add...` methods
append entries.

```go
cfg := config.New().
    WithMetadataDirectory("./data/metadata").
    WithQuotaDirectory("./data/quota").
    WithVolumes(
        config.NewVolumeConfig().
            WithVolumeID("primary").
            WithMountPath("./data/storage").
            WithShardingDepth(2),
    ).
    WithTenants(
        config.NewTenantConfig("tenant-001").WithQuota(100_000),
    ).
    WithRetryPolicy(
        config.NewRetryPolicyConfig().
            WithMaxRetryCount(5).
            WithInitialRetryDelay(2 * time.Second),
    )

if err := cfg.Validate(); err != nil {
    return err
}
runtime, err := venue.NewVenue(cfg)
```

The runtime clones the configuration during construction, so mutating `cfg`
after `NewVenue` returns does not reconfigure the running instance.

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

Viper support is optional and lives in the top-level
`github.com/cocosip/venue/viperconfig` package. The dependency direction is
`viperconfig -> config`; applications that do not need file binding do not
depend on Viper. `Logging` is runtime-only (`json:"-" yaml:"-"
mapstructure:"-"`) and must be injected from Go code after file binding.

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

## API Usage

Venue is a managed file queue, not a caller-managed filesystem. `WriteFile`
generates the `fileKey`; callers never choose a physical path or volume.

```text
WriteFile -> Pending -> GetNextFileForProcessing -> Processing
                                                       |
                         MarkAsFailed <- retry --------+
                                                       |
                                      MarkAsCompleted -> Completed -> cleanup
```

### Runtime Lifecycle

Create one runtime for each independent configuration, start it before using
background services, and stop it during application shutdown:

| Method | Purpose |
| --- | --- |
| `venue.NewVenue(*config.Config)` | Clone, validate, and initialize one runtime |
| `Venue.Start()` | Start enabled cleanup, watcher, and health services |
| `Venue.Stop()` | Stop services and close repositories |
| `Venue.IsRunning()` | Report whether the runtime is started |

`Stop` is safe to call from a deferred shutdown path. The caller owns any
logging handler and writer configured on the instance.

### StoragePool

`Venue.StoragePool()` returns the unified storage and queue interface. Every
operation carries an explicit `core.TenantContext` except completion and
failure, which use the tenant-scoped lease returned by queue allocation.

| Method | Use |
| --- | --- |
| `WriteFile(ctx, tenant, content, originalName)` | Store a stream and generate a `fileKey` |
| `WriteFileToDirectory(ctx, tenant, content, originalName, directory)` | Store a file and account it against a logical directory quota |
| `ReadFile(ctx, tenant, fileKey)` | Open an `io.ReadCloser`; the caller must close it |
| `GetFileInfo(ctx, tenant, fileKey)` | Read size, status, and creation time |
| `GetFileLocation(ctx, tenant, fileKey)` | Read volume and physical-path diagnostics |
| `GetNextFileForProcessing(ctx, tenant)` | Atomically claim the next available file |
| `GetNextBatchForProcessing(ctx, tenant, batchSize)` | Atomically claim up to `batchSize` files |
| `MarkAsCompleted(ctx, lease)` | Commit successful processing; cleanup deletes later |
| `MarkAsFailed(ctx, lease, message)` | Schedule retry or mark permanently failed |
| `GetFileStatus(ctx, tenant, fileKey)` | Read the current queue status |
| `GetTotalCapacity(ctx)` | Sum capacity across configured volumes |
| `GetAvailableSpace(ctx)` | Sum available space across configured volumes |

`GetNextFileForProcessing` and `GetNextBatchForProcessing` are atomic, so
concurrent workers do not receive the same pending file. An empty result means
there is currently no available work; it is not an error.

### TenantManager

`Venue.TenantManager()` returns the tenant lifecycle and isolation interface:

| Method | Use |
| --- | --- |
| `GetTenant(ctx, tenantID)` | Resolve a tenant context; may auto-create it |
| `IsTenantEnabled(ctx, tenantID)` | Check whether a tenant can perform operations |
| `CreateTenant(ctx, tenantID)` | Create a tenant explicitly |
| `EnableTenant(ctx, tenantID)` / `DisableTenant(ctx, tenantID)` | Change tenant availability |
| `GetAllTenants(ctx)` | List all known tenants |

All reads, writes, claims, status changes, and quota operations are scoped by
tenant. The same `fileKey` in two tenants refers to two different files.

### Process One File

The lease must be passed unchanged to `MarkAsCompleted` or `MarkAsFailed`:

```go
ctx := context.Background()
tenant, err := runtime.TenantManager().GetTenant(ctx, "tenant-001")
if err != nil {
    return err
}

originalName := "invoice.pdf"
fileKey, err := runtime.StoragePool().WriteFile(
    ctx, tenant, strings.NewReader("file contents"), &originalName,
)
if err != nil {
    return err
}
fmt.Printf("queued %s\n", fileKey)

file, err := runtime.StoragePool().GetNextFileForProcessing(ctx, tenant)
if err != nil {
    return err
}
if file == nil || file.Lease == nil {
    return fmt.Errorf("no file available for processing")
}

reader, err := runtime.StoragePool().ReadFile(ctx, tenant, file.FileKey)
if err != nil {
    return err
}
_, processErr := io.Copy(io.Discard, reader) // replace with application processing
closeErr := reader.Close()
if processErr != nil {
    if markErr := runtime.StoragePool().MarkAsFailed(
        ctx, *file.Lease, "processing failed",
    ); markErr != nil {
        return fmt.Errorf("process %s: %v; mark failed: %w", file.FileKey, processErr, markErr)
    }
    return fmt.Errorf("process %s: %w", file.FileKey, processErr)
}
if closeErr != nil {
    if markErr := runtime.StoragePool().MarkAsFailed(
        ctx, *file.Lease, "close failed",
    ); markErr != nil {
        return fmt.Errorf("close %s: %v; mark failed: %w", file.FileKey, closeErr, markErr)
    }
    return fmt.Errorf("close %s: %w", file.FileKey, closeErr)
}

return runtime.StoragePool().MarkAsCompleted(ctx, *file.Lease)
```

Completion first commits `Completed` and releases the processing lease. The
physical file and metadata remain until the cleanup service reaches the
configured `CompletedRecordRetentionPeriod`; with the default zero retention,
the next cleanup cycle removes them. When background cleanup is disabled,
invoke `runtime.CleanupService().CleanupCompletedFiles` explicitly.

### Process a Batch

Use the batch method when the worker can process several claims concurrently:

```go
files, err := runtime.StoragePool().GetNextBatchForProcessing(ctx, tenant, 100)
if err != nil {
    return err
}
for _, file := range files {
    if file.Lease == nil {
        continue
    }
    // Read and process file.FileKey, then use this exact lease:
    if err := runtime.StoragePool().MarkAsCompleted(ctx, *file.Lease); err != nil {
        return err
    }
}
```

If timeout recovery or another worker has replaced a claim, completion or
failure returns `core.ErrProcessingLeaseMismatch` and does not change the newer
claim. Use `errors.Is` for classification and `errors.As` with
`*core.FileProcessingLeaseMismatchError` for diagnostics.

### Other Runtime Services

The runtime exposes the lower-level services for operational workflows:

| Accessor | Interface |
| --- | --- |
| `FileScheduler()` | Queue transitions and timeout recovery |
| `TenantQuotaManager()` | Tenant file-count quota checks and updates |
| `DirectoryQuotaManager()` | Logical-directory file-count quotas |
| `CleanupService()` | Completed, failed, timed-out, orphan, and database cleanup |
| `FileWatcher()` | Watched-directory import management |
| `DatabaseHealthChecker()` | Startup and on-demand database health checks |
| `Volumes()` | Configured storage volumes and their health/capacity |
| `Config()` | Cloned runtime configuration |
| `Logging()` | Instance-scoped logging runtime |

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
