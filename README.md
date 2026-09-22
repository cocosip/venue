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
- Optional orphan-file recovery that rebuilds metadata lost in the write window.
- Optional bounded in-process statistics with an optional periodic log summary.
- Opt-in consistent metadata backups with offline restore.
- Per-volume startup health retry and an optional advisory write-path warm-up.
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
| `MetadataDirectory` | `metadataDirectory` | Per-tenant SQLite metadata database root |
| `QuotaDirectory` | `quotaDirectory` | Per-tenant SQLite directory-quota database root |
| `FileWatcherConfigurationDirectory` | `fileWatcherConfigurationDirectory` | Watcher runtime state root: imported-file history and enable/disable state |
| `AutoCreateTenants` | `autoCreateTenants` | Allow unknown tenants to be created on demand |
| `DefaultTenantQuota` | `defaultTenantQuota` | Default maximum file count; `0` is unlimited |
| `EnableDatabaseHealthCheck` | `enableDatabaseHealthCheck` | Start database and volume health checks |
| `FailFastOnStartupRecoveryFailure` | `failFastOnStartupRecoveryFailure` | Fail startup when a metadata database was quarantined and no backup could be restored |
| `RetryPolicy` | `retryPolicy` | Retry count, delay, and backoff behavior |
| `TenantManager` | `tenantManagerOptions` | Tenant metadata path and cache TTL |
| `Metadata` | `metadataOptions` | Active metadata cache settings |
| `Sqlite` | `sqliteOptions` | SQLite journal, synchronous, cache, handle, recovery, and backup settings |
| `Volumes` | `volumes` | Storage volumes available for writes |
| `Tenants` | `tenants` | Statically configured tenants |
| `FileWatchers` | `fileWatchers` | Directory import watchers |
| `FileWatcherRoots` | `watcherRoots` | Root templates that derive one watcher per tenant directory |
| `FileWatcherService` | `watcherServiceOptions` | Global watcher service enablement, polling bounds, and scan parallelism |
| `EnableBackgroundCleanup` | `enableBackgroundCleanup` | Start the periodic cleanup service |
| `Cleanup` | `cleanupOptions` | Cleanup intervals, retention, and processing timeout |
| `OrphanRecovery` | `orphanRecoveryOptions` | Optional orphan-file recovery (disabled by default) |
| `DatabaseHealthCheck` | `databaseHealthCheckOptions` | Health-check retry and scheduling settings |
| `Statistics` | `statisticsOptions` | In-process runtime statistics and optional periodic log output |
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
| `SqliteConfig` | `config.NewSqliteConfig()` | SQLite journal, synchronous, cache, and backup settings |
| `FileWatcherConfig` | `config.NewFileWatcherConfig()` | Watched-directory imports |
| `FileWatcherRootConfig` | `config.NewFileWatcherRootConfig(path)` | Per-tenant watcher derivation from a root |
| `FileWatcherServiceConfig` | `config.NewFileWatcherServiceConfig()` | Global watcher service options |
| `CleanupConfig` | `config.NewCleanupConfig()` | Cleanup and processing timeouts |
| `OrphanRecoveryConfig` | `config.NewOrphanRecoveryConfig()` | Opt-in orphan-file recovery |
| `DatabaseHealthCheckConfig` | `config.NewDatabaseHealthCheckConfig()` | Startup and periodic checks |
| `StatisticsConfig` | `config.NewStatisticsConfig()` | In-process statistics, dimensions, and periodic output |
| `StatisticsDimensionConfig` | `config.NewStatisticsDimensionConfig()` | Retained statistics dimensions |
| `StatisticsOutputConfig` | `config.NewStatisticsOutputConfig()` | Periodic statistics log output |
| `DeadLetterConfig` | `config.NewDeadLetterConfig()` | Dead-letter root, tenant/date partitioning, and sharding |
| `RetiredVolumeConfig` | `config.NewRetiredVolumeConfig(id)` | Retired-volume disposition |

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
| SQLite journal mode | `WAL` |
| SQLite synchronous mode | `NORMAL` |
| SQLite page cache | `-4000` (4,000 KiB, one cache per tenant connection) |
| SQLite busy timeout | `5000` (`5s`) |
| SQLite WAL checkpoint after batch | disabled |
| SQLite connections per tenant | `1` |
| Simultaneously open tenant databases | unlimited (`0`); handles live until `Stop` |
| Idle tenant database timeout | `0` (handles are never reclaimed) |
| SQLite backup integrity verification | enabled (`skipBackupVerification: false`) |
| VACUUM of idle tenant databases | disabled |
| Default volume sharding depth | `2` |
| Default volume fsync | enabled |
| Background cleanup | enabled |
| Cleanup interval | `1h` |
| Processing timeout | `30m` |
| Timed-out reclaim on empty queue | enabled (`30s` per-tenant cooldown) |
| Corrupted database recovery | disabled (quarantined tenant database files are kept `72h`) |
| Volume health probe cache | `30s` |
| Volume startup initial delay | `2s` (applied before the first retry only) |
| Volume startup health-check delay | `500ms` between attempts |
| Volume warm-up write | disabled |
| Permanently failed retention | `3d` |
| Permanently failed disposition | `MoveToDeadLetter` |
| Dead-letter root | `.deadletter` (per volume, tenant + date partition, shard depth `2`) |
| Junk-file cleanup | enabled (`20m` minimum sweep interval) |
| Invalid database backup cleanup | enabled |
| Completed-file cleanup | enabled |
| Completed-file retention | `0` (next cleanup cycle) |
| Orphaned-metadata cleanup | disabled (opt-in; one existence check per record) |
| Orphan-file recovery | disabled (`6h` interval, `10s` initial delay when enabled) |
| Empty-queue timed-out reclaim batch | `32` |
| Background timed-out reclaim | enabled (`8` per pass) |
| Metadata backup runner | disabled (empty `backupDirectory`); `1h` interval and `168h` retention per tenant when enabled |
| Automatic restore from backup | disabled (per tenant; requires recovery and a backup directory) |
| Fail-fast on unrecoverable startup recovery | disabled (degraded state is reported and startup continues) |
| Runtime statistics | disabled (`5m` window, `1h` retention, `16,384` max series) |
| Statistics dimensions | `volume_id`, `watcher_id`, `operation` retained; `tenant_id` dropped |
| Statistics log output | disabled (`Logging` sink, `1m` interval, `15m` query window) |
| Advanced watcher settings | `60s` tenant-directory cache, `100ms` stability delay, `1m` stability skip age |
| Watcher history throttle/debounce | both enabled (`5m` prune interval, `2s` flush interval) |
| Watcher polling interval | `30s` |
| Watcher polling bounds | `5s` minimum, `1h` maximum |
| Watcher parallel scans | `4` |
| Watcher minimum file age | `5s` |
| Watcher concurrent imports | `4` |

Watcher defaults are applied for durations, capacities, patterns, and the
post-import action. `enabled` and `includeSubdirectories` are booleans: the Go
constructor `config.NewFileWatcherConfig()` sets them to `true` (matching Locus),
but a boolean key that is absent from a bound file cannot be distinguished from
an explicit `false`. Always set both explicitly in file configuration.

The two advanced housekeeping switches are the deliberate exception:
`disableImportedFilesPruneThrottle` and
`disableImportedFilesHistoryFlushDebounce` are named in the negative precisely so
that their zero value means "keep the default behavior" (the throttle and the
debounce are on), and only an explicit `true` turns them off.

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
| `Venue.Start()` | Start enabled cleanup, orphan-recovery, watcher, and health services |
| `Venue.Stop()` | Stop services, close repositories, and release database locks |
| `Venue.IsRunning()` | Report whether the runtime is started |

`NewVenue` opens the metadata and quota repositories, so `Stop` owns releasing
them. `Stop` is idempotent and safe in a deferred shutdown path: calling it
without `Start` still closes the repositories and releases the per-tenant SQLite
handles, and calling it repeatedly returns `nil`. After `Stop`, the synchronous
accessors must not be used, and a stopped runtime cannot be restarted — create a
new instance instead. The caller owns any logging handler and writer configured
on the instance.

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
there is currently no available work; it is **not** an error:
`GetNextFileForProcessing` returns `(nil, nil)` and `GetNextBatchForProcessing`
returns an empty slice. Only real failures (for example a disabled tenant or an
unavailable database) are returned as errors.

### Tenant Identifiers

Tenant IDs come from callers and configuration, and Venue uses them as path
segments for tenant metadata and physical storage. Every entry point therefore
validates them first: `GetTenant`, `CreateTenant`, `EnableTenant`,
`DisableTenant`, `IsTenantEnabled`, and configuration validation reject IDs
that contain `/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`, control characters, a
leading dot, a trailing dot, surrounding whitespace, or a Windows device name,
as well as IDs longer than 128 bytes. Invalid IDs fail with
`core.ErrInvalidArgument` (traversal-shaped IDs also match
`core.ErrPathTraversalAttempt`), and nothing is written outside the configured
metadata directory.

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
    // The queue is empty; this is not an error.
    return nil
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

Cleanup also removes metadata whose physical file has disappeared, but that
sweep is opt-in (`Cleanup.CleanupOrphanedMetadata`, default `false`) because it
performs one file-existence check per tracked record. Deletion happens only
after the absence is confirmed: a volume that is missing or a read error is
skipped, never treated as "file gone". Enable it when files can disappear
out-of-band, for example on a volume that was lost or pruned manually.

### Permanently Failed Payloads

When a file exceeds its retry budget it becomes `PermanentlyFailed`. Once
`Cleanup.FailedFileRetentionPeriod` elapses, `Cleanup.PermanentlyFailedDisposition`
decides what happens to the payload:

| Disposition | Behaviour |
| --- | --- |
| `MoveToDeadLetter` (default) | Move the payload into the dead-letter area, set `DeadLetteredAt`, and release its tenant/directory quota. The record stays for operator visibility. |
| `Keep` | Leave payload and metadata untouched. The file keeps consuming quota. |
| `Delete` | Delete the payload and the metadata record. |

The dead-letter area is per volume and always stays inside the volume root:

```text
<volume>/<rootPath>/[<tenantId>/][<yyyyMMdd>/][<shard>/...]/<fileKey><ext>
```

with `Cleanup.DeadLetter` controlling `RootPath` (default `.deadletter`),
`IncludeTenantInPath`, `IncludeDatePartition`, and `ShardingDepth` (default `2`).
Dead-lettered records do not count toward quota and cleanup never reaps them, so
retention of dead-lettered payloads stays an operator decision. Set
`permanentlyFailedDisposition: Delete` to restore the previous destructive
behaviour.

### Junk Files, Retired Volumes, Quarantined Databases

- `Cleanup.CleanupJunkFiles` (default `true`) recursively removes `Thumbs.db`,
  `.DS_Store`, and `desktop.ini` from the volumes, throttled by
  `JunkFileCleanupInterval` (default `20m`). Only those names are touched;
  managed payloads (`<32 hex><ext>`) are never matched.
- `Cleanup.RetiredVolumes` declares volumes that were intentionally removed. The
  default `Keep` disposition skips their records as before; `PurgeMetadataOnly`
  removes the metadata and quota rows without touching physical storage.
- `Cleanup.CleanupInvalidDatabaseBackups` (default `true`) removes quarantined
  database files (`<db>.corrupted.<stamp>`) whose
  `Sqlite.CorruptedDatabaseRetention` has elapsed. `NewVenue` also prunes them
  during startup.
- `Venue.CleanupService().CumulativeStatistics()` reports process-lifetime
  cleanup totals (monotonic counters), in addition to the per-call statistics
  each operation returns.

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

A worker whose process died leaves its file in `Processing` until either the
cleanup cycle or a reclaim pass releases it. Reclaim is enabled by default on two
independent paths:

- When a claim finds no available work, Venue resets up to
  `Cleanup.EmptyQueueReclaimBatchSize` (default `32`) timed-out files for that
  tenant and retries once. `Cleanup.RecoverTimedOutOnEmptyQueue` disables this
  path, and a negative batch size disables the synchronous reclaim as well.
- A successful claim also triggers an opportunistic background pass bounded by
  `Cleanup.BackgroundTimedOutReclaimBatchSize` (default `8`), so timed-out
  records are recovered even when cleanup is disabled or its interval is long.
  `Cleanup.EnableBackgroundTimedOutReclaim` (default `true`) turns it off.

`Cleanup.TimedOutReclaimCooldown` (default `30s`) bounds each path separately:
the scheduler keeps independent per-tenant cooldown state for the synchronous
empty-queue reclaim and for the background pass, so a recent background pass
cannot delay the emergency empty-queue reclaim, while both paths use the same
configured cooldown value. Reclaim always goes through the lease check, so a
stale worker can never unseat a newer claim.

### Quota Repair

Tenant and directory counts are rebuilt from persisted metadata during
`NewVenue`. If a counter drifts at runtime — a process killed mid-transition, or
metadata changed out-of-band — repair it without a restart:

```go
if err := runtime.ReconcileQuotaCounts(ctx); err != nil {
    return err
}
```

The same recount runs during startup, in bounded pages, so a large store is not
loaded into memory at once.

### Orphan File Recovery

`WriteFile` creates the physical file before the metadata record, so a crash,
power loss, or storage failure inside that window can leave a durable file that
no `fileKey` references. Cleanup only handles the opposite direction (metadata
whose physical file is gone); such a file would stay unreachable and keep
consuming disk space.

Orphan recovery closes that window. It is **disabled by default** and must be
enabled explicitly:

```go
cfg := config.New().
    WithVolumes(/* ... */).
    WithOrphanRecovery(
        config.NewOrphanRecoveryConfig().
            WithEnabled(true).
            WithRunOnStartup(true).
            WithRecoveryInterval(6 * time.Hour),
    )
```

```yaml
venue:
  orphanRecoveryOptions:
    enabled: true
    runOnStartup: true
    recoveryInterval: 6h
    initialDelay: 10s
```

When enabled, `Venue.Start` runs a periodic scan; `Venue.OrphanRecovery()
.RecoverNow(ctx)` performs one scan on demand and returns a
`core.OrphanRecoveryReport`. Recovery is deliberately conservative: a file is
only re-registered when its volume-relative path resolves to a valid tenant ID
and a 32-character hexadecimal `fileKey`, and when no metadata exists for that
pair. Recovered files are inserted as `Pending` with the root logical directory
and immediately re-enter the queue. Quota increments that cannot be applied are
rolled back, so recovery never pushes a tenant past its quota.

### Other Runtime Services

The runtime exposes the lower-level services for operational workflows:

| Accessor | Interface |
| --- | --- |
| `FileScheduler()` | Queue transitions and timeout recovery |
| `TenantQuotaManager()` | Tenant file-count quota checks and updates |
| `TenantQuotaAdministrator()` | Optional per-tenant and global limit administration; `(nil, false)` only for a caller-supplied manager |
| `DirectoryQuotaManager()` | Logical-directory file-count quotas |
| `MetadataRepository()` | Metadata persistence and status queries |
| `CleanupService()` | Completed, failed, timed-out, orphan-metadata, junk-file, and database cleanup |
| `OrphanRecovery()` | Orphan-file recovery (`nil` unless `OrphanRecovery.Enabled`) |
| `FileWatcher()` | Watched-directory import management |
| `FileWatcherAutoManager()` | Watcher derivation from configured roots (`nil` when no roots) |
| `FileWatcherService()` | Background watcher service, including its global options |
| `DatabaseHealthChecker()` | Startup and on-demand database health checks |
| `Statistics()` | Aggregated in-process statistics snapshots (never `nil`) |
| `StatisticsRecorder()` | Records application measurements into the same bounded window set (never `nil`) |
| `MetadataBackupService()` | Periodic metadata backup runner (`nil` unless backups are configured) |
| `Volumes()` | Configured storage volumes and their health/capacity |
| `Config()` | Cloned runtime configuration |
| `Logging()` | Instance-scoped logging runtime |

`TenantQuotaAdministrator` is an optional capability rather than part of
`TenantQuotaManager`, so a read-only or test implementation of the manager stays
valid. The runtime's own manager always provides it:

```go
admin, ok := runtime.TenantQuotaAdministrator()
if ok {
    limit, err := admin.GetEffectiveLimit(ctx, "tenant-001") // override, else global
    if err != nil {
        return err
    }
    if err := admin.SetTenantLimit(ctx, "tenant-001", 250_000); err != nil {
        return err
    }
    if err := admin.RemoveTenantLimit(ctx, "tenant-001"); err != nil { // falls back to the global limit
        return err
    }
    if err := admin.SetGlobalLimit(ctx, int64(limit)); err != nil {
        return err
    }
    _, err = admin.GetGlobalLimit(ctx)
    if err != nil {
        return err
    }
}
```

Two cleanup and recovery capabilities are also optional and are reached by type
assertion, so a minimal `core.CleanupService` or `core.OrphanRecoveryService`
implementation or test double remains valid:

| Capability | Obtained from | Adds |
| --- | --- | --- |
| `core.DatabaseOptimizationService` | `Venue.CleanupService()` | `OptimizeDatabasesDetailed(ctx)` and its reclaimed-byte totals |
| `core.TenantCleanupService` | `Venue.CleanupService()` | `CleanupEmptyDirectoriesForTenant(ctx, tenantID)` |
| `core.TenantOrphanRecoveryService` | `Venue.OrphanRecovery()` | `RecoverOrphanedFilesForTenant(ctx, tenantID)` |

```go
if detailed, ok := runtime.CleanupService().(core.DatabaseOptimizationService); ok {
    result, err := detailed.OptimizeDatabasesDetailed(ctx) // includes reclaimed bytes
}
if perTenant, ok := runtime.CleanupService().(core.TenantCleanupService); ok {
    stats, err := perTenant.CleanupEmptyDirectoriesForTenant(ctx, "tenant-001")
}
```

`EnableWatcher`/`DisableWatcher` decisions are persisted under
`FileWatcherConfigurationDirectory` (`watcher-state.json`, written atomically).
A persisted decision wins over the configured `enabled` flag when the watcher is
registered again, so an operator's choice survives a restart. Persisting is
best-effort: a failed write keeps the in-memory change and never fails the call.
Watcher *definitions* still come from configuration, which the application entry
point owns.

### Deriving Watchers From Roots

`FileWatcherRoots` treats a directory as a template: every immediate
subdirectory becomes one single-tenant watcher, so a multi-tenant deployment
does not enumerate tenants by hand.

```yaml
venue:
  watcherRoots:
    - rootPath: ./venue-data/watch/shared
      multiTenantMode: true
      enabled: true
      includeSubdirectories: true
      filePatterns: ["*.*"]
      postImportAction: Delete
      pollingInterval: 30s
      minFileAge: 5s
      maxConcurrentImports: 4
  watcherServiceOptions:
    enabled: true
    defaultPollingInterval: 30s
    minimumPollingInterval: 5s
    maximumPollingInterval: 1h
    disabledCheckInterval: 1m
    maxParallelWatcherScans: 4
```

Derived watchers get the ID `auto-<rootBase>-<tenantDir>`; a directory name that
is not a valid tenant identifier is skipped and reported. Applying a root is
idempotent, and `Venue.FileWatcherAutoManager().RemoveAllWatchers(ctx)` removes
only derived watchers, never manually registered ones. The global service
options (including enablement) are persisted in
`file-watcher-options.json` under the same configuration directory, and
`Venue.FileWatcherService().UpdateOptions(ctx, ...)` applies them at runtime.
Setting `watcherServiceOptions.enabled: false` really starts the service
disabled: the initial construction state carries the configured value
explicitly, so no watcher is scanned until an operator re-enables it. A
persisted operator decision still outranks the configured value. Like the
per-watcher booleans, this key must be written explicitly in a bound file,
because an absent key also binds as `false`. The service itself only exists when
at least one entry in `fileWatchers` or `watcherRoots` is configured.

The advanced per-watcher settings on `fileWatchers[]` and `watcherRoots[]` tune
the import pipeline:

| Setting | Default | Effect |
| --- | --- | --- |
| `autoCreateTenantDirectoriesCacheTtl` | `60s` | How long the tenant list used by `autoCreateTenantDirectories` is cached |
| `fileStabilityCheckDelay` | `100ms` | Delay before the second stability probe; negative disables the probe |
| `skipStabilityCheckAfterAge` | `1m` | Skip the second probe for candidates at least this old; negative always probes |
| `disableImportedFilesPruneThrottle` | `false` | Deliberately inverted: the prune throttle is **on** by default, so the zero value keeps it on and `true` turns it off |
| `importedFilesPruneInterval` | `5m` | Minimum delay between prune runs while the throttle is on |
| `disableImportedFilesHistoryFlushDebounce` | `false` | Deliberately inverted: the write debounce is **on** by default, so the zero value keeps it on and `true` turns it off |
| `importedFilesHistoryFlushInterval` | `2s` | Minimum delay between import-history persistence writes while the debounce is on |

The double-negative naming is intentional: both switches are on by default, so a
configuration file that omits them keeps the throttled, debounced behavior
instead of silently disabling it.

The `Venue` local filesystem volume also implements the optional volume
capabilities `core.StorageVolumeHealthProbe` (`ProbeHealth`, the forced probe
that bypasses and refreshes the health cache),
`core.StorageVolumeWritePathWarmup` (`WarmWritePathCache`, the throwaway write
behind `volumes[].warmupOnStartup`), and
`core.StorageVolumeWritePathDiagnostics` (`WritePathStatistics`, an aggregated
write-path snapshot).

### Statistics and Metadata Backups

In-process statistics are disabled by default. When `Statistics.Enabled` is
true, the runtime records bounded windowed counters for the storage, metadata,
and watcher paths; `Venue.Statistics()` returns a reader whose `Snapshot(query)`
aggregates the measurements in `[query.From, query.To)` and returns a
`core.StatisticsSnapshot` with the known totals filled in. Both
`Venue.Statistics()` and `Venue.StatisticsRecorder()` are never `nil`: when
statistics are disabled they return the noop implementations, so a `Snapshot`
call answers with an empty snapshot and a `Record` call drops its value.

```go
snapshot := runtime.Statistics().Snapshot(core.StatisticsQuery{
    From: time.Now().Add(-15 * time.Minute),
    To:   time.Now(),
})
fmt.Println(snapshot.WriteFileCount, snapshot.WriteBytes)

runtime.StatisticsRecorder().Record("app.batch.size", int64(len(batch)), time.Now(), nil)
```

`Statistics.Dimensions` selects which low-cardinality dimensions are retained;
`tenant_id` is off by default because it is high-cardinality.
`Statistics.MaxSeries` (default `16,384`, valid range `1024`–`262144`) bounds the
retained series so a high-cardinality workload cannot grow memory without limit,
and `Statistics.Retention` must be greater than or equal to
`Statistics.WindowSize`. When `Statistics.Output.Enabled` is true and
`Statistics.Output.Sink` is `Logging` (the only supported sink), the runtime logs
a periodic summary through the injected handler at `Output.Interval`, covering
`Output.QueryWindow`; `Output.IncludeEmptySnapshots` also emits an all-zero
summary.

Metadata backups are opt-in and need an operator-owned directory:

```yaml
venue:
  sqliteOptions:
    backupDirectory: ./venue-backups
    backupInterval: 1h
    backupRetention: 168h
    autoRestoreFromBackup: false
```

With `backupDirectory` set and a positive `backupInterval`, the runtime starts a
backup runner with the other background services and writes one consistent
online backup per tenant into that tenant's own directory, named
`{backupDirectory}/{tenantId}/metadata.<yyyyMMddTHHmmssZ>.bak`; the run does not
stop the queue or block writers. `backupRetention` (default `168h`; `0` disables
pruning) removes backups older than the window per tenant directory on the next
cycle, and `autoRestoreFromBackup` restores the newest readable backup of a
tenant whose database had to be quarantined instead of leaving it empty.
`autoRestoreFromBackup` requires `recoverCorruptedDatabase: true` and a
non-empty `backupDirectory`, and the restored state is only as new as the newest
backup. Every produced backup is checked with `PRAGMA integrity_check(1)` before
it is accepted unless `skipBackupVerification: true` turns that check off.

- `Venue.BackupMetadata(ctx, w)` writes one ad-hoc backup stream to `w` and
  returns the engine sequence number it was taken at.
- `venue.RestoreMetadata(ctx, cfg, r)` restores that stream into the metadata
  database directory `cfg` describes. It is an offline step: the directory must
  be absent or empty, and the caller must not have a runtime running against it
  (restore first, then call `NewVenue`). A non-empty directory is rejected and
  left untouched, and a failed restore removes the directory it created.
- `Venue.MetadataBackupInfo()` reports the newest backup on disk
  (`core.MetadataBackupInfo`: path, timestamp, count, and total bytes). It reads
  the filesystem rather than the running service, so it also works when backup
  scheduling is disabled.
- `Venue.MetadataBackupService()` returns the periodic runner (`nil` unless
  backups are configured); its `LatestBackup` accessor is the observable "closest
  recoverable point".

Venue's recoverability is therefore **backup-period granularity**, not Locus's
per-event queue journal: a restore returns every record committed before the
backup instant and nothing committed after it.

If a metadata database has to be quarantined and no backup can be restored, the
runtime reports a degraded state and continues with an empty database. Set
`FailFastOnStartupRecoveryFailure: true` to fail startup instead, for example
when serving an empty queue is worse than not starting.

### Startup Volume Preparation

`NewVenue` waits for every configured volume to be usable before any component
can select one, and performs the optional warm-up write:

- A volume that answers the first probe is never delayed.
- A volume that does not answer is retried with `volumes[].initialDelay`
  (default `2s`) before the first retry, then up to **10** attempts separated by
  `volumes[].healthCheckDelay` (default `500ms`), and must answer **two
  consecutive** healthy probes to count as ready.
- A volume that never becomes healthy fails startup with
  `core.ErrStorageVolumeUnavailable`.
- `volumes[].warmupOnStartup` (default `false`) performs one throwaway write
  through the real write path after the health checks, so the first real write
  does not pay for a cold path. Warm-up is advisory: a failure is logged and
  never disables the volume.

Both delays are validated as non-negative. They are `time.Duration` fields on
`VolumeConfig`, so set them by direct assignment or in the configuration file
(there is no fluent setter for them).

`DatabaseHealthChecker` verifies the real metadata and quota databases with a
cross-platform structural check of each tenant's
(`<metadataDirectory>/<tenantId>/metadata.db` and
`<quotaDirectory>/<tenantId>/quotas.db`): the file must exist and start with the
readable `SQLite format 3` header. It never opens a live database read-only,
which is unsupported on Windows and would conflict with Venue's own lock on
other platforms. A missing file is reported as "no database yet" for a fresh
deployment, not as corruption.

The health check is diagnostic only: it never repairs a database. For that,
`Sqlite.RecoverCorruptedDatabase` (default `false`) makes the repository
quarantine a per-tenant database file that cannot be opened, recreate an empty
one, and continue; the quarantined file is kept for
`Sqlite.CorruptedDatabaseRetention` (default `72h`) and then removed during
startup. This is a **destructive repair** — the quarantined data is not
re-imported, so enable it only when an operator can restore from the backup, or
when failing to start is the worse outcome. A file that is unreachable because
another process holds its lock, or because of a permission failure, is never
treated as corruption and is never quarantined.

## Locus Alignment and Intentional Divergences

Venue is a Go implementation of the Locus file-queue behaviour. The queue
contract, status values, retry arithmetic, quota-counting semantics, path
safety, lease handling, and the storage engine follow Locus `v2.0.0`: one
SQLite database file per tenant below the metadata and quota roots
(`{metadataDirectory}/{tenantId}/metadata.db` and
`{quotaDirectory}/{tenantId}/quotas.db`). The following differences are
deliberate and are part of the supported behaviour:

- **Configuration model**: `config.Config` is public, source-independent, and
  has no configuration-file dependency; the optional Viper adapter lives in
  `viperconfig`. Locus binds `IConfiguration`.
- **Lifecycle**: `venue.NewVenue` + explicit `Start`/`Stop` replace
  `IHostedService` and the Locus startup coordinator.
- **Logging**: an instance-scoped `slog` runtime that never touches
  `slog.Default()` replaces `ILogger<T>`.
- **Errors**: stable sentinel errors plus `errors.Is`/`errors.As` replace
  exception types.
- **Orphan recovery is opt-in** (`OrphanRecovery.Enabled`), matching Locus's
  default-disabled recovery service.
- **Tenant quota `0` means unlimited.** Locus treats a stored `0` as "inherit
  the global limit". Venue's semantics are explicit: a `nil` tenant quota
  inherits `DefaultTenantQuota`; an explicit `0` is unlimited.
- **Processing-timeout recovery** runs on the cleanup cycle, immediately when a
  claim finds an empty queue (`Cleanup.RecoverTimedOutOnEmptyQueue`, default
  enabled), and opportunistically in the background after a successful claim
  (`Cleanup.EnableBackgroundTimedOutReclaim`, default enabled). Each path has its
  own batch bound (`Cleanup.EmptyQueueReclaimBatchSize` default `32`,
  `Cleanup.BackgroundTimedOutReclaimBatchSize` default `8`) and its own per-tenant
  cooldown state, both bounded by `Cleanup.TimedOutReclaimCooldown` (default
  `30s`), so a recent background pass cannot delay the emergency reclaim.
- **Status scans are paged** for maintenance paths (`core.StatusPageReader`,
  500 records per page), so a large store is never materialised at once.
- **Startup volume preparation** retries a volume that does not answer its first
  probe with `volumes[].initialDelay` (default `2s`) before the first retry, up to
  10 attempts separated by `volumes[].healthCheckDelay` (default `500ms`), and
  requires two consecutive healthy probes. `volumes[].warmupOnStartup` performs
  one advisory throwaway write through the real write path.
- **Metadata recoverability is backup-based.** Locus rebuilds from its per-event
  queue journal; Venue writes opt-in consistent per-tenant SQLite backups
  (`sqliteOptions.backupDirectory`, `backupInterval` default `1h`,
  `backupRetention` default `168h`) and restores them offline. A restore is
  therefore only as new as the newest backup, and
  `FailFastOnStartupRecoveryFailure` (default `false`) decides whether an
  unrecoverable quarantine fails startup or continues degraded.
- **Runtime statistics are in-process and bounded**
  (`statisticsOptions.enabled`, default `false`), with optional periodic log
  output through the injected handler rather than an external metrics sink.
- **Tenant metadata is not migrated from Locus.** Locus persists numeric
  `TenantStatus` values with different numbers (`Enabled = 1`); Venue uses
  `Enabled = 0`. Do not copy Locus tenant JSON files into a Venue metadata
  directory.

Not implemented (out of scope for this project, listed for completeness):

- The durable per-tenant `queue.log` journal with projections, snapshots, and
  compaction, together with the queue-projection observability surface built on
  it (lag, snapshot, gap, and corrupt-tail diagnostics). Venue's metadata is one
  SQLite database per tenant with transactional writes, so queue state cannot be
  replayed event by event; per-tenant SQLite backups are the recovery path, with
  backup-interval granularity rather than per-event replay.
- The Locus quota projection/compensation managers. Venue rebuilds tenant and
  directory counts from stored metadata at startup and on demand through
  `ReconcileQuotaCounts`, and compensates on the write path.

Implemented and aligned with Locus: permanently-failed disposition with
dead-letter storage, junk-file cleanup, quarantined-database cleanup,
retired-volume policy, cumulative cleanup statistics, watcher root derivation,
global watcher options with a global enable/disable switch and a persisted
operator decision, `UpdateWatcher`, the advanced per-watcher import knobs,
runtime statistics with optional periodic log output, metadata backup with
offline restore, per-volume startup health retry and warmup, configurable
timed-out reclaim bounds, tenant quota limit administration, per-tenant cleanup
and orphan-recovery entry points, and the volume probe/warmup/diagnostics
capabilities.

## Build and Verification

The SQLite driver is pure Go, so the runtime builds and runs without cgo. A
`CGO_ENABLED=0` build is a required gate, not an optional matrix entry: a
cgo-only dependency or build tag must fail here rather than at a deployment.

```powershell
$env:CGO_ENABLED='0'; go build ./...
go build ./...
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
```

On a restricted environment where the default Go cache is not writable, point
the caches at the ignored repository-local cache directory:

```powershell
$env:GOCACHE="$PWD/.cache/go-build"
$env:GOLANGCI_LINT_CACHE="$PWD/.cache/golangci-lint"
$env:GOTMPDIR="$PWD/.cache/tmp"
$env:TMP="$PWD/.cache/tmp"
$env:TEMP="$PWD/.cache/tmp"
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
pkg/metadata/        SQLite metadata repositories and active-data cache
pkg/pool/            Storage-pool facade
pkg/scheduler/       Queue scheduling and retries
pkg/tenant/          Tenant management
pkg/quota/           Tenant and directory quotas
pkg/volume/          Storage volumes and path safety
pkg/cleanup/         Cleanup services
pkg/recovery/        Opt-in orphan-file recovery
pkg/watcher/         Watched-directory imports
pkg/health/          Database and volume health checks
pkg/logging/         Instance-scoped slog runtime
pkg/statistics/      Bounded in-process runtime statistics
examples/            Runnable examples
test/benchmark/      Public-entry system benchmarks
```

See [`examples/README.md`](examples/README.md) for runnable examples and
[`venue-config-example.yaml`](venue-config-example.yaml) for the complete file
configuration shape.

## License

MIT
