# Venue Architecture Design

> This document describes the structure and contracts of Venue in the current working tree. **Every statement about the code carries `path:line` evidence** (repository-relative path plus the 1-based line number).
> Anything that cannot be confirmed from the code goes into Section 10, "Unverified / Open"; nothing is inferred.
> Baseline: Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4` (`AGENTS.md:7`).

## Documentation conventions

- Evidence format is `repository-relative-path:line`, for example `pkg/pool/storage_pool.go:170` or `venue.go:77`.
- Locus evidence uses the `locus/` prefix, for example `locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`. The Locus `v2.0.0` reference checkout is **not** part of this repository; keep a read-only reference checkout available locally to verify such citations.
- Every behavioral claim must be checkable against the cited lines.

## 1. Scope and Goals

### 1.1 What it is

Venue is a Go multi-tenant file storage queue (`AGENTS.md:7`). It provides two things at once:

- **Managed file storage**: `StoragePool.WriteFile` generates the `fileKey`, and callers never select a physical path or a storage volume (`AGENTS.md:9-15`; generation logic `pkg/pool/storage_pool.go:214-215`, `pkg/pool/storage_pool.go:757-760`).
- **Queue semantics**: workers atomically claim Pending files through `GetNextFileForProcessing`/`GetNextBatchForProcessing`, while `MarkAsCompleted`/`MarkAsFailed` write durable state and physical deletion is left to cleanup (`pkg/core/interfaces.go:58-93`; `pkg/scheduler/file_scheduler.go:173-241`, `pkg/scheduler/file_scheduler.go:407-466`).

One in-process instance is assembled by `venue.NewVenue(*config.Config)` (`venue.go:77`), `Start` launches the background services (`venue.go:935`), and `Stop` shuts them down in reverse order and releases the repositories (`venue.go:992`).

### 1.2 What it does not do

| Not done | Evidence |
| --- | --- |
| It does not let callers select physical paths or storage volumes | `AGENTS.md:9-15`; paths are derived only by `storagePool.buildPhysicalPath` (`pkg/pool/storage_pool.go:468-481`) and by the volume's `BuildPhysicalPath` (`pkg/volume/local_volume.go:656-667`) |
| It does not treat the filesystem as a caller-managed resource (no handles, locks, or mount policy are exposed) | `core.StorageVolume` exposes only volume-relative path operations (`pkg/core/interfaces.go:188-217`) |
| It does not implement the Locus per-tenant `queue.log` journal/projection/snapshot/compaction | `README.md:939-946`; `pkg/core/models.go:76-87` states explicitly that no runtime code produces `DeleteRequested`/`DeleteSucceeded` |
| It does not implement the Locus `IHostedService` lifecycle | Replaced by `NewVenue` + `Start`/`Stop` (`README.md:898-899`; `venue.go:935`, `venue.go:992`) |
| It does not implement Locus `IConfiguration` binding | `config.Config` is configuration-source independent, and `config` performs no file I/O and does not import Viper (`AGENTS.md:57-67`, verified in §6.1) |
| It does not rebuild databases from data | The health check only diagnoses (`pkg/health/database_health_check_service.go:261-264`); a corrupted tenant database file is quarantined (`pkg/metadata/sqlite_corruption.go:32-41`) and, when restore-from-backup is configured, replaced by the newest verified backup instead (`:163-191`); there is no journal and no physical-file rebuild, and no BadgerDB-era recovery path remains in the tree |

### 1.3 Division of labour with existing documents

| Document | Status | Division of labour |
| --- | --- | --- |
| `AGENTS.md` | Present | Sole authority for repository rules and ownership boundaries; this document uses it as a skeleton but verifies it against the current code (`AGENTS.md:33-51`) |
| `docs/locus-alignment.md` | Present | Per-capability alignment matrix against Locus `v2.0.0`, the mechanism differences M1-M7, the deliberate divergences, and the open/unconfirmed lists (`docs/README.md:13`) |
| `docs/sqlite-storage-design.md` | Present | Design of the metadata and directory-quota storage engine: directory and file layout, schema, PRAGMA and connection policy, CAS SQL, paging, backup and corruption recovery (`docs/README.md:14`). The migration described there has landed: `venue.NewVenue` constructs only the per-tenant SQLite repositories (`metadata.NewSQLiteMetadataRepository`, `venue.go:354`; `quota.NewSQLiteDirectoryQuotaRepository`, `venue.go:403`) from `config.SqliteConfig` (`config/config.go:91`), and `config.BadgerDBConfig` and the BadgerDB repository implementations no longer exist in the tree. This document describes that engine in §5.1-§5.3, §7.2 and §8 |

## 2. Layering and Package Responsibilities

Dependencies point upward: `config`/`core` → base implementations (`tenant`/`metadata`/`quota`/`volume`/`logging`/`statistics`) → facades (`pool`/`scheduler`/`cleanup`/`recovery`/`watcher`/`health`) → the top-level `venue`. `viperconfig` and `test/benchmark` are optional components on the entry-point side.

| Package | Responsibility | Key files | Public entry points (constructors/interfaces) | Explicitly not responsible for |
| --- | --- | --- | --- | --- |
| `config` | Public, configuration-source-independent runtime model: defaults, validation, cloning, fluent construction | `config/config.go`, `config/fluent.go` | `config.New`/`config.DefaultConfig` (`config/config.go:526`, `config/config.go:531`), `(*Config).Clone` (`config/config.go:1356`), `ApplyDefaults` (`config/config.go:661`), `Validate` (`config/config.go:896`), all `With...`/`Add...` (`config/fluent.go:10`, `config/fluent.go:80`, `config/fluent.go:91`) | It does not import Viper and does not perform file I/O (`AGENTS.md:65`; verified: among non-test files only `viperconfig/viper.go:10` and `examples/viper-config/main.go:13` import `spf13/viper`) |
| `viperconfig` | Optional Viper → `config.Config` adapter; `NewWithDefaults` seeds every documented key, including the statistics, watcher-service, cleanup-reclaim, startup-fail-fast and metadata-backup keys (`viperconfig/viper.go:78`, `:97-99`, `:108-126`, `:130-132`) | `viperconfig/viper.go` | `Load` (`viperconfig/viper.go:14`), `LoadFromFile` (`:35`), `LoadFromFileSection` (`:41`), `LoadSection` (`:56`), `NewWithDefaults` (`:69-151`) | It does not own environment variables or reload policy, and does not define a second configuration model (`AGENTS.md:67`) |
| `pkg/core` | Public interfaces, shared models, status enums, domain errors | `pkg/core/interfaces.go`, `pkg/core/models.go`, `pkg/core/errors.go`, `pkg/core/tenantid.go`, `pkg/core/orphan.go` | Interfaces (from `pkg/core/interfaces.go:12`), `ValidateTenantID` (`pkg/core/tenantid.go:40`), sentinel errors (`pkg/core/errors.go:12-129`), parsers (`pkg/core/models.go:362`, `pkg/core/models.go:404`, `pkg/core/interfaces.go:625`), `DefaultFileRetryPolicy` (`pkg/core/models.go:433`) | It contains no implementation and imports no `pkg/*` (only the stdlib dependencies of `pkg/core/tenantid.go:1-8`) |
| `pkg/tenant` | Tenant lifecycle, tenant metadata JSON storage, tenant context cache | `pkg/tenant/manager.go`, `pkg/tenant/metadata_store.go` | `tenant.NewTenantManager` (`pkg/tenant/manager.go:79`) | It does not own physical volumes, does not select volumes, and does not count quotas |
| `pkg/metadata` | Metadata persistence in one SQLite database per tenant, keyset paging, active-record cache, file-level corruption quarantine, and per-tenant backup/restore | `pkg/metadata/sqlite_repository.go`, `sqlite_backup.go`, `sqlite_corruption.go`, `sqlite_paging.go`, `metadata_support.go`, `conflict_retry.go`, `cache.go` | `NewSQLiteMetadataRepository` (`pkg/metadata/sqlite_repository.go:394`, wired at `venue.go:354`); the optional capabilities are asserted at `pkg/metadata/sqlite_repository.go:378-383` (`core.StatusPageReader`, `core.MetadataBackupService`, `SQLiteMetadataTenantBackupService` declared at `:363-374`) | It does not select volumes and does not write physical payloads; it does not do quotas |
| `pkg/pool` | Storage + queue facade: quotas, volume selection, physical paths, physical writes, metadata persistence and rollback | `pkg/pool/storage_pool.go`, `path_generator.go`, `volume_selector.go` | `pool.NewStoragePool` (`pkg/pool/storage_pool.go:127`), `PathGenerator` interface (`:55`) | It does not implement transactions/CAS (delegated to `pkg/scheduler` + `pkg/metadata`) and does not decide the sharding layout (delegated to the volume) |
| `pkg/scheduler` | Atomic queue state transitions, lease validation, retry scheduling, synchronous empty-queue reclaim, opportunistic background timeout reclaim, and scheduler close | `pkg/scheduler/file_scheduler.go` | `scheduler.NewFileScheduler` (`pkg/scheduler/file_scheduler.go:182`), `DefaultFileSchedulerOptions` (`:63`), `DefaultEmptyQueueReclaimBatchSize` (`:56`), `DefaultBackgroundTimedOutReclaimBatchSize` (`:60`), `CloseFileScheduler` (`:649`) | It holds no volume mapping (`pkg/scheduler/file_scheduler.go:102-106`) and does no physical I/O; the background pass only writes through the metadata repository |
| `pkg/quota` | Tenant quotas (override value + global fallback) and directory quotas (per-directory sharded locks) plus directory-quota persistence in one SQLite database per tenant | `pkg/quota/tenant_quota_manager.go`, `directory_quota_manager.go`, `sqlite_directory_quota_repository.go`, `sqlite_directory_quota_corruption.go` | `quota.NewTenantQuotaManager` (`pkg/quota/tenant_quota_manager.go:74`), `quota.NewDirectoryQuotaManager` (`pkg/quota/directory_quota_manager.go:20`), `quota.NewSQLiteDirectoryQuotaRepository` (`pkg/quota/sqlite_directory_quota_repository.go:290`, wired at `venue.go:403`) | It does not judge physical space (`ErrInsufficientStorage` is produced by volume selection, `pkg/pool/storage_pool.go:427`) |
| `pkg/volume` | Local filesystem volumes, path safety, sharding layout, intra-volume atomic moves, health-probe cache plus forced probe, write-path warmup, and write-path statistics | `pkg/volume/local_volume.go`, `write_path_statistics.go`, `path_sanitizer.go`, `local_volume_windows.go`, `local_volume_unix.go` | `volume.NewLocalFileSystemVolume` (`pkg/volume/local_volume.go:112`), returning `core.StorageVolume`; the optional capabilities are asserted at `pkg/volume/local_volume.go:22-28` (`ProbeHealth` `:221`, `WarmWritePathCache` `:248`, `WritePathStatistics` `:289`) | It does no quotas and does not select volumes; it does not manage volume lifecycle or mounting (`AGENTS.md:43`) |
| `pkg/cleanup` | Cleanup (empty directories, timed-out, permanently failed, completed, orphaned metadata, junk, invalid database backups, optimization with reclaimed-byte detail) + cumulative statistics + dead-letter layout + retired-volume disposition + exact shard-depth protection + single-tenant empty-directory sweep + background scheduling | `pkg/cleanup/cleanup_service.go`, `database_optimization.go`, `dead_letter.go`, `junk_files.go`, `invalid_database_backups.go`, `cleanup_statistics.go`, `background_cleanup_service.go` | `cleanup.NewCleanupService` (`pkg/cleanup/cleanup_service.go:140`), `cleanup.NewBackgroundCleanupService` (`pkg/cleanup/background_cleanup_service.go:124`), `DefaultDeadLetterOptions` (`pkg/cleanup/dead_letter.go:48`); optional capabilities asserted at `pkg/cleanup/cleanup_service.go:26-29`, implemented by `OptimizeDatabasesDetailed` (`pkg/cleanup/database_optimization.go:40`) and `CleanupEmptyDirectoriesForTenant` (`pkg/cleanup/cleanup_service.go:340`) | It does not decide *when* to clean (that is decided by the scheduling and configuration of `BackgroundCleanupService`) |
| `pkg/recovery` | Opt-in: scan physical files, rebuild missing metadata through `ParsePhysicalPath`, and re-enqueue; a tenant-scoped entry point restricts the scan to one tenant | `pkg/recovery/orphan_recovery_service.go` | `recovery.NewOrphanRecoveryService` (`pkg/recovery/orphan_recovery_service.go:110`); the tenant-scoped capability is asserted at `:29` and implemented by `RecoverOrphanedFilesForTenant` (`:297`) | Disabled by default (`config/config.go:688-693`); it does not guess tenant ownership (`pkg/recovery/orphan_recovery_service.go:536-559`) and fails closed when no `TenantManager` was configured (`:306-314`) |
| `pkg/watcher` | Watched-directory imports: scanning, deduplication, import, durable source cleanup reservations/leases and retries, history persistence (with optional prune throttle and flush debounce), global option persistence, root derivation, and the advanced stability/cache knobs | `pkg/watcher/file_watcher.go`, `source_cleanup_store.go`, `source_cleanup_worker.go`, `background_file_watcher_service.go`, `file_watcher_auto_manager.go`, `watcher_state.go` | `watcher.NewFileWatcher` (`pkg/watcher/file_watcher.go:184`), `OpenSourceCleanupStore` (`pkg/watcher/source_cleanup_store.go:126`), `NewSourceCleanupWorker` (`pkg/watcher/source_cleanup_worker.go:47`), `NewBackgroundFileWatcherService` (`pkg/watcher/background_file_watcher_service.go:227`), `NewFileWatcherAutoManager` (`pkg/watcher/file_watcher_auto_manager.go:91`) | It does not own watcher definitions (`README.md:598-599`) and does not select physical paths (it goes through `StoragePool.WriteFile`, `pkg/watcher/file_watcher.go:953`) |
| `pkg/health` | Startup/periodic database health checks (structural file check — 16-byte SQLite header plus a minimum size, never opening the database), orphan-tenant detection, database size | `pkg/health/database_health_checker.go`, `database_health_check_service.go` | `health.NewDatabaseHealthChecker` (`pkg/health/database_health_checker.go:96`), `health.NewDatabaseHealthCheckService` (`pkg/health/database_health_check_service.go:61`) | It does not repair databases (`pkg/health/database_health_check_service.go:261-264`); `Start` does not fail because the database is unhealthy (same location) |
| `pkg/logging` | Instance-scoped structured logging `Runtime`: disabled means silent, never touches `slog.Default`, isolates handler panics | `pkg/logging/logging.go` | `logging.New` (`pkg/logging/logging.go:40`), `logging.Disabled` (`:51`), `(*Runtime).Emit` (`:79`) | It does not own the handler/writer (`pkg/logging/logging.go:11-14`) and does not configure a global logger (`:33-34`) |
| `pkg/statistics` | Bounded-window in-memory statistics recorder/reader plus optional periodic log output | `pkg/statistics/recorder.go` | `statistics.NewRecorder` (`pkg/statistics/recorder.go:251`), `statistics.Noop` (`:675`), `statistics.NewOutputService` (`:711`), `DefaultOptions` (`:129`), `MinimumSeries`/`MaximumSeries` (`:36`, `:40`) | It does no persistence or export (`pkg/statistics/recorder.go:4-15`) and never blocks the caller |
| `internal/directorypath` | Shared logical-directory normalization | `internal/directorypath/path.go` | `Normalize` (`internal/directorypath/path.go:7`) | It does no path resolution against volumes and performs no I/O; it is used by `pkg/pool` (`pkg/pool/storage_pool.go:193`) and `pkg/quota` (`pkg/quota/directory_quota_manager.go:188`) |
| `test/benchmark` | Public-entry system benchmarks | `test/benchmark/system_bench_test.go`, `cleanup_scale_bench_test.go` | `setupBenchmarkSystem` (`test/benchmark/system_bench_test.go:25`) → `venue.NewVenue` (`:42`); `newCleanupBenchVenue` (`test/benchmark/cleanup_scale_bench_test.go:70`) → `venue.NewVenue` (`:83`) | It does not bypass the public entry point (`AGENTS.md:53`) |

Note: `internal/directorypath` is a package in this repository and appears in the ownership list of `AGENTS.md:50`, but it is still absent from the Repository Layout in `README.md:1024-1044` (see §10.2, U2).

## 3. Public Contracts

### 3.1 Base interfaces

| Interface | Purpose | Key invariants | Implementation and usage evidence |
| --- | --- | --- | --- |
| `StoragePool` (`pkg/core/interfaces.go:12`) | The single facade for queue + storage | An empty queue returns `(nil, nil)` rather than an error (`:62-64`; the batch form returns an empty slice `:76`); a failed claim must be surfaced to the caller (`:68-69`) | Implemented by `storagePool` (`pkg/pool/storage_pool.go:66`), constructed by `NewStoragePool` (`:127`) and wired at `venue.go:507` |
| `TenantManager` (`pkg/core/interfaces.go:106`) | Tenant lifecycle and resolution | `TryGetTenant` never writes to disk and never materializes a directory (`:114-121`); `IsTenantEnabled` is implemented on top of it (`pkg/tenant/manager.go:216-226`) | Implemented by `TenantManager` (`pkg/tenant/manager.go:63`), constructed at `:79`; wired at `venue.go:312` |
| `FileScheduler` (`pkg/core/interfaces.go:151`) | Queue state transitions and timeout reclaim | Completion/failure must carry a still-valid lease (`:166-172`); after `ResetTimedOutFiles` a file is immediately claimable (`:179-184`) | Implemented by `fileScheduler` (`pkg/scheduler/file_scheduler.go:107`), constructed at `:182`; wired at `venue.go:480` |
| `MetadataRepository` (`pkg/core/interfaces.go:505`) | Metadata read/write + CAS + optimization + close | `UpdateStatus` is an administrative path and does no lease validation (`:531-539`); queue paths must use `CompareAndTransitionToProcessing`/`CompareAndUpdateProcessing` (`:541-556`) | Implemented by `*metadata.SQLiteMetadataRepository` (`pkg/metadata/sqlite_repository.go:281`, assertion `:379`, constructed at `:394`); the runtime wires it at `venue.go:354` |
| `DirectoryQuotaRepository` (`pkg/core/interfaces.go:569`) | Directory-quota persistence | Tenant isolation is the database file itself: each tenant's rows live in `{quotaDirectory}/{tenantId}/quotas.db` and the primary key is the directory path (`pkg/quota/sqlite_directory_quota_repository.go:22-26`, `:55-62`), so no key prefix is needed | Implemented by `*quota.sqliteDirectoryQuotaRepository` (`pkg/quota/sqlite_directory_quota_repository.go:222`, assertion `:279`, constructed at `:290`, wired at `venue.go:403`) |
| `CleanupService` (`pkg/core/interfaces.go:440`) | 9 classes of maintenance operation + cumulative statistics | Every operation returns `*CleanupStatistics`; `CumulativeStatistics` is monotonic and returns a copy (`:469-471`) | Implemented by `cleanupService` (`pkg/cleanup/cleanup_service.go:102`), constructed at `:140`; wired at `venue.go:535` |
| `OrphanRecoveryService` (`pkg/core/orphan.go:39`) | Physical file → metadata rebuild | `RecoverNow` may run concurrently with normal storage; it rebuilds only when no metadata exists (`:41-42`) | Implemented by `recovery.OrphanRecoveryService` (`pkg/recovery/orphan_recovery_service.go:84`), constructed at `:110`; wired at `venue.go:723` |
| `FileWatcher` (`pkg/core/interfaces.go:744`) | Watched-directory import management | `UpdateWatcher` does not interrupt a scan, and an unknown ID returns `ErrWatcherNotFound` (`:748-752`); `GetWatchersForTenant` does not return multi-tenant watchers (`:764-767`) | Implemented by `fileWatcher` (`pkg/watcher/file_watcher.go:101`), constructed at `:184`; wired at `venue.go:595` |
| `FileWatcherAutoManager` (`pkg/core/interfaces.go:791`) | Derive per-tenant watchers from a root template | Application is idempotent; `RemoveAllWatchers` deletes only derived watchers (`:789-790`) | Implemented by `fileWatcherAutoManager` (`pkg/watcher/file_watcher_auto_manager.go:67`), constructed at `:91`; wired at `venue.go:700` |
| `StorageVolume` (`pkg/core/interfaces.go:188`) | Volume abstraction (local/network/cloud); the optional probe, warmup and diagnostics capabilities stay separate (`:371-398`) | All paths are volume-relative and are sanitized by the volume itself (`pkg/volume/path_sanitizer.go:28-60`) | Implemented by `LocalFileSystemVolume` (`pkg/volume/local_volume.go:78`), constructed at `:112`; wired at `venue.go:449`; the local volume also implements the three write-path capabilities (`pkg/volume/local_volume.go:22-28`, §3.2) |
| `TenantQuotaManager` (`pkg/core/interfaces.go:254`) | Per-tenant file-count quota | `IncrementFileCount` returns `ErrTenantQuotaExceeded` when over the limit (`:260-262`); `IncrementFileCount` re-reads the effective limit on every call so that `SetGlobalLimit` takes effect immediately (`pkg/quota/tenant_quota_manager.go:110-113`) | Implemented by `TenantQuotaManager` (`pkg/quota/tenant_quota_manager.go:52`), constructed at `:74`; wired at `venue.go:380` |
| `StatisticsRecorder` (`pkg/core/interfaces.go:306`) / `StatisticsReader` (`:315`) | Low-overhead in-process statistics write/aggregate read | Implementations never block the caller's I/O; when disabled all deltas are dropped; `nil` is equivalent to disabled (`:300-305`); a query whose `To` is not later than `From` returns an empty snapshot (`:317-319`) | Implemented by `*statistics.Recorder` (`pkg/statistics/recorder.go:226`, `Record` `:284`, `Snapshot` `:370`) and `NoopRecorder` (`:671`); wired at `venue.go:121` (`initializeStatistics`), the optional output service at `venue.go:134`; the read side type-asserts at `venue.go:1257` |
| `DatabaseHealthChecker` (`pkg/core/interfaces.go:860`) | Structural database health checks and orphan-tenant detection | It checks structure and never opens the database (`pkg/health/database_health_checker.go:592-637`); a missing database file does not count as corruption (`:344-347`, `:611-614`) | Implemented by `databaseHealthChecker` (`pkg/health/database_health_checker.go:80`), constructed at `:96`; wired at `venue.go:748` |

### 3.2 Optional capability interfaces

Optional capabilities are used exclusively through type assertions. Every capability below has an implementation; the table records who type-asserts it at runtime and where the capability is implemented.

| Interface | Purpose | type-assert / usage | Compile-time implementation assertion |
| --- | --- | --- | --- |
| `StatusPageReader` (`pkg/core/interfaces.go:493`) | Paged enumeration by status so long tasks do not load whole tables | `venue.go:808` (quota reconciliation), `pkg/cleanup/cleanup_service.go:232` (all status-based scans); both have a `GetByStatus` fallback (`venue.go:828`, `pkg/cleanup/cleanup_service.go:234`) | `pkg/metadata/sqlite_repository.go:380`; implementation `pkg/metadata/sqlite_paging.go:74-141` |
| `FileMover` (`pkg/core/interfaces.go:284`) | Zero-copy atomic intra-volume move (dead-letter optimization) | `pkg/cleanup/dead_letter.go:195`; when unimplemented it falls back to read+write+delete (`:199-215`) | `pkg/volume/local_volume.go:23` |
| `StorageVolumePathBuilder` (`pkg/core/interfaces.go:221`) | Volume-defined physical layout (the volume decides sharding) | `pkg/pool/storage_pool.go:477`; without an implementation it falls back to the date-directory generator (`:480`) | Method-level at `pkg/volume/local_volume.go:656` (not asserted in a package-level `var` block) |
| `MetadataBackupService` (`pkg/core/interfaces.go:363`) | Online consistent backup stream | `venue_metadata_backup.go:83` (`(*Venue).BackupMetadata` asserts the capability on the metadata repository and fails with `ErrInvalidArgument` when it is absent, `:85`); `newMetadataBackupService` errors when backups are enabled but the repository has no per-tenant capability (`venue_metadata_backup.go:171-174`) | `pkg/metadata/sqlite_repository.go:381`; implementation `:1207-1244` (`Backup`, a zip of one `VACUUM INTO` copy per tenant) plus `BackupTenant` (`pkg/metadata/sqlite_backup.go:30-62`) declared by `SQLiteMetadataTenantBackupService` (`pkg/metadata/sqlite_repository.go:363-374`) |
| `TenantQuotaAdministrator` (`pkg/core/interfaces.go:331`) | Global/override limit administration | `venue.go:1246` (`Venue.TenantQuotaAdministrator`) | `pkg/quota/tenant_quota_manager.go:65` |
| `ShardingDepthProvider` (`pkg/core/interfaces.go:296`) | Report sharding depth so shard directories are protected | `pkg/cleanup/cleanup_service.go:525` (`volumeShardDepth`); the reported depth is then used exactly (`:573-580`), and the structural heuristic is only the fallback for a volume that does not report one (`:582`, `:609-625`) | `pkg/volume/local_volume.go:24`; implementation at `pkg/volume/local_volume.go:176`; test assertion `pkg/volume/local_volume_move_test.go:502-503` |
| `StorageVolumeHealthProbe` (`pkg/core/interfaces.go:376`) | Force a probe and refresh the health cache | `venue.go:199` (`volumeProbe` prefers the forced probe over a possibly cached `IsHealthy`) | `pkg/volume/local_volume.go:25`; implementation at `pkg/volume/local_volume.go:221` |
| `StorageVolumeWritePathWarmup` (`pkg/core/interfaces.go:385`) | Write-path warmup at startup | `venue.go:261` (`warmVolume`); the result is advisory — a failure is logged, never fatal (`venue.go:256-276`) | `pkg/volume/local_volume.go:26`; implementation at `pkg/volume/local_volume.go:248` |
| `StorageVolumeWritePathDiagnostics` (`pkg/core/interfaces.go:394`) | Write-path observation snapshot | Not type-asserted by the runtime: a caller reaches the volumes through `Venue.Volumes()` (`venue.go:1314`) and asserts the capability itself | `pkg/volume/local_volume.go:27`; implementation at `pkg/volume/local_volume.go:289` |
| `DatabaseOptimizationService` (`pkg/core/interfaces.go:405`) | Per-database reclaimed-byte detail | Not type-asserted by the runtime: a caller asserts on `Venue.CleanupService()` (`venue.go:1281`) | `pkg/cleanup/cleanup_service.go:27`; implementation at `pkg/cleanup/database_optimization.go:40` |
| `TenantCleanupService` (`pkg/core/interfaces.go:417`) | Single-tenant empty-directory cleanup | Not type-asserted by the runtime: a caller asserts on `Venue.CleanupService()` (`venue.go:1281`) | `pkg/cleanup/cleanup_service.go:28`; implementation at `pkg/cleanup/cleanup_service.go:340` |
| `TenantOrphanRecoveryService` (`pkg/core/interfaces.go:429`) | Single-tenant orphan recovery | Not type-asserted by the runtime; construction supplies the `TenantManager` the tenant-scoped path verifies against (`venue.go:723`, `pkg/recovery/orphan_recovery_service.go:306-314`) | `pkg/recovery/orphan_recovery_service.go:29`; implementation at `pkg/recovery/orphan_recovery_service.go:297` |

Conclusion: every interface that used to be "contract established, implementation absent" now has an implementation. `ShardingDepthProvider`, `StorageVolumeHealthProbe` and `StorageVolumeWritePathWarmup` are additionally used by runtime code; `DatabaseOptimizationService`, `TenantCleanupService`, `TenantOrphanRecoveryService` and `StorageVolumeWritePathDiagnostics` are implemented but reached only through the public accessors, so a caller must type-assert them itself. No optional capability is left as a declaration without an implementation.

## 4. Lifecycle

### 4.1 `NewVenue`: clone → ApplyDefaults → Validate → initialize

| Step | Lines | Behavior |
| --- | --- | --- |
| nil check | `venue.go:78-80` | `cfg == nil` → `ErrInvalidArgument` |
| clone | `venue.go:77` | `cfg.Clone()`, deep-copying the volume/tenant/watcher slices, each tenant's `Quota` pointer and the `Logging` pointer (`config/config.go:1356-1385`) |
| defaults | `venue.go:83` | `runtimeConfig.ApplyDefaults()` (`config/config.go:661`) |
| validation | `venue.go:84-86` | `runtimeConfig.Validate()`, wrapping a failure as `invalid configuration` |
| logging runtime | `venue.go:88-95` | Defaults to `logging.Disabled()`; when `Logging != nil`, `logging.New` (a failure returns `invalid logging configuration`) |
| assemble instance | `venue.go:97-100` | Writes only `config` and `logger` |
| initialize | `venue.go:103-108` | On failure, `v.closeRepositories()` runs before the error is wrapped and returned |
| success event | `venue.go:110` | `initialized` |

### 4.2 `initialize`: construction order

| Order | Component | Lines | Notes |
| --- | --- | --- | --- |
| 0 | statistics | `venue.go:306` | Before every instrumented component, so that no operation writes into a missing recorder (`venue.go:303-304`) |
| 1 | tenant manager + configured tenants | `venue.go:310-345` | An existing tenant is treated as idempotent (`venue.go:328`), then enable/disable is applied |
| 2 | metadata repository | `venue.go:347-358` | One façade serves every tenant and opens no database until a tenant is first touched: each tenant resolves to `{MetadataDirectory}/{tenantId}/metadata.db`, created lazily with the tenant directory (`pkg/metadata/sqlite_repository.go:1488-1502`), and the handle table is bounded by `Sqlite.MaxOpenDatabases` with optional `Sqlite.OpenDatabaseIdleTimeout` eviction of the least recently used idle handle (`:1405-1457`). The quarantine, recovery-incomplete and statistics callbacks are injected at `venue.go:350-353`; the construction call is `metadata.NewSQLiteMetadataRepository` (`venue.go:354`) |
| 2b | periodic backup runner | `venue.go:360-376` | Condition: `Sqlite.BackupDirectory != ""` and `Sqlite.BackupInterval > 0` (both fields are on the SQLite configuration section, `config/config.go:148`, `:152`) |
| 3 | tenant quota + reconciliation | `venue.go:378-401` | `reconcileTenantQuotaCounts` (`:399`) |
| 3b | SQLite directory-quota repository + manager + reconciliation | `venue.go:403-427` | `quota.NewSQLiteDirectoryQuotaRepository` (`:403`); `reconcileDirectoryQuotaCounts` (`:423`) |
| 3c | tenant database validation + startup recovery decision | `venue.go:429-436` | `validateTenantDatabases` (`:431`) then `checkStartupRecovery` (`:434`). A metadata database that had to be quarantined and could not be restored from a backup is recorded by `recoveryIncompleteReporter` (`venue.go:1116-1128`, event `database_recovery_incomplete`); `checkStartupRecovery` (`:1173`) turns that into a construction failure only when `FailFastOnStartupRecoveryFailure` is set (`:1182`), otherwise the runtime continues degraded (`:1183-1186`) |
| 4 | storage volumes | `venue.go:438-466` | An unsupported type errors out (`:458`); an empty volume set errors out (`:468-470`) |
| 4b | volume readiness and warmup | `venue.go:472-476` | `prepareVolumes` (`:177`) probes every configured volume immediately (`:184`), retries one that did not answer (`:212-251`), and then performs the optional advisory warm-up write (`:256-276`) |
| 5 | file scheduler | `venue.go:478-503` | The reclaim batch sizes, the background-reclaim switch and the scheduler logging runtime all come from configuration (`:487-498`) |
| 6 | storage pool | `venue.go:505-519` | Injects the statistics recorder |
| 7 | cleanup core | `venue.go:521-560` | The disposition enum is parsed at construction time and errors there (`:523-534`) |
| 8 | background cleanup | `venue.go:563-589` | Condition: `EnableBackgroundCleanup` (`:563`) |
| 9 | watcher (core + background + root derivation) | `venue.go:591-718` | Condition: there are watchers or roots (`:591`) |
| 10 | orphan recovery | `venue.go:720-739` | Condition: `OrphanRecovery.Enabled` (`:720`); the `TenantManager` is passed so the tenant-scoped entry point can verify a tenant (`:726`) |
| 11 | database health check | `venue.go:741-775` | Condition: `EnableDatabaseHealthCheck` (`:741`) |

### 4.3 `Start`: order and rollback

- Mutual exclusion and state gating: `venue.go:939-944` (already stopped → error advising a new instance; already running → error).
- Derived lifecycle context: `venue.go:946-947`.
- Service list (dependency order): health check → cleanup → orphan recovery → file watcher → source cleanup → metadata backup → statistics output (`venue.go:953-964`). Source cleanup is gated by the global watcher option and the current enabled watcher set (`venue.go:1100-1124`).
- Missing/typed-nil services are skipped: `venue.go:968`, decided by `isNilService` (`venue.go:1198-1204`, which uses `reflect` to detect typed nils).
- Startup failure rollback: `Stop` the already-started services in reverse order, reset `running=false`, and return an error carrying the service name (`venue.go:971-977`).
- Each started service emits a `*_started` event (`venue.go:979`).

### 4.4 `Stop`: reverse stop, release repositories, idempotent

- Idempotency gate: when `closed`, return `nil` immediately (`venue.go:996-998`).
- Only when `running`, stop in reverse order: statistics output → metadata backup → source cleanup → file watcher → orphan recovery → cleanup → health check (`venue.go:1004-1023`). The source cleanup worker stops before the watcher, and its store closes after the watcher history flushes (`venue.go:1050-1053`, `:1080-1090`).
- Stop errors are only logged, never returned (`venue.go:1020-1022`, event name `*_stop_failed`).
- Repository release: `closeRepositories()` (`venue.go:1028` → `:1062-1097`). The order is: close the file scheduler first, so in-flight background timeout reclaim passes stop before the repository they write to is closed (`:1065-1070`, `scheduler.CloseFileScheduler` at `pkg/scheduler/file_scheduler.go:650`); then the watcher (optional `Close() error` capability asserted at `:1076`, used to flush import history), then the metadata repository (`:1084-1089`), then the directory-quota repository (`:1091-1096`). Both repositories checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) before closing each tenant handle, which is what empties the `-wal` sidecar and releases the file handle (`pkg/metadata/sqlite_repository.go:1277-1292`; `pkg/quota/sqlite_directory_quota_repository.go:607-635`).
- Cancel the context and set `closed`: `venue.go:1030-1034`; after `closed`, the synchronous accessors must not be used (`venue.go:986-991`).

### 4.5 Resource release on construction failure

`NewVenue` calls `closeRepositories` when `initialize` fails (`venue.go:106`). `closeRepositories` achieves idempotency through nil checks plus setting fields to nil (`venue.go:1062-1096`), so it can run on both the construction-failure path and the `Stop` path. Note: when `initialize` fails midway, resources that are already open but **not yet assigned to a Venue field** are not released by that path — for example, `dirQuotaRepo` has no failure point after successful creation and before assignment (`venue.go:403-416`). The SQLite repositories narrow that window: both façades open no database at construction (handles are created lazily on first tenant access) and every failed open or failed schema application closes its own connection before returning (`pkg/metadata/sqlite_repository.go:1561-1585`; `pkg/quota/sqlite_directory_quota_repository.go:908-935`). A volume that never becomes healthy fails construction from `prepareVolumes` with `core.ErrStorageVolumeUnavailable` (`venue.go:246-251`), and `closeRepositories` then releases everything opened up to that point.

### 4.6 Background service inventory and adaptation

Adaptation surface: `backgroundService` (`Start() error`/`Stop() error`, `venue.go:903-906`); services whose lifecycle cannot fail are wrapped in `voidBackgroundService` (`venue.go:911-928`).

| Service | Venue field | Start mechanism | Enablement condition |
| --- | --- | --- | --- |
| health check | `healthCheckService` (`venue.go:58`) | Native `Start` (`pkg/health/database_health_check_service.go:109`) | `EnableDatabaseHealthCheck` (`venue.go:742`) |
| cleanup | `cleanupService` (`venue.go:56`) | Native `Start` (`pkg/cleanup/background_cleanup_service.go:205`) | `EnableBackgroundCleanup` (`venue.go:564`) |
| orphan recovery | `orphanRecovery` (`venue.go:59`) | Native `Start` (`pkg/recovery/orphan_recovery_service.go:157`) | `OrphanRecovery.Enabled` (`venue.go:721`) |
| file watcher | `fileWatcherService` (`venue.go:57`) | Native `Start` (`pkg/watcher/background_file_watcher_service.go:490`) | There are watchers or roots and construction succeeded (`venue.go:592`, `:656`) |
| metadata backup | `metadataBackupCore` (`venue.go:60`) | `voidBackgroundService` (`venue.go:1043-1048`) | `BackupDirectory` is non-empty and `BackupInterval > 0` (`venue.go:362`) |
| statistics output | `statisticsOutput` (`venue.go:66`) | `voidBackgroundService` (`venue.go:1053-1058`) | `Statistics.Enabled` and `Statistics.Output.Enabled` (`venue.go:123`, `:139`) |

## 5. Key Data Flows

### 5.1 Write path

The actual order is: tenant gate → tenant-quota increment → **volume candidate selection** → directory-quota increment → physical write → metadata persistence → (only on success) statistics. Note that `CanAddFile` does not participate in the write path; exceeding the limit is decided inside `IncrementFileCount`.

| Step | Behavior | Evidence |
| --- | --- | --- |
| 1 | Tenant-enabled gate: `!tenant.IsEnabled()` → `ErrTenantDisabled` | `pkg/pool/storage_pool.go:204-206` |
| 2 | Tenant ID validation (before consuming any resource) | `pkg/pool/storage_pool.go:210-212` → `pkg/core/tenantid.go:40` |
| 3 | Generate the `fileKey` (UUID → 32 lowercase hex characters) | `pkg/pool/storage_pool.go:215`, `:757-760` |
| 4 | The extension is kept for diagnostics only | `pkg/pool/storage_pool.go:218-221` |
| 5 | Register the rollback defer (rollback also happens when a panic is recovered) | `pkg/pool/storage_pool.go:227-242` |
| 6 | Increment the tenant quota; over the limit → `ErrTenantQuotaExceeded` | `pkg/pool/storage_pool.go:245-250`; `pkg/quota/tenant_quota_manager.go:141-143` |
| 7 | Select volume candidates (filtered by remaining bytes when seekable) | `pkg/pool/storage_pool.go:253`, `:418-437`; no candidate → `ErrInsufficientStorage` (`:427`). The default selector is `MostAvailableSpaceSelector` (assigned at construction, `:156`; implemented at `pkg/pool/volume_selector.go:49-101`: it skips unhealthy volumes, volumes that error, volumes with zero free space, and volumes with less than `requiredBytes`, then sorts by descending free space with volume ID as tie-breaker); the optional extension interface `VolumeCandidateSelector` (`pkg/pool/volume_selector.go:23-30`) is type-asserted at `pkg/pool/storage_pool.go:421`, and when absent it degrades to single-volume selection (`:432-436`) |
| 8 | Increment the directory quota; over the limit → `ErrDirectoryQuotaExceeded` | `pkg/pool/storage_pool.go:259-264`; `pkg/quota/directory_quota_manager.go:65-67` |
| 9 | Write per candidate: pessimistically record the target → `WriteFile` → delete the partial file on failure → move to the next candidate only when seekable | `pkg/pool/storage_pool.go:328-370` (record `:342-344`, write `:346`, delete partial file `:353`, rewind `:334-338`) |
| 10 | Physical path generation: `tenantID/<shard...>/<fileKey><ext>` | `pkg/pool/storage_pool.go:468-481` → `pkg/volume/local_volume.go:656-667` → `pkg/volume/path_sanitizer.go:186-219` |
| 11 | Physical write + fsync when configured | `pkg/volume/local_volume.go:344-398` (`file.Sync()` `:391`, guarded by `v.enableFsync` `:389`) |
| 12 | Build the metadata (`Status = Pending`, `RetryCount = 0`) | `pkg/pool/storage_pool.go:273-287` |
| 13 | Persist the metadata in the tenant's own database: one `INSERT ... ON CONFLICT(file_key) DO UPDATE` inside a single `BEGIN IMMEDIATE` transaction | `pkg/pool/storage_pool.go:290` → `pkg/metadata/sqlite_repository.go:488-518` (upsert SQL `:150-179`, transaction `:1722-1736`); the `files` table and its five indexes are created once per tenant database by the idempotent schema script (`:66-134`), so no secondary index is maintained by hand |
| 14 | Failure rollback: delete the physical file first, and if it cannot be deleted the **quota is retained**; only after the physical file is gone are the directory and tenant quotas rolled back | `pkg/pool/storage_pool.go:378-413` (order at `:386-410`); the error is wrapped in `volumeCleanupError` to avoid leaking physical paths (`:109-124`) |
| 15 | Statistics are recorded only on the success path | `pkg/pool/storage_pool.go:294-297`; `recordStatistic` `:167-178` |

Read-path self-healing: on a read failure the canonical path is rebuilt, the bytes are confirmed to exist, the correction is persisted, and the read is retried once (`pkg/pool/storage_pool.go:512-525`, `:536-560`).

### 5.2 Claiming (CAS + lease + empty queue)

- The facade forwards and counts: `pkg/pool/storage_pool.go:613-622`; `recordDequeue` counts only non-nil results (`:640-646`).
- Single claim: `pkg/scheduler/file_scheduler.go:263-296`. The bounded candidate window is `claimCandidateLimit = 10` (`:78`, `:357`); candidates are tried one by one with CAS, retryable contention errors are skipped and other errors are propagated (`:365-378`, `claimRetryable` `:98-100`).
- **Empty-queue semantics**: when there is no claimable candidate and no reclaim ran, `(nil, nil)` is returned (`pkg/scheduler/file_scheduler.go:282-284`).
- Batch claim: `pkg/scheduler/file_scheduler.go:312-347`; the fetch size is `batchSize*2` capped at `batchCandidateLimit = 100` (`:81`, `:388-391`); an empty result returns an empty slice (`:398-400`).
- CAS transition (Pending → Processing): one conditional `UPDATE` must match both `status = Pending` and `available_for_processing_at <= now` (`pkg/metadata/sqlite_repository.go:855-869`), and `RowsAffected` decides the outcome. When it matches nothing, the same transaction classifies the loss with a probe `SELECT` (`:904-929`): a missing row is `ErrFileNotFound` (`:912-913`), a different status or an unarrived availability window is a wrapped `ErrFileNotClaimable` (`:918-925`), and "Pending and available yet no row updated" is reported as a lost write race (`:926-928`). The winner writes `processing_start_time` in the same statement (`:858`). The whole attempt runs under bounded conflict retry (`:853-884`); only `SQLITE_BUSY`/`SQLITE_LOCKED` is retried (`:1758-1778`, `pkg/metadata/conflict_retry.go:46-48`), and any other failure becomes `ErrDatabaseError` (`pkg/metadata/sqlite_repository.go:891`).
- **Lease semantics**: `core.FileProcessingLease{TenantID, FileKey, ProcessingStartTimeUTC}` (`pkg/core/models.go:120-131`), derived by `FileMetadata.ToFileLocation` from the active `ProcessingStartTime` (`:248-256`). `CompareAndUpdateProcessing` requires `Status == Processing` and `ProcessingStartTime` **value-for-value equal** to the lease (`pkg/metadata/sqlite_repository.go:974-987`), and repeats both predicates in the lease-guarded `UPDATE` itself (`:1038-1053`); a mismatch returns `*core.FileProcessingLeaseMismatchError` (`pkg/metadata/metadata_support.go:64-80`, raised at `pkg/metadata/sqlite_repository.go:969`, `:986`, `:1015`), whose `Unwrap` yields `ErrProcessingLeaseMismatch` (`pkg/core/errors.go:112-115`). Releasing the same lease twice is an idempotent no-op (released marker `ReleasedProcessingStartTimeUTC`, `pkg/core/models.go:230-234`; decision and no-write return at `pkg/metadata/sqlite_repository.go:977-987`, marker write at `:996-1003`). The callback must not change identity (`:992-994`).
- **Timeout reclaim**:
  - The metadata scan selects `processing_start_time < now - timeout` (`pkg/metadata/sqlite_repository.go:1092`, `:1115-1122`), that is, strictly more than the timeout has elapsed.
  - Two callers share the same lease-checked reset (`pkg/scheduler/file_scheduler.go:790-837`): the cleanup-cycle full reclaim `ResetTimedOutFiles` uses an unbounded scan (`:779-780`, comment at `:779`), while the two paths below pass a positive limit.
  - **Synchronous reclaim on an empty queue**: `reclaimTimedOutOnEmptyQueue` (`:433-457`), gated by `RecoverTimedOutOnEmptyQueue` and by a positive `EmptyQueueReclaimBatchSize` (`:434-436`), with a per-tenant cooldown reservation and rollback (`:440-454`; `reserveTimedOutReclaim` `:467-483`, `rollbackTimedOutReclaim` `:488-496`); after a successful reclaim the caller retries the pending scan **exactly once** (`:286-294` for the single claim, `:339-345` for the batch).
  - **Opportunistic background reclaim**: a successful claim schedules it (`:274`, `:293`, `:327`, `:344`). The pass requires `BackgroundTimedOutReclaimEnabled`, a positive batch size and a positive processing timeout (`:508-512`); it starts at most one pass per tenant through the in-flight marker (`:517-519`), reserves its own cooldown window (`:525-528`), registers with the lifecycle gate (`:530-536`), and runs `runBackgroundTimedOutReclaim` (`:571-604`) bounded by `BackgroundTimedOutReclaimBatchSize` (`:589-594`). Failures are contained and logged through the injected runtime (`:600-603`, `:610-621`), and `Close` cancels the lifecycle context and waits for every registered pass (`:632-641`).
  - Reclaimed files are immediately available: `availableAt := now` (`pkg/scheduler/file_scheduler.go:822`), combined with the inclusive availability predicate on the metadata side (`available_for_processing_at <= now`, `pkg/metadata/sqlite_repository.go:768`, `:922-925`).
  - Wiring: `RecoverTimedOutOnEmptyQueue` and `TimedOutReclaimCooldown` come from configuration (`venue.go:488-489`, configuration fields `config/config.go:435-436`). Both reclaim batch sizes, the background switch and the scheduler logging runtime are passed through as well (`venue.go:490-498`; fields `config/config.go:451-454` and `:456-464`; defaults `:618-620`). A configured `EmptyQueueReclaimBatchSize` of zero selects `scheduler.DefaultEmptyQueueReclaimBatchSize` (`pkg/scheduler/file_scheduler.go:56`, substitution at `:217-219`) and a negative value disables the synchronous pass (`:434-436`); a non-positive background batch size disables the background pass (`:509`).
  - **The two paths keep independent per-tenant cooldown state on purpose** (deliberate divergence, see `docs/locus-alignment.md` §4.7): `reclaimDeadlines` belongs to the synchronous path and `backgroundReclaimDeadlines` to the background pass (`pkg/scheduler/file_scheduler.go:136-139`, `:233-234`), and each path reserves its own map (`:440`, `:525`). An emergency reclaim on an empty queue must never be suppressed by a recent opportunistic pass, because the caller blocked on an empty queue is the one that needs the timed-out records back now; both maps use the same `TimedOutReclaimCooldown`, so neither path can run more often than configured (`:127-135`, `:459-465`). Note: the option's doc comment at `pkg/scheduler/file_scheduler.go:29-33` still describes the cooldown as shared, which contradicts the two-map implementation at `:136-139`.

### 5.3 Completion / failure

- `MarkAsCompleted`: validate non-empty (`pkg/scheduler/file_scheduler.go:674-679`) → lease-validated update: `Completed`, clearing `ProcessingStartTime`/`AvailableForProcessingAt`/`LastError`, and writing `CompletedAt` (`:682-690`) → failure wrapping (`:691-693`). The facade additionally looks up the volume to fill in the statistics dimension, and a failed lookup does not affect completion (`pkg/pool/storage_pool.go:649-665`).
- `MarkAsFailed`: `RetryCount++`, `LastFailedAt`, `LastError`, clearing `ProcessingStartTime` (`pkg/scheduler/file_scheduler.go:710-714`).
  - **Permanent-failure decision**: `RetryCount >= MaxRetryCount` → `PermanentlyFailed`, clearing `AvailableForProcessingAt` (`:716-720`).
  - **Retry backoff**: otherwise back to `Pending` with `AvailableForProcessingAt = now + CalculateRetryDelay(RetryCount)` (`:722-726`).
  - Backoff arithmetic: `delay = InitialRetryDelay * 2^(retryCount-1)`, capped by `MaxRetryDelay`; when exponential backoff is disabled it degrades to a fixed `InitialRetryDelay` (`pkg/core/models.go:443-462`). Defaults are `3 / 5s / true / 5m` (`pkg/core/models.go:433-440`, configuration mirror `config/config.go:538-543`).
- Note that `FileStatusFailed` is only a Locus compatibility value: the scheduler writes a failure straight back to `Pending` (`pkg/core/models.go:66-71`).

### 5.4 Cleanup

| Operation | Evidence | Counters/side effects |
| --- | --- | --- |
| `CleanupEmptyDirectories` | `pkg/cleanup/cleanup_service.go:298-320` | `EmptyDirectoriesRemoved` (`:313`); rescans until no progress (`:408-455`); protected-directory decision `:457-495`, resolved by `isProtectedSystemDirectory` (`:538-583`) |
| `CleanupEmptyDirectoriesForTenant` | `:340-373` | The same counters, restricted to one tenant: the tenant ID is validated before it can become a path segment (`:348-352`), an unknown tenant is rejected with `ErrTenantNotFound` without being created (`:354-358`), and each volume sweep is scoped to that tenant's directory (`:365`, `:388-404`) with the same protection rules |
| `CleanupTimedOutProcessingFiles` | `:669-691` | `TimedOutFilesReset` (`:687`); delegates to `scheduler.ResetTimedOutFiles` (`:683`) |
| `CleanupPermanentlyFailedFiles` | `:710-778` | See "the three dispositions" below |
| `CleanupCompletedFiles` | `:848-916` | `CompletedRecordsRemoved` (`:907`), `SpaceFreed` (`:908`); on failure it returns without counting (`:873-875`, `:880-882`, `:897-905`) |
| `CleanupOrphanedMetadata` | `:919-1007` | `OrphanedMetadataRemoved` (`:998`); it only handles records "confirmed absent" (`:938-971`) |
| `CleanupJunkFiles` | `pkg/cleanup/junk_files.go:67-89` | `JunkFilesRemoved`/`SpaceFreed` (`:81-82`) |
| `CleanupInvalidDatabaseBackups` | `pkg/cleanup/invalid_database_backups.go:48-80` | `InvalidDatabaseBackupsRemoved`/`SpaceFreed` (`:72-73`) |
| `OptimizeDatabases` | `pkg/cleanup/cleanup_service.go:1010-1027` | `MetadataDatabasesOptimized` (`:1017`), `QuotaDatabasesOptimized` (`:1023`) |
| `OptimizeDatabasesDetailed` | `pkg/cleanup/database_optimization.go:40-86` | Runs the same repository optimization work and additionally reports the sizes: `MetadataDatabasesOptimized`/`QuotaDatabasesOptimized` on the result (`:55`, `:71`), `SizeBefore` and the measured `SizeAfter` (`:57-58`, `:73-74`, `:94-103`), and `SpaceReclaimed` as the positive part of `SizeBefore-SizeAfter` (`:81-83`). A database tree whose size cannot be measured is skipped entirely — neither optimized nor counted (`:50`, `:59-62`, `:66`, `:75-78`, `:111-136`) — and the counts are folded into the cumulative counters (`:43`, `:56`, `:72`) |
| `CumulativeStatistics` | `pkg/cleanup/cleanup_statistics.go:88-90` | Returns a copy of the atomic counters (`:57-71`) |

**The three dispositions of permanently failed files** (`config.Cleanup.PermanentlyFailedDisposition`, default `MoveToDeadLetter`, `config/config.go:655`):

1. Gate and eligibility: the disposition enum errors during parsing (`config/config.go:1308-1310`); `Keep` skips the entire sweep (`pkg/cleanup/cleanup_service.go:721-725`); an unrecognized value is treated as "keep" with a warning (`:727-734`); `DeadLettered` rows go through a re-entry guard (`:747-749`); rows with an empty `LastFailedAt` or an unexpired retention are not processed (`:750-755`).
2. `Delete`: physical file → directory quota → tenant quota → metadata, where each step compensates the preceding steps on failure (`pkg/cleanup/cleanup_service.go:783-845`).
3. `MoveToDeadLetter`: `applyDeadLetterDisposition` (`pkg/cleanup/dead_letter.go:290-379`) — it computes the target path first; if the payload does not exist it only performs the state transition (`:321-328`); if it is already at the target it only migrates (`:329-331`); a failed move keeps the record (`:332-337`); it then writes `Status=DeadLettered`, `DeadLetteredAt`, `PhysicalPath=<dead-letter path>` (`:341-344`) and rolls back if persistence fails (`:345`/`:387-409`); finally it releases the directory/tenant quotas (`:352-376`); on success it counts `DeadLetteredFiles` (`:378`).
4. **Dead-letter layout**: `<RootPath>/[<tenantID>/][<yyyyMMdd>/][<shard pairs>/]<fileKey><ext>` (`pkg/cleanup/dead_letter.go:20-22`, `:93-114`); the date format is `20060102` (`:15`); shards accept only two lowercase hex characters (`:124-143`); root path and file name are validated (`:147-186`); the relative path always stays inside the volume root (`:111-114`).
5. **Junk files**: the list is `thumbs.db`/`.ds_store`/`desktop.ini` (case-insensitive, `pkg/cleanup/junk_files.go:16-20`); the managed payload shape `<32 lowercase hex><ext>` is explicitly excluded (`:27-34`, `:38-54`); the walk deletes and counts (`:96-134`).
6. **Invalid database backups**: named `<dbDir>.corrupted.<stamp>` and marked `.corrupted.` (`pkg/cleanup/invalid_database_backups.go:21-23`); matching requires content on both sides (`:30-36`); retention defaults to 72h and a negative value disables it (`:56-63`); both the metadata and quota roots are scanned and deduplicated (`:83-100`); the deepest match is deleted first to avoid double-counting bytes (`:125-151`); a scan failure only warns and does not fail the whole sweep (`:114-123`).
7. **Retired volumes**: `RetiredVolumeDisposition` = `Keep`/`PurgeMetadataOnly` (`pkg/core/models.go:377-415`); an undeclared volume is always `Keep` (`pkg/cleanup/dead_letter.go:271-280`); `PurgeMetadataOnly` deletes only metadata and releases quotas, and never touches physical storage (`:227-267`).
8. **Cumulative statistics**: 11 `atomic.Int64` counters (`pkg/cleanup/cleanup_statistics.go:18-30`), with `defer s.recordCumulative(stats)` in every operation (for example `pkg/cleanup/cleanup_service.go:300`, `pkg/cleanup/junk_files.go:69`).
9. **Background scheduling**: initial delay → ticker (`pkg/cleanup/background_cleanup_service.go:266-295`); per-cycle panic isolation (`:299-309`); 8 steps executed in order (`:312-454`); junk and optimization are each throttled by their own interval (`:333`/`:488-499`, `:424`/`:477-484`); `Stop` does not hold a lock while waiting for the running goroutine (`:229-252`).

### 5.5 Orphan recovery (opt-in)

- Disabled by default (`config/config.go:554-566`, `config/config.go:688-693`), and constructed only when `OrphanRecovery.Enabled` (`venue.go:721`).
- Scanning: volumes are traversed in ID order (`pkg/recovery/orphan_recovery_service.go:263-271`, `:572-580`); `filepath.Walk` plus a batch cap (default 1000, `:24`, applied at `:370-383`); `ParsePhysicalPath` accepts only `{tenantID}/[{shard}/...]/{32hex}[.ext]` (`:536-559`, `:561-570`); files that already have metadata are skipped (`:406-415`).
- Tenant scoping: `RecoverOrphanedFilesForTenant` (`:297-330`) validates the tenant ID, rejects an unknown tenant read-only through `TryGetTenant` (`:302-314`), walks only that tenant's directory per volume (`:319-327`, `:351-368`), and additionally requires every parsed path to resolve to that tenant (`:399-404`).
- Stability check: a re-stat must show unchanged size and mtime, and (optionally) the minimum file age must be satisfied (`:441-453`).
- Rebuild: reserve the tenant quota first, then the directory quota, and roll back the reserved items if either fails (`:472-490`); write the metadata (`Status = Pending`, logical directory fixed to `/`, `:492-505`, `:523-527`); roll back both quotas if persistence fails (`:507-515`).
- Lifecycle: `Start`/`Stop`/`IsRunning` (`:157-203`); the loop covers `RunOnStartup` (`:218-225`) and panic isolation (`:208-213`); `RecoverNow`, the periodic scan and the tenant-scoped call are serialized by `scanMu` (`:100-101`, `:260-261`, `:316-317`).

### 5.6 Watcher import

| Stage | Evidence |
| --- | --- |
| Scan entry point (single result path) | `pkg/watcher/file_watcher.go:539-551`; statistics are recorded exactly once (`:553-560`, `recordScanStatistics` `:635-657`) |
| Single-tenant scan | `:659-685` |
| Multi-tenant scan (the subdirectory name is the tenant ID; the tenant is resolved per directory) | `:687-743` |
| Discovery filtering (MinFileAge / MaxFileSize / patterns) | `:822-901`; glob validation and the default `*` (`:1185-1219`); Windows case-insensitivity (`:1236-1252`) |
| Concurrent-import cap | `:752-807` |
| **fingerprint** | Versioned size/mtime plus SHA-256 over bounded beginning/middle/end samples; same-size, same-timestamp content changes are detected |
| **In-flight and durable deduplication** | `reserveImportSlot` prevents overlap in one process; `core.IdempotentStoragePool` persists the stable tenant/path/fingerprint operation ID in SQLite, so a restart cannot duplicate the storage write |
| Import | The watcher prefers `WriteFileIdempotently`; the storage pool uses a fixed 256-lock stripe set and the tenant database's unique operation-ID index, with no growing in-memory ID map |
| **Post-import actions** | Delete/Move state is synchronously persisted before the action. Failures use exponential backoff, then quarantine the source revision after the configured attempt limit; retries do not repeat storage |
| **Durable source cleanup** | Enabled watchers reserve an `Importing` row before storage, transition it to a lease-claimed Delete/Move/Keep job after import, and process it independently with bounded concurrency. Stale importing reservations are recovered, terminal records are pruned, source fingerprints are rechecked before actions, and a replaced source revision immediately supersedes the old job (`pkg/watcher/source_cleanup_store.go:194-247`, `:392-416`; `pkg/watcher/source_cleanup_worker.go:128-164`, `:167-249`; `pkg/watcher/file_watcher.go:1131-1195`, `:1266-1322`). Runtime processing pauses when the global watcher service is disabled or all watchers are disabled (`pkg/watcher/source_cleanup_worker.go:128-131`, `venue.go:1120-1137`). |
| History policy | Successful Delete/Move removes the pending record; Keep retains its fingerprint. Shutdown waits for pending history writes and flushes dirty state |
| History persistence | File `imported-files.json` (`:22-27`), capped at 10000 (`:29-31`); loading drops sources that no longer exist and trims the list (`:1322-1371`); saving uses temp+rename (`:1389-1427`); writes are coalesced by a single flusher goroutine (`:1789-1829`); `Close` waits for in-flight writes before flushing (`:247-282`) and is called by `venue.closeRepositories` (`venue.go:1076`) |
| Background scan service | Each watcher advances its schedule when dispatched, one scan per watcher may be in flight, and a global bound limits total scans. A slow watcher cannot delay an unrelated watcher; shutdown cancels and joins every scan |
| Per-watcher start/stop persistence | `watcher-state.json` (`pkg/watcher/watcher_state.go:12-15`); the persisted decision takes precedence over the configured `Enabled` (`pkg/watcher/watcher_state.go:93-107`, applied at `pkg/watcher/file_watcher.go:424-426`); writes use temp+sync+rename (`watcher_state.go:163-171`) |
| Advanced knobs | Stability re-probe delay and skip age (`pkg/watcher/file_watcher.go:1135-1166`), auto-created-directory cache TTL (`:1087-1120`), import-history prune throttle (`:1666-1688`), and import-history flush debounce (`:1694-1752`); the runtime defaults are at `:43-58`, and mapping from configuration happens at `venue.go:632-638` (single watcher) and `venue.go:689-695` (root template) |
| Root derivation | ID `auto-<rootBase>-<tenantDir>` (`pkg/watcher/file_watcher_auto_manager.go:577-584`); applied/updated per subdirectory (`:181-256`, register `:241`, update `:251`); an illegal tenant name is skipped with an error (`:226-232`); `RemoveAllWatchers` deletes only the `auto-` prefix and drops the corresponding root (`:339-379`, `:440-468`); the root list is persisted to `watcher-roots.json` (`:18-21`, `:487-542`); persisted roots are merged at construction (`:114`, `:621-645`); generated watchers inherit every advanced knob from the template (`:265-287`) |

### 5.7 Quota reconciliation `ReconcileQuotaCounts`

- Public entry point `Venue.ReconcileQuotaCounts` (`venue.go:791-799`); construction calls the same two halves (`venue.go:399`, `venue.go:423`).
- Counted status set: `Pending`/`Processing`/`Completed`/`Failed`/`PermanentlyFailed`/`DeleteRequested` (`venue.go:892-899`) — it does **not** include `DeadLettered` (whose quota was already released during the dead-letter move, `pkg/cleanup/dead_letter.go:352-376`).
- Paged reads: when the repository implements `StatusPageReader`, 500 records per page (`quotaReconcilePageSize`, `venue.go:782`, `venue.go:808-825`); otherwise it falls back to the unbounded `GetByStatus(...,0)` (`venue.go:828-836`); ctx is checked before each status and before each record (`venue.go:805`, `:816`).
- Tenant counts: counted per tenant and then overwritten with `SetFileCount` (`venue.go:841-859`); `SetFileCount` does not install an override value (`pkg/quota/tenant_quota_manager.go:160-163`).
- Directory counts: seeded first from existing quota rows (preserving explicit limits), then accumulated and overwritten using `directorypath.Normalize(record.DirectoryPath)` (`venue.go:861-890`).

## 6. Configuration Model

### 6.1 Uniqueness and no file I/O

- The only public runtime model is `config.Config` (`AGENTS.md:57-58`), and `NewVenue` accepts `*config.Config` directly (`venue.go:77`).
- `config` does not import Viper and performs no file I/O (`AGENTS.md:65`). Verified: among non-test `.go` files, only `viperconfig/viper.go:10` and `examples/viper-config/main.go:13` import `spf13/viper`; `config/boundary_test.go:51`, `:108-115` are boundary tests forbidding `config` from importing Viper.
- `viperconfig` placement: a top-level package and the only Viper → `config.Config` mapping point (`viperconfig/viper.go:14-32`): it starts from `DefaultConfig()`, uses `ZeroFields=false` (`:19-21`), forces `Logging = nil` (`:26`), then `ApplyDefaults` (`:27`) and `Validate` (`:28`).

### 6.2 Every nested type supports chainable construction

Constructors and fluent methods: `config/fluent.go:148` (FileWatcherService), `:155` (SourceCleanup), `:185` (FileWatcherRoot), `:358` (Statistics), `:413` (StatisticsDimension), `:442` (StatisticsOutput), `:475` (RetryPolicy), `:505` (TenantManager), `:523` (Metadata), `:541` (Sqlite), `:669` (Volume), `:696` (Tenant), `:719` (FileWatcher), `:853` (Cleanup), `:986` (DeadLetter), `:1016` (RetiredVolumes). Top level: `New`/`DefaultConfig` (`config/config.go:526`, `:531`); the SQLite engine configuration has its own chainable set (`config/fluent.go:64`, `:541`, `:547`, `:553`, `:559`, `:565`, `:572`, `:578`, `:584`, `:590`, `:597`, `:603`, `:610`, `:619`, `:626`, `:633`).

Collection semantics: `WithVolumes`/`WithTenants`/`WithFileWatchers` **replace** (`config/fluent.go:72`, `:91`, `:110`; rule at `AGENTS.md:61`); `AddVolume`/`AddTenant`/`AddFileWatcherRoot`/`AddFileWatcher`/`AddRetiredVolume` **append** (`:83`, `:102`, `:132`, `:302`, `:978`).

### 6.3 Binding tag requirements

Every exported configuration field must carry `json`/`yaml`/`mapstructure` simultaneously (`AGENTS.md:63`), for example `config/config.go:43-69`. Runtime-only fields must be `-`: `Config.Logging` (`config/config.go:69`), `logging.Config.Handler` (`pkg/logging/logging.go:17`).

### 6.4 `ApplyDefaults` semantics and the boolean zero-value trap

`ApplyDefaults` fills only **zero-value** settings (paths, durations, capacities, string enums), and the documentation states plainly that "boolean defaults must start from `DefaultConfig`/`New`" (`config/config.go:659-660`). The full list of fields it handles is at `config/config.go:661-852`.

Consequently, `&config.Config{}` + `ApplyDefaults()` cannot restore the following booleans that default to **true** (or carry non-zero semantics):

| Field | Default | Default location | Handled by `ApplyDefaults`? |
| --- | --- | --- | --- |
| `AutoCreateTenants` | true | `config/config.go:536` | No |
| `EnableDatabaseHealthCheck` | true | `config/config.go:537` | No |
| `EnableBackgroundCleanup` | true | `config/config.go:586` | No |
| `RetryPolicy.UseExponentialBackoff` | true | `config/config.go:541` | No (it only fills three durations/counts, `:675-683`) |
| `Volumes[i].EnableFsync` | true | `config/config.go:572` | No (it only fills Type/InitialDelay/HealthCheckDelay, `:823-833`) |
| `Cleanup.CleanupEmptyDirectories` / `CleanupTimedOutFiles` / `CleanupPermanentlyFailedFiles` / `CleanupCompletedRecords` / `CleanupJunkFiles` / `CleanupInvalidDatabaseBackups` / `OptimizeDatabases` | all true | `config/config.go:590`, `:591`, `:595`, `:604`, `:609`, `:611`, `:613` | No (it only fills durations/batch sizes/enums/RootPath, `:693-728`) |
| `Cleanup.EnableBackgroundTimedOutReclaim` | true | `config/config.go:619` | No. The trap is observable at runtime: the zero value reaches the scheduler as `false` and disables the opportunistic background pass (`pkg/scheduler/file_scheduler.go:508-512`, wiring `venue.go:494`) |
| `Cleanup.DeadLetter.IncludeTenantInPath` / `IncludeDatePartition` | both true | `config/config.go:599`, `:600` | No (it only fills `RootPath` and `ShardingDepth`, `:723-728`) |
| `FileWatcherService.Enabled` | true | `config/config.go:579` | No; the constructor itself is default-safe (it starts enabled unless told otherwise) and configuration can now really disable it at construction through `InitialEnabled` (`venue.go:651-657`, `pkg/watcher/background_file_watcher_service.go:269-279`); a persisted document still outranks that input (§6.5) |
| `FileWatchers[i].Enabled` / `FileWatcherRoots[i].Enabled` | Caller-dependent; the configuration example sets true | `config/config.go:228`, `:283` | No |
| `Statistics.Dimensions.VolumeID` / `WatcherID` / `Operation` | all true | `config/config.go:644`, `:645`, `:646` | No (it only fills WindowSize/Retention/MaxSeries/Output, `:753-770`) |

A typical observable consequence: when constructed from `&config.Config{}`, `Cleanup.DeadLetter` is filled in by `ApplyDefaults` with `RootPath=".deadletter"` and `ShardingDepth=2` (`config/config.go:723-728`), so the "apply defaults only when all fields are zero" branch of `DeadLetterOptions.withDefaults()` (`pkg/cleanup/dead_letter.go:59-67`) no longer fires, and the resulting dead-letter paths **lose the tenant and date partitions**. The default values themselves live in `DefaultConfig` (`config/config.go:597-602`).

For contrast: the throttling/debounce switches of `config.FileWatcherConfig`/`FileWatcherRootConfig` use inverted naming (`DisableImportedFilesPruneThrottle`, `DisableImportedFilesHistoryFlushDebounce`, `config/config.go:251-268`, `:322-337`) and therefore avoid the zero-value trap. The positive-polarity runtime fields in `pkg/core.FileWatcherConfiguration` (`EnableImportedFilesPruneThrottle` and friends, `pkg/core/interfaces.go:700-716`) are inverted back when `venue.go` maps the configuration (`venue.go:635`, `:637`, `:692`, `:694`) and are consumed by the import path (`pkg/watcher/file_watcher.go:1667`, `:1695`).

### 6.5 Construction-time watcher enablement versus the persisted document

`FileWatcherService.Enabled` is not the only input that decides whether the background watcher service scans. The constructor first folds the deprecated direct fields into the service options, then starts from "enabled" and applies the explicit construction-time override when it is present (`pkg/watcher/background_file_watcher_service.go:252-279`): only `InitialEnabled` can turn scanning off at construction, because a plain `bool` cannot distinguish "unset" from an explicit `false`. `venue.go` always passes the configured value as `InitialEnabled` (`venue.go:651-657`), so `watcherServiceOptions.enabled: false` in configuration now really starts the service disabled. That override is a construction-time default, not a decision of record: it is never written to `file-watcher-options.json`, and a persisted document produced by `SetEnabled` or `UpdateOptions` replaces the options snapshot entirely and outranks it (`pkg/watcher/background_file_watcher_service.go:115-126`, `:289-306`).

## 7. Logging and Errors

### 7.1 Instance-scoped `logging.Runtime`

- The state is an `atomic.Pointer[runtimeState]`; `slog.Default` is never read or written (`pkg/logging/logging.go:33-37`).
- `New` builds `slog.New` from the **injected handler** (`:40-48`); `Disabled` leaves the state nil (`:51-53`), in which case `Emit` returns early (`:86-90`) — that is, "disabled means silent".
- `Config.Logging == nil` means disabled (`AGENTS.md:75`; `venue.go:88-95`).
- Handler panic isolation: both `Emit` and `Enabled` swallow panics with `defer recover()` (`:64-69`, `:79-82`), guaranteeing that logging cannot affect storage operations (`AGENTS.md:77`).
- The caller owns the handler and the writer (`pkg/logging/logging.go:11-14`; `AGENTS.md:76`).
- Structure: every `Emit` always attaches the `component` and `event` attributes (`:92-97`); `Record` is defined at `:20-27`.
- Composition-time defaults: `venue.go:83`; `Venue.emit` always emits with `component=venue` (`venue.go:1328-1332`); errors log only the type, never the original text (`errorTypeAttr`, `venue.go:1334-1336`).
- Examples of implementations that do not leak physical paths: quarantine logging records only the base name (`venue.go:1102-1108`); the SQLite repository's degraded-state warnings carry only a `reason` label (`pkg/metadata/sqlite_repository.go:2023-2041`), which the quarantine and restore paths use (`pkg/metadata/sqlite_corruption.go:167`, `:171`, `:182`, `:185`, `:189`); periodic backup-cycle failures record only the error type (`venue_metadata_backup.go:341-353`); scan errors carry no path or original name (`pkg/watcher/file_watcher.go:724-732`).

### 7.2 Stable sentinel errors (`pkg/core/errors.go`)

| Error | Line | Main usage sites |
| --- | --- | --- |
| `ErrTenantNotFound` | `:12` | `pkg/tenant/manager.go:150`, `:198` |
| `ErrTenantDisabled` | `:15` | `pkg/pool/storage_pool.go:205`, `pkg/scheduler/file_scheduler.go:266`, `:315` |
| `ErrTenantSuspended` | `:18` | Not returned by any runtime code (definition only) |
| `ErrTenantAlreadyExists` | `:21` | `venue.go:328`, `pkg/tenant/manager.go:252` |
| `ErrTenantQuotaExceeded` | `:26` | `pkg/quota/tenant_quota_manager.go:142` |
| `ErrDirectoryQuotaExceeded` | `:29` | `pkg/quota/directory_quota_manager.go:66` |
| `ErrInsufficientStorage` | `:34` | `pkg/pool/storage_pool.go:427`, `pkg/pool/volume_selector.go:55`, `:86`, `:116`, `:131`, `:153`, `:167` |
| `ErrStorageVolumeUnavailable` / `ErrVolumeAlreadyMounted` | `:37`, `:40` | `ErrStorageVolumeUnavailable` is returned by `venue.waitForVolume` when a volume never answers two consecutive probes (`venue.go:246-251`); `ErrVolumeAlreadyMounted` remains definition only |
| `ErrFileNotFound` | `:45` | `pkg/metadata/sqlite_repository.go:597`, `:808`, `:913`; `pkg/volume/local_volume.go:413`, `:494` |
| `ErrFileAlreadyProcessing` | `:48` | Definition only; no runtime return site |
| `ErrNoFilesAvailable` | `:55` | **Deprecated**: an empty queue now returns `(nil, nil)` (comment `:50-54`) |
| `ErrWatcherNotFound` | `:58` | `pkg/watcher/file_watcher.go:1274`, `:1279` |
| `ErrInvalidFileKey` / `ErrFileAlreadyExists` | `:61`, `:64` | Definition only; no runtime return site |
| `ErrFileNotClaimable` | `:73` | `pkg/metadata/sqlite_repository.go:920`, `:924`, `:928`; semantic classification in `pkg/scheduler/file_scheduler.go:98-100` |
| `ErrProcessingLeaseMismatch` | `:76` | Exposed by `FileProcessingLeaseMismatchError.Unwrap` (`:112-115`); constructed by `newLeaseMismatchError` (`pkg/metadata/metadata_support.go:64-80`) |
| `ErrInvalidArgument` | `:120` | Argument validation across the repository |
| `ErrOperationCanceled` | `:123` | Definition only; cancellation paths return `ctx.Err()` (for example `pkg/scheduler/file_scheduler.go:366-368`) |
| `ErrDatabaseError` | `:126` | `pkg/metadata/sqlite_repository.go:891`, `:1026`, `:1142`, `:1314`; `venue.go:1192` |
| `ErrPathTraversalAttempt` | `:129` | `pkg/volume/path_sanitizer.go:48`, `:56`, `pkg/core/tenantid.go:87` |

Convention: domain checks use `errors.Is` (`AGENTS.md:119`); wrapping carries safe context (tenant, fileKey, component) and uses `%w` (`AGENTS.md:118`).

## 8. Concurrency and Durability Invariants

### 8.1 Metadata transactions

Metadata lives in one SQLite database per tenant, `{MetadataDirectory}/{tenantId}/metadata.db`, created lazily on the tenant's first access (`pkg/metadata/sqlite_repository.go:30-33`, `:1488-1502`). Every write path is a statement (or a small prepared loop) executed inside one explicit transaction: `AddOrUpdate` (`pkg/metadata/sqlite_repository.go:488-518`), `AddOrUpdateBatch` (`:520-570`), `Delete` (`:606-634`), `DeleteBatch` (`:636-682`), `UpdateStatus` (`:772-824`), `CompareAndTransitionToProcessing` (`:826-898`), `CompareAndUpdateProcessing` (`:931-1033`). The DSN's `_txlock=immediate` makes `BeginTx` issue `BEGIN IMMEDIATE`, so the write lock is taken when the transaction starts instead of failing on a mid-transaction lock upgrade (`pkg/sqlite/sqlite.go:19-23`, `:229`; `pkg/metadata/sqlite_repository.go:1717-1736`).

Connection and pragma policy: the DSN carries `journal_mode`, `synchronous`, `cache_size`, `busy_timeout`, `foreign_keys=off`, `temp_store=MEMORY` and `_txlock=immediate`, so every pooled connection is configured before the first statement runs (`pkg/sqlite/sqlite.go:203-231`), and the repository re-asserts the same values on open, which also makes the effective values observable through a query (`pkg/metadata/sqlite_repository.go:1587-1605`). Each handle is pinned to exactly one connection (`SetMaxOpenConns(1)`/`SetMaxIdleConns(1)`, `pkg/sqlite/sqlite.go:275-277`), so one tenant's statements serialize on one connection and maintenance statements (`VACUUM`, checkpoint) cannot interleave with a transaction body (`pkg/metadata/sqlite_repository.go:339-343`, `:1180-1187`).

The schema is idempotent and applied on every first access: one `files` table with `file_key` as the primary key, status/ordering and lease indexes, and a partial unique `(tenant_id, import_operation_id)` index. Schema version 2 adds the nullable operation-ID column to existing version-1 tenant databases before creating the index; a database written by a newer schema revision is refused.

Consistency strategy: bounded retries for transient lock conflicts, retrying only `SQLITE_BUSY`/`SQLITE_LOCKED` (`pkg/metadata/sqlite_repository.go:1758-1778`), with 3 attempts by default (`pkg/metadata/conflict_retry.go:13`) and a linear backoff starting at 5ms (`:18`, `:53`), while respecting ctx (`:38-40`, `:54-58`). A canceled context keeps its own identity and everything else becomes `ErrDatabaseError` (`pkg/metadata/sqlite_repository.go:1738-1752`).

### 8.2 CAS / lease

See §5.2 and §5.3. In short: a claim must satisfy both the status and the availability time in one conditional `UPDATE` (`pkg/metadata/sqlite_repository.go:855-869`); completion/failure/reclaim must carry a matching `ProcessingStartTime`, checked in the transaction and repeated as a predicate of the guarded `UPDATE` (`:974-987`, `:1038-1053`), so **a stale worker cannot overwrite a newer claim** (`AGENTS.md:97`; comment at `pkg/scheduler/file_scheduler.go:819-821`). The scheduler close path additionally stops in-flight background reclaim passes before the repository is closed (`pkg/scheduler/file_scheduler.go:632-641`, `venue.go:1065-1070`).

### 8.3 Per-directory synchronization

The directory-quota manager uses 256 mutexes with FNV-1a hash sharding, keyed by `tenantID + 0xff + directoryPath` (`pkg/quota/directory_quota_manager.go:16`, `:191-206`); every operation that reads or writes a count takes the lock first (`:36-37`, `:55-56`, `:83-84`, `:100-101`, `:118-119`, `:149-150`, `:170-171`).

### 8.4 Atomics and `sync.Map`

| Mechanism | Location | Protected object |
| --- | --- | --- |
| `sync.Map` | `pkg/watcher/file_watcher.go:104`, `:140` | Watcher registry, import history (including `InFlightToken`) |
| `sync.Map` + CAS loop | `pkg/quota/tenant_quota_manager.go:53` | Per-tenant quota entries |
| `sync.Map` | `pkg/statistics/recorder.go:230` | Statistics series (creation/trimming serialized by `mu`, `:234`) |
| `atomic.Int64` ×11 | `pkg/cleanup/cleanup_statistics.go:19-29` | Cumulative cleanup statistics |
| `atomic.Int64` | `pkg/quota/tenant_quota_manager.go:58` | Global quota limit |
| `atomic.Uint64` | `pkg/watcher/file_watcher.go:1548` | In-flight reservation sequence number |
| `atomic.Int64` | `pkg/statistics/recorder.go:206`, `:241` | Per-series count, record count (triggers trimming) |
| `atomic.Pointer` | `pkg/watcher/background_file_watcher_service.go:181`, `pkg/logging/logging.go:36` | Global watcher option snapshot, logging runtime state |
| `sync.Mutex`/`RWMutex` | `pkg/pool/storage_pool.go:82`, `pkg/scheduler/file_scheduler.go:126`, `pkg/cleanup/cleanup_service.go:136`, `pkg/volume/local_volume.go:100-103`, `pkg/metadata/cache.go:30` | Capacity cache, reclaim cooldown, cleanup-service volume snapshot/cumulative counters, health-probe single-flight, metadata LRU |
| `sync.RWMutex` + per-tenant `sync.Mutex` | `pkg/metadata/sqlite_repository.go:305-312`, `:343`; `pkg/quota/sqlite_directory_quota_repository.go:237-239`, `:261` | The per-tenant handle table (including the in-flight-open map) and each tenant's own statement serialization |

Immutable-snapshot convention: watcher configuration is handed out as a copy (`snapshotConfig`, `pkg/watcher/file_watcher.go:1286-1294`; `watcherEntry.mu`, `:178-181`); root templates are handed out as copies (`pkg/watcher/file_watcher_auto_manager.go:605-619`); the `storagePool` volumes/pathGenerator/selector are construction-time dependencies that are never replaced afterwards, so they can be read without a lock (`pkg/pool/storage_pool.go:63-65`); the metadata cache `get`/`set` both return/store copies (`pkg/metadata/cache.go:70-72`, `:96-97`) and refuse to let an older `UpdatedAt` overwrite a newer value (`:84-94`).

### 8.5 FIFO

FIFO is the index order of the `files` table, not a separately maintained structure: `idx_files_status_available` is `(status, available_for_processing_at, created_at, file_key)` (`pkg/metadata/sqlite_repository.go:89-91`), and every queue read orders by exactly that triple — `GetPendingFiles` with the inclusive predicate `available_for_processing_at <= now` (`:763-770`) and `GetByStatus` without it (`:719-724`). Therefore the iteration order is "availability time → arrival time → fileKey", and a file that is immediately available is stored with the sentinel `0` so it sorts first (`:1969-1988`). The keyset page query repeats the same three-part total order and continues from `(available, created, file_key)`, so pages neither overlap nor skip records (`pkg/metadata/sqlite_paging.go:28-47`, `:61-68`).

### 8.6 Long-task paging and cancellation

- Cleanup status scans prefer `core.StatusPageReader` with 500 records per page (`pkg/cleanup/cleanup_service.go:21`, `:232-283`), and return an explicit error when the cursor does not advance (`:278-280`).
- Quota reconciliation pages the same way (`venue.go:808-825`).
- ctx checks inside loops: claiming (`pkg/scheduler/file_scheduler.go:366-368`, `:407-409`), cleanup (`pkg/cleanup/cleanup_service.go:308-310`, `:361-363`, `:426-428`), import (`pkg/watcher/file_watcher.go:772`, `:879`), recovery (`pkg/recovery/orphan_recovery_service.go:264-266`, `:320-322`), health check (`pkg/health/database_health_checker.go:175-177`).

### 8.7 Explicit durability trade-offs (`synchronous` / fsync)

| Location | Configuration | Behavior |
| --- | --- | --- |
| Metadata and quota databases | `Sqlite.SynchronousMode` (`config/config.go:97-100`; default `NORMAL` at `:553`) and `Sqlite.JournalMode` (`:92-95`; default `WAL` at `:552`) | Both travel in the DSN as `_synchronous`/`_journal_mode` (`pkg/sqlite/sqlite.go:224-225`) and are re-asserted as PRAGMAs (`pkg/metadata/sqlite_repository.go:1592-1593`). `NORMAL` is safe against a process crash while a power failure can lose the most recent commits; `FULL` fsyncs every commit (`pkg/sqlite/sqlite.go:119-124`) |
| Lock waiting | `Sqlite.BusyTimeoutMs` (`config/config.go:107-108`; default 5000 at `:555`) | Bound in the DSN as `_busy_timeout` (`pkg/sqlite/sqlite.go:223`); a statement waits that long for a contended lock before failing with `SQLITE_BUSY` (`pkg/sqlite/sqlite.go:129-131`) |
| Write-ahead-log growth | `Sqlite.CheckpointAfterBatch` (`config/config.go:110-112`; default false at `:556`) | Not read by the engine layer: the repository that commits a batch is responsible for calling `PRAGMA wal_checkpoint(PASSIVE)` after a successful commit only (`pkg/sqlite/sqlite.go:132-140`, `:315-332`) |
| Physical files | `VolumeConfig.EnableFsync` (`config/config.go:191`, default true at `config/config.go:572`) | `file.Sync()` after writing (`pkg/volume/local_volume.go:389-391`) |
| Backup files | No switch | Every per-tenant backup is a complete `VACUUM INTO` copy, verified with `PRAGMA integrity_check(1)` (`pkg/metadata/sqlite_backup.go:87-131`); a restore stages a copy, `Sync`s it and only renames it onto the database path after it verifies (`pkg/metadata/sqlite_corruption.go:199-241`) |
| Watcher runtime state / global options / root list | No switch | All use temp + `Sync` + rename (`pkg/watcher/watcher_state.go:180-209`, `pkg/watcher/background_file_watcher_service.go:977-1006`, `pkg/watcher/file_watcher_auto_manager.go:505-534`) |
| Import history | No switch | temp + rename only, with **no fsync** (`pkg/watcher/file_watcher.go:1389-1427`) |
| Intermediate copy for cross-device moves | No switch | staging `Sync()` followed by rename (`pkg/volume/local_volume.go:578`) |

Reclaiming database space is not an ad-hoc sweep: deletion and maintenance reclaim it through WAL checkpointing plus `VACUUM` on the maintenance cycle (`AGENTS.md:112`). The metadata repository runs `PRAGMA wal_checkpoint(TRUNCATE)` and then `VACUUM` per open tenant handle during `Optimize` (`pkg/metadata/sqlite_repository.go:1124-1205`), the quota repository does the same (`pkg/quota/sqlite_directory_quota_repository.go:528-583`), and both checkpoint before closing a handle so the `-wal` sidecar is empty and the file handle can be released (`pkg/metadata/sqlite_repository.go:1273-1292`; `pkg/quota/sqlite_directory_quota_repository.go:607-635`).

### 8.8 Backup, quarantine, structural health and the pure-Go build constraint

- **Per-tenant backup.** `VACUUM INTO` reads the source inside a read transaction and rewrites it into a new complete, directly openable database file, so the tenant keeps serving reads and writes while the copy is made and a failed copy leaves the source intact (`pkg/sqlite/sqlite.go:382-412`). The repository exposes one tenant at a time (`BackupTenant`, `pkg/metadata/sqlite_backup.go:19-62`) and a whole-repository zip container with one `"<tenantId>/metadata.db"` entry per tenant (`Backup`, `pkg/metadata/sqlite_repository.go:1207-1244`); the returned `since` is always `0` because SQLite has no engine sequence number (`:1210-1212`). The periodic runner writes `{BackupDirectory}/{tenantId}/metadata.<stamp>.bak` (`venue_metadata_backup.go:271-293`, naming `:355-361`), prunes by retention (`:295-320`), and is enabled only when `Sqlite.BackupDirectory` is non-empty and `Sqlite.BackupInterval` is positive (`venue.go:362`). Verification with `PRAGMA integrity_check(1)` runs on every produced backup unless `Sqlite.SkipBackupVerification` is set, and a backup that fails the check is deleted rather than kept (`pkg/metadata/sqlite_backup.go:54-60`, `:117-131`).
- **File-level quarantine.** Only a positive corruption verdict quarantines a file: `IsCorruptionError` accepts `SQLITE_CORRUPT` (including extended codes), `SQLITE_NOTADB` and `SQLITE_IOERR` plus the canonical SQLite corruption messages, and deliberately rejects `SQLITE_BUSY`, `SQLITE_LOCKED`, `SQLITE_READONLY` and unknown errors (`pkg/sqlite/sqlite.go:414-458`). On a corruption verdict with `Sqlite.RecoverCorruptedDatabase` enabled, the repository renames `metadata.db` to a timestamped `.corrupted.<stamp>` sibling (`pkg/metadata/sqlite_corruption.go:25-62`), quarantines the `-wal`/`-shm` sidecars with it (`:64-84`), recreates an empty database, and then tries to replace it from the newest readable backup when `Sqlite.AutoRestoreFromBackup` is set (`pkg/metadata/sqlite_repository.go:1504-1552`; candidate walk `pkg/metadata/sqlite_corruption.go:163-191`). Quarantined files are pruned on the next open once `Sqlite.CorruptedDatabaseRetention` (default 72h, negative disables) has elapsed (`pkg/metadata/sqlite_corruption.go:86-129`; default `pkg/metadata/metadata_support.go:42-44`); the same retention drives `CleanupInvalidDatabaseBackups` (`venue.go:555`, `pkg/cleanup/invalid_database_backups.go:48-80`).
- **Structural health check.** The health checker never opens a database, so it can run while the process holds the live handles and never touches WAL locks: it enumerates `{MetadataDirectory}/{tenantId}/metadata.db` and the matching `{QuotaDirectory}/{tenantId}/quotas.db` from the metadata directory names, and classifies each file structurally — regular file, at least 100 bytes, starting with the 16-byte `SQLite format 3\0` header (`pkg/health/database_health_checker.go:318-353`, `:592-637`). A missing file means "no database yet" and is healthy (`:344-347`, `:611-614`); a file that fails the rules is reported as corrupted with a path-free reason. Enumeration reads directory names only (`:507-553`) and the sizes are collected separately (`:355-371`).
- **Pure-Go build.** The driver is `modernc.org/sqlite` (pure Go, no cgo), registered under `sqlite.DriverName`; the package must build and test with `CGO_ENABLED=0`, and no build tag or fallback selects a cgo binding (`pkg/sqlite/sqlite.go:12-14`, `:42-53`). `CGO_ENABLED=0 go build ./...` is a required gate (`docs/README.md:52-65`).

## 9. Testing and Verification Conventions

### 9.1 Commands

The required gate set for every behavioral change is: `go build ./...`, `go vet ./...`, `go test -count=1 ./...`, `go test -race -count=1 ./...`, `golangci-lint run`, and `CGO_ENABLED=0 go build ./...` — the project must build without cgo (`docs/README.md:52-65`). `AGENTS.md:137-146` lists the same commands except the cgo check and the `-count=1` flag. A changed behavior must come with a regression test that fails first and passes afterwards (`AGENTS.md:135`); run the race tests before any commit (`AGENTS.md:157`).

### 9.2 Go caches and the temporary directory

When the default Go cache is not writable in the current environment, keep the Go caches and the temporary directory inside the repository's ignored build area (`AGENTS.md:160-168`, `README.md:977-986`). Machine-specific paths must not be committed to documentation or configuration (`docs/README.md:67-69`). No repository file records which directories this environment actually resolves to; see §10.6.

### 9.3 Benchmark entry points

- All system benchmarks go through the public entry point: `setupBenchmarkSystem` builds the configuration fluently with `config.New()` and then calls `venue.NewVenue` (`test/benchmark/system_bench_test.go:31-45`) and `Start` (`:46`), shutting down through `runtime.Stop()` (`:49-53`).
- Public benchmarks: `BenchmarkWriteFile` (`:68`), `BenchmarkWriteFile_Parallel` (`:87`), `BenchmarkReadFile` (`:110`), `BenchmarkGetNextFileForProcessing` (`:137`), `BenchmarkCompleteWorkflow` (`:174`), `BenchmarkMetadataOperations` (`:210`), `BenchmarkQuotaOperations` (`:277`), `BenchmarkConcurrentProcessing` (`:310`).
- The cleanup-scale benchmark `BenchmarkCleanupCompletedRecords` (`test/benchmark/cleanup_scale_bench_test.go:95`) also uses `venue.NewVenue` (`:83`), but seeds records through the public accessor `runtime.MetadataRepository().AddOrUpdateBatch` (`:42`) and measures with `runtime.CleanupService().CleanupCompletedFiles` (`:117`).
- Conventions: fixture generation stays outside the timed sections (`b.ResetTimer`, `system_bench_test.go:75`, `:120`; `b.StopTimer`/`b.StartTimer`, `cleanup_scale_bench_test.go:113-115`); `b.ReportAllocs` (`system_bench_test.go:76`); an empty queue is a normal result, not an error (`:161-169`, `:334-337`).

### 9.4 Other conventions

- Filesystem tests use `t.TempDir()` (`AGENTS.md:151`; `system_bench_test.go:29`, `cleanup_scale_bench_test.go:72`).
- Shutdown is registered before the temporary directory is cleaned up (`AGENTS.md:152`; `system_bench_test.go:49-53`, `cleanup_scale_bench_test.go:90`).
- Integration paths go through `venue.NewVenue`, unless the test targets a lower-level component (`AGENTS.md:153`). Existing examples: `venue_lifecycle_test.go:34` (`Stop` without `Start` still releases the repositories), `:56` (idempotent `Stop`), `:83` (a failed construction releases already-opened resources), `:499` (immediate reclaim on an empty queue is wired), `:349` (reconciliation repairs drift), `:263`/`:300` (orphan recovery is opt-in and re-enqueues).
- Same-`fileKey` behavior across tenants must have a test (`AGENTS.md:154`); the tenant-isolation tests are `pkg/metadata/sqlite_repository_test.go:308` and `pkg/quota/sqlite_directory_quota_repository_test.go:363`.

## 10. Unverified / Open

### 10.1 Referenced documents

This document references `docs/locus-alignment.md` and `docs/sqlite-storage-design.md`, both of which are present, and §1.3 records the division of labour between the three surviving design documents (`docs/README.md:10-15`). One earlier review document (`docs/venue-locus-alignment-review.md`) has no copy in the tree and must not be cited; the two findings it carried are now inline: the health-probe cache TTL is exposed as `VolumeConfig.HealthCheckCacheTTL` (`config/config.go:193-196`) and cached by the volume (`pkg/volume/local_volume.go:185-206`), and its "still not implemented" list is superseded by the resolved-gap table in `docs/locus-alignment.md` §5.1.

### 10.2 README/AGENTS vs code inconsistencies

The README documents the reclaim bounds, startup volume preparation and the Locus divergences, including backup-based recoverability and the fail-fast switch, as implemented. The direct watcher-registration minimum-age default is also aligned at `5s`.

| # | Location | Problem | Evidence |
| --- | --- | --- | --- |
| U2 | `README.md:1040-1058` | The Repository Layout still does not list `internal/directorypath`, `pkg/sqlite` or `pkg/metadata/sqlite_repository.go`, although those exist and `internal/directorypath` is in the ownership list of `AGENTS.md:33-51` (`:50`) | `README.md:1040-1058` vs `AGENTS.md:49-50`, `internal/directorypath/path.go`, `pkg/sqlite/sqlite.go`, `pkg/metadata/sqlite_repository.go` |
| U3 | `README.md:945` | A reference to a `docs/` document that no longer exists in the tree | `README.md:945` vs the file listing of `docs/` |

### 10.3 Declared configuration with no runtime consumer

The rows that this section previously listed as "declared but not wired" are now consumed at runtime; only the two optional recovery tuning fields remain unpassed by the public entry point.

| Field | Declared at | Current state |
| --- | --- | --- |
| `OrphanRecoveryServiceOptions.BatchSize`, `MinimumFileAge` | `pkg/recovery/orphan_recovery_service.go:66-74` | Not passed by `venue.go:723-733`, so they stay at the runtime defaults: `BatchSize` 1000 (`pkg/recovery/orphan_recovery_service.go:24`, applied at `:131-134`) and `MinimumFileAge` 0 (`:152`) |

Now-consumed fields, with the evidence that closes them:

- `Cleanup.EmptyQueueReclaimBatchSize` (`config/config.go:451-454`) → `venue.go:493` → `pkg/scheduler/file_scheduler.go:434-436` (zero selects the default, negative disables; test `venue_startup_recovery_test.go:150`).
- `Cleanup.EnableBackgroundTimedOutReclaim` / `BackgroundTimedOutReclaimBatchSize` (`config/config.go:456-464`) → `venue.go:494-495` → `pkg/scheduler/file_scheduler.go:508-512`, `:589-594` (test `venue_startup_recovery_test.go:219`).
- `Config.FailFastOnStartupRecoveryFailure` (`config/config.go:50-53`) → `venue.go:1182` (tests `venue_startup_recovery_test.go:48`, `:91`).
- `VolumeConfig.WarmupOnStartup` (`config/config.go:208-211`), `InitialDelay` (`:198-202`), `HealthCheckDelay` (`:204-206`) → `venue.go:217`, `:240`, `:262-271`.
- `VolumeConfig.HealthCheckCacheTTL` (`config/config.go:193-196`) → `venue.go:455` → `pkg/volume/local_volume.go:139-142`, `:185-206`.
- `FileWatcherConfig` and `FileWatcherRootConfig` advanced knobs (`config/config.go:238-268`, `:309-337`) → mapped at `venue.go:632-638` and `venue.go:689-695` → consumed at `pkg/watcher/file_watcher.go:1087-1120`, `:1135-1166`, `:1666-1688`, `:1694-1752`; root templates propagate them to generated watchers (`pkg/watcher/file_watcher_auto_manager.go:280-286`).
- The positive-polarity `core.FileWatcherConfiguration` switches (`pkg/core/interfaces.go:700-716`) → written inverted at `venue.go:635`, `:637`, `:692`, `:694` → read at `pkg/watcher/file_watcher.go:1667`, `:1695`.
- `FileWatcherService.Enabled` (`config/config.go:342-345`) → `venue.go:651-657` through `InitialEnabled` (`pkg/watcher/background_file_watcher_service.go:269-279`, §6.5).

### 10.4 Verification performed in this revision

The storage-engine migration is complete in the working tree, so no statement in this document describes BadgerDB as the live construction path, and none of the engine text was carried over unverified:

- The BadgerDB repository implementations and their support files no longer exist (`pkg/metadata/badger_repository.go`, `backup.go`, `corrupted_database.go`, `status_page.go`, `migration.go`, `pkg/quota/directory_quota_repository.go`), `config.BadgerDBConfig` and its fluent methods are gone from `config/config.go` and `config/fluent.go`, and `config.SqliteConfig` (`config/config.go:91-175`) is the only engine configuration.
- `venue.go`, `venue_metadata_backup.go`, `pkg/health/database_health_checker.go`, `config/config.go`, `config/fluent.go`, `pkg/sqlite/sqlite.go`, `pkg/metadata/sqlite_repository.go`, `pkg/metadata/sqlite_backup.go`, `pkg/metadata/sqlite_corruption.go`, `pkg/metadata/sqlite_paging.go`, `pkg/metadata/metadata_support.go`, `pkg/metadata/conflict_retry.go` and `pkg/quota/sqlite_directory_quota_repository.go` were read in this revision. Every engine citation in §1.2-§1.3, §2, §3.1-§3.2, §4.2-§4.5, §5.1-§5.3, §6, §7, §8 and §10 was resolved against the file and line it names, and §9's toolchain and test-file citations were corrected against `AGENTS.md`, `README.md` and the test files. Non-engine citations in the same sections (the `pkg/pool`, `pkg/volume` and `pkg/cleanup` write-path and cleanup rows) were not all re-derived.
- The earlier anchor-drift table and the note that `config/`, `venue.go`, `venue_metadata_backup.go`, `pkg/health/database_health_checker.go` and the SQLite packages were "one revision behind" or unread were removed: those files were re-read here, and the citations were renumbered to the current lengths (for example `venue.go` is 1336 lines, `config/config.go` 1385, `pkg/metadata/sqlite_repository.go` 2041).
- §5.4-§5.7, §8.3, §8.4 and §8.6 describe subsystems that the engine migration did not change (`pkg/cleanup`, `pkg/recovery`, `pkg/watcher`, the directory-quota lock sharding) and their citations were spot-checked rather than re-derived from scratch; §10.5 records what was not read at all.

### 10.5 Not yet read line by line

- The platform implementations of `TotalCapacity`/`AvailableSpace` in `pkg/volume/local_volume_windows.go` and `local_volume_unix.go` were not read; only that they are called at `pkg/pool/storage_pool.go:716-721`.
- `venue-config-example.yaml` was not compared key by key against `config.Config` beyond the keys this revision cites; the reference keys are present in the example and documented in the README.
- For `test/benchmark` only the entry points and the timing conventions were confirmed, not the strength of every assertion.
- No `go build`/`go test`/`golangci-lint` was run (the Go toolchain was left alone to avoid contention over the build cache), so every "compile-time assertion" conclusion comes from the `var _ = ...` declarations in the sources rather than from actual compiler output. For the engine that means the interface assertions at `pkg/metadata/sqlite_repository.go:378-383` and `pkg/quota/sqlite_directory_quota_repository.go:279` were read, not compiled.

### 10.6 Operational facts to verify in this environment

`AGENTS.md:160-168` and `README.md:977-986` document only that, when the default Go cache is not writable, the caches and the temporary directory are pointed at an ignored repository-local directory; they do not record the paths this host actually resolves to. No Go command was run while producing this revision, so the temporary directories actually in use were not observed either.
