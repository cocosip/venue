# Venue Architecture Design

> This document describes the structure and contracts of Venue in the current working tree. **Every statement about the code carries `path:line` evidence** (repository-relative path plus the 1-based line number).
> Anything that cannot be confirmed from the code goes into Section 10, "Unverified / Open"; nothing is inferred.
> Baseline: Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4` (`AGENTS.md:7`).

## Documentation conventions

- Evidence format is `repository-relative-path:line`, for example `pkg/pool/storage_pool.go:170` or `venue.go:75`.
- Locus evidence uses the `locus/` prefix, for example `locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`. The Locus `v2.0.0` reference checkout is **not** part of this repository; keep a read-only reference checkout available locally to verify such citations.
- Every behavioral claim must be checkable against the cited lines.

## 1. Scope and Goals

### 1.1 What it is

Venue is a Go multi-tenant file storage queue (`AGENTS.md:7`). It provides two things at once:

- **Managed file storage**: `StoragePool.WriteFile` generates the `fileKey`, and callers never select a physical path or a storage volume (`AGENTS.md:9-15`; generation logic `pkg/pool/storage_pool.go:214-215`, `pkg/pool/storage_pool.go:757-760`).
- **Queue semantics**: workers atomically claim Pending files through `GetNextFileForProcessing`/`GetNextBatchForProcessing`, while `MarkAsCompleted`/`MarkAsFailed` write durable state and physical deletion is left to cleanup (`pkg/core/interfaces.go:58-93`; `pkg/scheduler/file_scheduler.go:173-241`, `pkg/scheduler/file_scheduler.go:407-466`).

One in-process instance is assembled by `venue.NewVenue(*config.Config)` (`venue.go:75`), `Start` launches the background services (`venue.go:751`), and `Stop` shuts them down in reverse order and releases the repositories (`venue.go:808`).

### 1.2 What it does not do

| Not done | Evidence |
| --- | --- |
| It does not let callers select physical paths or storage volumes | `AGENTS.md:9-15`; paths are derived only by `storagePool.buildPhysicalPath` (`pkg/pool/storage_pool.go:468-481`) and by the volume's `BuildPhysicalPath` (`pkg/volume/local_volume.go:497-509`) |
| It does not treat the filesystem as a caller-managed resource (no handles, locks, or mount policy are exposed) | `core.StorageVolume` exposes only volume-relative path operations (`pkg/core/interfaces.go:188-217`) |
| It does not implement the Locus per-tenant `queue.log` journal/projection/snapshot/compaction | `README.md:690-697`; `pkg/core/models.go:76-87` states explicitly that no runtime code produces `DeleteRequested`/`DeleteSucceeded` |
| It does not implement the Locus `IHostedService` lifecycle | Replaced by `NewVenue` + `Start`/`Stop` (`README.md:667-668`; `venue.go:751`, `venue.go:808`) |
| It does not implement Locus `IConfiguration` binding | `config.Config` is configuration-source independent, and `config` performs no file I/O and does not import Viper (`AGENTS.md:57-67`, verified in §6.1) |
| It does not repair databases | The health check only diagnoses (`pkg/health/database_health_check_service.go:261-264`); a corrupted database is quarantined, never rebuilt, from data (`pkg/metadata/corrupted_database.go:96-128`) |

### 1.3 Division of labour with existing documents

| Document | Status | Division of labour |
| --- | --- | --- |
| `AGENTS.md` | Present | Sole authority for repository rules and ownership boundaries; this document uses it as a skeleton but verifies it against the current code (`AGENTS.md:33-51`) |
| `docs/locus-feature-gaps.md` | Present | Locus gap list, batch status, mechanism differences (M1-M6), and documentation/behaviour mismatches (D1-D10). This document does not repeat the gap judgements, it only references them |
| `docs/locus-alignment.md` | Present | Per-capability alignment matrix against Locus `v2.0.0` (`docs/README.md:13`). The task brief asked for this division of labour to be stated; see §10.1 |
| `docs/sqlite-storage-design.md` | Present | Design of the metadata and directory-quota storage migration from BadgerDB to SQLite (`docs/README.md:14`). Note: the current implementation uses BadgerDB while Locus uses one SQLite database per tenant (`README.md:661-663`; `pkg/metadata/badger_repository.go:296-309`) |

## 2. Layering and Package Responsibilities

Dependencies point upward: `config`/`core` → base implementations (`tenant`/`metadata`/`quota`/`volume`/`logging`/`statistics`) → facades (`pool`/`scheduler`/`cleanup`/`recovery`/`watcher`/`health`) → the top-level `venue`. `viperconfig` and `test/benchmark` are optional components on the entry-point side.

| Package | Responsibility | Key files | Public entry points (constructors/interfaces) | Explicitly not responsible for |
| --- | --- | --- | --- | --- |
| `config` | Public, configuration-source-independent runtime model: defaults, validation, cloning, fluent construction | `config/config.go`, `config/fluent.go` | `config.New`/`config.DefaultConfig` (`config/config.go:474`, `config/config.go:479`), `(*Config).Clone` (`config/config.go:1271`), `ApplyDefaults` (`config/config.go:604`), `Validate` (`config/config.go:839`), all `With...`/`Add...` (`config/fluent.go:10`, `config/fluent.go:72`, `config/fluent.go:83`) | It does not import Viper and does not perform file I/O (`AGENTS.md:65`; verified: among non-test files only `viperconfig/viper.go:10` and `examples/viper-config/main.go:13` import `spf13/viper`) |
| `viperconfig` | Optional Viper → `config.Config` adapter | `viperconfig/viper.go` | `Load` (`viperconfig/viper.go:14`), `LoadFromFile` (`:35`), `LoadFromFileSection` (`:41`), `LoadSection` (`:56`), `NewWithDefaults` (`:69`) | It does not own environment variables or reload policy, and does not define a second configuration model (`AGENTS.md:67`) |
| `pkg/core` | Public interfaces, shared models, status enums, domain errors | `pkg/core/interfaces.go`, `pkg/core/models.go`, `pkg/core/errors.go`, `pkg/core/tenantid.go`, `pkg/core/orphan.go` | Interfaces (from `pkg/core/interfaces.go:12`), `ValidateTenantID` (`pkg/core/tenantid.go:40`), sentinel errors (`pkg/core/errors.go:12-129`), parsers (`pkg/core/models.go:362`, `pkg/core/models.go:404`, `pkg/core/interfaces.go:625`), `DefaultFileRetryPolicy` (`pkg/core/models.go:433`) | It contains no implementation and imports no `pkg/*` (only the stdlib dependencies of `pkg/core/tenantid.go:1-8`) |
| `pkg/tenant` | Tenant lifecycle, tenant metadata JSON storage, tenant context cache | `pkg/tenant/manager.go`, `pkg/tenant/metadata_store.go` | `tenant.NewTenantManager` (`pkg/tenant/manager.go:79`) | It does not own physical volumes, does not select volumes, and does not count quotas |
| `pkg/metadata` | BadgerDB metadata projection, secondary status indexes, cache, migration, isolation/backup/restore | `pkg/metadata/badger_repository.go`, `status_page.go`, `backup.go`, `corrupted_database.go`, `conflict_retry.go`, `cache.go`, `migration.go` | `NewBadgerMetadataRepository` (`pkg/metadata/badger_repository.go:170`), `NewBackupService` (`pkg/metadata/backup.go:468`), `BackupRepository` (`:61`), `RestoreDatabase` (`:254`), `LatestBackup` (`:176`), `OpenBadgerWithRecovery` (`pkg/metadata/corrupted_database.go:86`) | It does not select volumes and does not write physical payloads; it does not do quotas |
| `pkg/pool` | Storage + queue facade: quotas, volume selection, physical paths, physical writes, metadata persistence and rollback | `pkg/pool/storage_pool.go`, `path_generator.go`, `volume_selector.go` | `pool.NewStoragePool` (`pkg/pool/storage_pool.go:127`), `PathGenerator` interface (`:55`) | It does not implement transactions/CAS (delegated to `pkg/scheduler` + `pkg/metadata`) and does not decide the sharding layout (delegated to the volume) |
| `pkg/scheduler` | Atomic queue state transitions, lease validation, retry scheduling, timeout reclaim | `pkg/scheduler/file_scheduler.go` | `scheduler.NewFileScheduler` (`pkg/scheduler/file_scheduler.go:115`), `DefaultFileSchedulerOptions` (`:42`), `DefaultEmptyQueueReclaimBatchSize` (`:39`) | It holds no volume mapping (`pkg/scheduler/file_scheduler.go:79-83`) and does no physical I/O |
| `pkg/quota` | Tenant quotas (override value + global fallback) and directory quotas (per-directory sharded locks) plus directory-quota persistence | `pkg/quota/tenant_quota_manager.go`, `directory_quota_manager.go`, `directory_quota_repository.go` | `quota.NewTenantQuotaManager` (`pkg/quota/tenant_quota_manager.go:74`), `quota.NewDirectoryQuotaManager` (`pkg/quota/directory_quota_manager.go:20`), `quota.NewBadgerDirectoryQuotaRepository` (`pkg/quota/directory_quota_repository.go:86`) | It does not judge physical space (`ErrInsufficientStorage` is produced by volume selection, `pkg/pool/storage_pool.go:427`) |
| `pkg/volume` | Local filesystem volumes, path safety, sharding layout, intra-volume atomic moves, health-probe cache | `pkg/volume/local_volume.go`, `path_sanitizer.go`, `local_volume_windows.go`, `local_volume_unix.go` | `volume.NewLocalFileSystemVolume` (`pkg/volume/local_volume.go:85`), returning `core.StorageVolume` | It does no quotas and does not select volumes; it does not manage volume lifecycle or mounting (`AGENTS.md:43`) |
| `pkg/cleanup` | Cleanup (empty directories, timed-out, permanently failed, completed, orphaned metadata, junk, invalid database backups, optimization) + cumulative statistics + dead-letter layout + retired-volume disposition + background scheduling | `pkg/cleanup/cleanup_service.go`, `dead_letter.go`, `junk_files.go`, `invalid_database_backups.go`, `cleanup_statistics.go`, `background_cleanup_service.go` | `cleanup.NewCleanupService` (`pkg/cleanup/cleanup_service.go:132`), `cleanup.NewBackgroundCleanupService` (`pkg/cleanup/background_cleanup_service.go:124`), `DefaultDeadLetterOptions` (`pkg/cleanup/dead_letter.go:48`) | It does not decide *when* to clean (that is decided by the scheduling and configuration of `BackgroundCleanupService`) |
| `pkg/recovery` | Opt-in: scan physical files, rebuild missing metadata through `ParsePhysicalPath`, and re-enqueue | `pkg/recovery/orphan_recovery_service.go` | `recovery.NewOrphanRecoveryService` (`pkg/recovery/orphan_recovery_service.go:96`) | Disabled by default (`config/config.go:572-577`); it does not guess tenant ownership (`pkg/recovery/orphan_recovery_service.go:425-453`) |
| `pkg/watcher` | Watched-directory imports: scanning, deduplication, import, post-import actions, history persistence, global option persistence, root derivation | `pkg/watcher/file_watcher.go`, `background_file_watcher_service.go`, `file_watcher_auto_manager.go`, `watcher_state.go` | `watcher.NewFileWatcher` (`pkg/watcher/file_watcher.go:119`), `NewBackgroundFileWatcherService` (`pkg/watcher/background_file_watcher_service.go:206`), `NewFileWatcherAutoManager` (`pkg/watcher/file_watcher_auto_manager.go:91`) | It does not own watcher definitions (`README.md:598-599`) and does not select physical paths (it goes through `StoragePool.WriteFile`, `pkg/watcher/file_watcher.go:854`) |
| `pkg/health` | Startup/periodic database health checks (structural, never opening the database), orphan-tenant detection, database size | `pkg/health/database_health_checker.go`, `database_health_check_service.go` | `health.NewDatabaseHealthChecker` (`pkg/health/database_health_checker.go:78`), `health.NewDatabaseHealthCheckService` (`pkg/health/database_health_check_service.go:61`) | It does not repair databases (`pkg/health/database_health_check_service.go:261-264`); `Start` does not fail because the database is unhealthy (same location) |
| `pkg/logging` | Instance-scoped structured logging `Runtime`: disabled means silent, never touches `slog.Default`, isolates handler panics | `pkg/logging/logging.go` | `logging.New` (`pkg/logging/logging.go:40`), `logging.Disabled` (`:51`), `(*Runtime).Emit` (`:79`) | It does not own the handler/writer (`pkg/logging/logging.go:11-14`) and does not configure a global logger (`:33-34`) |
| `pkg/statistics` | Bounded-window in-memory statistics recorder/reader plus optional periodic log output | `pkg/statistics/recorder.go` | `statistics.NewRecorder` (`pkg/statistics/recorder.go:251`), `statistics.Noop` (`:670`), `statistics.NewOutputService` (`:706`), `DefaultOptions` (`:129`), `MinimumSeries`/`MaximumSeries` (`:36`, `:40`) | It does no persistence or export (`pkg/statistics/recorder.go:4-15`) and never blocks the caller |
| `internal/directorypath` | Shared logical-directory normalization | `internal/directorypath/path.go` | `Normalize` (`internal/directorypath/path.go:7`) | It does no path resolution against volumes and performs no I/O; it is used by `pkg/pool` (`pkg/pool/storage_pool.go:193`) and `pkg/quota` (`pkg/quota/directory_quota_manager.go:188`) |
| `test/benchmark` | Public-entry system benchmarks | `test/benchmark/system_bench_test.go`, `cleanup_scale_bench_test.go` | `setupBenchmarkSystem` (`test/benchmark/system_bench_test.go:25`) → `venue.NewVenue` (`:42`); `newCleanupBenchVenue` (`test/benchmark/cleanup_scale_bench_test.go:70`) → `venue.NewVenue` (`:83`) | It does not bypass the public entry point (`AGENTS.md:53`) |

Note: `internal/directorypath` is a package in this repository and appears in the ownership list of `AGENTS.md:50`, but it is still absent from the Repository Layout in `README.md:777-796` (see §10.2, U4).

## 3. Public Contracts

### 3.1 Base interfaces

| Interface | Purpose | Key invariants | Implementation and usage evidence |
| --- | --- | --- | --- |
| `StoragePool` (`pkg/core/interfaces.go:12`) | The single facade for queue + storage | An empty queue returns `(nil, nil)` rather than an error (`:62-64`; the batch form returns an empty slice `:76`); a failed claim must be surfaced to the caller (`:68-69`) | Implemented by `storagePool` (`pkg/pool/storage_pool.go:66`), constructed by `NewStoragePool` (`:127`) and wired at `venue.go:349` |
| `TenantManager` (`pkg/core/interfaces.go:106`) | Tenant lifecycle and resolution | `TryGetTenant` never writes to disk and never materializes a directory (`:114-121`); `IsTenantEnabled` is implemented on top of it (`pkg/tenant/manager.go:216-226`) | Implemented by `TenantManager` (`pkg/tenant/manager.go:63`), constructed at `:79`; wired at `venue.go:176` |
| `FileScheduler` (`pkg/core/interfaces.go:151`) | Queue state transitions and timeout reclaim | Completion/failure must carry a still-valid lease (`:166-172`); after `ResetTimedOutFiles` a file is immediately claimable (`:179-184`) | Implemented by `fileScheduler` (`pkg/scheduler/file_scheduler.go:84`), constructed at `:115`; wired at `venue.go:328` |
| `MetadataRepository` (`pkg/core/interfaces.go:505`) | Metadata read/write + CAS + optimization + close | `UpdateStatus` is an administrative path and does no lease validation (`:531-539`); queue paths must use `CompareAndTransitionToProcessing`/`CompareAndUpdateProcessing` (`:541-556`) | Implemented by `BadgerMetadataRepository` (`pkg/metadata/badger_repository.go:139`), constructed at `:170`; wired at `venue.go:217` |
| `DirectoryQuotaRepository` (`pkg/core/interfaces.go:569`) | Directory-quota persistence | Keys are isolated by a tenant prefix: `dirquota:<base64url tenant>:` (`pkg/quota/directory_quota_repository.go:516-523`) | Implemented by `badgerDirectoryQuotaRepository`, constructed at `pkg/quota/directory_quota_repository.go:86`; wired at `venue.go:266` |
| `CleanupService` (`pkg/core/interfaces.go:440`) | 9 classes of maintenance operation + cumulative statistics | Every operation returns `*CleanupStatistics`; `CumulativeStatistics` is monotonic and returns a copy (`:469-471`) | Implemented by `cleanupService` (`pkg/cleanup/cleanup_service.go:94`), constructed at `:132`; wired at `venue.go:377` |
| `OrphanRecoveryService` (`pkg/core/orphan.go:39`) | Physical file → metadata rebuild | `RecoverNow` may run concurrently with normal storage; it rebuilds only when no metadata exists (`:41-42`) | Implemented by `recovery.OrphanRecoveryService` (`pkg/recovery/orphan_recovery_service.go:71`), constructed at `:96`; wired at `venue.go:540` |
| `FileWatcher` (`pkg/core/interfaces.go:744`) | Watched-directory import management | `UpdateWatcher` does not interrupt a scan, and an unknown ID returns `ErrWatcherNotFound` (`:748-752`); `GetWatchersForTenant` does not return multi-tenant watchers (`:764-767`) | Implemented by `fileWatcher` (`pkg/watcher/file_watcher.go:84`), constructed at `:119`; wired at `venue.go:436` |
| `FileWatcherAutoManager` (`pkg/core/interfaces.go:791`) | Derive per-tenant watchers from a root template | Application is idempotent; `RemoveAllWatchers` deletes only derived watchers (`:789-790`) | Implemented by `fileWatcherAutoManager` (`pkg/watcher/file_watcher_auto_manager.go:67`), constructed at `:91`; wired at `venue.go:517` |
| `StorageVolume` (`pkg/core/interfaces.go:188`) | Volume abstraction (local/network/cloud) | All paths are volume-relative and are sanitized by the volume itself (`pkg/volume/path_sanitizer.go:28-60`) | Implemented by `LocalFileSystemVolume` (`pkg/volume/local_volume.go:61`), constructed at `:85`; wired at `venue.go:303` |
| `TenantQuotaManager` (`pkg/core/interfaces.go:254`) | Per-tenant file-count quota | `IncrementFileCount` returns `ErrTenantQuotaExceeded` when over the limit (`:260-262`); `IncrementFileCount` re-reads the effective limit on every call so that `SetGlobalLimit` takes effect immediately (`pkg/quota/tenant_quota_manager.go:110-113`) | Implemented by `TenantQuotaManager` (`pkg/quota/tenant_quota_manager.go:52`), constructed at `:74`; wired at `venue.go:243` |
| `StatisticsRecorder` (`pkg/core/interfaces.go:306`) / `StatisticsReader` (`:315`) | Low-overhead in-process statistics write/aggregate read | Implementations never block the caller's I/O; when disabled all deltas are dropped; `nil` is equivalent to disabled (`:300-305`); a query whose `To` is not later than `From` returns an empty snapshot (`:317-319`) | Implemented by `*statistics.Recorder` (`pkg/statistics/recorder.go:226`, `Record` `:284`, `Snapshot` `:370`) and `NoopRecorder` (`:666`); wired at `venue.go:126`, `venue.go:122`; the read side type-asserts at `venue.go:978` |
| `DatabaseHealthChecker` (`pkg/core/interfaces.go:860`) | Structural database health checks and orphan-tenant detection | It checks structure and never opens the database (`pkg/health/database_health_checker.go:242-249`); a missing directory does not count as corruption (`:266-272`) | Implemented by `databaseHealthChecker` (`pkg/health/database_health_checker.go:63`), constructed at `:78`; wired at `venue.go:565` |

### 3.2 Optional capability interfaces

Optional capabilities are used exclusively through type assertions; the table distinguishes "wired" from "contract only".

| Interface | Purpose | type-assert / usage | Compile-time implementation assertion |
| --- | --- | --- | --- |
| `StatusPageReader` (`pkg/core/interfaces.go:493`) | Paged enumeration by status so long tasks do not load whole tables | `venue.go:624` (quota reconciliation), `pkg/cleanup/cleanup_service.go:224` (all status-based scans); both have a `GetByStatus` fallback (`venue.go:644`, `pkg/cleanup/cleanup_service.go:226`) | `pkg/metadata/status_page.go:24` |
| `FileMover` (`pkg/core/interfaces.go:284`) | Zero-copy atomic intra-volume move (dead-letter optimization) | `pkg/cleanup/dead_letter.go:195`; when unimplemented it falls back to read+write+delete (`:199-215`) | `pkg/volume/local_volume.go:23` |
| `StorageVolumePathBuilder` (`pkg/core/interfaces.go:221`) | Volume-defined physical layout (the volume decides sharding) | `pkg/pool/storage_pool.go:477`; without an implementation it falls back to the date-directory generator (`:480`) | `pkg/volume/local_volume.go:498` (method-level, not asserted in a package-level `var` block) |
| `MetadataBackupService` (`pkg/core/interfaces.go:363`) | Online consistent backup stream | `pkg/metadata/backup.go:69` (`BackupRepository`), `pkg/metadata/backup.go:486` (`NewBackupService`: errors if enabled but unsupported, `:488-490`) | No package-level assertion; `pkg/metadata/badger_repository.go:1377` provides `Backup` |
| `TenantQuotaAdministrator` (`pkg/core/interfaces.go:331`) | Global/override limit administration | `venue.go:968` (`Venue.TenantQuotaAdministrator`) | `pkg/quota/tenant_quota_manager.go:65` |
| `ShardingDepthProvider` (`pkg/core/interfaces.go:296`) | Report sharding depth so shard directories are protected | **No runtime type-assert**: only the implementation assertion at `pkg/volume/local_volume.go:24`, the implementation at `pkg/volume/local_volume.go:146`, and the test assertion at `pkg/volume/local_volume_move_test.go:499-503`. `pkg/cleanup` actually identifies shard directories with a structural heuristic (`pkg/cleanup/cleanup_service.go:476-492`) | `pkg/volume/local_volume.go:24` |
| `StorageVolumeHealthProbe` (`pkg/core/interfaces.go:376`) | Force a probe and refresh the health cache | **No implementation, no usage** (a repository-wide grep matches only the interface definition) | None |
| `StorageVolumeWritePathWarmup` (`pkg/core/interfaces.go:385`) | Write-path warmup at startup | **No implementation, no usage**; `config.VolumeConfig.WarmupOnStartup` is not wired (see §10.3) | None |
| `StorageVolumeWritePathDiagnostics` (`pkg/core/interfaces.go:394`) | Write-path observation snapshot | **No implementation, no usage**; `core.StorageVolumeWritePathStatistics` exists only as a type definition (`pkg/core/models.go:826-855`) | None |
| `DatabaseOptimizationService` (`pkg/core/interfaces.go:405`) | Per-database reclaimed-byte detail | **No implementation, no usage**; `core.DatabaseOptimizationResult` has only a type and derived methods (`pkg/core/models.go:786-815`). Only the aggregate counter `CleanupStatistics.MetadataDatabasesOptimized` exists today (`pkg/cleanup/cleanup_service.go:884`) | None |
| `TenantCleanupService` (`pkg/core/interfaces.go:417`) | Single-tenant empty-directory cleanup | **No implementation, no usage** | None |
| `TenantOrphanRecoveryService` (`pkg/core/interfaces.go:429`) | Single-tenant orphan recovery | **No implementation, no usage**; `OrphanRecoveryService` exposes only a global `RecoverNow` (`pkg/core/orphan.go:43`) | None |

Conclusion: `ShardingDepthProvider`/`DatabaseOptimizationService`/`TenantCleanupService`/`TenantOrphanRecoveryService` and the three volume capability interfaces are in the "contract established, implementation absent" state, while `docs/locus-feature-gaps.md` marks several of them (W1.6, W5.6, W5.7, W5.9) as complete, which does not match the current code.

## 4. Lifecycle

### 4.1 `NewVenue`: clone → ApplyDefaults → Validate → initialize

| Step | Lines | Behavior |
| --- | --- | --- |
| nil check | `venue.go:76-78` | `cfg == nil` → `ErrInvalidArgument` |
| clone | `venue.go:80` | `cfg.Clone()`, deep-copying slices and the `Quota` pointer (`config/config.go:1271-1300`) |
| defaults | `venue.go:81` | `runtimeConfig.ApplyDefaults()` (`config/config.go:604`) |
| validation | `venue.go:82-84` | `runtimeConfig.Validate()`, wrapping a failure as `invalid configuration` |
| logging runtime | `venue.go:86-93` | Defaults to `logging.Disabled()`; when `Logging != nil`, `logging.New` (a failure returns `invalid logging configuration`) |
| assemble instance | `venue.go:95-98` | Writes only `config` and `logger` |
| initialize | `venue.go:101-106` | On failure, `v.closeRepositories()` runs before the error is wrapped and returned |
| success event | `venue.go:108` | `initialized` |

### 4.2 `initialize`: construction order

| Order | Component | Lines | Notes |
| --- | --- | --- | --- |
| 0 | statistics | `venue.go:170` | Before every instrumented component, so that no operation writes into a missing recorder (`venue.go:167-168`) |
| 1 | tenant manager + configured tenants | `venue.go:176-209` | An existing tenant is treated as idempotent (`venue.go:192`), then enable/disable is applied |
| 2 | metadata repository | `venue.go:213-221` | Injects the isolation callback and the statistics recorder |
| 2b | periodic backup runner | `venue.go:225-239` | Condition: `BackupDirectory != ""` and `BackupInterval > 0` |
| 3 | tenant quota + reconciliation | `venue.go:243-264` | `reconcileTenantQuotaCounts` (`:262`) |
| 3b | directory-quota repository + manager + reconciliation | `venue.go:266-290` | Half-size Badger parameters (`:270-272`); `reconcileDirectoryQuotaCounts` (`:288`) |
| 4 | storage volumes | `venue.go:294-320` | An unsupported type errors out (`:311-312`); an empty volume set errors out (`:322-324`) |
| 5 | file scheduler | `venue.go:328-345` | See §5.2 for the note about batch size |
| 6 | storage pool | `venue.go:349-361` | Injects the statistics recorder |
| 7 | cleanup core | `venue.go:365-402` | The disposition enum is parsed at construction time and errors there (`:365-376`) |
| 8 | background cleanup | `venue.go:405-430` | Condition: `EnableBackgroundCleanup` |
| 9 | watcher (core + background + root derivation) | `venue.go:433-535` | Condition: there are watchers or roots |
| 10 | orphan recovery | `venue.go:538-555` | Condition: `OrphanRecovery.Enabled` |
| 11 | database health check | `venue.go:558-591` | Condition: `EnableDatabaseHealthCheck` |

### 4.3 `Start`: order and rollback

- Mutual exclusion and state gating: `venue.go:752-760` (already stopped → error advising a new instance; already running → error).
- Derived lifecycle context: `venue.go:762-763`.
- Service list (dependency order): health check → cleanup → orphan recovery → file watcher → metadata backup → statistics output (`venue.go:769-780`).
- Missing/typed-nil services are skipped: `venue.go:784`, decided by `isNilService` (`venue.go:919-925`, which uses `reflect` to detect typed nils).
- Startup failure rollback: `Stop` the already-started services in reverse order, reset `running=false`, and return an error carrying the service name (`venue.go:787-793`).
- Each started service emits a `*_started` event (`venue.go:795`).

### 4.4 `Stop`: reverse stop, release repositories, idempotent

- Idempotency gate: when `closed`, return `nil` immediately (`venue.go:812-814`).
- Only when `running`, stop in reverse order: statistics output → metadata backup → file watcher → orphan recovery → cleanup → health check (`venue.go:820-831`).
- Stop errors are only logged, never returned (`venue.go:836-839`, event name `*_stop_failed`).
- Repository release: `closeRepositories()` (`venue.go:844` → `:878-904`): close the watcher first (optional `Close() error` capability asserted at `:883`, used to flush import history), then the metadata repository (`:891`), then the directory-quota repository (`:898`).
- Cancel the context and set `closed`: `venue.go:846-850`; after `closed`, the synchronous accessors are no longer usable (`venue.go:806-807`).

### 4.5 Resource release on construction failure

`NewVenue` calls `closeRepositories` when `initialize` fails (`venue.go:104`). `closeRepositories` achieves idempotency through nil checks plus setting fields to nil (`venue.go:878-904`), so it can run on both the construction-failure path and the `Stop` path. Note: when `initialize` fails midway, resources that are already open but **not yet assigned to a Venue field** are not released by that path — for example, `dirQuotaRepo` has no failure point after successful creation and before assignment (`venue.go:266-281`), while the metadata repository closes its own `db` when `migrateLegacyMetadata` fails (`pkg/metadata/badger_repository.go:230-233`).

### 4.6 Background service inventory and adaptation

Adaptation surface: `backgroundService` (`Start() error`/`Stop() error`, `venue.go:719-722`); services whose lifecycle cannot fail are wrapped in `voidBackgroundService` (`venue.go:727-744`).

| Service | Venue field | Start mechanism | Enablement condition |
| --- | --- | --- | --- |
| health check | `healthCheckService` (`venue.go:57`) | Native `Start` (`pkg/health/database_health_check_service.go:109`) | `EnableDatabaseHealthCheck` (`venue.go:558`) |
| cleanup | `cleanupService` (`venue.go:55`) | Native `Start` (`pkg/cleanup/background_cleanup_service.go:205`) | `EnableBackgroundCleanup` (`venue.go:405`) |
| orphan recovery | `orphanRecovery` (`venue.go:58`) | Native `Start` (`pkg/recovery/orphan_recovery_service.go:142`) | `OrphanRecovery.Enabled` (`venue.go:538`) |
| file watcher | `fileWatcherService` (`venue.go:56`) | Native `Start` (`pkg/watcher/background_file_watcher_service.go:464`) | There are watchers or roots and construction succeeded (`venue.go:433`, `:479`) |
| metadata backup | `metadataBackupCore` (`venue.go:59`) | `voidBackgroundService` (`venue.go:859-864`) | `BackupDirectory` is non-empty and `BackupInterval > 0` (`venue.go:225`) |
| statistics output | `statisticsOutput` (`venue.go:65`) | `voidBackgroundService` (`venue.go:869-874`) | `Statistics.Enabled` and `Statistics.Output.Enabled` (`venue.go:121`, `:132`) |

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
| 10 | Physical path generation: `tenantID/<shard...>/<fileKey><ext>` | `pkg/pool/storage_pool.go:468-481` → `pkg/volume/local_volume.go:497-509` → `pkg/volume/path_sanitizer.go:186-219` |
| 11 | Physical write + fsync when configured | `pkg/volume/local_volume.go:204-241` (`file.Sync()` `:234-238`) |
| 12 | Build the metadata (`Status = Pending`, `RetryCount = 0`) | `pkg/pool/storage_pool.go:273-287` |
| 13 | Persist the metadata (single transaction, including the status index) | `pkg/pool/storage_pool.go:290` → `pkg/metadata/badger_repository.go:479-552` (index maintenance `:519-532`) |
| 14 | Failure rollback: delete the physical file first, and if it cannot be deleted the **quota is retained**; only after the physical file is gone are the directory and tenant quotas rolled back | `pkg/pool/storage_pool.go:378-413` (order at `:386-410`); the error is wrapped in `volumeCleanupError` to avoid leaking physical paths (`:109-124`) |
| 15 | Statistics are recorded only on the success path | `pkg/pool/storage_pool.go:294-297`; `recordStatistic` `:167-178` |

Read-path self-healing: on a read failure the canonical path is rebuilt, the bytes are confirmed to exist, the correction is persisted, and the read is retried once (`pkg/pool/storage_pool.go:512-525`, `:536-560`).

### 5.2 Claiming (CAS + lease + empty queue)

- The facade forwards and counts: `pkg/pool/storage_pool.go:613-622`; `recordDequeue` counts only non-nil results (`:640-646`).
- Single claim: `pkg/scheduler/file_scheduler.go:173-202`. The bounded candidate window is `claimCandidateLimit = 10` (`:55`, `:251`); candidates are tried one by one with CAS, retryable contention errors are skipped and other errors are propagated (`:259-272`, `claimRetryable` `:75-77`).
- **Empty-queue semantics**: when there is no claimable candidate, `(nil, nil)` is returned (`pkg/scheduler/file_scheduler.go:191-193`).
- Batch claim: `pkg/scheduler/file_scheduler.go:214-241`; the fetch size is `batchSize*2` capped at `batchCandidateLimit = 100` (`:58`, `:282-285`); an empty result returns an empty slice (`:292-294`).
- CAS transition (Pending → Processing): the transaction must satisfy both `Status == Pending` (`pkg/metadata/badger_repository.go:1073-1076`) and the availability time having arrived (`:1079-1082`), otherwise it returns a wrapped `ErrFileNotClaimable`; it writes `ProcessingStartTime` (`:1091-1094`) and swaps the index in the same transaction (`:1085-1105`); `badger.ErrConflict` is also classified as contention (`:1121-1124`), and other errors are wrapped with `ErrDatabaseError` (`:1126`).
- **Lease semantics**: `core.FileProcessingLease{TenantID, FileKey, ProcessingStartTimeUTC}` (`pkg/core/models.go:120-131`), derived by `FileMetadata.ToFileLocation` from the active `ProcessingStartTime` (`:248-256`). `CompareAndUpdateProcessing` requires `Status == Processing` and `ProcessingStartTime` **value-for-value equal** to the lease (`pkg/metadata/badger_repository.go:1188-1190`); a mismatch returns `FileProcessingLeaseMismatchError` (`:1257-1272`), whose `Unwrap` yields `ErrProcessingLeaseMismatch` (`pkg/core/errors.go:112-115`). Releasing the same lease twice is an idempotent no-op (released marker `ReleasedProcessingStartTimeUTC`, `pkg/core/models.go:230-234`; decision at `pkg/metadata/badger_repository.go:1194-1200`, write at `:1214-1219`). The callback must not change identity (`:1208-1210`).
- **Timeout reclaim**:
  - The metadata scan uses `elapsed > timeout` (`pkg/metadata/badger_repository.go:1316-1321`).
  - The scheduler's full reclaim `ResetTimedOutFiles` uses an unbounded scan (`pkg/scheduler/file_scheduler.go:503-514`, comment at `:512`).
  - Immediate reclaim on an empty queue: `reclaimTimedOutOnEmptyQueue` (`:327-352`), with a per-tenant cooldown gate plus reservation/rollback (`:359-389`) and a batch cap of `emptyQueueReclaimBatchSize`; after a successful reclaim the pending scan is retried **only once** (`:195-201`, `:236-240`).
  - Reclaimed files are immediately available: `availableAt := now` (`:555`), combined with the inclusive availability check on the metadata side (`pkg/metadata/badger_repository.go:907-912`, where a comment explains the Windows clock granularity).
  - Wiring: `RecoverTimedOutOnEmptyQueue` and `TimedOutReclaimCooldown` come from configuration (`venue.go:336-337`, configuration fields `config/config.go:383-384`); the batch size uses the package constant `scheduler.DefaultEmptyQueueReclaimBatchSize` (`venue.go:340`, constant at `pkg/scheduler/file_scheduler.go:39`), so `Cleanup.EmptyQueueReclaimBatchSize` (`config/config.go:399-402`) **currently has no effect** (see §10.3).
  - **The low-frequency background reclaim does not exist in the code**: `Cleanup.EnableBackgroundTimedOutReclaim`/`BackgroundTimedOutReclaimBatchSize` (`config/config.go:404-412`) have no consumer anywhere in the repository; `README.md:678-682` also states that the cleanup cycle covers it.

### 5.3 Completion / failure

- `MarkAsCompleted`: validate non-empty (`pkg/scheduler/file_scheduler.go:408-413`) → lease-validated update: `Completed`, clearing `ProcessingStartTime`/`AvailableForProcessingAt`/`LastError`, and writing `CompletedAt` (`:415-424`) → failure wrapping (`:426`). The facade additionally looks up the volume to fill in the statistics dimension, and a failed lookup does not affect completion (`pkg/pool/storage_pool.go:649-665`).
- `MarkAsFailed`: `RetryCount++`, `LastFailedAt`, `LastError`, clearing `ProcessingStartTime` (`pkg/scheduler/file_scheduler.go:442-448`).
  - **Permanent-failure decision**: `RetryCount >= MaxRetryCount` → `PermanentlyFailed`, clearing `AvailableForProcessingAt` (`:449-453`).
  - **Retry backoff**: otherwise back to `Pending` with `AvailableForProcessingAt = now + CalculateRetryDelay(RetryCount)` (`:455-459`).
  - Backoff arithmetic: `delay = InitialRetryDelay * 2^(retryCount-1)`, capped by `MaxRetryDelay`; when exponential backoff is disabled it degrades to a fixed `InitialRetryDelay` (`pkg/core/models.go:443-462`). Defaults are `3 / 5s / true / 5m` (`pkg/core/models.go:433-440`, configuration mirror `config/config.go:486-491`).
- Note that `FileStatusFailed` is only a Locus compatibility value: the scheduler writes a failure straight back to `Pending` (`pkg/core/models.go:66-71`).

### 5.4 Cleanup

| Operation | Evidence | Counters/side effects |
| --- | --- | --- |
| `CleanupEmptyDirectories` | `pkg/cleanup/cleanup_service.go:289-318` | `EmptyDirectoriesRemoved` (`:311`); rescans until no progress (`:329-368`); protected-directory decision `:420-492` |
| `CleanupTimedOutProcessingFiles` | `:536-558` | `TimedOutFilesReset` (`:554`); delegates to `scheduler.ResetTimedOutFiles` (`:550`) |
| `CleanupPermanentlyFailedFiles` | `:577-645` | See "the three dispositions" below |
| `CleanupCompletedFiles` | `:715-783` | `CompletedRecordsRemoved` (`:774`), `SpaceFreed` (`:775`); on failure it returns without counting (`:741`, `:748`) |
| `CleanupOrphanedMetadata` | `:786-874` | `OrphanedMetadataRemoved` (`:865`); it only handles records "confirmed absent" (`:809-838`) |
| `CleanupJunkFiles` | `pkg/cleanup/junk_files.go:67-89` | `JunkFilesRemoved`/`SpaceFreed` (`:81-82`) |
| `CleanupInvalidDatabaseBackups` | `pkg/cleanup/invalid_database_backups.go:48-80` | `InvalidDatabaseBackupsRemoved`/`SpaceFreed` (`:72-73`) |
| `OptimizeDatabases` | `pkg/cleanup/cleanup_service.go:877-894` | `MetadataDatabasesOptimized` (`:884`), `QuotaDatabasesOptimized` (`:890`) |
| `CumulativeStatistics` | `pkg/cleanup/cleanup_statistics.go:88-90` | Returns a copy of the atomic counters (`:57-71`) |

**The three dispositions of permanently failed files** (`config.Cleanup.PermanentlyFailedDisposition`, default `MoveToDeadLetter`, `config/config.go:539`):

1. Gate and eligibility: the disposition enum errors during parsing (`config/config.go:1106-1108`); `Keep` skips the entire sweep (`pkg/cleanup/cleanup_service.go:589-592`); an unrecognized value is treated as "keep" with a warning (`:594-600`); `DeadLettered` rows go through a re-entry guard (`:614-616`); rows with an empty `LastFailedAt` or an unexpired retention are not processed (`:620-622`).
2. `Delete`: physical file → directory quota → tenant quota → metadata, where each step compensates the preceding steps on failure (`pkg/cleanup/cleanup_service.go:650-712`).
3. `MoveToDeadLetter`: `applyDeadLetterDisposition` (`pkg/cleanup/dead_letter.go:290-379`) — it computes the target path first; if the payload does not exist it only performs the state transition (`:321-328`); if it is already at the target it only migrates (`:329-331`); a failed move keeps the record (`:332-337`); it then writes `Status=DeadLettered`, `DeadLetteredAt`, `PhysicalPath=<dead-letter path>` (`:341-344`) and rolls back if persistence fails (`:345`/`:387-409`); finally it releases the directory/tenant quotas (`:352-376`); on success it counts `DeadLetteredFiles` (`:378`).
4. **Dead-letter layout**: `<RootPath>/[<tenantID>/][<yyyyMMdd>/][<shard pairs>/]<fileKey><ext>` (`pkg/cleanup/dead_letter.go:20-22`, `:93-114`); the date format is `20060102` (`:15`); shards accept only two lowercase hex characters (`:124-143`); root path and file name are validated (`:147-186`); the relative path always stays inside the volume root (`:111-114`).
5. **Junk files**: the list is `thumbs.db`/`.ds_store`/`desktop.ini` (case-insensitive, `pkg/cleanup/junk_files.go:16-20`); the managed payload shape `<32 lowercase hex><ext>` is explicitly excluded (`:27-34`, `:38-54`); the walk deletes and counts (`:96-134`).
6. **Invalid database backups**: named `<dbDir>.corrupted.<stamp>` and marked `.corrupted.` (`pkg/cleanup/invalid_database_backups.go:21-23`); matching requires content on both sides (`:30-36`); retention defaults to 72h and a negative value disables it (`:56-63`); both the metadata and quota roots are scanned and deduplicated (`:83-100`); the deepest match is deleted first to avoid double-counting bytes (`:125-151`); a scan failure only warns and does not fail the whole sweep (`:114-123`).
7. **Retired volumes**: `RetiredVolumeDisposition` = `Keep`/`PurgeMetadataOnly` (`pkg/core/models.go:377-415`); an undeclared volume is always `Keep` (`pkg/cleanup/dead_letter.go:271-280`); `PurgeMetadataOnly` deletes only metadata and releases quotas, and never touches physical storage (`:227-267`).
8. **Cumulative statistics**: 11 `atomic.Int64` counters (`pkg/cleanup/cleanup_statistics.go:18-30`), with `defer s.recordCumulative(stats)` in every operation (for example `pkg/cleanup/cleanup_service.go:291`, `pkg/cleanup/junk_files.go:69`).
9. **Background scheduling**: initial delay → ticker (`pkg/cleanup/background_cleanup_service.go:266-295`); per-cycle panic isolation (`:299-309`); 8 steps executed in order (`:312-454`); junk and optimization are each throttled by their own interval (`:333`/`:488-499`, `:424`/`:477-484`); `Stop` does not hold a lock while waiting for the running goroutine (`:229-252`).

### 5.5 Orphan recovery (opt-in)

- Disabled by default (`config/config.go:459-471`, `config/config.go:572-577`), and constructed only when `OrphanRecovery.Enabled` (`venue.go:538`).
- Scanning: volumes are traversed in ID order (`pkg/recovery/orphan_recovery_service.go:248-256`, `:468-476`); `filepath.Walk` plus a batch cap (default 1000, `:22-24`, `:283-285`); `ParsePhysicalPath` accepts only `{tenantID}/[{shard}/.../{32hex}[.ext]` (`:425-453`, `:457-466`); files that already have metadata are skipped (`:302-311`).
- Stability check: a re-stat must show unchanged size and mtime, and (optionally) the minimum file age must be satisfied (`:330-349`).
- Rebuild: reserve the tenant quota first, then the directory quota, and roll back the reserved items if either fails (`:368-386`); write the metadata (`Status = Pending`, logical directory fixed to `/`, `:388-401`, `:419-423`); roll back both quotas if persistence fails (`:403-411`).
- Lifecycle: `Start`/`Stop`/`IsRunning` (`:142-188`); the loop covers `RunOnStartup` (`:203-210`) and panic isolation (`:193-198`); `RecoverNow` and the periodic scan are serialized by `scanMu` (`:84-86`, `:245-246`).

### 5.6 Watcher import

| Stage | Evidence |
| --- | --- |
| Scan entry point (single result path) | `pkg/watcher/file_watcher.go:511-546`; statistics are recorded exactly once (`:543`, `:552-573`) |
| Single-tenant scan | `:576-601` |
| Multi-tenant scan (the subdirectory name is the tenant ID; the tenant is resolved per directory) | `:604-658` (`:629-638`) |
| Discovery filtering (MinFileAge / MaxFileSize / patterns) | `:739-809` (`:748`, `:753`, `:758`); glob validation and the default `*` (`:975-1006`); Windows case-insensitivity (`:1026-1042`) |
| Concurrent-import cap | `:669-722` (semaphore `:679-684`) |
| **fingerprint** | `size:mtime.UnixNano()` (`:1044-1051`); `-` when it cannot be computed (`:36-38`) |
| **In-flight deduplication** | In-memory `sync.Map` + `InFlightToken` (`:64-75`, `:94-97`); `reserveImportSlot` uses `LoadOrStore`/`CompareAndSwap` with a unique token (token generation `:843`, reservation `:1272-1312`); `releaseImportSlot` uses `CompareAndDelete` only when the token matches (`:1315-1335`) |
| Import | `storagePool.WriteFile` (`:854`); the source handle is closed before post-import actions run (Windows semantics, `:859-863`) |
| **Post-import actions** | `performPostImportAction` `:888-915`: `Delete` → `os.Remove` (`:894-895`); `Move` → create the directory + `moveWithoutOverwrite` (`:897-906`, collision suffix `:917-942`); `Keep` → leave in place (`:908-910`). A failed action means the history is **not** recorded (ordering invariant `:811-815`, `:865-870`) |
| History policy | After a successful `Delete`/`Move` the history entry is removed (the source path no longer exists, `:872-876`); `Keep` writes the history (`:877-879`) |
| History persistence | File `imported-files.json` (`:22-27`), capped at 10000 (`:29-31`); loading drops sources that no longer exist and trims the list (`:1109-1160`); saving uses temp+rename (`:1174-1216`); writes are coalesced by a single flusher goroutine (`:1391-1419`); `Close` waits for in-flight writes before flushing (`:172-190`) and is called by `venue.closeRepositories` (`venue.go:883`) |
| Background scan service | Option precedence: persisted document > `ServiceOptions` > deprecated direct fields > defaults (`pkg/watcher/background_file_watcher_service.go:231-305`); scheduling by watcher due time (`:609-674`, `:802-831`); parallel-scan cap (`:689-739`); polling-interval clamping (`:875-893`); when disabled it re-checks every `DisabledCheckInterval` instead of exiting (`:579-588`) |
| Per-watcher start/stop persistence | `watcher-state.json` (`pkg/watcher/watcher_state.go:15`); the persisted decision takes precedence over the configured `Enabled` (`pkg/watcher/file_watcher.go:344-347`); writes use temp+sync+rename (`watcher_state.go:163-215`) |
| Root derivation | ID `auto-<rootBase>-<tenantDir>` (`pkg/watcher/file_watcher_auto_manager.go:565-567`); applied/updated per subdirectory (`:181-256`, register `:241`, update `:251`); an illegal tenant name is skipped with an error (`:226-232`); `RemoveAllWatchers` deletes only the `auto-` prefix and drops the corresponding root (`:327-362`, `:428-452`); the root list is persisted to `watcher-roots.json` (`:18-21`, `:475-529`); persisted roots are merged at construction (`:110-115`, `:609-633`) |

### 5.7 Quota reconciliation `ReconcileQuotaCounts`

- Public entry point `Venue.ReconcileQuotaCounts` (`venue.go:607-615`); construction calls the same two halves (`venue.go:262`, `venue.go:288`).
- Counted status set: `Pending`/`Processing`/`Completed`/`Failed`/`PermanentlyFailed`/`DeleteRequested` (`venue.go:708-715`) — it does **not** include `DeadLettered` (whose quota was already released during the dead-letter move, `pkg/cleanup/dead_letter.go:352-376`).
- Paged reads: when the repository implements `StatusPageReader`, 500 records per page (`quotaReconcilePageSize`, `venue.go:598`, `venue.go:624-641`); otherwise it falls back to the unbounded `GetByStatus(...,0)` (`venue.go:644-652`); ctx is checked before each status and before each record (`venue.go:621`, `:631`).
- Tenant counts: counted per tenant and then overwritten with `SetFileCount` (`venue.go:657-675`); `SetFileCount` does not install an override value (`pkg/quota/tenant_quota_manager.go:160-163`).
- Directory counts: seeded first from existing quota rows (preserving explicit limits), then accumulated and overwritten using `directorypath.Normalize(record.DirectoryPath)` (`venue.go:683-703`).

## 6. Configuration Model

### 6.1 Uniqueness and no file I/O

- The only public runtime model is `config.Config` (`AGENTS.md:57-58`), and `NewVenue` accepts `*config.Config` directly (`venue.go:75`).
- `config` does not import Viper and performs no file I/O (`AGENTS.md:65`). Verified: among non-test `.go` files, only `viperconfig/viper.go:10` and `examples/viper-config/main.go:13` import `spf13/viper`; `config/boundary_test.go:51`, `:108` are boundary tests forbidding `config` from importing Viper.
- `viperconfig` placement: a top-level package and the only Viper → `config.Config` mapping point (`viperconfig/viper.go:14-32`): it starts from `DefaultConfig()`, uses `ZeroFields=false` (`:19-21`), forces `Logging = nil` (`:26`), then `ApplyDefaults` (`:27`) and `Validate` (`:28`).

### 6.2 Every nested type supports chainable construction

Constructors and fluent methods: `config/fluent.go:148` (FileWatcherService), `:185` (FileWatcherRoot), `:358` (Statistics), `:413` (StatisticsDimension), `:442` (StatisticsOutput), `:475` (RetryPolicy), `:505` (TenantManager), `:523` (Metadata), `:541` (BadgerDB), `:643` (Volume), `:670` (Tenant), `:693` (FileWatcher), `:827` (Cleanup), `:960` (DeadLetter), `:990` (RetiredVolume), `:1014` (DatabaseHealthCheck), `:1020` (OrphanRecovery). Top level: `New`/`DefaultConfig` (`config/config.go:474`, `:479`).

Collection semantics: `WithVolumes`/`WithTenants`/`WithFileWatchers` **replace** (`config/fluent.go:72`, `:91`, `:110`; rule at `AGENTS.md:61`); `AddVolume`/`AddTenant`/`AddFileWatcher`/`AddFileWatcherRoot`/`AddRetiredVolume` **append** (`:83`, `:102`, `:302`, `:132`, `:952`).

### 6.3 Binding tag requirements

Every exported configuration field must carry `json`/`yaml`/`mapstructure` simultaneously (`AGENTS.md:63`), for example `config/config.go:43-68`. Runtime-only fields must be `-`: `Config.Logging` (`config/config.go:68`), `logging.Config.Handler` (`pkg/logging/logging.go:17`).

### 6.4 `ApplyDefaults` semantics and the boolean zero-value trap

`ApplyDefaults` fills only **zero-value** settings (paths, durations, capacities, string enums), and the documentation states plainly that "boolean defaults must start from `DefaultConfig`/`New`" (`config/config.go:602-604`). The full list of fields it handles is at `config/config.go:609-794`.

Consequently, `&config.Config{}` + `ApplyDefaults()` cannot restore the following booleans that default to **true** (or carry non-zero semantics):

| Field | Default | Default location | Handled by `ApplyDefaults`? |
| --- | --- | --- | --- |
| `AutoCreateTenants` | true | `config/config.go:484` | No |
| `EnableDatabaseHealthCheck` | true | `config/config.go:485` | No |
| `EnableBackgroundCleanup` | true | `config/config.go:529` | No |
| `RetryPolicy.UseExponentialBackoff` | true | `config/config.go:489` | No (it only fills three durations/counts, `:618-626`) |
| `Volumes[i].EnableFsync` | true | `config/config.go:515` | No (it only fills Type/InitialDelay/HealthCheckDelay, `:766-776`) |
| `Cleanup.CleanupEmptyDirectories` / `CleanupTimedOutFiles` / `CleanupPermanentlyFailedFiles` / `CleanupCompletedRecords` / `CleanupJunkFiles` / `CleanupInvalidDatabaseBackups` / `OptimizeDatabases` | all true | `config/config.go:533`, `:534`, `:538`, `:547`, `:552`, `:554`, `:556` | No (it only fills durations/batch sizes/enums/RootPath, `:651-686`) |
| `Cleanup.EnableBackgroundTimedOutReclaim` | true | `config/config.go:562` | No |
| `Cleanup.DeadLetter.IncludeTenantInPath` / `IncludeDatePartition` | both true | `config/config.go:542`, `:543` | No (it only fills `RootPath` and `ShardingDepth`, `:681-686`) |
| `FileWatcherService.Enabled` | true | `config/config.go:522` | No; moreover it is unconditionally overwritten at construction (see §10.4, `pkg/watcher/background_file_watcher_service.go:253`) |
| `FileWatchers[i].Enabled` / `FileWatcherRoots[i].Enabled` | Caller-dependent; the configuration example sets true | `config/config.go:176`, `:231` | No |
| `Statistics.Dimensions.VolumeID` / `WatcherID` / `Operation` | all true | `config/config.go:587`, `:588`, `:589` | No (it only fills WindowSize/Retention/MaxSeries/Output, `:696-713`) |

A typical observable consequence: when constructed from `&config.Config{}`, `Cleanup.DeadLetter` is filled in by `ApplyDefaults` with `RootPath=".deadletter"` and `ShardingDepth=2`, so the "apply defaults only when all fields are zero" branch of `DeadLetterOptions.withDefaults()` (`pkg/cleanup/dead_letter.go:59-67`) no longer fires, and the resulting dead-letter paths **lose the tenant and date partitions**. This matches the risk described in `docs/locus-feature-gaps.md:101` (D8).

For contrast: the throttling/debounce switches of `config.FileWatcherConfig`/`FileWatcherRootConfig` use inverted naming (`DisableImportedFilesPruneThrottle`, `DisableImportedFilesHistoryFlushDebounce`, `config/config.go:199-202`, `:208-211`, `:270-272`, `:278-280`) and therefore avoid the zero-value trap; but the positive-polarity fields in `pkg/core.FileWatcherConfiguration` (`EnableImportedFilesPruneThrottle` and friends, `pkg/core/interfaces.go:700-716`) are neither mapped nor consumed (see §10.3).

## 7. Logging and Errors

### 7.1 Instance-scoped `logging.Runtime`

- The state is an `atomic.Pointer[runtimeState]`; `slog.Default` is never read or written (`pkg/logging/logging.go:33-37`).
- `New` builds `slog.New` from the **injected handler** (`:40-48`); `Disabled` leaves the state nil (`:51-53`), in which case `Emit` returns early (`:86-90`) — that is, "disabled means silent".
- `Config.Logging == nil` means disabled (`AGENTS.md:75`; `venue.go:86`).
- Handler panic isolation: both `Emit` and `Enabled` swallow panics with `defer recover()` (`:64-69`, `:79-82`), guaranteeing that logging cannot affect storage operations (`AGENTS.md:77`).
- The caller owns the handler and the writer (`pkg/logging/logging.go:11-14`; `AGENTS.md:76`).
- Structure: every `Emit` always attaches the `component` and `event` attributes (`:92-97`); `Record` is defined at `:20-27`.
- Composition-time defaults: `venue.go:86`; `Venue.emit` always emits with `component=venue` (`venue.go:1049-1053`); errors log only the type, never the original text (`errorTypeAttr`, `venue.go:1055-1057`).
- Examples of implementations that do not leak physical paths: quarantine logging records only the base name (`venue.go:906-915`); automatic-recovery warnings record only `reason`/`error_kind` (`pkg/metadata/backup.go:444-463`); backup failures do the same (`pkg/metadata/backup.go:608-629`); scan errors carry no path or original name (`pkg/watcher/file_watcher.go:724-732`).

### 7.2 Stable sentinel errors (`pkg/core/errors.go`)

| Error | Line | Main usage sites |
| --- | --- | --- |
| `ErrTenantNotFound` | `:12` | `pkg/tenant/manager.go:150`, `:198` |
| `ErrTenantDisabled` | `:15` | `pkg/pool/storage_pool.go:205`, `pkg/scheduler/file_scheduler.go:176` |
| `ErrTenantSuspended` | `:18` | Not returned by any runtime code (definition only) |
| `ErrTenantAlreadyExists` | `:21` | `venue.go:192`, `pkg/tenant/manager.go:252` |
| `ErrTenantQuotaExceeded` | `:26` | `pkg/quota/tenant_quota_manager.go:142` |
| `ErrDirectoryQuotaExceeded` | `:29` | `pkg/quota/directory_quota_manager.go:66` |
| `ErrInsufficientStorage` | `:34` | `pkg/pool/storage_pool.go:427`, `pkg/pool/volume_selector.go:55`, `:86`, `:116`, `:131`, `:153`, `:167` |
| `ErrStorageVolumeUnavailable` / `ErrVolumeAlreadyMounted` | `:37`, `:40` | Definition only; no runtime return site |
| `ErrFileNotFound` | `:45` | `pkg/metadata/badger_repository.go:665`, `pkg/volume/local_volume.go:255`, `:336` |
| `ErrFileAlreadyProcessing` | `:48` | Definition only; no runtime return site |
| `ErrNoFilesAvailable` | `:55` | **Deprecated**: an empty queue now returns `(nil, nil)` (comment `:50-54`) |
| `ErrWatcherNotFound` | `:58` | `pkg/watcher/file_watcher.go:1064` |
| `ErrInvalidFileKey` / `ErrFileAlreadyExists` | `:61`, `:64` | Definition only; no runtime return site |
| `ErrFileNotClaimable` | `:73` | `pkg/metadata/badger_repository.go:1075`, `:1081`, `:1124`; semantic classification in `pkg/scheduler/file_scheduler.go:65-77` |
| `ErrProcessingLeaseMismatch` | `:76` | Exposed by `FileProcessingLeaseMismatchError.Unwrap` (`:112-115`) |
| `ErrInvalidArgument` | `:120` | Argument validation across the repository |
| `ErrOperationCanceled` | `:123` | Definition only; cancellation paths return `ctx.Err()` (for example `pkg/scheduler/file_scheduler.go:260-262`) |
| `ErrDatabaseError` | `:126` | `pkg/metadata/badger_repository.go:166`, `:1126`, `:1243` |
| `ErrPathTraversalAttempt` | `:129` | `pkg/volume/path_sanitizer.go:48`, `:56`, `pkg/core/tenantid.go:87` |

Convention: domain checks use `errors.Is` (`AGENTS.md:119`); wrapping carries safe context (tenant, fileKey, component) and uses `%w` (`AGENTS.md:118`).

## 8. Concurrency and Durability Invariants

### 8.1 Metadata transactions

Every write path is a single `db.Update` transaction, with the primary key and the secondary indexes updated in the same transaction: `AddOrUpdate` (`pkg/metadata/badger_repository.go:508-535`), `AddOrUpdateBatch` (`:583-617`), `Delete` (`:710-723`), `DeleteBatch` (`:763-781`), `UpdateStatus` (`:954-1000`), `CompareAndTransitionToProcessing` (`:1051-1111`), `CompareAndUpdateProcessing` (`:1176-1237`).

Consistency strategy: bounded retries for optimistic conflicts, retrying only `badger.ErrConflict` (`pkg/metadata/conflict_retry.go:48`), with 3 attempts by default (`:16`) and a linear backoff starting at 5ms (`:21`, `:55`), while respecting ctx (`:40-42`, `:56-60`).

### 8.2 CAS / lease

See §5.2 and §5.3. In short: a claim must satisfy both the status and the availability time (`pkg/metadata/badger_repository.go:1073-1082`); completion/failure/reclaim must carry a matching `ProcessingStartTime` (`:1188-1190`), so **a stale worker cannot overwrite a newer claim** (`AGENTS.md:97`; comment at `pkg/scheduler/file_scheduler.go:544-554`).

### 8.3 Per-directory synchronization

The directory-quota manager uses 256 mutexes with FNV-1a hash sharding, keyed by `tenantID + 0xff + directoryPath` (`pkg/quota/directory_quota_manager.go:16`, `:191-206`); every operation that reads or writes a count takes the lock first (`:36-37`, `:55-56`, `:83-84`, `:100-101`, `:118-119`, `:149-150`, `:170-171`).

### 8.4 Atomics and `sync.Map`

| Mechanism | Location | Protected object |
| --- | --- | --- |
| `sync.Map` | `pkg/watcher/file_watcher.go:87`, `:96` | Watcher registry, import history (including `InFlightToken`) |
| `sync.Map` + CAS loop | `pkg/quota/tenant_quota_manager.go:53` | Per-tenant quota entries |
| `sync.Map` | `pkg/statistics/recorder.go:230` | Statistics series (creation/trimming serialized by `mu`, `:234`) |
| `atomic.Int64` ×11 | `pkg/cleanup/cleanup_statistics.go:19-29` | Cumulative cleanup statistics |
| `atomic.Int64` | `pkg/quota/tenant_quota_manager.go:58` | Global quota limit |
| `atomic.Uint64` | `pkg/watcher/file_watcher.go:1338` | In-flight reservation sequence number |
| `atomic.Int64` | `pkg/statistics/recorder.go:206`, `:241` | Per-series count, record count (triggers trimming) |
| `atomic.Pointer` | `pkg/watcher/background_file_watcher_service.go:166`, `pkg/logging/logging.go:36` | Global watcher option snapshot, logging runtime state |
| `sync.Mutex`/`RWMutex` | `pkg/pool/storage_pool.go:82`, `pkg/scheduler/file_scheduler.go:95`, `pkg/cleanup/cleanup_service.go:128`, `pkg/volume/local_volume.go:78-81`, `pkg/metadata/cache.go:30` | Capacity cache, reclaim cooldown, cleanup-service volume snapshot/cumulative counters, health-probe single-flight, metadata LRU |

Immutable-snapshot convention: watcher configuration is handed out as a copy (`snapshotConfig`, `pkg/watcher/file_watcher.go:1076-1084`; `watcherEntry.mu`, `:113-116`); root templates are handed out as copies (`pkg/watcher/file_watcher_auto_manager.go:593-605`); the `storagePool` volumes/pathGenerator/selector are construction-time dependencies that are never replaced afterwards, so they can be read without a lock (`pkg/pool/storage_pool.go:63-65`); the metadata cache `get`/`set` both return/store copies (`pkg/metadata/cache.go:70-72`, `:96-97`) and refuse to let an older `UpdatedAt` overwrite a newer value (`:84-94`).

### 8.5 FIFO

The status index key is `v3:idx:status:{base64url tenant}:{status}:{availableUTC}:{createdUTC}:{fileKey}` (`pkg/metadata/migration.go:279-299`), with both timestamps in a fixed-width UTC layout (`:20-24`) and immediately available files using the lexicographically smallest `unavailableTimestamp` placeholder (`:26-29`). Therefore the iteration order of `GetPendingFiles` is "availability time → arrival time → fileKey" (`pkg/metadata/badger_repository.go:885-887`), and the availability test is inclusive (`:907-919`).

### 8.6 Long-task paging and cancellation

- Cleanup status scans prefer `core.StatusPageReader` with 500 records per page (`pkg/cleanup/cleanup_service.go:18-21`, `:224-275`), and return an explicit error when the cursor does not advance (`:270-272`).
- Quota reconciliation pages the same way (`venue.go:624-641`).
- ctx checks inside loops: claiming (`pkg/scheduler/file_scheduler.go:260-262`, `:302-304`), cleanup (`pkg/cleanup/cleanup_service.go:306-308`, `:348-350`, `:621`), import (`pkg/watcher/file_watcher.go:689-691`), recovery (`pkg/recovery/orphan_recovery_service.go:249-251`, `:277-279`), health check (`pkg/health/database_health_checker.go:175-177`).

### 8.7 Explicit `SyncWrites` / fsync trade-offs

| Location | Configuration | Behavior |
| --- | --- | --- |
| Metadata database | `BadgerDB.SyncWrites` (`config/config.go:90`; default false because `DefaultConfig` does not set it) | Passed to Badger as `WithSyncWrites` (`pkg/metadata/badger_repository.go:309`); the comment describes it as a performance/durability trade-off (`config/config.go:60-62`) |
| Physical files | `VolumeConfig.EnableFsync` (`config/config.go:139`, default true at `config/config.go:515`) | `file.Sync()` (`pkg/volume/local_volume.go:232-238`) |
| Backup files | No switch | staging `file.Sync()` followed by rename (`pkg/metadata/backup.go:365-378`) |
| Watcher runtime state / global options / root list | No switch | All use temp + `Sync` + rename (`pkg/watcher/watcher_state.go:187-213`, `pkg/watcher/background_file_watcher_service.go:958-984`, `pkg/watcher/file_watcher_auto_manager.go:500-526`) |
| Import history | No switch | temp + rename only, with **no fsync** (`pkg/watcher/file_watcher.go:1205-1213`) |
| Intermediate copy for cross-device moves | No switch | staging `Sync()` followed by rename (`pkg/volume/local_volume.go:420-429`) |

Database deletion requires periodic value-log GC (`AGENTS.md:111`): the background GC runs on `BadgerDB.GCInterval` (`pkg/metadata/badger_repository.go:1461-1483`), and `badger.ErrNoRewrite` is treated as a normal "no work" outcome (`:1357-1359`).

## 9. Testing and Verification Conventions

### 9.1 Commands

The required gate set for every behavioral change is: `go build ./...`, `go vet ./...`, `go test -count=1 ./...`, `go test -race -count=1 ./...`, `golangci-lint run`, and `CGO_ENABLED=0 go build ./...` — the project must build without cgo (`docs/README.md:50-62`). `AGENTS.md:137-143` lists the same commands except the cgo check and the `-count=1` flag. A changed behavior must come with a regression test that fails first and passes afterwards (`AGENTS.md:134`); run the race tests before any commit (`AGENTS.md:155`).

### 9.2 Go caches and the temporary directory

When the default Go cache is not writable in the current environment, keep the Go caches and the temporary directory inside the repository's ignored build area (`AGENTS.md:158-166`, `README.md:716-725`). Machine-specific paths must not be committed to documentation or configuration (`docs/README.md:67-69`). No repository file records which directories this environment actually resolves to; see §10.6.

### 9.3 Benchmark entry points

- All system benchmarks go through the public entry point: `setupBenchmarkSystem` builds the configuration fluently with `config.New()` and then calls `venue.NewVenue` (`test/benchmark/system_bench_test.go:31-45`) and `Start` (`:46`), shutting down through `runtime.Stop()` (`:49-53`).
- Public benchmarks: `BenchmarkWriteFile` (`:68`), `BenchmarkWriteFile_Parallel` (`:87`), `BenchmarkReadFile` (`:110`), `BenchmarkGetNextFileForProcessing` (`:137`), `BenchmarkCompleteWorkflow` (`:174`), `BenchmarkMetadataOperations` (`:210`), `BenchmarkQuotaOperations` (`:277`), `BenchmarkConcurrentProcessing` (`:310`).
- The cleanup-scale benchmark `BenchmarkCleanupCompletedRecords` (`test/benchmark/cleanup_scale_bench_test.go:95`) also uses `venue.NewVenue` (`:83`), but seeds records through the public accessor `runtime.MetadataRepository().AddOrUpdateBatch` (`:42`) and measures with `runtime.CleanupService().CleanupCompletedFiles` (`:117`).
- Conventions: fixture generation stays outside the timed sections (`b.ResetTimer`, `system_bench_test.go:75`, `:120`; `b.StopTimer`/`b.StartTimer`, `cleanup_scale_bench_test.go:113-115`); `b.ReportAllocs` (`system_bench_test.go:76`); an empty queue is a normal result, not an error (`:161-169`, `:334-337`).

### 9.4 Other conventions

- Filesystem tests use `t.TempDir()` (`AGENTS.md:149`; `system_bench_test.go:29`, `cleanup_scale_bench_test.go:72`).
- Shutdown is registered before the temporary directory is cleaned up (`AGENTS.md:150`; `system_bench_test.go:49-53`, `cleanup_scale_bench_test.go:90`).
- Integration paths go through `venue.NewVenue`, unless the test targets a lower-level component (`AGENTS.md:151`). Existing examples: `venue_lifecycle_test.go:34` (`Stop` without `Start` still releases the repositories), `:56` (idempotent `Stop`), `:83` (a failed construction releases already-opened resources), `:492` (immediate reclaim on an empty queue is wired), `:349` (reconciliation repairs drift), `:263`/`:300` (orphan recovery is opt-in and re-enqueues).
- Same-`fileKey` behavior across tenants must have a test (`AGENTS.md:152`); the tenant-isolation test file is `pkg/metadata/tenant_isolation_test.go`.

## 10. Unverified / Open

### 10.1 Referenced documents that do not exist

The task brief for this document referenced `docs/locus-alignment.md` and `docs/sqlite-storage-design.md`; when this finding was recorded, `docs/` contained only `locus-feature-gaps.md`, so that division of labour could not be checked. Both documents are present in the current working tree (`docs/README.md:10-15`), and §1.3 now describes them. In addition, `docs/locus-feature-gaps.md:3`, `:102-103` record that `docs/venue-locus-alignment-review.md` was deleted by mistake and has no copy, which likewise cannot be checked against anything in the tree.

### 10.2 README/AGENTS vs code inconsistencies

| # | Location | Problem | Evidence |
| --- | --- | --- | --- |
| U1 | `README.md:255` | It states "Watcher minimum file age = `5s`", but calling `RegisterWatcher` directly with `MinFileAge=0` falls back to **3s** (only the configuration path, which goes through `ApplyDefaults`, yields 5s) | `README.md:255` vs `pkg/watcher/file_watcher.go:326-328`, `config/config.go:781-783`, `config/config.go:752-754` (`docs/locus-feature-gaps.md:98` D5) |
| U2 | `README.md:690-697` | It lists "the in-process statistics recorder/reader and its periodic log output" as **not implemented**, but it is implemented and is assembled and exposed by `Venue` | `README.md:693-694` vs `venue.go:119-137`, `venue.go:977-994`, `pkg/statistics/recorder.go:251`, `pkg/statistics/recorder.go:706` |
| U3 | `README.md:690-697` | It lists the "Locus volume write-path diagnostics/warmup surface" as not implemented, which matches the code (`core` has the contract, no implementation), but the same sentence mixes statistics in, leaving the whole list out of date | `pkg/core/interfaces.go:376-398` (no implementation) versus `pkg/statistics/recorder.go` (implemented) |
| U4 | `README.md:777-796` | The Repository Layout does not list `pkg/statistics` or `internal/directorypath`, although both exist as packages and are now listed in the ownership list of `AGENTS.md:33-51` (`:49-50`) | `README.md:777-796` vs `AGENTS.md:49-50` |
| U5 | `README.md:680-682` | It says the background low-frequency reclaim is "covered by the cleanup cycle", while the configuration still carries a dedicated switch and batch field that nobody consumes | `README.md:680-682` vs `config/config.go:404-412` (no consumer anywhere in the repository) |
| U6 | `docs/locus-feature-gaps.md:28-32` (W1.6) | It marks "volume reports depth; shard directories are not deleted by mistake" as complete, but `ShardingDepthProvider` has **no type-assert usage site** in runtime code, and cleanup still uses a structural heuristic | `pkg/core/interfaces.go:296`, `pkg/volume/local_volume.go:146` vs `pkg/cleanup/cleanup_service.go:476-492` |
| U7 | `docs/locus-feature-gaps.md:71-72` (W5.6/W5.7), `:74` (W5.9) | The optional capability interfaces are defined in `core` but have **no implementation and no call site** | `pkg/core/interfaces.go:405-437`, `pkg/core/models.go:786-815` |

### 10.3 Declared configuration with no runtime consumer

| Field | Declared at | Current state |
| --- | --- | --- |
| `Cleanup.EmptyQueueReclaimBatchSize` | `config/config.go:399-402` | Not wired: `venue.go:340` uses `scheduler.DefaultEmptyQueueReclaimBatchSize` (`pkg/scheduler/file_scheduler.go:39`), with a comment explaining why (`venue.go:338-339`) |
| `Cleanup.EnableBackgroundTimedOutReclaim`, `Cleanup.BackgroundTimedOutReclaimBatchSize` | `config/config.go:404-412` | No consumer anywhere in the repository (they appear only in `config`/`config/fluent.go:629-641`) |
| `Config.FailFastOnStartupRecoveryFailure` | `config/config.go:50-53` | No consumer anywhere in the repository (only `config/fluent.go:342-347`) |
| `VolumeConfig.WarmupOnStartup` | `config/config.go:156-159` | `venue.go:303-310` does not pass this field; there is also no `StorageVolumeWritePathWarmup` implementation (§3.2) |
| `VolumeConfig.HealthCheckCacheTTL` | `config/config.go:141-144` | Wired (`venue.go:309`), implemented at `pkg/volume/local_volume.go:112-115`, `:155-176` |
| `FileWatcherConfig` advanced knobs (`AutoCreateTenantDirectoriesCacheTTL`, `FileStabilityCheckDelay`, `SkipStabilityCheckAfterAge`, `DisableImportedFilesPruneThrottle`, `ImportedFilesPruneInterval`, `DisableImportedFilesHistoryFlushDebounce`, `ImportedFilesHistoryFlushInterval`) | `config/config.go:186-216` | Not mapped by `venue.go:455-470`, and `pkg/watcher` references none of them (grep matches only `config` and `pkg/core`), so the second stability probe, the per-tenant directory cache TTL, history-prune throttling, and flush debouncing do not take effect at runtime (`docs/locus-feature-gaps.md:76` W5.11 does not match the code) |
| Same-named `FileWatcherRootConfig` knobs | `config/config.go:257-285` | Not mapped by `venue.go:502-514`; `pkg/watcher/file_watcher_auto_manager.go:260-276` also does not carry these fields when it constructs derived watchers |
| The positive-polarity switches of `core.FileWatcherConfiguration`, `EnableImportedFilesPruneThrottle` and `EnableImportedFilesHistoryFlushDebounce` (plus three duration fields) | `pkg/core/interfaces.go:685-716` | No read or write site at all |
| `OrphanRecoveryServiceOptions.BatchSize`, `MinimumFileAge` | `pkg/recovery/orphan_recovery_service.go:53-61` | Not passed by `venue.go:540-549`, so they are fixed at runtime to 1000 (`:117-120`) and 0 (`:137`) |

### 10.4 `FileWatcherService.Enabled = false` is ignored at construction

The constructor first reads `options := opts.ServiceOptions` and then **unconditionally** sets `options.Enabled = true`, with a comment stating that "a zero value does not mean disabled; use `SetEnabled(false)` or the persisted document" (`pkg/watcher/background_file_watcher_service.go:234`, `:248-253`). Both `venue.go:482` and `venue.go:485` pass `FileWatcherService.Enabled` in, but it is only honoured when `file-watcher-options.json` exists (`:266-280`). Therefore "write `watcherServiceOptions.enabled: false` in the configuration" **does not** make the service stop scanning on the current code path — a point that leaves room for interpretation against `README.md:632-634` and `config/config.go:291-293`, and I cannot confirm from the code whether this is deliberate.

### 10.5 Not yet read line by line

- The platform implementations of `TotalCapacity`/`AvailableSpace` in `pkg/volume/local_volume_windows.go` (59 lines) and `local_volume_unix.go` (33 lines) were not read; I only confirmed that they are called at `pkg/pool/storage_pool.go:716-721`.
- `venue-config-example.yaml` (9378 bytes) was not compared key by key against `config.Config`; `docs/locus-feature-gaps.md:99` (D6) also marks that check as "to be reviewed".
- For `test/benchmark` I confirmed only the entry points and timing conventions, not the strength of every assertion.
- No `go build`/`go test`/`golangci-lint` was run (per the task constraints), so every "compile-time assertion" conclusion comes from the `var _ = ...` declarations in the sources rather than from actual compiler output.

### 10.6 Operational facts to verify in this environment

`AGENTS.md:158-166` and `README.md:716-725` document only that, when the default Go cache is not writable, the caches and the temporary directory are pointed at an ignored repository-local directory; they do not record the paths this host actually resolves to. I also ran no Go command to observe the temporary directories in use (running the Go toolchain was forbidden by the task, to avoid contention with other agents over the build cache).
