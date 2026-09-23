# Venue and Locus Alignment Document

> This document compares Locus `v2.0.0` with the current Venue working tree, capability domain by capability domain. Every conclusion
> carries two-sided `file:line` evidence. Items with evidence on only one side are always explicitly marked "single-sided evidence, unconfirmed".

## 1. Baseline and method

### 1.1 Baseline

| Item | Value |
| --- | --- |
| Behavioral baseline | Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4` |
| Watcher update baseline | Locus `v1.5.5`, commit `06c152a`; durable source cleanup is the current watcher behavior reference |
| Locus read-only export | `locus/` (the reference checkout for Locus v2.0.0 is not part of this repository; `locus/src/Locus`, `locus/src/Locus.Core`, `locus/src/Locus.MultiTenant`, `locus/src/Locus.FileSystem`, `locus/src/Locus.Storage`) |
| Venue counterpart | Current working tree based on HEAD `a217552`, including the uncommitted watcher-alignment changes described here; every conclusion follows the code on disk |
| Storage-engine design | `docs/sqlite-storage-design.md` (directory and file layout, schema, PRAGMA and connection policy, CAS SQL, backup and corruption recovery) |

### 1.2 Classification

| Classification | Meaning | Handling |
| --- | --- | --- |
| `PRESENT` | Venue already has it with equivalent behavior | No action |
| `OBSERVABLE-GAP` | Callers or operators can observe a difference through the public API, the filesystem, or behavior | Must be closed; see section 5 |
| `MECHANISM` | Locus implements it with some internal mechanism and Venue substitutes another mechanism | Must state item by item whether the observable guarantee is equivalent |
| `DIVERGENCE` | Venue has a documented, deliberate difference | Must state the reason and the caller-visible impact |

### 1.3 Evidence convention

- Locus evidence is written as `locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687` (repository-relative; the Locus v2.0.0 reference checkout is not part of this repository).
- Venue evidence is written as `pkg/watcher/file_watcher.go:247` (repository-relative).
- A conclusion is recorded only when both sides are cited; an item missing one side is marked "single-sided evidence, unconfirmed" and moves to section 6.
- No evidence-free judgments of the "probably / should" kind are used.

---

## 2. Overview matrix

| # | Capability domain | Classification | Locus evidence | Venue evidence | One-line conclusion |
| --- | --- | --- | --- | --- | --- |
| 1 | Storage engine and metadata model | `PRESENT` (engine layout converged) | `locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`, `:559-588`, `locus/src/Locus.Core/Models/SqliteOptions.cs:18-42` | `pkg/metadata/sqlite_repository.go:379-383` (capability assertions), `:394` (`NewSQLiteMetadataRepository`), `pkg/quota/sqlite_directory_quota_repository.go:279`, `:290`, `pkg/sqlite/sqlite.go:1-30`, `config/config.go:91-175`, wired at `venue.go:354` and `venue.go:403` | Locus uses one SQLite `metadata.db` per tenant; Venue now stores both metadata (`metadata.db`) and directory quotas (`quotas.db`) the same way on the runtime path, so the storage layout converges. `config.SqliteConfig` is the only engine configuration and the BadgerDB implementations are gone from the tree; the engine design lives in `docs/sqlite-storage-design.md` (see 4.1 and M6) |
| 2 | Queue table and projection mechanism | `MECHANISM` | `locus/src/Locus.Storage/FileQueueEventJournal.cs:1085-1090`, `locus/src/Locus.Storage/QueueEventProjectionService.cs:211-296`, `:1407-1427` | `pkg/metadata/sqlite_repository.go:826-898` (conditional claim), `:719-724` (keyset order), `pkg/metadata/sqlite_paging.go:36-47`, `pkg/scheduler/file_scheduler.go:386-423`, `:433-457` | Locus uses an append-only `queue.log` plus projection, snapshot, and compaction; Venue uses synchronous SQLite transactions plus a status index over the `files` table, with no journal, no sequence-gap self-healing, and no projection-lag observability |
| 3 | Metadata write path | `MECHANISM` | `locus/src/Locus.Storage/Data/MetadataRepository.cs:20-56`, `:127-166`, `locus/src/Locus/MetadataRepositoryOptions.cs:45-83` | `pkg/pool/storage_pool.go:227-292`, `pkg/metadata/sqlite_repository.go:488-518` with the upsert at `:150-179` | Locus queues write-behind persistence (a crash loses in-flight metadata, which then becomes orphan recovery); Venue uses synchronous transactions plus physical rollback, so durability is stronger |
| 4 | Quota (projection/compensation/reconciliation vs recompute/compensation) | `MECHANISM` | `locus/src/Locus.Storage/TenantQuotaManager.cs:47-64`, `:237-267`, `:374-386`; `locus/src/Locus.Storage/StorageCleanupService.cs:1006-1049` | `venue.go:791-799`, `:841-890`, `pkg/pool/storage_pool.go:378-410`, `pkg/quota/tenant_quota_manager.go:288-356` | "Counts can be reconciled" is preserved; Locus self-corrects from events, while Venue needs a startup recompute or an explicit `ReconcileQuotaCounts` |
| 5 | Cleanup and data retention | Mixed (retention sub-items `PRESENT`, see 2.1) | `locus/src/Locus.Storage/BackgroundCleanupService.cs:262-294`, `:367-448`; `locus/src/Locus.Storage/StorageCleanupService.cs:1598-1649`, `:2233-2271`, `:2390-2401` | `pkg/cleanup/dead_letter.go:29-53`, `pkg/cleanup/junk_files.go:16-20`, `pkg/cleanup/invalid_database_backups.go:21-23`, `pkg/cleanup/cleanup_statistics.go:18-69`, `pkg/cleanup/database_optimization.go:40-86`, `pkg/cleanup/cleanup_service.go:340-373` | All six retention sub-items have landed, and the detailed optimization result plus the single-tenant sweep are implemented as well; the `CleanupStatistics` field set is wider than Locus, but the orphan counters differ in name and semantics |
| 6 | Timed-out reclaim | `PRESENT` | `locus/src/Locus.Storage/FileScheduler.cs:602-692`, `:621-639`; `locus/src/Locus/StoragePoolOptions.cs:24-43` | `pkg/scheduler/file_scheduler.go:433-457`, `:508-539`, `:571-604`, `:770-781`, `venue.go:487-498`, `config/config.go:435-436`, `:451-464` | Immediate reclaim on an empty queue is equivalent, and the low-frequency background reclaim is now wired with its own switch, batch size, cooldown and contained failure logging; the two paths keep independent cooldown maps (4.7) |
| 7 | Watcher | `PRESENT` + persistence `MECHANISM` | Locus `v1.5.5`: `FileWatcher.cs` operation ID, sampled fingerprint, pending action retry/quarantine, independent scheduling, durable source cleanup reservations and leases | `pkg/watcher/file_watcher.go`, `pkg/watcher/source_cleanup_store.go`, `pkg/watcher/source_cleanup_worker.go`, `pkg/watcher/background_file_watcher_service.go`, `pkg/pool/storage_pool.go`, `pkg/metadata/sqlite_repository.go` | Observable behavior is aligned. Venue persists operation IDs directly in tenant SQLite metadata and source cleanup jobs in a dedicated pure-Go SQLite database; it intentionally avoids Locus's queue-journal projection mechanism |
| 8 | Runtime statistics | `PRESENT` | `locus/src/Locus.Core/Abstractions/ILocusStatisticsRecorder.cs`, `locus/src/Locus.Core/Abstractions/ILocusStatisticsReader.cs:13`, `locus/src/Locus.Core/Models/LocusStatisticsOptions.cs:13-43`, `locus/src/Locus.Storage/Statistics/LocusStatisticsOutputService.cs:43` | `pkg/statistics/recorder.go:63-166`, `:284`, `:370-429`, `:675`, `:711`, `venue.go:121-139`, `venue.go:963`, `venue.go:1009`, `venue.go:1256-1261` | recorder/reader/window aggregation/dimension filtering/bounded series/optional periodic output are all present, and defaults and bounds match Locus |
| 9 | Database health and corruption recovery | `MECHANISM` | `locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs:142-186`, `:533-563`, `locus/src/Locus/LocusOptions.cs:76-83` | `pkg/metadata/sqlite_corruption.go:32-41`, `:163-191`, `pkg/metadata/sqlite_backup.go:30`, `:54-60`, `venue_metadata_backup.go:390`, `:410`, `venue.go:58`, `venue.go:1173-1194`; health verdict `pkg/health/database_health_checker.go:592-637` | Locus prefers rebuilding from the queue journal and then scans physical files; Venue quarantines a damaged tenant database and, when configured, replaces it from the newest verified backup, degrading recovery granularity from per-event to the backup period, and reports an unrecoverable quarantine explicitly (`database_recovery_incomplete`) with an optional fail-fast switch |
| 10 | Volume capabilities (probe/warmup/diagnostics) | `PRESENT` | `locus/src/Locus.Core/Abstractions/IStorageVolumeHealthProbe.cs:11`, `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathWarmup.cs:14`, `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathDiagnostics.cs:13`, `locus/src/Locus.Core/Abstractions/IStorageVolume.cs:41`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:281`, `:333` | `pkg/core/interfaces.go:371-398`, `pkg/volume/local_volume.go:22-28` (`ProbeHealth` `:221`, `WarmWritePathCache` `:248`, `WritePathStatistics` `:289`), `venue.go:184-202`, `:261-281`, `config/config.go:193-211`, `pkg/core/models.go:853-882` | All three capabilities are implemented by the local volume; the forced probe and the optional warmup write are wired into startup, and the observation snapshot is reachable through `Venue.Volumes()` |
| 11 | Tenant management and read-only lookup | `PRESENT` (side-effect difference in 4.5) | `locus/src/Locus.Core/Abstractions/ITenantManager.cs:26`, `locus/src/Locus.MultiTenant/TenantManager.cs:85-88`, `:147-157` | `pkg/tenant/manager.go:174-226`, `pkg/core/interfaces.go:121` | Both sides have a read-only `TryGetTenant`; Venue's `IsTenantEnabled` is read-only and thus has one fewer side effect (no implicit tenant creation) than Locus |
| 12 | Quota administration API | `PRESENT` | `locus/src/Locus.Core/Abstractions/ITenantQuotaManager.cs:52-81` | `pkg/quota/tenant_quota_manager.go:288-356`, `pkg/core/interfaces.go:335-352`, `venue.go:1246` | All five administration methods are present, and the `0 = unlimited` semantics match on both sides |
| 13 | Public return models | `PRESENT` | `locus/src/Locus.Core/Models/FileInfo.cs:14-39`, `locus/src/Locus.Core/Models/DatabaseOptimizationResult.cs:11-41` | `pkg/core/models.go:193-211`, `:813-828`, `:844-882`, `pkg/cleanup/database_optimization.go:40-86` | `FileInfo` fields are aligned, and `DatabaseOptimizationResult` is now populated by `OptimizeDatabasesDetailed` (reclaimed bytes plus before/after sizes) |
| 14 | Configuration model and lifecycle | `PRESENT` + naming `DIVERGENCE` | `locus/src/Locus/LocusOptions.cs:11-153`, `:159-241` | `config/config.go:42-69`, `:531-852`, `venue.go:77`, `:938-987`, `:995-1041` | Every Locus configuration section has a corresponding Go structure; construction/start/stop semantics differ but are equivalent |

**Driver constraint of the SQLite engine**: the engine must use a **pure-Go driver** (no cgo), so this alignment cannot depend on a cgo-based SQLite binding and the repository gates must pass with `CGO_ENABLED=0`. The adopted driver is `modernc.org/sqlite` (`pkg/sqlite/sqlite.go:12-14`, `:42-53`; `docs/sqlite-storage-design.md:63`, `:267`), chosen precisely because it builds with `CGO_ENABLED=0` (`docs/sqlite-storage-design.md:287`); the cgo-based `github.com/mattn/go-sqlite3` is rejected for this reason (`docs/sqlite-storage-design.md:297`). The gates themselves are the required build/test/race/vet/lint commands listed in `AGENTS.md` and `docs/README.md:52-65`.

### 2.1 Cleanup and retention sub-item verdicts

| Sub-item | Classification | Locus evidence | Venue evidence |
| --- | --- | --- | --- |
| R1 Permanent-failure disposition and dead letter | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:367-383`, `:421-446`; `locus/src/Locus.Storage/StorageCleanupService.cs:1598-1649`, `:2233-2271` | `pkg/core/models.go:89-93` (`DeadLettered=7`), `:328-375` (`Keep=0`/`MoveToDeadLetter=1`/`Delete=2`, default MoveToDeadLetter), `:477`; `pkg/cleanup/dead_letter.go:15`, `:29-53`, `:78-123`, `:290` |
| R2 Junk-file cleanup | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:262`, `:269`; `locus/src/Locus.Storage/StorageCleanupService.cs:73-75` | `pkg/cleanup/junk_files.go:16-20`, `:67-89`, `config/config.go:444-445` |
| R3 Invalid database-backup cleanup | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:294`; `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:121` | `pkg/cleanup/invalid_database_backups.go:48`, `:72-73`, `pkg/core/interfaces.go:464` |
| R4 Retired-volume disposition | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:388-416`; `locus/src/Locus.Storage/StorageCleanupService.cs:2047-2051` | `config/config.go:486-494`, `pkg/cleanup/dead_letter.go:227-267`, `:271-280` |
| R5 Cumulative cleanup statistics | `PRESENT` | `locus/src/Locus.Storage/StorageCleanupService.cs:2390-2401` | `pkg/cleanup/cleanup_statistics.go:18-69`, `pkg/core/interfaces.go:469-471` |
| R6 Exact sharded-directory protection | `PRESENT` | `locus/src/Locus.Core/Abstractions/IStorageVolume.cs:41` | `pkg/volume/local_volume.go:171-178`, `pkg/core/interfaces.go:291-296` (`ShardingDepthProvider`), `pkg/cleanup/cleanup_service.go:524-536` (exact use), `:538-583` (protection decision), `:609-625` (heuristic fallback only) |

---

## 3. Mechanism differences and guarantee comparison

### M1 · per-tenant `queue.log` + projection + snapshot + compaction → synchronous SQLite transactions + a status index

- **Locus**: one `queue.log` per tenant (`locus/src/Locus.Storage/FileQueueEventJournal.cs:1085`) plus `queue.state.json` (`:1090`); the default is `AckMode = Durable` (`locus/src/Locus.Storage/QueueEventJournalOptions.cs:81`), records are encoded as binary v1 (`:48`); the projection service offers `GetTenantStateAsync`/`ReplayTenantAsync`/`SnapshotTenantAsync`/`RebuildTenantAsync` (`locus/src/Locus.Storage/QueueEventProjectionService.cs:211-296`), with automatic snapshotting every 15 minutes or per 1 MB (`QueueEventJournalOptions.cs:131`, `:137`), a compaction threshold of 4 MB (`:151`), and exposed lag/gap/corrupt-tail state (`QueueEventProjectionService.cs:1407-1427`).
- **Venue**: state is the data; `AddOrUpdate`/`UpdateStatus`/`CompareAndTransitionToProcessing` each complete inside one `BEGIN IMMEDIATE` transaction on that tenant's database (`pkg/metadata/sqlite_repository.go:488-518`, `:772-824`, `:826-898`; transaction wrapper `:1722-1736`), Pending order is guaranteed FIFO by the index `(status, available_for_processing_at, created_at, file_key)` (`:89-91`, `:763-770`), and claiming is in `pkg/scheduler/file_scheduler.go:386-423`.
- **Guarantee comparison**:
  - Durability: Venue is **stronger**. Locus's default `AckMode = Durable` (`locus/src/Locus.Storage/QueueEventJournalOptions.cs:79-81`) already fsyncs, but the optional `Balanced`/`Async` defer the flush (`locus/src/Locus.Storage/QueueEventJournalAckMode.cs:13-22`), leaving a window of unflushed appends; Venue commits a transaction on every state change.
  - Rebuild capability: Venue is **weaker**. Locus can replay the journal event by event to rebuild metadata and quotas (`locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs:533-563`); Venue has no journal, only backup-snapshot restore (`README.md:924-930` states that a restore is only as new as the newest backup; `pkg/metadata/sqlite_repository.go:1210-1216` and `venue_metadata_backup.go:70-73` note that each tenant's entry reflects its own backup instant).
  - Sequence-gap self-healing: Venue has **no counterpart**. Locus detects gaps and triggers recovery (`QueueEventProjectionService.cs:489-531`).
  - Lag observability: Venue has **no counterpart**; Locus has `LagBytes`/`HasSnapshot`/`SnapshotCursorOffset` (`QueueEventProjectionService.cs:1414-1418`).
  - Counts can be reconciled: preserved on both sides, but Locus's quotas advance with the event projection (`locus/src/Locus.Storage/TenantQuotaManager.cs:237-267`), while Venue relies on startup or an explicit recompute (`venue.go:791-799`).
- **Conclusion**: durability is stronger, the rebuild capability is lost and is mitigated only by the per-tenant SQLite backup/restore path (M6, 4.1). Verified against both sides; no conflict.

### M2 · metadata write-behind queue → synchronous transactions + bounded conflict retry

- **Locus**: `AddOrUpdateAsync`/`RemoveAsync` update the in-memory cache first, then enqueue to a channel for asynchronous SQLite persistence (`locus/src/Locus.Storage/Data/MetadataRepository.cs:20-56`, `:127-166`), with the channel consumption loop at `:4215`; option defaults are `MaxQueueSize=100000`, `DrainBatchSize=2000`, `SoftMergeThresholdPercent=90`, `ShutdownDrainTimeoutSeconds=30` (`locus/src/Locus/MetadataRepositoryOptions.cs:14-83`). When the channel is full, entries merge by file key (`MetadataRepository.cs:51-53`); if the process crashes, in-memory metadata is lost, the physical files become orphans, and they are recovered as Pending on the next start (`:36-49`).
- **Venue**: `WriteFile` writes the physical file first and then synchronously calls `AddOrUpdate`; on failure `rollbackWrite` releases the physical file and the quota (`pkg/pool/storage_pool.go:227-292`, `:378-410`); lower-level concurrency conflicts are handled with bounded retries (`pkg/metadata/conflict_retry.go`).
- **Guarantee comparison**:
  - Crash semantics: Venue is **stronger**. A Locus crash can lose in-flight metadata (`MetadataRepository.cs:29`), whereas in Venue metadata and the physical file are one logical operation that rolls back on failure.
  - Write-latency observability: Locus exposes channel depth/peak/merge depth (`MetadataRepository.cs:162-165`); Venue has **no counterpart observation point**.
  - Behavior when the database is unavailable: Locus keeps writing files and retries persistence (`MetadataRepository.cs:40-43`); Venue returns an error directly and rolls back the physical file (`pkg/pool/storage_pool.go:241`, `:290-292`). This is an observable difference: the direction differs, but the guarantee is stronger (no silent backlog).
- **Conclusion**: conclusions agree (equivalent and stricter). Verified, no conflict.

### M3 · quota projection/compensation/reconciliation manager → startup recompute + write-path compensation

- **Locus**: `TenantQuotaManager` implements the projection/compensation/reconciliation interfaces at once (`locus/src/Locus.Storage/TenantQuotaManager.cs:15`); `CanAddFileAsync` reserves using `ProjectedCount + ReservationCount` (`:47-64`, `:79`); after an event lands, `ApplyAcceptedProjectionAsync` consumes the reservation and persists it (`:237-267`); the reconciliation entry point is in the cleanup service (`locus/src/Locus.Storage/StorageCleanupService.cs:1006-1049`). Totals and directory counts use the per-tenant override when present and the global value otherwise, in `GetEffectiveLimitCoreAsync` (`TenantQuotaManager.cs:374-386`).
- **Venue**: tenant and directory counts are recomputed at startup (`venue.go:399`, `:423`), and the explicit administration entry point `ReconcileQuotaCounts` is retained (`venue.go:791-799`); status records are read in pages to avoid unbounded queries (`venue.go:803-839`); write-path failures compensate by decrementing (`pkg/pool/storage_pool.go:378-410`).
- **Guarantee comparison**:
  - Counts can be reconciled: **preserved**. Both sides can recompute counts back into agreement with the metadata.
  - Correction latency: Venue is **weaker**. Locus advances the projection on every event, so drift self-heals inside the event stream; Venue corrects only at startup or on an explicit call, so runtime drift needs operator intervention.
  - Reservation semantics: Locus has a Reservation concept for concurrent over-sell protection (`TenantQuotaManager.cs:79`); Venue checks and increments with per-call `IncrementFileCount` before writing (`pkg/pool/storage_pool.go:244-250`) and introduces no separate reservation counter. **Single-sided evidence, unconfirmed** whether this produces an observable transient overshoot under concurrency (no Venue-side concurrency-limit comparison test was found).
- **Conclusion**: conclusions agree ("counts can be reconciled" preserved, event-driven correction degraded to explicit calls). Verified, no conflict.

### M4 · `CleanupFilesByStatusAsync` single pass vs per-status execution

- **Locus**: a single traversal handles timed-out and permanently failed files together (`locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:85-96`).
- **Venue**: executes per status, `CleanupTimedOutProcessingFiles` (`pkg/cleanup/cleanup_service.go:669`) and `CleanupPermanentlyFailedFiles` (`:710`), but both use paged reads (`pkg/cleanup/cleanup_service.go:222`, `pkg/core/interfaces.go:501`).
- **Guarantee comparison**: the result sets are identical; the difference is only the number of I/O operations and the transaction span of a single call. Paged reads make Venue's memory footprint **more controllable** for very large tenants (`pkg/cleanup/cleanup_paging_test.go`). Observable differences are limited to the number of calls and the shape of statistics aggregation.
- **Conclusion**: conclusions agree. Verified, no conflict.

### M5 · `IMetadataStore` / `MetadataSnapshot` → the per-tenant metadata repository

- The Locus-side types exist (`locus/src/Locus.Core/Abstractions/IMetadataStore.cs`, `locus/src/Locus.Core/Models/MetadataSnapshot.cs`), but under `locus/src/Locus.Storage` there are only adapter types such as `MetadataRepository{Projection,ProjectionMaintenance,QueueProjection*}` (directory listing of `locus/src/Locus.Storage/`), and **no implementation class of `IMetadataStore` was found**.
- Conclusion: this is not an observable gap; no counterpart implementation line could be found on the Locus side, so the item remains "single-sided evidence, unconfirmed" (the Venue side is `pkg/metadata/sqlite_repository.go:281`). See U2 for details.

### M6 · `SqliteOptions` → `SqliteConfig`, and the engine convergence

- Locus: `locus/src/Locus.Core/Models/SqliteOptions.cs:18` (WAL), `:28` (NORMAL), `:35` (cache), `:42` (busy timeout 5000), `:53` (checkpoint after batch).
- Venue: `config.SqliteConfig` (`config/config.go:91-175`: `JournalMode` `:92-95`, `SynchronousMode` `:97-100`, `CacheSizeKb` `:102-105`, `BusyTimeoutMs` `:107-108`, `CheckpointAfterBatch` `:110-112`, connection and handle limits `:114-174`) is the only engine configuration, and it drives the per-tenant databases (`venue.go:354`, `venue.go:403`) through the pure-Go `modernc.org/sqlite` driver selected by `pkg/sqlite/sqlite.go:42-53`. Its defaults mirror the Locus PRAGMA set (`config/config.go:549-569`). `config.BadgerDBConfig` and its fluent methods no longer exist.
- Guarantee comparison: Locus defaults to `synchronous=NORMAL` (tolerating roughly one second of lost commits on power loss, `locus/src/Locus.Core/Models/SqliteOptions.cs:22-24`); Venue exposes the same switch as `Sqlite.SynchronousMode`, whose default is defined by `DefaultConfig` rather than by `ApplyDefaults` (`config/config.go:553`). Both expose the durability trade-off as configuration; **single-sided evidence, unconfirmed**: there is no documentation or test assertion for how large the loss window is on the Venue side for any given value, so this document draws no conclusion.
- **Conclusion**: engine convergence, documented in `docs/sqlite-storage-design.md`. Verified, no conflict.

### 3.1 Mechanism-divergence summary (M1-M7)

| # | Locus mechanism | Venue replacement | Guarantee conclusion |
| --- | --- | --- | --- |
| M1 | Per-tenant `queue.log` plus projection plus snapshot plus compaction | Synchronous SQLite transactions plus a status index over the `files` table | Durability is **stronger** (no ACK window); the rebuild capability is **lost** and is mitigated only by the per-tenant backup/restore path; sequence gaps, corrupt-tail self-healing and lag observation have no counterpart |
| M2 | Write-behind metadata queue | Synchronous transactions plus bounded conflict retry (`pkg/metadata/conflict_retry.go:31-63`) | Equivalent and stricter (nothing queued can be lost) |
| M3 | Quota projection/compensation/reconciliation managers | Startup recompute plus `ReconcileQuotaCounts` plus write-path compensation | "Counts can be reconciled" is preserved; event-driven correction degrades into an explicit call |
| M4 | `CleanupFilesByStatusAsync` in a single pass | Each status is processed separately, with keyset paging | Same result, different number of I/O passes |
| M5 | `IMetadataStore`/`MetadataSnapshot` | The per-tenant SQLite repository | Locus v2.0.0 contains no implementing class, so this is not an observable gap (single-sided evidence, see U2) |
| M6 | `SqliteOptions` | `SqliteConfig`, the only engine configuration on the runtime path | Converged engine configuration: `config.SqliteConfig` (`config/config.go:91-175`) mirrors the Locus PRAGMA set and drives the runtime's per-tenant databases (`venue.go:354`, `venue.go:403`). Details in `docs/sqlite-storage-design.md` |
| M6a | One SQLite `metadata.db` per tenant, WAL, 8 indexes | One SQLite database per tenant for both metadata (`metadata.db`) and directory quotas (`quotas.db`), pure-Go driver with `CGO_ENABLED=0` | The engine `DIVERGENCE` is resolved: the layout converges on Locus. Details and DDL in `docs/sqlite-storage-design.md`; recoverability is the SQLite online backup path (`pkg/metadata/sqlite_backup.go`, `venue_metadata_backup.go`) |
| M7 | The immediate timed-out reclaim always runs and only the background path tests the cooldown (`locus/src/Locus.Storage/FileScheduler.cs:602-692`) | Two independent per-tenant cooldown maps in one scheduler: `reclaimDeadlines` for the empty-queue path and `backgroundReclaimDeadlines` for the opportunistic pass (`pkg/scheduler/file_scheduler.go:136-139`, `:233-234`, reserved at `:440` and `:525`), both using the same configured `TimedOutReclaimCooldown` | Deliberate `DIVERGENCE` (see 4.7): an emergency reclaim must never be suppressed by a recent opportunistic pass, so a tenant can reclaim up to twice per cooldown window instead of once. Claim/lease safety is unchanged, because both paths use the same lease-checked transition (`pkg/scheduler/file_scheduler.go:790-837`) |

---

## 4. Deliberate divergences (DIVERGENCE)

### 4.1 Storage engine: from a shared BadgerDB store to one SQLite database per tenant

- Locus: `{metadataDirectory}/{tenantId}/metadata.db` per tenant (`locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`), with a table schema containing `files` and 8 indexes (`:559-588`) and PRAGMAs including WAL/NORMAL/cache/busy_timeout (`locus/src/Locus.Core/Models/SqliteOptions.cs:88-105`).
- Venue: the storage engine is SQLite on the runtime path. `venue.NewVenue` constructs `metadata.NewSQLiteMetadataRepository` (`venue.go:354`) and `quota.NewSQLiteDirectoryQuotaRepository` (`venue.go:403`) from `config.SqliteConfig` (`config/config.go:91-175`), so the disk layout is one database per tenant like Locus: `{MetadataDirectory}/{tenantId}/metadata.db` (`pkg/metadata/sqlite_repository.go:30-33`) and `{QuotaDirectory}/{tenantId}/quotas.db` (`pkg/quota/sqlite_directory_quota_repository.go:22-26`). The BadgerDB implementations and `config.BadgerDBConfig` no longer exist in the tree.
- **Why the divergence existed**: the shared Badger store predated the migration; the design basis for the move (driver, per-tenant layout, schema, backup/restore, staged rollout, gate thresholds) is `docs/sqlite-storage-design.md`, which also registers the deliberate physical divergences such as timestamp encoding and per-tenant file layout.
- **Caller-visible impact**: the disk layout is `{metadataDirectory}/{tenantId}/...` with a real per-tenant file boundary that supports isolated backup and deletion (`pkg/metadata/sqlite_backup.go:19-62`), and space is reclaimed through WAL checkpointing plus `VACUUM` rather than a whole-store value-log GC (`pkg/metadata/sqlite_repository.go:1180-1205`, `AGENTS.md:111-112`). Backup is online and does not block readers or writers, while restore is an explicit offline step: `RestoreMetadata` writes the per-tenant databases back into an absent-or-empty metadata directory and verifies each restored file before it is published (`venue_metadata_backup.go:410-426`, `:433-467`, `:540-552`).

### 4.2 Go naming and interface style (including sentinel errors)

- Locus expresses domain failures with exception types (`TenantNotFoundException`, `FileAlreadyProcessingException`, `TenantQuotaExceededException`, and others under `locus/src/Locus.Core/Exceptions/`).
- Venue uses sentinel errors plus `errors.Is` (`pkg/core/errors.go:12`, `:26`, `:48`, `:75`, `:129`).
- **Why it is retained**: Go convention; callers decide with `errors.Is` rather than `try/catch`.
- **Caller-visible impact**: error types change from "catchable types" to "matchable values"; `ErrFileNotClaimable` (`pkg/core/errors.go:73`) and `ErrProcessingLeaseMismatch` (`:76`) are finer splits that Locus's exception model does not separate.

### 4.3 Instance-scoped logging (`pkg/logging.Runtime`) instead of a global logger

- Locus injects `ILogger<T>` through DI (for example `locus/src/Locus.Storage/Data/MetadataRepository.cs:60`, `:613`).
- Venue emits structured events through an injected instance-scoped `pkg/logging.Runtime` (`venue.go:1328`, `pkg/logging/logging.go`), and `Config.Logging == nil` means silent (`config/config.go:69`).
- **Why it is retained**: it avoids a process-global logger and satisfies the repository logging rules.
- **Caller-visible impact**: the caller owns the writer and handler behind logging; without injection there is no logging. Locus's `ILogger` is provided by the host by default.

### 4.4 `0` quota semantics

- Locus: the `DefaultTenantQuota` comment says `0 = unlimited` (`locus/src/Locus/LocusOptions.cs:105`); the limit comparison passes through directly when `effectiveLimit == 0` (`locus/src/Locus.Storage/TenantQuotaManager.cs:55-56`); in `GetEffectiveLimitCoreAsync` an override takes effect only when it is `> 0` (`:377`).
- Venue: `GetEffectiveLimit` is documented as "A zero limit means unlimited" (`pkg/quota/tenant_quota_manager.go:285-287`), `SetTenantLimit`/`SetGlobalLimit` reject negative numbers (`:299-305`, `:344-351`), and `tenantQuota.canAddFile` passes through for 0 (`:34`).
- **Caller-visible impact**: the semantics match, with no difference; the semantics are retained to avoid breaking compatibility.

### 4.5 `IsTenantEnabled` made read-only (one fewer side effect than Locus)

- Locus: `IsTenantEnabledAsync` calls `GetTenantAsync` (`locus/src/Locus.MultiTenant/TenantManager.cs:147-153`), and `GetTenantAsync` allows implicit tenant creation (`:78-82`, `:119-127`) — that is, with automatic tenant creation enabled, a mere "check" materializes a tenant on disk.
- Venue: `IsTenantEnabled` goes through `TryGetTenant` and explicitly does not write to disk (`pkg/tenant/manager.go:210-226`, `:167-173`).
- **Why it is retained**: health checks and watcher scans must not produce write side effects.
- **Caller-visible impact**: for an unknown tenant, Locus may incidentally create tenant files while Venue does not; this is an observable and stronger guarantee (`pkg/tenant/readonly_lookup_test.go` asserts that no file is written).

### 4.6 Optional capabilities as interfaces plus type assertions, rather than piling methods into the main interface

- Locus: compensation/projection/reconciliation are split into independent interfaces implemented by the same class (`locus/src/Locus.Storage/TenantQuotaManager.cs:15`), but the administration methods remain in the main interface (`locus/src/Locus.Core/Abstractions/ITenantQuotaManager.cs:52-81`).
- Venue: administration methods and optional capabilities are split further, for example `TenantQuotaAdministrator` (`pkg/core/interfaces.go:335-352`, exposed via `venue.go:1246`), `MetadataBackupService` (`:363-369`), `TenantCleanupService` (`:417-425`), `TenantOrphanRecoveryService` (`:429-437`).
- **Caller-visible impact**: optional capabilities are discoverable only through type assertion; a failed assertion silently falls back to the base path. The gaps this caused are recorded in §5.1.

### 4.7 Independent per-tenant cooldown state for the two timed-out reclaim paths

- Locus runs the immediate path unconditionally and only the background path tests the cooldown within the same scheduler (`locus/src/Locus.Storage/FileScheduler.cs:602-692`: the immediate reclaim at `:602-609` runs on every empty queue, the background pass at `:621-639` is the part gated by the cooldown).
- Venue keeps **two** per-tenant cooldown maps: `reclaimDeadlines` for the synchronous empty-queue reclaim and `backgroundReclaimDeadlines` for the opportunistic pass (`pkg/scheduler/file_scheduler.go:136-139`, `:233-234`); each path reserves its own map (`:440`, `:525`) while both use the same `TimedOutReclaimCooldown` (`:127-135`, `:459-465`).
- **Why it is retained**: an emergency reclaim must never be suppressed by a recent opportunistic pass. The caller blocked on an empty queue is the one that needs the timed-out records back immediately, so sharing one cooldown map (as the older option comment at `pkg/scheduler/file_scheduler.go:29-33` still describes) would let a background pass delay the synchronous path by up to one cooldown window while the queue appears empty.
- **Caller-visible impact**: a tenant can see an immediate reclaim shortly after a background reclaim (and vice versa), up to twice as many reclaim runs per cooldown window as a single shared map would allow; both remain bounded by the same configured cooldown, so the reclaim rate is still configurable. Nothing about claim/lease safety changes: both paths go through the same lease-checked transition (`pkg/scheduler/file_scheduler.go:790-837`).

---

## 5. Gap status: resolved and outstanding

### 5.1 Resolved gaps (G1-G8, reclassified `OBSERVABLE-GAP` → `PRESENT`)

These eight items were the "interface or configuration is in place but the runtime is not wired" set. Each one now has a runtime implementation and a wiring site; the Locus evidence is unchanged from the original finding.

| # | Former gap | Area | Locus evidence | Venue evidence that closed it |
| --- | --- | --- | --- | --- |
| G1 | `Cleanup.EmptyQueueReclaimBatchSize` had no effect: the field promised "0 selects the default, a negative value disables immediate reclaim", but the runtime overwrote it with a hardcoded scheduler default | Empty-queue reclaim bound | `locus/src/Locus/StoragePoolOptions.cs:19-24`, `locus/src/Locus.Storage/FileScheduler.cs:52`, `:602-609` | `config/config.go:451-454` (documented semantics) → `venue.go:493` (the configured value is passed through) → `pkg/scheduler/file_scheduler.go:217-219` (zero selects `DefaultEmptyQueueReclaimBatchSize`, `:56`) and `:434-436` (a non-positive value disables the synchronous pass); `config/config.go:714-716` only patches the default for a zero value. Tests: `venue_startup_recovery_test.go:150-213` (a negative value disables, a positive value keeps reclaim) |
| G2 | Low-frequency background timed-out reclaim was not wired in: the configuration fields existed but no code read them | Background timed-out reclaim | `locus/src/Locus.Storage/FileScheduler.cs:621-692`, `locus/src/Locus/StoragePoolOptions.cs:26-43` | `config/config.go:456-464`, defaults `:618-620` → `venue.go:494-495` → `pkg/scheduler/file_scheduler.go:508-539` (one pass per tenant, its own cooldown reservation, lifecycle gate) and `:571-604` (bounded by the batch size, contained failure logging). Tests: `venue_startup_recovery_test.go:219-275` (a successful claim recovers the abandoned record with the synchronous path switched off) |
| G3 | Volume `ProbeHealth` (forced probe that refreshes the cache) was not implemented | Volume health probe | `locus/src/Locus.Core/Abstractions/IStorageVolumeHealthProbe.cs:6-12`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:281` | `pkg/core/interfaces.go:376-380`; implementation `pkg/volume/local_volume.go:221-231` (probe outside the cache mutex, then one short critical section publishes the result), capability assertion `:25`; wired at `venue.go:198-202`, where `volumeProbe` prefers the forced probe over a possibly cached `IsHealthy` |
| G4 | Write-path warmup was not implemented and `WarmupOnStartup` had no consumer | Write-path warmup | `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathWarmup.cs:6-15` | `pkg/core/interfaces.go:385-390`; implementation `pkg/volume/local_volume.go:248-275` (one throwaway write through `WriteFile`, deleted on every outcome), capability assertion `:26`; wired at `venue.go:256-276`, where a failure only warns; field `config/config.go:208-211`, fluent `config/fluent.go:648` |
| G5 | Write-path diagnostics snapshot was not implemented | Write-path diagnostics | `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathDiagnostics.cs:6-14`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:333` | `pkg/core/interfaces.go:394-398`; model `pkg/core/models.go:853-882`; implementation `pkg/volume/local_volume.go:289-291` over the atomic counters in `pkg/volume/write_path_statistics.go:17-31`, capability assertion `:27`; the counters are updated by the real write path (`pkg/volume/local_volume.go:344-398`). Reachable through `Venue.Volumes()` (`venue.go:1314`) |
| G6 | Startup mount-health retry was not implemented: `InitialDelay`/`HealthCheckDelay` were validated but did not participate in startup | Startup mount-health retry | `locus/src/Locus.Storage/StoragePool.cs:164-209` (at most 10 attempts, two consecutive healthy checks required, otherwise throws `StorageVolumeUnavailableException`); `locus/src/Locus/LocusOptions.cs:190-201` (2000ms/500ms) | `venue.go:177-194` probes every configured volume immediately and never delays one that answers (`:184-188`); `venue.go:212-251` waits `InitialDelay` before the first retry (`:217`), then retries up to `maxVolumeHealthCheckAttempts = 10` (`:167`) separated by `HealthCheckDelay` (`:240`), requires two consecutive healthy probes (`:227-237`), and returns `core.ErrStorageVolumeUnavailable` when the budget is exhausted (`:246-251`). Fields `config/config.go:198-202`, `:204-206`, defaults `config/config.go:28-29`, `:827-832`; invalid negative values are rejected (`config/config.go:956`, `:959`) |
| G7 | `OptimizeDatabasesDetailed` was not implemented: no path populated the reclaimed bytes or the before/after sizes | Optimization result detail | `locus/src/Locus.Core/Models/DatabaseOptimizationResult.cs:11-41`; `locus/src/Locus.Storage/StorageCleanupService.cs:2405-2417` | `pkg/core/interfaces.go:405-409`; model and derived methods `pkg/core/models.go:813-842`; implementation `pkg/cleanup/database_optimization.go:40-86` (per-tree before/after sizes, `SpaceReclaimed` clamped at zero, an unmeasurable tree skipped without failing the pass), capability assertion `pkg/cleanup/cleanup_service.go:27`; reachable via `venue.CleanupService()` (`venue.go:1281`) and a `core.DatabaseOptimizationService` type assertion |
| G8 | Per-tenant cleanup and recovery entry points were not implemented | Per-tenant cleanup and recovery | `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:18`, `:25`, `:54` | `pkg/core/interfaces.go:417-425` + `pkg/cleanup/cleanup_service.go:340-373` (validates the tenant ID, rejects an unknown tenant read-only, sweeps only that tenant's directory); `pkg/core/interfaces.go:429-437` + `pkg/recovery/orphan_recovery_service.go:297-330` (same validation, tenant-only walk, capability assertion `:29`); construction supplies the `TenantManager` both paths verify against (`venue.go:723`) |

### 5.2 Outstanding gaps (OBSERVABLE-GAP)

| # | Gap | Area | Locus evidence | Current Venue evidence | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| G9 | `CleanupStatistics` orphan counters differ in name and semantics | Orphan counters | `locus/src/Locus.Core/Models/CleanupStatistics.cs:26` (`OrphanedFilesRemoved` counts "files"); `:31` (`OrphanedFilesRecovered` counts "recovered files") | `pkg/core/models.go:490-491` (`OrphanedMetadataRemoved` counts "metadata records"); on the recovery side `OrphanRecoveryReport.FilesRecovered` (`pkg/core/orphan.go:15`) lives in a separate model and does not enter cumulative cleanup statistics | Document the naming and counting-unit difference explicitly; if external consumers select fields by name, provide equivalent fields or migration guidance |

**Note**: G9 is the only remaining `OBSERVABLE-GAP`; every other item that this document previously classified as an observable gap is now `PRESENT` (§5.1) or a deliberate divergence (§4). The queue-projection diagnostics surface (lag/snapshot/gap/corrupt tail) has **no counterpart** in Venue: `Venue.MetadataBackupInfo` (`venue_metadata_backup.go:390-392`, model `pkg/core/models.go:790-805`) exposes the equivalent operational information of "most recent restorable point in time / number of backups / bytes", but the three projection states of queue lag, sequence gap, and corrupt tail are not observable anywhere (compare `locus/src/Locus.Storage/QueueEventProjectionService.cs:1407-1427`). That residual is recorded as U5 and is not listed as a separate acceptance gap.

---

## 6. Unconfirmed

| # | Item | Missing evidence |
| --- | --- | --- |
| U1 | Whether the absence of quota reservations causes an observable transient overshoot under concurrency, and whether Locus's write-behind timing semantics for tenant quotas produce an observable difference | Locus has `ReservationCount` (`locus/src/Locus.Storage/TenantQuotaManager.cs:79`), while Venue calls `IncrementFileCount` before writing (`pkg/pool/storage_pool.go:244-250`). **Single-sided evidence, unconfirmed**: no Venue-side comparison test for concurrent overshoot was found, and neither the Locus concurrency paths nor its write-behind quota timing were exhausted |
| U2 | Whether `IMetadataStore`/`MetadataSnapshot` really have no implementation class in Locus | Only `locus/src/Locus.Core/Abstractions/IMetadataStore.cs` and the file listing of `locus/src/Locus.Storage/` (`MetadataRepositoryProjection*` and similar) were verified; no file-by-file confirmation of an implicit implementation. **Single-sided evidence, unconfirmed** |
| U3 | Whether Locus's checkpoint+journal watcher-history mechanism has operational advantages beyond the aligned observable behavior | Venue uses a dedicated durable SQLite source-cleanup store for enabled watchers and retains atomic `imported-files.json` only as a compatibility fallback when source cleanup is disabled. Operation IDs in SQLite prevent duplicate storage after a crash, but the internal persistence mechanism remains different |
| U4 | Whether every section of `docs/sqlite-storage-design.md` still matches the landed implementation | The implementation itself was read in this revision: per-tenant `{metadataDirectory}/{tenantId}/metadata.db` and `{quotaDirectory}/{tenantId}/quotas.db` (`pkg/metadata/sqlite_repository.go:30-33`, `pkg/quota/sqlite_directory_quota_repository.go:22-26`), the pure-Go driver and DSN policy (`pkg/sqlite/sqlite.go:42-53`, `:203-231`), the `files` schema (`pkg/metadata/sqlite_repository.go:66-134`), the CAS and keyset SQL (`:826-898`, `:1038-1053`, `:719-724`), and the `VACUUM INTO` backup with quarantine (`pkg/metadata/sqlite_backup.go:87-131`, `pkg/metadata/sqlite_corruption.go:25-84`). The design document itself was only cross-checked at the sections cited here (2, 4.1, M6), so whether every remaining section still matches the code is **single-sided evidence, unconfirmed** |
| U5 | Which fields of Locus `QueueProjectionTenantState` are "must be observable" for operations | The missing Venue surface has been verified (`locus/src/Locus.Storage/QueueEventProjectionService.cs:1407-1427` vs no Venue counterpart), but whether external callers depend on those fields is unconfirmed, so it is recorded only as an M1 guarantee difference and not listed as a separate acceptance gap |

The former U6 (whether the reclaim batch-size and background-reclaim keys were ever written by an external configuration file) is closed: all three keys are wired (`venue.go:493-495`) and documented in the example file (`venue-config-example.yaml:285-293`), so no "declared but not wired" condition remains for them.

### 6.1 Documentation/behaviour mismatches still open

One finding of the documentation review remains open and is also recorded as code-adjacent debt in `docs/architecture.md`.

| Location | Description | Evidence |
| --- | --- | --- |
| `config.ApplyDefaults` | The two dead-letter booleans are not restored by `ApplyDefaults`, which fills only `RootPath` and `ShardingDepth` (`config/config.go:723-728`), while `DefaultConfig` sets both to true (`:597-602`). A configuration built from `&config.Config{}` plus `ApplyDefaults()` therefore loses the dead-letter tenant and date partitions, and the README does not warn about this pair | `config/config.go:723-728`, `:597-602`; `pkg/cleanup/dead_letter.go:59-67`; see `docs/architecture.md` §6.4 |

The remaining findings of the same review are closed: the README's reclaim text (`README.md:545-555`), its status-scan claim (`README.md:917-918`), its "not implemented" list (`README.md:939-944`; one dangling `docs/` reference in that list is recorded in `docs/architecture.md` §10.2) and its Locus-divergence list (`README.md:895-937`) now match the code; `FileStatusDeadLettered` is documented as produced by cleanup (`pkg/core/models.go:89-93`); the retention keys are present in `venue-config-example.yaml` and documented in the README; and the volume health-probe cache TTL is exposed as configuration (`config/config.go:193-196`, `pkg/volume/local_volume.go:185-206`).

---

## Appendix: evidence index for this document

- Key Locus files: `locus/src/Locus/LocusOptions.cs`, `locus/src/Locus/MetadataRepositoryOptions.cs`, `locus/src/Locus/StoragePoolOptions.cs`, `locus/src/Locus.Storage/Data/MetadataRepository.cs`, `locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs`, `locus/src/Locus.Storage/FileQueueEventJournal.cs`, `locus/src/Locus.Storage/QueueEventProjectionService.cs`, `locus/src/Locus.Storage/QueueEventJournalOptions.cs`, `locus/src/Locus.Storage/FileScheduler.cs`, `locus/src/Locus.Storage/FileWatcher.cs`, `locus/src/Locus.Storage/FileWatcherOptionsManager.cs`, `locus/src/Locus.Storage/FileWatcherAutoManager.cs`, `locus/src/Locus.Storage/StorageCleanupService.cs`, `locus/src/Locus.Storage/BackgroundCleanupService.cs`, `locus/src/Locus.Storage/TenantQuotaManager.cs`, `locus/src/Locus.Storage/Statistics/LocusStatisticsOutputService.cs`, `locus/src/Locus.MultiTenant/TenantManager.cs`, `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs`, `locus/src/Locus.Core/Models/*.cs`, `locus/src/Locus.Core/Abstractions/IStorageVolume*.cs`.
- Key Venue files: `venue.go`, `venue_metadata_backup.go`, `config/config.go`, `config/fluent.go`, `pkg/core/models.go`, `pkg/core/interfaces.go`, `pkg/core/errors.go`, `pkg/core/orphan.go`, `pkg/sqlite/sqlite.go`, `pkg/metadata/sqlite_repository.go`, `pkg/metadata/sqlite_backup.go`, `pkg/metadata/sqlite_corruption.go`, `pkg/metadata/sqlite_paging.go`, `pkg/metadata/metadata_support.go`, `pkg/metadata/conflict_retry.go`, `pkg/quota/sqlite_directory_quota_repository.go`, `pkg/pool/storage_pool.go`, `pkg/pool/volume_selector.go`, `pkg/scheduler/file_scheduler.go`, `pkg/quota/tenant_quota_manager.go`, `pkg/tenant/manager.go`, `pkg/volume/local_volume.go`, `pkg/health/database_health_checker.go`, `pkg/cleanup/cleanup_service.go`, `pkg/cleanup/dead_letter.go`, `pkg/cleanup/cleanup_statistics.go`, `pkg/cleanup/junk_files.go`, `pkg/cleanup/invalid_database_backups.go`, `pkg/watcher/file_watcher.go`, `pkg/watcher/source_cleanup_store.go`, `pkg/watcher/source_cleanup_worker.go`, `pkg/watcher/file_watcher_auto_manager.go`, `pkg/watcher/background_file_watcher_service.go`, `pkg/statistics/recorder.go`, `pkg/recovery/orphan_recovery_service.go`.
- Migration design document: `docs/sqlite-storage-design.md`.
