# Venue and Locus Alignment Document

> This document compares Locus `v2.0.0` with the current Venue working tree, capability domain by capability domain. Every conclusion
> carries two-sided `file:line` evidence. Items with evidence on only one side are always explicitly marked "single-sided evidence, unconfirmed".

## 1. Baseline and method

### 1.1 Baseline

| Item | Value |
| --- | --- |
| Behavioral baseline | Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4` |
| Locus read-only export | `locus/` (the reference checkout for Locus v2.0.0 is not part of this repository; `locus/src/Locus`, `locus/src/Locus.Core`, `locus/src/Locus.MultiTenant`, `locus/src/Locus.FileSystem`, `locus/src/Locus.Storage`) |
| Venue counterpart | Current working tree, HEAD `d497100e725ab9adea695debafb782313ac97600` (`git status` shows a large number of uncommitted modifications; the conclusions here follow the code on disk, not HEAD) |
| Existing gap list | `docs/locus-feature-gaps.md` (used here only as cross-check input; not modified) |

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

### 1.4 Baseline conflicts with `docs/locus-feature-gaps.md` (this document corrects them from code evidence)

1. **Export path**: the gap list states the baseline is `tmp/locus-v2` (`docs/locus-feature-gaps.md:5`), while the actual export directory is the Locus reference checkout. This document cites the reference checkout as `locus/src/...` and never uses that recorded export path; the string is quoted only to document the stale baseline.
2. **Counterpart HEAD**: the gap list states the counterpart is `HEAD 06ee97d + fix rounds` (`docs/locus-feature-gaps.md:6`), while the current HEAD is `d497100e`. Everything has been re-verified against the current working tree.
3. **`docs/sqlite-storage-design.md` now exists**: the storage-engine item is meant to point at `docs/sqlite-storage-design.md`, and that migration design document is now present in `docs/` (deleted earlier, now restored/added). The storage-engine divergence, including the planned migration to SQLite, is therefore tracked there, with driver selection, schema, and migration steps specified. See 4.1 and overview row 1.
4. **The gap list's W1/W2/W3/W4 statuses match the code, but its W1.3/W1.4 text (`docs/locus-feature-gaps.md:29-30`) is stale** where it says "heuristic only" and "a missing volume is always skipped": current Venue has retired-volume disposition (`config/config.go:436-442`, `pkg/cleanup/dead_letter.go:271`) and sharding-depth protection (`pkg/volume/local_volume.go:146`), so code evidence takes precedence.

---

## 2. Overview matrix

| # | Capability domain | Classification | Locus evidence | Venue evidence | One-line conclusion |
| --- | --- | --- | --- | --- | --- |
| 1 | Storage engine and metadata model | `MECHANISM` (current state) + engine `DIVERGENCE` | `locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`, `:559-588`, `locus/src/Locus.Core/Models/SqliteOptions.cs:18-42` | `pkg/metadata/badger_repository.go:139`, `:1447`, `config/config.go:84-90` | Locus uses one SQLite `metadata.db` per tenant; Venue today is a single shared BadgerDB directory; this is the largest difference and the migration to SQLite is now tracked in `docs/sqlite-storage-design.md` (see 1.4 and 4.1) |
| 2 | Queue table and projection mechanism | `MECHANISM` | `locus/src/Locus.Storage/FileQueueEventJournal.cs:1085-1090`, `locus/src/Locus.Storage/QueueEventProjectionService.cs:211-296`, `:1407-1427` | `pkg/metadata/badger_repository.go:864-933`, `:1510-1521`, `pkg/scheduler/file_scheduler.go:393-406` | Locus uses an append-only `queue.log` plus projection, snapshot, and compaction; Venue uses synchronous transactions plus a status index, with no journal, no sequence-gap self-healing, and no projection-lag observability |
| 3 | Metadata write path | `MECHANISM` | `locus/src/Locus.Storage/Data/MetadataRepository.cs:20-56`, `:127-166`, `locus/src/Locus/MetadataRepositoryOptions.cs:45-83` | `pkg/pool/storage_pool.go:227-292`, `pkg/metadata/badger_repository.go:479`, `pkg/metadata/conflict_retry.go` | Locus queues write-behind persistence (a crash loses in-flight metadata, which then becomes orphan recovery); Venue uses synchronous transactions plus physical rollback, so durability is stronger |
| 4 | Quota (projection/compensation/reconciliation vs recompute/compensation) | `MECHANISM` | `locus/src/Locus.Storage/TenantQuotaManager.cs:47-64`, `:237-267`, `:374-386`; `locus/src/Locus.Storage/StorageCleanupService.cs:1006-1049` | `venue.go:600-615`, `:657-675`, `pkg/pool/storage_pool.go:378-410`, `pkg/quota/tenant_quota_manager.go:288-356` | "Counts can be reconciled" is preserved; Locus self-corrects from events, while Venue needs a startup recompute or an explicit `ReconcileQuotaCounts` |
| 5 | Cleanup and data retention | Mixed (W1 sub-items `PRESENT`, see 2.1) | `locus/src/Locus.Storage/BackgroundCleanupService.cs:262-294`, `:367-448`; `locus/src/Locus.Storage/StorageCleanupService.cs:1598-1649`, `:2233-2271`, `:2390-2401` | `pkg/cleanup/dead_letter.go:29-53`, `pkg/cleanup/junk_files.go`, `pkg/cleanup/invalid_database_backups.go`, `pkg/cleanup/cleanup_statistics.go:18-69` | All six W1 items have landed; the `CleanupStatistics` field set is wider than Locus, but the orphan counters differ in name and semantics |
| 6 | Timed-out reclaim | Immediate `PRESENT` / background `OBSERVABLE-GAP` | `locus/src/Locus.Storage/FileScheduler.cs:602-692`, `:621-639`; `locus/src/Locus/StoragePoolOptions.cs:24-43` | `pkg/scheduler/file_scheduler.go:327-391`, `:503-513`, `venue.go:328-341`, `config/config.go:404-412` | Immediate reclaim on an empty queue is equivalent; the low-frequency background reclaim exists in Venue only as configuration fields and is not wired into the runtime |
| 7 | Watcher | `PRESENT` | `locus/src/Locus.Storage/FileWatcherOptionsManager.cs:51-192`, `locus/src/Locus.Storage/FileWatcherAutoManager.cs:63-192`, `locus/src/Locus.Core/Models/FileWatcherConfiguration.cs:43`, `:110-141` | `pkg/watcher/background_file_watcher_service.go:206-551`, `pkg/watcher/file_watcher_auto_manager.go:135-386`, `pkg/watcher/file_watcher.go:247-443`, `config/config.go:188-285` | Global option persistence, root-configuration derivation, `UpdateWatcher`, `GetWatchersForTenant`, and the advanced knobs are all aligned |
| 8 | Runtime statistics | `PRESENT` | `locus/src/Locus.Core/Abstractions/ILocusStatisticsRecorder.cs`, `ILocusStatisticsReader.cs:13`, `locus/src/Locus.Core/Models/LocusStatisticsOptions.cs:13-43`, `locus/src/Locus.Storage/Statistics/LocusStatisticsOutputService.cs:43` | `pkg/statistics/recorder.go:63-166`, `:284`, `:370-429`, `:689-706`, `venue.go:977-995` | recorder/reader/window aggregation/dimension filtering/bounded series/optional periodic output are all present, and defaults and bounds match Locus |
| 9 | Database health and corruption recovery | `MECHANISM` | `locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs:142-186`, `:533-563`, `locus/src/Locus/LocusOptions.cs:76-83` | `pkg/metadata/corrupted_database.go:86-129`, `pkg/metadata/backup.go`, `venue_metadata_backup.go:28-107`, `venue.go:53` | Locus prefers rebuilding from the queue journal and then scans physical files; Venue uses quarantine-and-rebuild plus backup-snapshot restore plus orphan scanning, degrading recovery granularity from per-event to the backup period |
| 10 | Volume capabilities (probe/warmup/diagnostics) | `OBSERVABLE-GAP` | `locus/src/Locus.Core/Abstractions/IStorageVolumeHealthProbe.cs:11`, `IStorageVolumeWritePathWarmup.cs:14`, `IStorageVolumeWritePathDiagnostics.cs:13`, `IStorageVolume.cs:41`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:281`, `:333` | `pkg/core/interfaces.go:376-398`, `pkg/volume/local_volume.go:22-25`, `:146`, `config/config.go:156-159` | Venue has only the interface declarations and configuration fields; the local volume does not implement probe/warmup/diagnostics, and `WarmupOnStartup` is read by no code |
| 11 | Tenant management and read-only lookup | `PRESENT` (side-effect difference in 4.5) | `locus/src/Locus.Core/Abstractions/ITenantManager.cs:26`, `locus/src/Locus.MultiTenant/TenantManager.cs:85-88`, `:147-157` | `pkg/tenant/manager.go:174-226`, `pkg/core/interfaces.go:121` | Both sides have a read-only `TryGetTenant`; Venue's `IsTenantEnabled` is read-only and thus has one fewer side effect (no implicit tenant creation) than Locus |
| 12 | Quota administration API | `PRESENT` | `locus/src/Locus.Core/Abstractions/ITenantQuotaManager.cs:52-81` | `pkg/quota/tenant_quota_manager.go:288-356`, `pkg/core/interfaces.go:335-352`, `venue.go:967` | All five administration methods are present, and the `0 = unlimited` semantics match on both sides |
| 13 | Public return models | `PRESENT` (`DatabaseOptimizationResult` detail is `OBSERVABLE-GAP`) | `locus/src/Locus.Core/Models/FileInfo.cs:14-39`, `DatabaseOptimizationResult.cs:11-41` | `pkg/core/models.go:193-211`, `:780-815`, `:464-501` | `FileInfo` fields are aligned; the `DatabaseOptimizationResult` model exists but no implementation populates it |
| 14 | Configuration model and lifecycle | `PRESENT` + naming `DIVERGENCE` | `locus/src/Locus/LocusOptions.cs:11-153`, `:159-241` | `config/config.go:42-68`, `:479-598`, `venue.go:75`, `:751-800`, `:808` | Every Locus configuration section has a corresponding Go structure; construction/start/stop semantics differ but are equivalent |

**Newly fixed item — driver constraint for the SQLite migration**: the SQLite migration must use a **pure-Go driver** (no cgo), and every repository gate must pass with `CGO_ENABLED=0`, so this alignment must not depend on a cgo-based SQLite binding. The adopted driver is `modernc.org/sqlite` (`docs/sqlite-storage-design.md:63`, `:267`) precisely because it builds with `CGO_ENABLED=0` (`docs/sqlite-storage-design.md:287`); the cgo-based `github.com/mattn/go-sqlite3` is explicitly rejected for this reason (`docs/sqlite-storage-design.md:297`). The gates themselves are the required build/test/race/vet/lint commands listed in `AGENTS.md`.

### 2.1 W1 sub-item verdicts

| W1 sub-item | Classification | Locus evidence | Venue evidence |
| --- | --- | --- | --- |
| W1.1 Permanent-failure disposition and dead letter | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:367-383`, `:421-446`; `locus/src/Locus.Storage/StorageCleanupService.cs:1598-1649`, `:2233-2271` | `pkg/core/models.go:89-93` (`DeadLettered=7`), `:328-375` (`Keep=0`/`MoveToDeadLetter=1`/`Delete=2`, default MoveToDeadLetter), `:477`; `pkg/cleanup/dead_letter.go:15`, `:29-53`, `:78-123`, `:290` |
| W1.2 Junk-file cleanup | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:262`, `:269`; `locus/src/Locus.Storage/StorageCleanupService.cs:73-75` | `pkg/cleanup/junk_files.go`, `config/config.go:392-393` |
| W1.3 Invalid database-backup cleanup | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:294`; `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:121` | `pkg/cleanup/invalid_database_backups.go:44`, `:73`, `pkg/core/interfaces.go:464` |
| W1.4 Retired-volume disposition | `PRESENT` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:388-416`; `locus/src/Locus.Storage/StorageCleanupService.cs:2047-2051` | `config/config.go:436-442`, `pkg/cleanup/dead_letter.go:227`, `:271` |
| W1.5 Cumulative cleanup statistics | `PRESENT` | `locus/src/Locus.Storage/StorageCleanupService.cs:2390-2401` | `pkg/cleanup/cleanup_statistics.go:18-69`, `pkg/core/interfaces.go:469-471` |
| W1.6 Exact sharded-directory protection | `PRESENT` | `locus/src/Locus.Core/Abstractions/IStorageVolume.cs:41` | `pkg/volume/local_volume.go:141-148`, `pkg/core/interfaces.go:291-296` (`ShardingDepthProvider`), `pkg/cleanup/cleanup_service.go:476-520` |

---

## 3. Mechanism differences and guarantee comparison

### M1 · per-tenant `queue.log` + projection + snapshot + compaction → synchronous Badger transactions + status index

- **Locus**: one `queue.log` per tenant (`locus/src/Locus.Storage/FileQueueEventJournal.cs:1085`) plus `queue.state.json` (`:1090`); the default is `AckMode = Durable` (`locus/src/Locus.Storage/QueueEventJournalOptions.cs:81`), records are encoded as binary v1 (`:48`); the projection service offers `GetTenantStateAsync`/`ReplayTenantAsync`/`SnapshotTenantAsync`/`RebuildTenantAsync` (`locus/src/Locus.Storage/QueueEventProjectionService.cs:211-296`), with automatic snapshotting every 15 minutes or per 1 MB (`QueueEventJournalOptions.cs:131`, `:137`), a compaction threshold of 4 MB (`:151`), and exposed lag/gap/corrupt-tail state (`QueueEventProjectionService.cs:1407-1427`).
- **Venue**: state is the data; `AddOrUpdate`/`UpdateStatus`/`CompareAndTransitionToProcessing` all complete inside a Badger transaction (`pkg/metadata/badger_repository.go:479`, `:936`, `:1030`), Pending order is guaranteed FIFO by the status-index key (availability time + arrival order) (`pkg/metadata/badger_repository.go:1510-1521`, `:885-887`), and claiming is in `pkg/scheduler/file_scheduler.go:393-406`.
- **Guarantee comparison**:
  - Durability: Venue is **stronger**. Locus's default `AckMode = Durable` (`locus/src/Locus.Storage/QueueEventJournalOptions.cs:79-81`) already fsyncs, but the optional `Balanced`/`Async` defer the flush (`locus/src/Locus.Storage/QueueEventJournalAckMode.cs:13-22`), leaving a window of unflushed appends; Venue commits a transaction on every state change.
  - Rebuild capability: Venue is **weaker**. Locus can replay the journal event by event to rebuild metadata and quotas (`locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs:533-563`); Venue has no journal, only backup-snapshot restore (`venue_metadata_backup.go:20-23` states explicitly "backup-period recoverability, not Locus's per-event queue journal").
  - Sequence-gap self-healing: Venue has **no counterpart**. Locus detects gaps and triggers recovery (`QueueEventProjectionService.cs:489-531`).
  - Lag observability: Venue has **no counterpart**; Locus has `LagBytes`/`HasSnapshot`/`SnapshotCursorOffset` (`QueueEventProjectionService.cs:1414-1418`).
  - Counts can be reconciled: preserved on both sides, but Locus's quotas advance with the event projection (`locus/src/Locus.Storage/TenantQuotaManager.cs:237-267`), while Venue relies on startup or an explicit recompute (`venue.go:600-615`).
- **Corresponds to `docs/locus-feature-gaps.md` M1**: conclusions agree (stronger durability, lost rebuild capability, W4-B adds recoverability). Verified, no conflict.

### M2 · metadata write-behind queue → synchronous transactions + bounded conflict retry

- **Locus**: `AddOrUpdateAsync`/`RemoveAsync` update the in-memory cache first, then enqueue to a channel for asynchronous SQLite persistence (`locus/src/Locus.Storage/Data/MetadataRepository.cs:20-56`, `:127-166`), with the channel consumption loop at `:4215`; option defaults are `MaxQueueSize=100000`, `DrainBatchSize=2000`, `SoftMergeThresholdPercent=90`, `ShutdownDrainTimeoutSeconds=30` (`locus/src/Locus/MetadataRepositoryOptions.cs:14-83`). When the channel is full, entries merge by file key (`MetadataRepository.cs:51-53`); if the process crashes, in-memory metadata is lost, the physical files become orphans, and they are recovered as Pending on the next start (`:36-49`).
- **Venue**: `WriteFile` writes the physical file first and then synchronously calls `AddOrUpdate`; on failure `rollbackWrite` releases the physical file and the quota (`pkg/pool/storage_pool.go:227-292`, `:378-410`); lower-level concurrency conflicts are handled with bounded retries (`pkg/metadata/conflict_retry.go`).
- **Guarantee comparison**:
  - Crash semantics: Venue is **stronger**. A Locus crash can lose in-flight metadata (`MetadataRepository.cs:29`), whereas in Venue metadata and the physical file are one logical operation that rolls back on failure.
  - Write-latency observability: Locus exposes channel depth/peak/merge depth (`MetadataRepository.cs:162-165`); Venue has **no counterpart observation point**.
  - Behavior when the database is unavailable: Locus keeps writing files and retries persistence (`MetadataRepository.cs:40-43`); Venue returns an error directly and rolls back the physical file (`pkg/pool/storage_pool.go:241`, `:290-292`). This is an observable difference: the direction differs, but the guarantee is stronger (no silent backlog).
- **Corresponds to M2**: conclusions agree (equivalent and stricter). Verified, no conflict.

### M3 · quota projection/compensation/reconciliation manager → startup recompute + write-path compensation

- **Locus**: `TenantQuotaManager` implements the projection/compensation/reconciliation interfaces at once (`locus/src/Locus.Storage/TenantQuotaManager.cs:15`); `CanAddFileAsync` reserves using `ProjectedCount + ReservationCount` (`:47-64`, `:79`); after an event lands, `ApplyAcceptedProjectionAsync` consumes the reservation and persists it (`:237-267`); the reconciliation entry point is in the cleanup service (`locus/src/Locus.Storage/StorageCleanupService.cs:1006-1049`). Totals and directory counts use the per-tenant override when present and the global value otherwise, in `GetEffectiveLimitCoreAsync` (`TenantQuotaManager.cs:374-386`).
- **Venue**: tenant and directory counts are recomputed at startup (`venue.go:262`, `:288`), and the explicit administration entry point `ReconcileQuotaCounts` is retained (`venue.go:600-615`); status records are read in pages to avoid unbounded queries (`venue.go:619-655`); write-path failures compensate by decrementing (`pkg/pool/storage_pool.go:378-410`).
- **Guarantee comparison**:
  - Counts can be reconciled: **preserved**. Both sides can recompute counts back into agreement with the metadata.
  - Correction latency: Venue is **weaker**. Locus advances the projection on every event, so drift self-heals inside the event stream; Venue corrects only at startup or on an explicit call, so runtime drift needs operator intervention.
  - Reservation semantics: Locus has a Reservation concept for concurrent over-sell protection (`TenantQuotaManager.cs:79`); Venue checks and increments with per-call `IncrementFileCount` before writing (`pkg/pool/storage_pool.go:244-250`) and introduces no separate reservation counter. **Single-sided evidence, unconfirmed** whether this produces an observable transient overshoot under concurrency (no Venue-side concurrency-limit comparison test was found).
- **Corresponds to M3**: conclusions agree ("counts can be reconciled" preserved, event-driven correction degraded to explicit calls). Verified, no conflict.

### M4 · `CleanupFilesByStatusAsync` single pass vs per-status execution

- **Locus**: a single traversal handles timed-out and permanently failed files together (`locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:85-96`).
- **Venue**: executes per status, `CleanupTimedOutProcessingFiles` (`pkg/cleanup/cleanup_service.go:536`) and `CleanupPermanentlyFailedFiles` (`:577`), but both use paged reads (`pkg/cleanup/cleanup_service.go:214`, `pkg/core/interfaces.go:501`).
- **Guarantee comparison**: the result sets are identical; the difference is only the number of I/O operations and the transaction span of a single call. Paged reads make Venue's memory footprint **more controllable** for very large tenants (`pkg/cleanup/cleanup_paging_test.go`). Observable differences are limited to the number of calls and the shape of statistics aggregation.
- **Corresponds to M4**: conclusions agree. Verified, no conflict.

### M5 · `IMetadataStore` / `MetadataSnapshot` → Badger repository

- The Locus-side types exist (`locus/src/Locus.Core/Abstractions/IMetadataStore.cs`, `locus/src/Locus.Core/Models/MetadataSnapshot.cs`), but under `locus/src/Locus.Storage` there are only adapter types such as `MetadataRepository{Projection,ProjectionMaintenance,QueueProjection*}` (directory listing of `locus/src/Locus.Storage/`), and **no implementation class of `IMetadataStore` was found**.
- Conclusion: this is not an observable gap; no counterpart implementation line could be found on the Locus side this round, so the item remains "single-sided evidence, unconfirmed" (the Venue side is `pkg/metadata/badger_repository.go`). See U2 for details.
- **Corresponds to M5**: conclusions agree. Verified, no conflict.

### M6 · `SqliteOptions` → `BadgerDBConfig`

- Locus: `locus/src/Locus.Core/Models/SqliteOptions.cs:18` (WAL), `:28` (NORMAL), `:35` (cache), `:42` (busy timeout 5000), `:53` (checkpoint after batch).
- Venue: `config/config.go:84-90` (`GCInterval`/`GCDiscardRatio`/`MemTableSize`/`ValueLogFileSize`/`BlockCacheSize`/`SyncWrites`).
- Guarantee comparison: Locus defaults to `synchronous=NORMAL` (tolerating roughly one second of lost commits on power loss, `SqliteOptions.cs:22-24`); Venue's `SyncWrites` is an explicit switch (`config/config.go:90`), and `DefaultConfig` does not set it to true (`config/config.go:497-512`), so the zero value is false by default. Both expose the durability trade-off as configuration; **single-sided evidence, unconfirmed**: there is no documentation or test assertion for how large the loss window is on the Venue side when `SyncWrites=false`, so this document draws no conclusion.
- **Corresponds to M6**: conclusions agree (engine difference, already documented in the README). Verified, no conflict.

---

## 4. Deliberate divergences (DIVERGENCE)

### 4.1 Storage engine: single shared BadgerDB (current state) → planned migration to SQLite

- Locus: `{metadataDirectory}/{tenantId}/metadata.db` per tenant (`locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`), with a table schema containing `files` and 8 indexes (`:559-588`) and PRAGMAs including WAL/NORMAL/cache/busy_timeout (`locus/src/Locus.Core/Models/SqliteOptions.cs:88-105`).
- Venue: a single BadgerDB directory whose keys are composed of `(tenantID, fileKey)` (`pkg/metadata/badger_repository.go:1447`, `:1532`), with status-index prefixes sharded by tenant and status (`:1510-1521`); configuration is in `config/config.go:84-90`. The runtime is therefore still one shared store for all tenants, while the target is one SQLite database per tenant (`{metadataDirectory}/{tenantId}/metadata.db`), which is what Locus does.
- **Why it is retained**: the current state is the Badger engine; the migration to SQLite is planned and is now tracked in `docs/sqlite-storage-design.md`, which is the single design basis for the migration (target driver, per-tenant layout, schema, backup/restore, staged rollout, and gate thresholds). That document also registers the deliberate physical divergences of the migration, such as timestamp physical encoding and per-tenant file layout (`docs/sqlite-storage-design.md` sections 16/13 and 6.5).
- **Caller-visible impact**: the disk layout is no longer "one `.db` per tenant"; there is no ready file boundary for per-tenant isolated backup or deletion; tenant-level VACUUM-style optimization degrades to whole-database Badger GC (`pkg/metadata/badger_repository.go:1338`, `:1461-1484`).

### 4.2 Go naming and interface style (including sentinel errors)

- Locus expresses domain failures with exception types (`TenantNotFoundException`, `FileAlreadyProcessingException`, `TenantQuotaExceededException`, and others under `locus/src/Locus.Core/Exceptions/`).
- Venue uses sentinel errors plus `errors.Is` (`pkg/core/errors.go:12`, `:26`, `:48`, `:75`, `:129`).
- **Why it is retained**: Go convention; callers decide with `errors.Is` rather than `try/catch`.
- **Caller-visible impact**: error types change from "catchable types" to "matchable values"; `ErrFileNotClaimable` (`pkg/core/errors.go:73`) and `ErrProcessingLeaseMismatch` (`:76`) are finer splits that Locus's exception model does not separate.

### 4.3 Instance-scoped logging (`pkg/logging.Runtime`) instead of a global logger

- Locus injects `ILogger<T>` through DI (for example `locus/src/Locus.Storage/Data/MetadataRepository.cs:60`, `:613`).
- Venue emits structured events through an injected instance-scoped `pkg/logging.Runtime` (`venue.go:1049`, `pkg/logging/logging.go`), and `Config.Logging == nil` means silent (`config/config.go:68`).
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
- Venue: administration methods and optional capabilities are split further, for example `TenantQuotaAdministrator` (`pkg/core/interfaces.go:335-352`, exposed via `venue.go:967`), `MetadataBackupService` (`:363-369`), `TenantCleanupService` (`:417-425`), `TenantOrphanRecoveryService` (`:429-437`).
- **Caller-visible impact**: optional capabilities are discoverable only through type assertion; a failed assertion silently falls back to the base path. **This is the cause of several gaps in section 5.**

---

## 5. Outstanding gaps (OBSERVABLE-GAP)

| # | Gap | Area | Locus evidence | Current Venue evidence | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| G1 | `Cleanup.EmptyQueueReclaimBatchSize` has no effect: the field's documentation in `CleanupConfig` promises "0 selects the default, a negative value disables immediate reclaim", but at runtime it is overwritten by a hardcoded scheduler default | W5.2 | `locus/src/Locus/StoragePoolOptions.cs:19-24`, `locus/src/Locus.Storage/FileScheduler.cs:52`, `:602-609` | `config/config.go:399-402` (the promised semantics) vs `venue.go:338-340` (writes `scheduler.DefaultEmptyQueueReclaimBatchSize` directly, and the comment admits there is "no configuration field"); the default-patching logic in `config/config.go:672-674` therefore has no runtime effect | Configure a non-default batch size and assert that the immediate reclaim ceiling changes accordingly; a negative value disables immediate reclaim |
| G2 | Low-frequency background timed-out reclaim is not wired in: the configuration fields exist but no code reads them, and Locus's `EnableBackgroundTimedOutReclaim` + `BackgroundTimedOutReclaimBatchSize` + cooldown have no runtime counterpart in Venue | W5.2 | `locus/src/Locus.Storage/FileScheduler.cs:621-692`, `locus/src/Locus/StoragePoolOptions.cs:26-43` | `config/config.go:404-412`, `:562-563` (fields and defaults exist), `config/fluent.go:631`, `:638`; a whole-repository grep hits only config and config tests, with no reader in scheduler/pool | After a successful claim, reclaim in the background by batch size; the cooldown takes effect; nothing triggers after the switch is turned off; files are still reclaimed even when the cleanup interval is very long |
| G3 | Volume `ProbeHealth` (forced probe that refreshes the cache) is not implemented | W5.7 | `locus/src/Locus.Core/Abstractions/IStorageVolumeHealthProbe.cs:6-12`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:281` | The interface is declared at `pkg/core/interfaces.go:376-380`; the capability assertion list in `pkg/volume/local_volume.go:22-25` does not include it, and the probe function is the private `performHealthProbe` (`:178-196`), with only the cached `IsHealthy` (`:155-176`) on the public surface | A forced probe bypasses and refreshes the cache; concurrent calls under `-race` share a single probe |
| G4 | Write-path warmup is not implemented and the `WarmupOnStartup` configuration field has no consumer | W5.7 | `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathWarmup.cs:6-15` | The interface is declared at `pkg/core/interfaces.go:382-390`; `config/config.go:156-159` and `config/fluent.go:620-623` have the field and fluent method; there is no read point for `WarmupOnStartup` anywhere in the repository, and `venue.go:303-310` does not pass it when constructing volumes | Perform one discarded write at startup for each volume that passed the health check; a failure only warns and does not disable the volume |
| G5 | Write-path diagnostics snapshot is not implemented | W5.7 | `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathDiagnostics.cs:6-14`; `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:333` | The interface is declared at `pkg/core/interfaces.go:392-398` and the model at `pkg/core/models.go:817-826`; there is no implementation and no call site | The snapshot is reachable and returns a copy; counters grow with the real write path |
| G6 | Startup mount-health retry is not implemented: `InitialDelay`/`HealthCheckDelay` are validated but do not participate in startup | W5.1 | `locus/src/Locus.Storage/StoragePool.cs:164-209` (at most 10 attempts, two consecutive healthy checks required, otherwise throws `StorageVolumeUnavailableException`); `locus/src/Locus/LocusOptions.cs:190-201` (2000ms/500ms) | `config/config.go:146-154`, `:770-774` (defaults 2s/500ms), `:898-902` (validated non-negative); `venue.go:295-319` writes volumes straight into the map after construction, with no health check or wait; at runtime `IsHealthy` is called only during volume selection (`pkg/pool/volume_selector.go:68`, `:125`, `:161`) | Wait/retry at startup; a persistently unhealthy volume fails startup with a stable error; healthy volumes are not needlessly delayed |
| G7 | `OptimizeDatabasesDetailed` is not implemented: no path populates the optimization result detail (bytes reclaimed, before/after size) | W5.6 | `locus/src/Locus.Core/Models/DatabaseOptimizationResult.cs:11-41`; `locus/src/Locus.Storage/StorageCleanupService.cs:2405-2417` | `pkg/core/interfaces.go:400-409` declares the interface; `pkg/core/models.go:786-815` defines the model and derived methods; `OptimizeDatabases` in `pkg/cleanup/cleanup_service.go:877-894` returns only `CleanupStatistics`, and there is no `DatabaseOptimizationResult` construction site anywhere in the repository | `SpaceReclaimed`/`SizeBefore`/`SizeAfter` are readable after optimization, reachable via a type assertion on `Venue.CleanupService()` |
| G8 | Per-tenant cleanup entry points are not implemented | W5.9 | `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:18`, `:25`, `:54` | `pkg/core/interfaces.go:417-425` (`TenantCleanupService`) and `:429-437` (`TenantOrphanRecoveryService`) are declarations only; `pkg/cleanup/cleanup_service.go` has no `CleanupEmptyDirectoriesForTenant`, and `pkg/recovery/orphan_recovery_service.go` has no `RecoverOrphanedFilesForTenant` | Single-tenant cleanup and recovery are usable and do not affect other tenants; an unknown tenant returns `ErrTenantNotFound` |
| G9 | `CleanupStatistics` orphan counters differ in name and semantics | W1 (close-out) | `locus/src/Locus.Core/Models/CleanupStatistics.cs:26` (`OrphanedFilesRemoved` counts "files"); `:31` (`OrphanedFilesRecovered` counts "recovered files") | `pkg/core/models.go:490-491` (`OrphanedMetadataRemoved` counts "metadata records"); on the recovery side `OrphanRecoveryReport.FilesRecovered` (`pkg/core/orphan.go:15`) lives in a separate model and does not enter cumulative cleanup statistics | Document the naming and counting-unit difference explicitly; if external consumers select fields by name, provide equivalent fields or migration guidance |

**Note**: of the nine items above, G1–G8 belong to "interface/configuration is in place but the runtime is not wired", and G9 is a semantic naming difference. W5.12 in `docs/locus-feature-gaps.md:77` (the queue-projection diagnostics surface: lag/snapshot/gap/corrupt tail) is **only partially replaced** under option B (backup/restore): `venue_metadata_backup.go:44-51` (`MetadataBackupInfo`, model `pkg/core/models.go:763-777`) provides the equivalent operational information of "most recent restorable point in time / number of backups / bytes", but the three projection states of queue lag, sequence gap, and corrupt tail have no counterpart observation surface in Venue (compare `locus/src/Locus.Storage/QueueEventProjectionService.cs:1407-1427`). That residual is recorded as U5 and is not listed as a separate acceptance gap.

---

## 6. Unconfirmed

| # | Item | Missing evidence |
| --- | --- | --- |
| U1 | Whether the absence of quota reservations causes an observable transient overshoot under concurrency | Locus has `ReservationCount` (`locus/src/Locus.Storage/TenantQuotaManager.cs:79`), while Venue calls `IncrementFileCount` before writing (`pkg/pool/storage_pool.go:244-250`). **Single-sided evidence, unconfirmed**: no Venue-side comparison test for concurrent overshoot was found, and the Locus concurrency paths were not exhausted |
| U2 | Whether `IMetadataStore`/`MetadataSnapshot` really have no implementation class in Locus | Only `locus/src/Locus.Core/Abstractions/IMetadataStore.cs` and the file listing of `locus/src/Locus.Storage/` (`MetadataRepositoryProjection*` and similar) were verified; no file-by-file confirmation of an implicit implementation. **Single-sided evidence, unconfirmed** |
| U3 | Whether the Locus `FileWatcher.cs` (`locus/src/Locus.Storage/FileWatcher.cs`, 85 KB) import pipeline still contains behavior that Venue does not cover | This round verified only the Venue-side `discoverFiles`/`importFile`/`reserveImportSlot` structure and the Locus option/manager contracts, without comparing the 1764-line scan loop section by section |
| U4 | The planned content of `docs/sqlite-storage-design.md` for the post-migration state | The document now exists in the repository and describes the planned migration to SQLite (per-tenant `metadata.db`, pure-Go driver, staged rollout). Whether its content is fully reflected in this alignment document is **single-sided evidence, unconfirmed**: only the sections cited in 1.4, 2, and 4.1 were cross-checked, not the whole design document |
| U5 | Which fields of Locus `QueueProjectionTenantState` are "must be observable" for operations | The missing Venue surface has been verified (`locus/src/Locus.Storage/QueueEventProjectionService.cs:1407-1427` vs no Venue counterpart), but whether external callers depend on those fields is unconfirmed, so it is recorded only as an M1 guarantee difference and not listed as a separate acceptance gap |
| U6 | Whether `EmptyQueueReclaimBatchSize`/`EnableBackgroundTimedOutReclaim`/`BackgroundTimedOutReclaimBatchSize` in `config.Cleanup` were ever written by an external configuration file | All references in `examples/`, the README, and `venue-config-example.yaml` were not searched, so it cannot be decided whether this "configuration exists but is not wired" condition is already exposed to users at the documentation level |

---

## Appendix: evidence index for this document

- Key Locus files: `locus/src/Locus/LocusOptions.cs`, `locus/src/Locus/MetadataRepositoryOptions.cs`, `locus/src/Locus/StoragePoolOptions.cs`, `locus/src/Locus.Storage/Data/MetadataRepository.cs`, `locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs`, `locus/src/Locus.Storage/FileQueueEventJournal.cs`, `locus/src/Locus.Storage/QueueEventProjectionService.cs`, `locus/src/Locus.Storage/QueueEventJournalOptions.cs`, `locus/src/Locus.Storage/FileScheduler.cs`, `locus/src/Locus.Storage/FileWatcher.cs`, `locus/src/Locus.Storage/FileWatcherOptionsManager.cs`, `locus/src/Locus.Storage/FileWatcherAutoManager.cs`, `locus/src/Locus.Storage/StorageCleanupService.cs`, `locus/src/Locus.Storage/BackgroundCleanupService.cs`, `locus/src/Locus.Storage/TenantQuotaManager.cs`, `locus/src/Locus.Storage/Statistics/LocusStatisticsOutputService.cs`, `locus/src/Locus.MultiTenant/TenantManager.cs`, `locus/src/Locus.FileSystem/LocalFileSystemVolume.cs`, `locus/src/Locus.Core/Models/*.cs`, `locus/src/Locus.Core/Abstractions/IStorageVolume*.cs`.
- Key Venue files: `venue.go`, `venue_metadata_backup.go`, `config/config.go`, `config/fluent.go`, `pkg/core/models.go`, `pkg/core/interfaces.go`, `pkg/core/errors.go`, `pkg/core/orphan.go`, `pkg/metadata/badger_repository.go`, `pkg/metadata/corrupted_database.go`, `pkg/metadata/backup.go`, `pkg/pool/storage_pool.go`, `pkg/pool/volume_selector.go`, `pkg/scheduler/file_scheduler.go`, `pkg/quota/tenant_quota_manager.go`, `pkg/tenant/manager.go`, `pkg/volume/local_volume.go`, `pkg/cleanup/cleanup_service.go`, `pkg/cleanup/dead_letter.go`, `pkg/cleanup/cleanup_statistics.go`, `pkg/cleanup/junk_files.go`, `pkg/cleanup/invalid_database_backups.go`, `pkg/watcher/file_watcher.go`, `pkg/watcher/file_watcher_auto_manager.go`, `pkg/watcher/background_file_watcher_service.go`, `pkg/statistics/recorder.go`, `pkg/recovery/orphan_recovery_service.go`.
- Migration design document: `docs/sqlite-storage-design.md`.
