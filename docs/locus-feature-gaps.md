# Venue vs Locus v2.0.0: Work Tracker

This file is the **work tracker** for aligning Venue with the Locus v2.0.0 behavioural
baseline. It records work items, their status, and the acceptance criteria that close
them. Behaviour statements, design rationale, and guarantee comparisons live in the
documents below; this file does not restate them.

## Related documents

| Document | Role |
| --- | --- |
| `docs/README.md` | Documentation index and conventions (evidence rules, maintenance rules, verification commands) |
| `docs/architecture.md` | Current Venue design |
| `docs/locus-alignment.md` | Per-capability alignment matrix, with outstanding gaps G1-G9 |
| `docs/sqlite-storage-design.md` | Storage-engine migration from one shared BadgerDB store to one SQLite database per tenant |

Behaviour statements live in those documents. This file only tracks work items and
statuses; a behaviour claim that contradicts them is an inconsistency to log under the
documentation/behaviour section below, not a source of truth.

## Baseline and evidence conventions

- Baseline: Locus `v2.0.0`, commit `292bd2cea7051ec277d97ca708443e668b40a2d4`.
- **The Locus v2.0.0 reference checkout is not part of this repository.** Every Locus
  citation below uses the repository-relative form `locus/src/...` (for example
  `locus/src/Locus.Storage/FileScheduler.cs:602-609`) and refers to that external
  reference checkout.
- Venue citations are repository-relative, for example `config/config.go:399-402`.
- A conclusion is recorded only after both the Locus public contract and the Venue
  implementation were read, and both sides carry a `file:line` citation.
- Classification used by the alignment matrix:
  - `OBSERVABLE`: callers or operators can observe the difference through a public API,
    the filesystem, or behaviour, so it must be closed.
  - `MECHANISM`: Locus uses one internal mechanism and Venue substitutes another; the
    question is whether the observable guarantee is equivalent.
  - `PRESENT`: Venue already has it.
  - `DIVERGENCE`: a documented, deliberate Venue choice.
- Marker `declared but not wired`: the configuration field, interface, or model exists
  and is validated, cloned, and documented, but no runtime consumer reads it. The
  observable behaviour therefore does not change when the field changes.

## Batch overview

| Batch | Content | Status |
| --- | --- | --- |
| W1 | Cleanup and data retention: permanently-failed disposition and dead letters, junk files, invalid database backups, retired volumes, cumulative statistics, exact sharding protection | **Complete** |
| W2 | Watcher operations: global service options and global enable/disable, derivation from root configuration, `UpdateWatcher`, per-tenant lookup | **Complete** |
| W3 | Runtime statistics: recorder, reader, windowed aggregation, optional periodic log output | **Complete** (in-process statistics recorder/reader plus optional periodic log output) |
| W4 | Recoverability after metadata corruption (plan B: consistent backup plus restore) | **In progress**; being re-implemented on SQLite as part of the engine migration, and the Badger-era backup implementation is being replaced by SQLite online backups |
| W5 | Small parity items: mount health retry, background timed-out reclaim, quota administration API, `TryGetTenant`, `FileInfo` fields, optimization result detail, volume probe/warmup/diagnostics, watcher advanced knobs, configurable batch sizes, per-tenant cleanup entry points, startup fail-fast switch, missing-defect wiring | **Partially complete**: W5.3 and W5.4 done; W5.1, W5.2 and W5.5-W5.11 remain |

## W1: Cleanup and data retention (complete)

| # | Gap | Locus evidence | Venue gap as found | Acceptance criteria |
| --- | --- | --- | --- | --- |
| W1.1 | `PermanentlyFailedDisposition` = `Keep`/`MoveToDeadLetter`(default)/`Delete`; dead-letter layout `DeadLetterOptions{RootPath=".deadletter", IncludeTenantInPath, IncludeDatePartition, ShardingDepth=2}` | `locus/src/Locus.Storage/BackgroundCleanupService.cs` (enum and defaults); `locus/src/Locus.Storage/StorageCleanupService.cs:1598-1712` | Hard delete only; `DeadLettered` never produced | After a move: `DeadLetteredAt`/`PhysicalPath`/quota release/idempotency; `Keep` leaves the file in place; an invalid configured value fails at configuration time |
| W1.2 | `CleanupJunkFiles` (default true) plus `JunkFileCleanupInterval` (20m); list `Thumbs.db`/`.DS_Store`/`desktop.ini` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:262,269`; `locus/src/Locus.Storage/StorageCleanupService.cs:73-75` | Not implemented | All three file kinds are deleted and counted; managed payloads (`<32hex><ext>`) are unaffected |
| W1.3 | `CleanupInvalidDatabaseBackups` (default true) plus `CleanupInvalidDatabaseFilesAsync` | `locus/src/Locus.Storage/BackgroundCleanupService.cs:294`; `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:121` | Cleanup ran once at startup only, with no switch and no API | Only expired `<db>.corrupted.<stamp>` files are deleted; a locked directory is left alone; the operation is idempotent |
| W1.4 | `RetiredVolumeOptions`/`RetiredVolumeDisposition` (`Keep` default / `PurgeMetadataOnly`) | `locus/src/Locus.Storage/BackgroundCleanupService.cs:388-416`; `locus/src/Locus.Storage/StorageCleanupService.cs:1246-1262` | A missing volume was always skipped | `PurgeMetadataOnly` deletes metadata plus quota only; undeclared volumes stay protected |
| W1.5 | Cumulative cleanup statistics `GetCleanupStatisticsAsync()` (process-lifetime atomic counters) | `locus/src/Locus.Storage/StorageCleanupService.cs:2390-2401` | Single-run statistics only | Monotonic, concurrency-safe, returns a copy, exposed through `Venue.CleanupService()` |
| W1.6 | `IStorageVolume.ShardingDepth` used to protect shard directories exactly | `locus/src/Locus.Core/Abstractions/IStorageVolume.cs:41` | Heuristic only | The volume reports its depth; shard directories at depth 0-3 are never deleted by mistake |

## W2: Watcher operations (complete)

| # | Gap | Locus evidence | Acceptance criteria |
| --- | --- | --- | --- |
| W2.1 | Global watcher options and global enable/disable: `Enabled`/`DefaultPollingInterval`/`Minimum`/`Maximum`/`DisabledCheckInterval`/`MaxParallelWatcherScans`; `IFileWatcherOptionsManager` with **persistence** | `locus/src/Locus.Storage/FileWatcherOptions.cs:15-48`; `locus/src/Locus.Storage/FileWatcherOptionsManager.cs:51-192` | Configuration fields plus validation; after a global disable every scan stops and stays stopped across a restart; parallel scans respect the limit (`-race`) |
| W2.2 | Watcher derivation from root configuration: `ApplyRootConfiguration`/`DiscoverAndCreateWatchers`/`RemoveAllWatchers`/`GetRootConfiguration` | `locus/src/Locus.Core/Abstractions/IFileWatcherAutoManager.cs`; `locus/src/Locus.Core/Models/FileWatcherRootConfiguration.cs`; `locus/src/Locus.Storage/FileWatcherAutoManager.cs:63-192` | One watcher per tenant directory, re-appliable; newly added tenant directories are discovered; only derived watchers are removed; the root configuration is retrievable after a restart |
| W2.3 | `UpdateWatcher` changes configuration without interrupting a scan | `locus/src/Locus.Core/Abstractions/IFileWatcher.cs:26` | The update takes effect while the scan schedule is preserved; an unknown ID returns `ErrWatcherNotFound` |
| W2.4 | `GetWatchersForTenant` | `locus/src/Locus.Core/Abstractions/IFileWatcher.cs:55` | Only single-tenant watchers of that tenant are returned |
| W2.5 | Advanced watcher knobs: stability re-probe delay and skip age, auto-created-directory cache TTL, import-history prune throttle and interval, import-history flush debounce and interval | `locus/src/Locus.Core/Models/FileWatcherConfiguration.cs:43,110-141` | See W5.11 (moved to W5 to reduce W2 risk) |

## W3: Runtime statistics (complete)

| # | Gap | Locus evidence | Acceptance criteria |
| --- | --- | --- | --- |
| W3.1 | `Record(name, value, timestamp, dimensions)` plus `GetSnapshot(query)`, windowed aggregation, dimension filtering, bounded series | `locus/src/Locus.Core/Abstractions/ILocusStatisticsRecorder.cs:14`; `locus/src/Locus.Core/Abstractions/ILocusStatisticsReader.cs:13`; `LocusStatistics*` models; `locus/src/Locus.Storage/Statistics/InMemoryLocusStatisticsRecorder.cs` | Exposed through `Venue`; a query returns write/dequeue/read/complete/import counts and bytes inside a window; dimension filtering works; `MaxSeries` is bounded and validated |
| W3.2 | Optional periodic log output | `locus/src/Locus.Storage/Statistics/LocusStatisticsOutputService.cs`; `locus/src/Locus.Core/Models/LocusStatisticsOutputOptions.cs:13-33` | With `Statistics.Output` enabled a summary is emitted at the configured interval; when disabled the cost is zero (noop) |

Status: complete. The recorder, reader, windowed aggregation, and the optional output
loop are implemented in `pkg/statistics/recorder.go`, exposed through
`venue.go:977-993`, and started/stopped with the other background services at
`venue.go:779` and `venue.go:825`.

## W4: Metadata recoverability (plan B) - in progress

Gap: Locus `IDatabaseRecoveryService.RebuildMetadataDatabaseAsync`/
`RebuildQuotaDatabaseAsync` rebuild from the journal first
(`locus/src/Locus.Storage/Data/DatabaseRecoveryService.cs:185,533-563`) and fall back to
scanning physical files (`:142`). Venue only isolates the store and rebuilds an empty
database (`pkg/metadata/corrupted_database.go:109-128`), so **queue state is lost**.

Plan B: provide a consistent snapshot through a Badger online backup plus restore, and
document the difference explicitly as a mechanism divergence:

- Capability: `core.MetadataBackupService` (optional capability)
  `Backup(ctx, w io.Writer) (since uint64, err error)`; `Venue.BackupMetadata(ctx, w)`
  (`venue_metadata_backup.go:28`).
- Schedule: `BadgerDB.BackupDirectory` (empty disables), `BackupInterval` (1h),
  `BackupRetention` (7d).
- Restore: `venue.RestoreMetadata(io.Reader)` (offline, empty store,
  `venue_metadata_backup.go:68`); optionally combined with `RecoverCorruptedDatabase`
  for automatic restore of the most recent backup (off by default).
- Acceptance: a backup restores into a new directory, is opened normally by
  `NewVenue`, and can list the original files; writes are not blocked during backup;
  expired backups are removed by the retention period; a failed restore does not damage
  the empty database.
- Guarantee comparison: Venue recoverability is limited by the backup period; Locus is
  per-event. README must state this.

Status: in progress. The Badger-era implementation above is being re-implemented on
SQLite as part of the engine migration below, so the backup and restore path will be
replaced by SQLite online backups rather than extended on Badger.

### Storage-engine decision (engine row)

| # | Mechanism | Venue replacement | Guarantee conclusion |
| --- | --- | --- | --- |
| M6a | Locus stores one SQLite `metadata.db` per tenant under `{metadataDirectory}/{tenantId}/` (`locus/src/Locus.Storage/Data/MetadataRepository.cs:678-687`), with `files` tables and 8 indexes (`:559-588`) and WAL/NORMAL/cache/busy_timeout PRAGMAs (`locus/src/Locus.Core/Models/SqliteOptions.cs:88-105`) | Venue is moving from one shared BadgerDB store to **one SQLite database per tenant**, using a **pure-Go** driver that requires `CGO_ENABLED=0` to build | Engine-level `DIVERGENCE` that converges on the Locus layout. The decision record, DDL, PRAGMAs, connection strategy, and migration steps live in `docs/sqlite-storage-design.md`. Consequence for this tracker: the W4 Badger-era backup/restore is replaced by SQLite online backups, and the Badger value-log GC requirement no longer applies to the metadata store |

## W5: Small parity items (ordered by value)

| # | Gap | Locus evidence | Acceptance criteria |
| --- | --- | --- | --- |
| W5.1 | Mount health retry and delay: `InitialDelayMs` (2000)/`HealthCheckDelayMs` (500), at most 10 attempts | `locus/src/Locus.Storage/StoragePool.cs:164-208`; `locus/src/Locus/LocusOptions.cs:195,201` | Configuration fields; startup waits and retries; a volume that never becomes healthy fails startup with a stable error |
| W5.2 | Low-frequency background timed-out reclaim: `BackgroundTimedOutReclaimBatchSize` (8) plus cooldown plus switch | `locus/src/Locus.Storage/FileScheduler.cs:621-691`; `locus/src/Locus/StoragePoolOptions.cs:31-43` | Configuration fields; timed-out files are reclaimed even when the cleanup cycle is long; the cooldown applies; a newer claim is never displaced |
| W5.3 | Tenant quota administration API: `GetEffectiveLimit`/`SetTenantLimit`/`RemoveTenantLimit`/`SetGlobalLimit`/`GetGlobalLimit` | `locus/src/Locus.Core/Abstractions/ITenantQuotaManager.cs:52-81` | **Done.** The effective limit is the override when present, otherwise the global limit; changing the global limit immediately affects tenants without an override; removing an override falls back to the global limit; negative values are validated |
| W5.4 | `TryGetTenant` (read-only, no side effects) | `locus/src/Locus.Core/Abstractions/ITenantManager.cs:26` | **Done.** An unknown tenant does not write to disk; `IsTenantEnabled` uses it; a test asserts no `*.json` write |
| W5.5 | `FileInfo` gains `TenantID`/`RetryCount` | `locus/src/Locus.Core/Models/FileInfo.cs:19,39` | `GetFileInfo` returns both fields and the documentation is updated |
| W5.6 | Database optimization result detail (bytes reclaimed, size before/after) | `locus/src/Locus.Core/Models/DatabaseOptimizationResult.cs:11-41` | The reclaimed amount is observable; exposed through `Venue.CleanupService()` |
| W5.7 | Volume `ProbeHealth` forced probe plus write-path warmup plus diagnostics snapshot | `locus/src/Locus.Core/Abstractions/IStorageVolumeHealthProbe.cs:11`; `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathWarmup.cs:14`; `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathDiagnostics.cs:13` | The forced probe bypasses and refreshes the cache; optional startup warmup; the snapshot accessor exists |
| W5.8 | Configurable cleanup/optimization batch sizes and concurrency parameters | `locus/src/Locus.Core/Models/CleanupOptions.cs:334-361`; `locus/src/Locus/StoragePoolOptions.cs:17-43` | Fields plus defaults plus validation plus clone plus fluent methods plus examples; the behaviour is observable through tests |
| W5.9 | Per-tenant cleanup entry points (empty directories / orphan recovery) | `locus/src/Locus.Core/Abstractions/IStorageCleanupService.cs:18,25,54` | Per-tenant sweep and recovery are available; an unknown tenant is rejected |
| W5.10 | Startup recovery failure fast-fail switch `FailFastOnStartupRecoveryFailure` (default false) | `locus/src/Locus/LocusOptions.cs:83` | Explicitly configurable; when false, isolate and continue with a degraded-state warning; documented |
| W5.11 | Advanced watcher knobs: stability second probe (delay plus skip age), auto-created-directory cache TTL, import-history prune throttle and interval, import-history flush debounce and interval | `locus/src/Locus.Core/Models/FileWatcherConfiguration.cs:43,110-141` | Configurable without breaking in-flight de-duplication (`-race`) |
| W5.12 | Queue projection diagnostics surface (lag/snapshot/gap/corrupt tail) | `locus/src/Locus.Storage/QueueProjectionTenantState.cs`; `locus/src/Locus.Core/Abstractions/IQueueProjectionMaintenanceService.cs` | Only meaningful if W4 adopts a journal-like mechanism; under plan B (backup/restore) provide the equivalent observable information as "backup time point / most recent backup" |

### W5 missing-defect items found by the review

These items were found while reviewing the configuration surface against its runtime
consumers. Every one is **`declared but not wired`**: the field exists and is
defaulted, cloned, validated, and documented, but changing it has no runtime effect.

| # | Gap (`declared but not wired`) | Evidence | Acceptance criteria |
| --- | --- | --- | --- |
| W5.13 | `Cleanup.EmptyQueueReclaimBatchSize` is ignored at wiring time: `config/config.go:399-402` documents "zero selects the default, negative disables immediate reclaim", but `venue.go:338-340` writes `scheduler.DefaultEmptyQueueReclaimBatchSize` into the scheduler options, so `config/config.go:672-674` has no runtime effect. Locus reads the configured value in `locus/src/Locus.Storage/FileScheduler.cs:602-609` (option declared at `:52`, configuration at `locus/src/Locus/StoragePoolOptions.cs:19-24`) | `config/config.go:399-402`; `config/config.go:672-674`; `venue.go:338-340`; `pkg/scheduler/file_scheduler.go:32-48,152` | Configure a non-default batch size and assert the immediate-reclaim bound changes; a negative value disables immediate reclaim |
| W5.14 | `Cleanup.EnableBackgroundTimedOutReclaim` and `Cleanup.BackgroundTimedOutReclaimBatchSize` have no runtime consumer: the fields and defaults exist (`config/config.go:404-412`, defaults at `:562-563`, fluent at `config/fluent.go:631,638`) but no scheduler or pool code reads them. Locus uses them in `locus/src/Locus.Storage/FileScheduler.cs:621-692` (options at `locus/src/Locus/StoragePoolOptions.cs:26-43`) | `config/config.go:404-412`; `config/config.go:562-563`; `config/fluent.go:631,638`; no reader outside `config/` | A successful claim reclaims timed-out files in the background according to the batch size; the cooldown applies; disabling the switch stops it; a file becomes reclaimable even when the cleanup cycle is long |
| W5.15 | `VolumeConfig.WarmupOnStartup` has no runtime consumer; `VolumeConfig.InitialDelay` / `HealthCheckDelay` are validated but do not participate in startup; `Config.FailFastOnStartupRecoveryFailure` is not consulted on the startup failure path; and all advanced watcher knobs have no runtime consumer. Locus performs the mount wait/retry in `locus/src/Locus.Storage/StoragePool.cs:164-209`, the warmup via `locus/src/Locus.Core/Abstractions/IStorageVolumeWritePathWarmup.cs:6-15` (`locus/src/Locus.FileSystem/LocalFileSystemVolume.cs:281`), the fail-fast decision at `locus/src/Locus/LocusOptions.cs:76-83`, and the watcher knobs at `locus/src/Locus.Core/Models/FileWatcherConfiguration.cs:43,110-141` | `config/config.go:146-159`; `config/config.go:50-53`; `config/config.go:188-285`; `config/fluent.go:342-345,616-623`; no runtime reader | Startup waits and retries per volume; a volume that never becomes healthy fails startup under the fail-fast switch; startup performs the optional throwaway warmup write; the watcher knobs change import behaviour |
| W5.16 | `ShardingDepthProvider` is implemented by the local volume but cleanup still uses structural heuristics: the capability exists (`pkg/core/interfaces.go:291-296`, implemented at `pkg/volume/local_volume.go:24,143`) yet the cleanup path does not consult it. Locus consumes `IStorageVolume.ShardingDepth` (`locus/src/Locus.Core/Abstractions/IStorageVolume.cs:41`) | `pkg/core/interfaces.go:291-296`; `pkg/volume/local_volume.go:24,141-148`; `pkg/cleanup/cleanup_service.go:476-520` (heuristics) | Cleanup asks the volume for its sharding depth and protects shard directories exactly, at depth 0-3 |
| W5.17 | The two boolean watcher knobs `DisableImportedFilesPruneThrottle` and `DisableImportedFilesHistoryFlushDebounce`, together with the four advanced watcher durations (`ImportedFilesPruneInterval`, `ImportedFilesHistoryFlushInterval`, `FileStabilityCheckDelay`, `SkipStabilityCheckAfterAge`), are not mapped at runtime. The fields exist on both `FileWatcherConfig` and `FileWatcherRootConfig` (`config/config.go:199-216`, `:270-285`) with fluent setters (`config/fluent.go:294-296,735-737`), but the import path does not apply them. Locus reads them at `locus/src/Locus.Core/Models/FileWatcherConfiguration.cs:110-141` | `config/config.go:199-216`; `config/config.go:270-285`; `config/fluent.go:294-296,735-737`; `pkg/watcher/file_watcher.go` (no reader) | Disabling the throttle or the debounce changes the write pattern; each duration is honoured; in-flight de-duplication still holds (`-race`) |

## Mechanism divergences (no parity work required, but the guarantee must be documented)

| # | Locus mechanism | Venue replacement | Guarantee conclusion |
| --- | --- | --- | --- |
| M1 | Per-tenant `queue.log` plus projection plus snapshot plus compaction | Synchronous Badger transactions plus status indexes | Durability is **stronger** (no ACK window); **rebuild capability is lost**, which W4-B restores as recoverability; sequence gaps, corrupt-tail self-healing, and lag observation have no counterpart |
| M2 | Write-behind metadata queue | Synchronous transactions plus bounded conflict retry | Equivalent and stricter (nothing queued can be lost) |
| M3 | Quota projection/compensation/reconciliation managers | Startup recompute plus `ReconcileQuotaCounts` plus write-path compensation | "Counts are reconcilable" is preserved; event-driven correction degrades into an explicit call |
| M4 | `CleanupFilesByStatusAsync` in a single pass | Each status is processed separately | Same result, different number of I/O passes |
| M5 | `IMetadataStore`/`MetadataSnapshot` | Badger repository | Locus v2.0.0 contains no implementing class, so this is not an observable gap |
| M6 | `SqliteOptions` | `BadgerDBConfig` | Engine difference, documented in `docs/sqlite-storage-design.md`; the migration to one SQLite database per tenant is the convergence plan (see M6a above) |
| M6a | One SQLite `metadata.db` per tenant, WAL, 8 indexes | One shared BadgerDB store today; moving to one SQLite database per tenant with a pure-Go driver and `CGO_ENABLED=0` required | Documented engine `DIVERGENCE` and migration; details and DDL in `docs/sqlite-storage-design.md`. W4 backup/restore moves to SQLite online backups |

## Documentation/behaviour mismatches (to fix)

| # | Location | Problem |
| --- | --- | --- |
| D1 | README "Locus Alignment" (`README.md:678-682`) | Still states that timed-out reclaim happens only on the cleanup cycle, but empty-queue immediate reclaim already exists. Code evidence: `pkg/scheduler/file_scheduler.go` immediate reclaim plus `venue.go:335-340` wiring, against the documented claim. This item is stale and should be closed after the README bullet is rewritten |
| D2 | README "Locus Alignment" (`README.md:683-684`) | The claim that status scans are not paged is stale: `core.StatusPageReader` and the paged maintenance paths already exist (`pkg/core/interfaces.go`, consumed by `pkg/cleanup/`). Close after the README bullet is reconciled |
| D3 | README "Locus Alignment" (`README.md:690-704`) | The "Not implemented" list still names dead letters, junk files, retired volumes, and invalid-backup cleanup, which conflicts with the completed W1 items; and the following "Implemented and aligned" paragraph still lists W3 statistics as outstanding while W3 is complete. Rewrite both paragraphs to match W1/W2/W3 completion |
| D4 | `pkg/core/models.go` status comment | Still claims there is no dead-letter storage and that `DeadLettered` is never produced, which conflicts with the added `DeadLetteredAt` field and disposition enum (`pkg/core/models.go:89-93,241-244,336,475-477`). Update the comment |
| D5 | Watcher `MinFileAge` | README records 5s, but `RegisterWatcher` falls back to 3s for a caller that passes 0 (`pkg/watcher/file_watcher.go:326-327`). Align the README or the fallback |
| D6 | `venue-config-example.yaml` | This round added the deadLetter/junk/retired/invalid-backup keys (`venue-config-example.yaml:197-220`) and they still need a cross-check against the README for consistency |
| D7 | README service table (`README.md:578-591`) | Stale: the `EnableWatcher`/`DisableWatcher` paragraph now sits after the table (`README.md:593-599`), so the table renders correctly. Close after confirming no other table break remains |
| D8 | `config.ApplyDefaults` | The two dead-letter boolean defaults cannot be restored by `ApplyDefaults` (the same risk class as the watcher booleans), so the README must state it explicitly |
| D9 | Deleted review document (previously `docs/venue-locus-alignment-review.md`) | The source file no longer exists and must not be cited. Its section 8.3 claimed the health-probe TTL was not exposed to config, which is stale: `VolumeConfig.HealthCheckCacheTTL` is exposed at `config/config.go:141-144` and the runtime caches the probe at `pkg/volume/local_volume.go:155-176`. The mismatch is recorded inline here instead of pointing at the deleted file |
| D10 | Deleted review document (previously `docs/venue-locus-alignment-review.md`) | The same deleted file's "still not implemented" list is obsolete. Replaced by the live tracker: W1 and W2 are complete; W3 is complete; W4 is in progress on SQLite; the remaining gaps are W5.1, W5.2, W5.5-W5.11 and the W5.13-W5.17 `declared but not wired` defects, with G1-G9 in `docs/locus-alignment.md` |

## Open questions

- Whether Locus's quota projection manager has **per-event accounting semantics** that
  Venue's recompute path lacks (the 4295-line `MetadataRepository.cs` was not compared
  exhaustively).
- Whether the Locus `FileWatcher` import pipeline (1764 lines) contains behaviour
  beyond O18 that is not listed here.
- Whether Locus's write-behind timing semantics for tenant quotas produce an observable
  difference.
- Whether any external consumer depends on the naming/semantic difference between
  Locus `CleanupStatistics.OrphanedFilesRemoved` (counts files) and Venue
  `OrphanedMetadataRemoved` (counts metadata records).
