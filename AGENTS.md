# AGENTS.md

This file defines repository architecture and development rules for Venue. User-facing setup and API examples belong in `README.md` and `examples/`.

## Project Scope

Venue is a Go 1.26 multi-tenant file storage queue. Locus `v2.0.0` commit `292bd2cea7051ec277d97ca708443e668b40a2d4` is the current behavioral reference baseline.

Venue is not a caller-managed filesystem:

- `WriteFile` generates the `fileKey`.
- Callers never select physical paths or storage volumes.
- Workers claim pending files through queue APIs.
- Completion records durable queue state; cleanup later deletes the managed file.
- Locus behavior is the compatibility target, but public APIs must follow Go conventions.

## Architecture

```text
venue.NewVenue(*config.Config)
    |
    +-- tenant manager
    +-- storage pool
    |   +-- volume selection and physical files
    |   +-- metadata repository and active cache
    |   +-- tenant and directory quotas
    |   +-- file scheduler
    +-- cleanup services
    +-- file watcher services
    +-- database health services
```

Repository ownership:

- `config/`: public, configuration-source-independent model, defaults, validation, cloning, and fluent methods.
- `viperconfig/`: optional Viper-to-`config.Config` adapter.
- `pkg/core/`: public interfaces, shared models, statuses, and domain errors.
- `pkg/tenant/`: tenant lifecycle and metadata cache.
- `pkg/metadata/`: BadgerDB metadata projection, indexes, migration, and active-data cache.
- `pkg/pool/`: storage and queue facade.
- `pkg/scheduler/`: atomic queue transitions and retry scheduling.
- `pkg/quota/`: tenant and directory quotas.
- `pkg/volume/`: physical storage volumes and path safety.
- `pkg/cleanup/`: cleanup and database optimization.
- `pkg/watcher/`: watched-directory imports.
- `pkg/health/`: startup and periodic health checks.
- `pkg/logging/`: instance-scoped structured logging runtime.
- `test/benchmark/`: public-entry system benchmarks.

Keep edits within these ownership boundaries. Do not create a second runtime configuration model or bypass the public entry point in integration tests and system benchmarks.

## Configuration Rules

- The only public runtime model is `config.Config`.
- `venue.NewVenue` accepts `*config.Config` directly.
- Applications may use direct field assignment or chainable `With...` methods.
- Every nested configuration type must support chainable construction.
- `WithVolumes`, `WithTenants`, and `WithFileWatchers` replace collections.
- `AddVolume`, `AddTenant`, and `AddFileWatcher` append to collections.
- Every exported configuration field must retain `json`, `yaml`, and `mapstructure` tags.
- Runtime-only values such as `slog.Handler` must use `json:"-" yaml:"-" mapstructure:"-"`.
- `config` must not import Viper or configuration-file parsers and must not perform file I/O.
- Viper integration belongs only in the top-level `viperconfig` package.
- File watching, environment variables, reload policy, and configuration persistence belong to the application entry point.

When adding a configuration field, update defaults, cloning, validation where relevant, fluent methods, binding-tag tests, adapter tests, example files, and README documentation together.

## Logging Rules

- Use `log/slog` only through the injected `pkg/logging.Runtime`.
- Never use, replace, or configure `slog.Default()`.
- `Config.Logging == nil` means logging is disabled and silent.
- The caller owns the handler and any writer behind it.
- Logging must not cause storage operations to fail; handler panics remain isolated.
- Do not log file contents, original file names, full physical paths, tenant-sensitive data, credentials, or raw errors that may contain those values.
- Use structured attributes and stable event names.

## Tenant Isolation

- Scope metadata primary keys, secondary indexes, caches, and mutations by `(tenantID, fileKey)`.
- Never infer a tenant from process-global state, headers, or a package global.
- Every caller-visible read, status transition, completion, and failure operation must carry tenant context.
- A tenant must not list, claim, read, mutate, or delete another tenant's metadata or files.
- Legacy metadata migrations must be restartable and idempotent.
- Rebuild indexes from stored tenant ownership rather than from path assumptions.

## Queue And Concurrency

- File allocation must be atomic; one pending file can be claimed by only one worker.
- Metadata transitions belong in BadgerDB transactions.
- Preserve FIFO ordering among files that are available for processing.
- Retry uses exponential backoff capped by the configured maximum.
- Processing timeout recovery must not permit a stale worker to mutate a newer claim.
- Use per-directory synchronization for directory quota changes.
- Protect tenant caches and lifecycle state with `sync.Map`, mutexes, or atomic state as appropriate.
- Do not reduce concurrency to hide races or contention defects.
- Check `context.Context` cancellation in loops and background services.

## Storage And Persistence

- Physical file creation and metadata persistence form one logical operation.
- Roll back the physical file when metadata persistence fails.
- Close every `io.ReadCloser` and database handle on all paths.
- Preserve original file extensions only for diagnostics; never use caller names as physical paths.
- Sanitize all relative paths and keep resolved paths under their configured volume root.
- BadgerDB deletion requires periodic value-log GC to reclaim disk space.
- Treat `badger.ErrNoRewrite` as a normal no-work GC outcome.
- Keep durability trade-offs such as `SyncWrites` and file `fsync` explicit in configuration.

## Error Handling

- Define stable domain errors in `pkg/core/errors.go`.
- Wrap errors with `%w` and include safe operational context such as tenant ID, file key, or component.
- Use `errors.Is` for domain checks.
- Never ignore an error unless cleanup is explicitly best-effort and the reason is clear.
- Defer transactional rollback immediately after opening a transaction; committing must remain explicit.

## Public API And Documentation

- Add godoc comments to every exported type, function, method, field whose meaning is not obvious, and interface behavior.
- Document ownership and lifecycle for readers, handlers, goroutines, and database resources.
- Keep README examples compilable against the current public API.
- Do not put roadmap status, implementation progress, or temporary task notes in `AGENTS.md`.
- Update README, `examples/`, configuration files, benchmarks, and tests whenever a public API changes.
- Use Locus as a behavioral reference, not as a reason to copy .NET naming or configuration framework patterns.

## Testing

Use test-first development for behavioral changes. A regression test must fail for the expected reason before the implementation is changed.

Required commands:

```powershell
go build ./...
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
```

Testing rules:

- Use table-driven tests for validation matrices and state transitions.
- Use `t.TempDir()` for filesystem-backed tests.
- Register repository and runtime shutdown before temporary directory cleanup.
- Exercise integration paths through `venue.NewVenue(*config.Config)` unless the test targets a lower-level component.
- Test same-`fileKey` behavior across different tenants.
- Test restart and migration behavior with literal legacy fixtures.
- Test background service cancellation and shutdown.
- Run race tests before any commit.
- Do not weaken assertions, add retries, or serialize tests merely to hide a race.

On this Windows workspace, if the default Go cache is inaccessible, use ignored repository-local caches:

```powershell
$env:GOCACHE='D:\Code\go\venue\tmp\go-build'
$env:GOLANGCI_LINT_CACHE='D:\Code\go\venue\tmp\golangci-lint'
```

## Benchmarks

Benchmarks must use real code paths and report allocations. System benchmarks must initialize through the public Venue API.

```powershell
go test -run '^$' -bench=. -benchmem ./...
go test -run '^$' -bench=. -benchmem ./test/benchmark
```

- Keep setup and fixture generation outside timed sections.
- Check every setup error; benchmarks must not continue with partially initialized components.
- Use `b.ResetTimer`, `b.StopTimer`, or `b.Loop` correctly.
- Record the Go version, OS/architecture, CPU, command, run count, and payload assumptions with published results.
- Treat benchmark values as host-specific regression evidence, not universal performance guarantees.

## Formatting And Review

- Run `gofmt` on every changed Go file.
- Run `git diff --check` before completion.
- Keep comments focused on why, ownership, or invariants.
- Preserve unrelated user changes in a dirty worktree.
- Do not stage, commit, pull, push, create branches, or rewrite history unless explicitly requested.
- Work on the current branch when requested; do not create a worktree or branch.
