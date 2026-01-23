# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Venue** is a high-performance, multi-tenant file storage pool system implemented in Go 1.25. It's a port of the [Locus](https://github.com/cocosip/Locus) .NET library, designed as a **file queue system** with the following characteristics:

- **Multi-tenant isolation**: Each tenant has isolated storage with enable/disable controls
- **Queue-based processing**: Files are processed as a queue with automatic retry on failure
- **Unlimited storage expansion**: Dynamically mount multiple storage volumes
- **High concurrency**: Thread-safe operations with per-tenant databases and active-data caching
- **System-managed files**: Users receive fileKeys, system handles all physical storage details

## Key Concept: File Queue System (Not Traditional File Storage)

**Critical**: Venue is NOT a traditional file system where users specify paths. Instead:

1. **Write** → System generates and returns a `fileKey` (UUID)
2. **Process** → Workers fetch next pending file from queue
3. **Complete/Retry** → Mark file as completed (deleted) or failed (retry)

Users never need to know which volume, directory, or path contains their files.

## Build and Test Commands

### Build
```bash
# Build all packages
go build ./...

# Build main application (when cmd/venue exists)
go build -o bin/venue ./cmd/venue

# Build with race detector
go build -race ./...
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with race detector (CRITICAL before commits)
go test -race ./...

# Run tests with coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run integration tests (when implemented)
go test -tags=integration ./...

# Run specific package tests
go test ./pkg/storage/...
go test ./pkg/tenant/...
```

### Benchmarking
```bash
# Run all benchmarks
go test -bench=. -benchmem ./...

# Run benchmarks for specific package
go test -bench=. -benchmem ./pkg/storage/metadata/

# Run specific benchmark
go test -bench=BenchmarkMetadataCache -benchmem ./pkg/storage/metadata/

# Compare benchmark results
go test -bench=. -benchmem ./... > old.txt
# (make changes)
go test -bench=. -benchmem ./... > new.txt
benchstat old.txt new.txt
```

### Linting
```bash
# Run golangci-lint (when configured)
golangci-lint run

# Run with auto-fix
golangci-lint run --fix

# Check formatting
gofmt -l .

# Auto-format
gofmt -w .
```

## Architecture Overview

### High-Level Design

```
┌─────────────────────────────────────────────┐
│   API Layer (StoragePool)                   │
│   - Unified storage + queue interface       │
├─────────────────────────────────────────────┤
│   Active-Data Cache (Per-Tenant)           │
│   - Only Pending/Processing/Failed files    │
│   - sync.Map for fast lookups               │
├─────────────────────────────────────────────┤
│   Persistence Layer (Per-Tenant BadgerDB)  │
│   - MetadataRepository: File metadata       │
│   - DirectoryQuotaRepository: Quota limits  │
│   - Auto GC and compression support         │
├─────────────────────────────────────────────┤
│   Tenant Management (JSON + Cache)         │
│   - TenantMetadata: Status, dates           │
│   - TTL cache (5 minutes)                   │
├─────────────────────────────────────────────┤
│   Storage Volumes (LocalFileSystemVolume)  │
│   - Configurable sharding (0-3 levels)      │
│   - Health monitoring                       │
└─────────────────────────────────────────────┘
```

### Package Structure

```
pkg/
├── core/                   # Core abstractions
│   ├── interfaces.go       # All interface definitions
│   ├── models.go           # Shared models (FileLocation, FileInfo, etc.)
│   └── errors.go           # Custom errors (TenantDisabledException, etc.)
├── tenant/                 # Multi-tenant management
│   ├── manager.go          # TenantManager implementation
│   ├── context.go          # TenantContext
│   └── metadata.go         # JSON-based tenant metadata
├── storage/                # Storage pool implementation
│   ├── pool.go             # StoragePool (main API)
│   ├── scheduler.go        # FileScheduler (queue processing)
│   ├── metadata/           # File metadata management
│   │   ├── repository.go   # MetadataRepository
│   │   └── cache.go        # Active-data caching
│   └── quota/              # Quota management
│       ├── directory.go    # DirectoryQuotaManager
│       └── tenant.go       # TenantQuotaManager
├── volume/                 # Storage volumes
│   ├── interface.go        # StorageVolume interface
│   ├── local.go            # LocalFileSystemVolume
│   └── health.go           # Health monitoring
└── cleanup/                # Background cleanup
    ├── service.go          # CleanupService
    └── background.go       # BackgroundCleanupService
```

## Key Design Patterns

### 1. Per-Tenant Database Isolation

Each tenant has **isolated BadgerDB databases**:
- `metadata/{tenantId}/` - File metadata (BadgerDB directory)
- `quota/{tenantId}/` - Directory quotas (BadgerDB directory)

**Why**:
- Prevents cross-tenant data leakage
- Simplifies backup/restore
- BadgerDB provides automatic compression via GC

**Database Choice**: BadgerDB v4
- Pure Go implementation (no CGO)
- Built-in compression: `db.RunValueLogGC(0.7)`
- High performance for our write-heavy workload
- See [DATABASE_SELECTION.md](doc/DATABASE_SELECTION.md) for detailed analysis

### 2. Active-Data Caching

Only cache files in **Pending/Processing/Failed** states in memory.

**Completed files** are immediately deleted from cache and database.

**Why**: Prevents memory bloat from millions of historical files.

### 3. Atomic File Allocation

`GetNextFileForProcessing` uses database transactions to ensure:
- Each file allocated to exactly one worker
- Status transition `Pending → Processing` is atomic
- No duplicate processing

**Implementation**:
```go
// Pseudocode
tx.Begin()
file := db.QueryFirst("SELECT * FROM files WHERE status=Pending AND (availableAt IS NULL OR availableAt <= NOW) ORDER BY createdAt LIMIT 1 FOR UPDATE")
if file != nil {
    file.Status = Processing
    file.ProcessingStartTime = Now()
    db.Update(file)
}
tx.Commit()
return file
```

### 4. Exponential Backoff Retry

When `MarkAsFailed` is called:
```
RetryCount++
if RetryCount >= MaxRetryCount:
    Status = PermanentlyFailed
else:
    Status = Pending
    delay = InitialDelay * 2^(RetryCount-1)
    AvailableForProcessingAt = Now() + min(delay, MaxRetryDelay)
```

**Why**: Prevents thundering herd, gives transient errors time to resolve.

### 5. File Extension Preservation

Original filenames are preserved for debugging:
```
WriteFile(tenant, stream, "invoice.pdf")
→ Physical: /storage/vol-001/tenant-001/a1/b2/a1b2c3d4...e5f6.pdf
                                                         └─ Extension preserved
```

**Why**: Easier to inspect files during debugging/recovery.

## Critical Concurrency Patterns

### Thread-Safety Requirements

1. **Metadata Operations**: Use database transactions
2. **Quota Operations**: Use mutex per directory
3. **Tenant Cache**: Use `sync.Map` or `sync.RWMutex`
4. **Volume Selection**: Read-only after initialization (safe)

### Race Detector is MANDATORY

**Always run tests with `-race` before committing**:
```bash
go test -race ./...
```

### Goroutine Patterns

**Background Cleanup**:
```go
go func() {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            cleanup()
        case <-ctx.Done():
            return
        }
    }
}()
```

**Worker Pool**:
```go
for i := 0; i < numWorkers; i++ {
    go func(workerID int) {
        for {
            file, err := pool.GetNextFileForProcessing(ctx, tenant)
            if err != nil {
                return
            }
            process(file)
        }
    }(i)
}
```

## Error Handling Conventions

### Custom Errors (pkg/core/errors.go)

All domain errors are defined as sentinel errors:

```go
var (
    ErrTenantDisabled = errors.New("tenant is disabled")
    ErrTenantNotFound = errors.New("tenant not found")
    ErrQuotaExceeded = errors.New("quota exceeded")
    ErrDirectoryQuotaExceeded = errors.New("directory quota exceeded")
    ErrInsufficientStorage = errors.New("insufficient storage space")
    ErrNoFilesAvailable = errors.New("no files available for processing")
)
```

**Usage**:
```go
if !tenant.IsEnabled() {
    return "", fmt.Errorf("tenant %s: %w", tenant.ID, ErrTenantDisabled)
}

// Caller can check:
if errors.Is(err, ErrTenantDisabled) {
    // Handle disabled tenant
}
```

### Transaction Rollback Pattern

```go
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Safe to call even after Commit

// ... operations ...

if err := tx.Commit(); err != nil {
    return err
}
```

### Physical File Rollback

When metadata write fails, delete physical file:
```go
fileKey, err := volume.WriteFile(ctx, stream, path)
if err != nil {
    return "", err
}

err = metadata.AddOrUpdate(ctx, fileMetadata)
if err != nil {
    // Rollback: delete physical file
    volume.DeleteFile(ctx, fileKey)
    return "", err
}

return fileKey, nil
```

## Database Patterns

### BadgerDB Key Structure

BadgerDB is a key-value store. We use structured keys for efficient querying.

#### Metadata Keys

```
# Primary key: file metadata
file:{fileKey} → JSON(FileMetadata)

# Secondary index: status + available time
status:{status}:{availableTime}:{fileKey} → fileKey

# Secondary index: created time
created:{createdTime}:{fileKey} → fileKey

# Example
file:a1b2c3d4e5f6... → {"fileKey":"a1b2c3d4...","status":0,...}
status:0:2026-01-22T10:00:00Z:a1b2c3d4... → a1b2c3d4...
created:2026-01-22T09:00:00Z:a1b2c3d4... → a1b2c3d4...
```

#### Directory Quota Keys

```
# Primary key: quota config
quota:{directoryPath} → JSON(DirectoryQuota)

# Example
quota:/tenant-001/vol-001/a1/b2 → {"currentCount":150,"maxCount":1000,...}
```

#### Indexing Pattern

Since BadgerDB doesn't have built-in indexing, we maintain secondary indexes manually:

```go
// Write with indexes
func (r *MetadataRepository) AddOrUpdate(ctx context.Context, m *FileMetadata) error {
    return r.db.Update(func(txn *badger.Txn) error {
        // 1. Write primary key
        key := []byte("file:" + m.FileKey)
        val, _ := json.Marshal(m)
        txn.Set(key, val)

        // 2. Write status index
        statusKey := fmt.Sprintf("status:%d:%s:%s",
            m.Status, m.AvailableForProcessingAt, m.FileKey)
        txn.Set([]byte(statusKey), []byte(m.FileKey))

        // 3. Write created index
        createdKey := fmt.Sprintf("created:%s:%s",
            m.CreatedAt.Format(time.RFC3339), m.FileKey)
        txn.Set([]byte(createdKey), []byte(m.FileKey))

        return nil
    })
}
```

### Connection Management

Use BadgerDB with optimized options:
```go
import "github.com/dgraph-io/badger/v4"

func OpenBadgerDB(path string) (*badger.DB, error) {
    opts := badger.DefaultOptions(path)

    // Performance optimizations
    opts.SyncWrites = false              // Async writes for performance
    opts.NumVersionsToKeep = 1           // Keep only latest version
    opts.CompactL0OnClose = true         // Compact on close
    opts.ValueLogFileSize = 64 << 20     // 64MB value log files
    opts.ValueThreshold = 1024           // Values >1KB go to value log

    // Compression settings
    opts.Compression = options.Snappy    // Enable Snappy compression
    opts.ZSTDCompressionLevel = 3        // Or use ZSTD level 3

    return badger.Open(opts)
}
```

**Key Settings**:
- `SyncWrites=false`: Better performance (durability via periodic sync)
- `NumVersionsToKeep=1`: Minimize storage (we don't need versioning)
- `CompactL0OnClose=true`: Cleanup on graceful shutdown
- `Compression`: Snappy (fast) or ZSTD (better ratio)

### Garbage Collection and Compression

**CRITICAL**: Run GC periodically to reclaim space after deletions.

```go
// Background GC service
func RunPeriodicGC(ctx context.Context, db *badger.DB, interval time.Duration) {
    ticker := time.NewTicker(interval) // e.g., 10 minutes
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            // Run GC with 0.7 threshold (reclaim if 70%+ space is garbage)
            err := db.RunValueLogGC(0.7)
            if err == badger.ErrNoRewrite {
                // No GC needed, skip
                continue
            }
            if err != nil {
                log.Error("GC failed", "error", err)
            } else {
                log.Info("GC completed successfully")
            }
        case <-ctx.Done():
            return
        }
    }
}
```

**GC Behavior**:
- `ErrNoRewrite`: No garbage to collect (< 70% threshold)
- Success: Space reclaimed, files compacted
- Error: GC failed, will retry next cycle

**Recommended Settings**:
- Run GC every 5-10 minutes
- Threshold: 0.5-0.7 (50-70% garbage)
- Monitor GC duration and space savings

## Testing Patterns

### Unit Test Structure

```go
func TestFeatureName(t *testing.T) {
    // Arrange
    ctx := context.Background()
    tenant := &TenantContext{ID: "test-tenant", Status: TenantStatusEnabled}
    pool := setupTestPool(t)
    defer cleanupTestPool(t, pool)

    // Act
    result, err := pool.SomeOperation(ctx, tenant, input)

    // Assert
    require.NoError(t, err)
    assert.Equal(t, expected, result)
}
```

### Table-Driven Tests

```go
func TestMultipleScenarios(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        wantErr bool
    }{
        {"valid input", "test", false},
        {"empty input", "", true},
        {"invalid input", "###", true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := Validate(tt.input)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

### Temporary Directory Pattern

```go
func setupTestPool(t *testing.T) *StoragePool {
    tempDir := t.TempDir() // Auto-cleanup on test completion

    pool, err := NewStoragePool(&StoragePoolOptions{
        RootPath: tempDir,
        // ... other options
    })
    require.NoError(t, err)

    return pool
}
```

### Benchmark Structure

```go
func BenchmarkMetadataGet(b *testing.B) {
    pool := setupBenchPool(b)
    defer pool.Close()

    ctx := context.Background()
    fileKey := "test-file-key"

    b.ResetTimer() // Exclude setup from benchmark

    for i := 0; i < b.N; i++ {
        _, err := pool.GetFileInfo(ctx, tenant, fileKey)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

## Performance Guidelines

### Target Benchmarks

Based on C# reference implementation (converted to Go expectations):

| Operation | Target | Notes |
|-----------|--------|-------|
| Metadata get (cache hit) | < 50 μs | In-memory read |
| Metadata get (cache miss) | < 20 ms | Database read |
| Metadata add/update | < 500 μs | Database write |
| Tenant get (cache hit) | < 1 μs | sync.Map read |
| Directory quota check | < 200 μs | Mutex + database |
| File write | < 1 ms | Excluding I/O |

### Memory Management

1. **Avoid memory leaks**:
   - Close all `io.ReadCloser` returned from `ReadFile`
   - Use `defer` for cleanup
   - Use `t.Cleanup()` in tests

2. **Cache cleanup**:
   - Remove completed files from cache immediately
   - Implement TTL for tenant cache (5 minutes)

3. **Buffer pools**:
   - Use `sync.Pool` for frequently allocated buffers
   - Reuse byte slices in hot paths

## Common Pitfalls to Avoid

### 1. Race Conditions
❌ **Bad**:
```go
var count int
go func() { count++ }()
go func() { count++ }()
```

✅ **Good**:
```go
var count int64
go func() { atomic.AddInt64(&count, 1) }()
go func() { atomic.AddInt64(&count, 1) }()
```

### 2. Forgetting to Close Resources
❌ **Bad**:
```go
file, _ := pool.ReadFile(ctx, tenant, fileKey)
// ... use file ...
// Forgot to close!
```

✅ **Good**:
```go
file, err := pool.ReadFile(ctx, tenant, fileKey)
if err != nil {
    return err
}
defer file.Close()
```

### 3. Not Checking Context Cancellation
❌ **Bad**:
```go
for {
    file, _ := pool.GetNextFileForProcessing(ctx, tenant)
    process(file)
}
```

✅ **Good**:
```go
for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }

    file, err := pool.GetNextFileForProcessing(ctx, tenant)
    if err != nil {
        return err
    }
    process(file)
}
```

### 4. Ignoring Errors
❌ **Bad**:
```go
pool.MarkAsCompleted(ctx, fileKey)
```

✅ **Good**:
```go
if err := pool.MarkAsCompleted(ctx, fileKey); err != nil {
    log.Error("failed to mark file as completed", "fileKey", fileKey, "error", err)
    return err
}
```

## Code Style Guidelines

### Naming Conventions
- **Interfaces**: Suffix with behavior (e.g., `StoragePool`, `TenantManager`)
- **Implementations**: Descriptive names (e.g., `LocalFileSystemVolume`)
- **Private functions**: Use lowercase (e.g., `selectVolumeForWrite`)
- **Constants**: Use `const` blocks with `iota` for enums

### Documentation
- **All exported functions** must have godoc comments
- **Interfaces** must document behavior and error conditions
- **Complex algorithms** must have inline comments explaining "why"

Example:
```go
// WriteFile stores a file in the storage pool and returns a system-generated fileKey.
// The file is initially in Pending status and will be available for processing via GetNextFileForProcessing.
//
// originalFileName is optional but recommended to preserve file extensions for debugging.
// If tenant is disabled, returns ErrTenantDisabled.
// If quota is exceeded, returns ErrQuotaExceeded.
// If no volumes have space, returns ErrInsufficientStorage.
func (p *StoragePool) WriteFile(ctx context.Context, tenant TenantContext, content io.Reader, originalFileName *string) (string, error) {
    // Implementation...
}
```

### Error Messages
- Include context (tenant ID, file key, etc.)
- Use `fmt.Errorf` with `%w` for wrapping
- Be actionable (user knows what to fix)

Example:
```go
return fmt.Errorf("failed to write file for tenant %s: %w", tenant.ID, err)
```

## Logging Conventions

Use structured logging with `log/slog`:

```go
slog.Info("file written successfully",
    "tenantId", tenant.ID,
    "fileKey", fileKey,
    "volumeId", volume.ID,
    "fileSize", fileSize,
)

slog.Error("failed to mark file as completed",
    "fileKey", fileKey,
    "error", err,
)
```

**Log levels**:
- `Debug`: Detailed tracing (disabled in production)
- `Info`: Normal operations (file written, processing started)
- `Warn`: Recoverable errors (retry, quota warning)
- `Error`: Unrecoverable errors (database failure, disk full)

## Configuration Patterns

Use functional options pattern:

```go
type StoragePoolOptions struct {
    RootPath             string
    MaxRetryCount        int
    InitialRetryDelay    time.Duration
    MaxRetryDelay        time.Duration
    EnableAutoTenantCreate bool
    TenantCacheTTL       time.Duration
    CleanupInterval      time.Duration
}

func WithMaxRetryCount(count int) func(*StoragePoolOptions) {
    return func(o *StoragePoolOptions) {
        o.MaxRetryCount = count
    }
}

// Usage:
pool := NewStoragePool(
    WithMaxRetryCount(5),
    WithCleanupInterval(30*time.Minute),
)
```

## Monitoring and Observability

### Metrics to Track (Future)
- Files written per tenant
- Files processed per tenant
- Queue depth per tenant
- Processing latency (p50, p95, p99)
- Retry rate
- Permanent failure rate
- Volume capacity utilization

### Health Checks
- Database connectivity
- Volume health (disk space, I/O errors)
- Cleanup service running

## Reference Implementation

This project is based on [Locus (C#/.NET)](https://github.com/cocosip/Locus).

When in doubt about design decisions or expected behavior:
1. Check `doc/README.md` for Locus architecture overview
2. Check `doc/REQUIREMENTS_AND_PLAN.md` for detailed requirements
3. Explore `Locus-Code/` directory for C# reference implementation
4. Maintain API compatibility with Locus (adapted to Go idioms)

## Development Workflow

1. **Start with tests**: Write failing tests first (TDD)
2. **Run with race detector**: `go test -race ./...`
3. **Benchmark critical paths**: Metadata, quota, scheduling
4. **Check coverage**: Aim for 80%+ (`go test -cover ./...`)
5. **Lint before commit**: `golangci-lint run`
6. **Update CLAUDE.md**: If architecture changes

## Project Status

**Current Phase**: Foundation (Phase 1)
- ✅ Requirements defined
- ✅ Architecture designed
- ⏳ Core interfaces being implemented
- ⏳ Testing framework being set up

See `doc/REQUIREMENTS_AND_PLAN.md` for full development plan (12-13 weeks, 11 phases).
